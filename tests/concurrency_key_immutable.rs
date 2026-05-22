#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! The immutability trigger rejects any UPDATE that changes concurrency_key,
//! and the normal claim/mark lifecycle preserves it.
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T {
    n: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_update_of_concurrency_key_is_rejected() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("q")
        .push(&mut tx, &T { n: 1 }, Some("k"))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let err = sqlx::query("UPDATE pgwq.jobs SET concurrency_key = 'other' WHERE public_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("immutable"),
        "expected immutability error, got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setting_concurrency_key_from_null_is_rejected() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("q")
        .push(&mut tx, &T { n: 1 }, None)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let err = sqlx::query("UPDATE pgwq.jobs SET concurrency_key = 'x' WHERE public_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("immutable"));
}

/// Push a keyed job, run it to completion through a real Worker, and after
/// shutdown verify: (a) status == 'done', (b) concurrency_key is still the
/// original value. This confirms that the claim UPDATE and mark_done UPDATE
/// do not trip the immutability trigger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_and_mark_done_preserve_concurrency_key() {
    let (pool, _c) = pg18_pool().await;
    let key = "preserve_key";

    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("immut_q")
        .push(&mut tx, &T { n: 42 }, Some(key))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T, _>::builder()
        .queue("immut_q")
        .pool(pool.clone())
        .concurrency(2)
        .concurrency_limits([("preserve_key".to_string(), 1u32)])
        .poll_interval(Duration::from_millis(100))
        .lease_timeout(Duration::from_secs(30))
        .handler(|_t: T, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    // Wait for the job to reach 'done'.
    let start = std::time::Instant::now();
    loop {
        let s: String =
            sqlx::query_scalar("SELECT status::text FROM pgwq.jobs WHERE public_id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        if s == "done" {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timeout waiting for job to reach done"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    // Verify the key is still intact after claim + mark_done UPDATEs.
    let stored_key: Option<String> =
        sqlx::query_scalar("SELECT concurrency_key FROM pgwq.jobs WHERE public_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(
        stored_key.as_deref(),
        Some(key),
        "concurrency_key must be preserved through claim→done lifecycle; got: {stored_key:?}"
    );
}
