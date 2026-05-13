#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `max_attempts` budget exhausted → row flips to `dead`, `attempts == N`,
//! `last_error` = ostatni retry reason. PLAN.md test list line 1826.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{BackoffPolicy, JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_always_retries_dies_after_max_attempts() {
    let (pool, _c) = common::pg18_pool().await;

    // Push one row.
    let pusher = Pusher::new("ma_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let worker = Worker::<Payload, _>::builder()
        .queue("ma_q")
        .pool(pool.clone())
        .max_attempts(3)
        // Backoff fixed(100ms) → 3 attempts × ~100ms ≈ 300ms baseline.
        // (100ms = MIN_BACKOFF_BASE floor.)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .handler(
            |_p: Payload, _ctx: JobContext| async move { Err::<(), _>(JobError::retry("fail")) },
        )
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait for status = dead.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s, attempts, last_error
             FROM pgwq.jobs WHERE queue = 'ma_q' ORDER BY id LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .expect("fetch");
        let s: String = row.try_get("s").expect("s");
        if s == "dead" {
            let attempts: i32 = row.try_get("attempts").expect("attempts");
            let last_error: Option<String> = row.try_get("last_error").expect("last_error");
            assert_eq!(attempts, 3, "attempts must == max_attempts");
            assert_eq!(
                last_error.as_deref(),
                Some("fail"),
                "last_error must be the most-recent retry reason"
            );
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for dead; status={s}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;
}
