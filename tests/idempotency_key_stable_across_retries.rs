#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `JobContext::idempotency_key` musi być stabilny across retries — równy
//! `public_id` push'a, niezmienny mimo `attempts++` przy każdym claim'ie.
//! Critical: użytkownicy używają tego jako Idempotency-Key na external API.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sqlx::Row;
use tokio::sync::Mutex;
use uuid::Uuid;

use pg_work_queue::{BackoffPolicy, JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotency_key_equals_public_id_across_retries() {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("idk_q");
    let mut tx = pool.begin().await.expect("tx");
    let pushed_id = pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let captured: Arc<Mutex<Vec<Uuid>>> = Arc::new(Mutex::new(Vec::new()));
    let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    let captured_h = captured.clone();
    let counter_h = counter.clone();

    let worker = Worker::<Payload, _>::builder()
        .queue("idk_q")
        .pool(pool.clone())
        .max_attempts(3)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(100))
        .handler(move |_p: Payload, ctx: JobContext| {
            let captured = captured_h.clone();
            let counter = counter_h.clone();
            async move {
                captured.lock().await.push(ctx.idempotency_key);
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err::<(), _>(JobError::retry("force_retry"))
                } else {
                    Ok(())
                }
            }
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait until done.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = sqlx::query("SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'idk_q'")
            .fetch_one(&pool)
            .await
            .expect("fetch");
        let s: String = row.try_get("s").expect("s");
        if s == "done" {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for done");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    let vec = captured.lock().await.clone();
    assert_eq!(
        vec.len(),
        3,
        "handler must be invoked exactly 3 times; got {}",
        vec.len()
    );
    for (i, key) in vec.iter().enumerate() {
        assert_eq!(
            *key,
            pushed_id,
            "attempt {} idempotency_key must equal push'ed public_id ({pushed_id}); got {key}",
            i + 1
        );
    }
}
