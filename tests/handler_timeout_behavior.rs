#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `handler_timeout` fires when the handler exceeds its budget — row flips
//! to `awaiting_retry` with `last_error = "handler_timeout"`. With a generous
//! timeout the same handler completes normally → `done`.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

async fn push_one(pool: &sqlx::PgPool, queue: &str) {
    let pusher = Pusher::new(queue);
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_fires_and_marks_retry_with_reason() {
    let (pool, _c) = common::pg18_pool().await;
    push_one(&pool, "to_q").await;

    let worker = Worker::<Payload, _>::builder()
        .queue("to_q")
        .pool(pool.clone())
        .concurrency(2)
        .max_attempts(3)
        // lease=10s ≫ handler_timeout=1s; mark_timeout default = 10-1-1 = 8s.
        .lease_timeout(Duration::from_secs(10))
        .handler_timeout(Duration::from_secs(1))
        .poll_interval(Duration::from_millis(100))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait up to 3s for the timeout to fire + mark_retry to commit.
    let start = std::time::Instant::now();
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s, last_error
             FROM pgwq.jobs WHERE queue = 'to_q' ORDER BY id LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .expect("fetch");
        let status: String = row.try_get("s").unwrap();
        if status == "awaiting_retry" {
            let last: Option<String> = row.try_get("last_error").unwrap();
            assert_eq!(
                last.as_deref(),
                Some("handler_timeout"),
                "last_error should be 'handler_timeout', got {last:?}"
            );
            break;
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("timeout waiting for awaiting_retry; current status = {status}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_timeout_lets_handler_finish_and_marks_done() {
    let (pool, _c) = common::pg18_pool().await;
    push_one(&pool, "to_long_q").await;

    let worker = Worker::<Payload, _>::builder()
        .queue("to_long_q")
        .pool(pool.clone())
        .concurrency(2)
        .max_attempts(3)
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(100))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    let start = std::time::Instant::now();
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'to_long_q' ORDER BY id LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .expect("fetch");
        let status: String = row.try_get("s").unwrap();
        if status == "done" {
            break;
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("timeout waiting for done; got {status}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;
}
