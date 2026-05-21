#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `Pusher::push_at(tx, payload, now() + 2s)`: claim_batch nie pick'uje
//! wiersza dopóki `run_at <= now()`. PLAN.md test list line 1838.
//!
//! Trzy fazy:
//! 1. Push z `run_at = now() + 2s`. Worker startuje natychmiast.
//! 2. Po 1s sprawdzamy: row jest `queued`, NIE picked.
//! 3. Po 2.5s (relative do push) sprawdzamy: row jest `done`.

mod common;

use std::time::Duration;

use chrono::Utc;
use sqlx::Row;

use pg_work_queue::{BackoffPolicy, JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_run_at_delays_pickup() {
    let (pool, _c) = common::pg18_pool().await;

    // Push z run_at = now() + 2s.
    let pusher = Pusher::new("sra_q");
    let mut tx = pool.begin().await.expect("tx");
    let scheduled_at = Utc::now() + chrono::Duration::seconds(2);
    pusher
        .push_at(&mut tx, &Payload { seq: 1 }, scheduled_at, None)
        .await
        .expect("push_at");
    tx.commit().await.expect("commit");
    let push_instant = std::time::Instant::now();

    let worker = Worker::<Payload, _>::builder()
        .queue("sra_q")
        .pool(pool.clone())
        .concurrency(2)
        .max_attempts(3)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait 1s — still queued (run_at jeszcze w przyszłości).
    tokio::time::sleep(Duration::from_secs(1)).await;
    let row = sqlx::query(
        "SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'sra_q' ORDER BY id LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("fetch");
    let status: String = row.try_get("s").expect("s");
    assert_eq!(
        status, "queued",
        "row must still be 'queued' before run_at; got {status}"
    );

    // Wait until done — should land between 2s and 3s after push.
    let deadline = push_instant + Duration::from_millis(2_800);
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'sra_q' ORDER BY id LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .expect("fetch");
        let s: String = row.try_get("s").expect("s");
        if s == "done" {
            // Sanity: must be ≥ 2s after push (scheduled_at).
            let elapsed = push_instant.elapsed();
            assert!(
                elapsed >= Duration::from_millis(1_900),
                "done too early ({elapsed:?}); scheduled was +2s"
            );
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for done; current = {s}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;
}
