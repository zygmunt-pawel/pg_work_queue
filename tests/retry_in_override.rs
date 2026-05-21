#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `JobError::retry_in(reason, d)` overrides the per-builder backoff policy.
//!
//! Override z `d = 3s` daje `run_at - last_attempted_at ≈ 3s`, mimo że
//! builder default to fixed(100ms). DB-observable.
//!
//! Companion test `retry_in_clamped_warn.rs` weryfikuje clamp-warn event
//! w izolowanym test binary (osobny global tracing subscriber).
//!
//! PLAN.md test list line 1840: `retry_in_override.rs`.

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::Row;

use pg_work_queue::{BackoffPolicy, JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

async fn push_one(pool: &sqlx::PgPool, queue: &str) {
    let pusher = Pusher::new(queue);
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");
}

async fn observe_first_retry_delay_seconds(pool: &sqlx::PgPool, queue: &str) -> f64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s, run_at, last_attempted_at
             FROM pgwq.jobs WHERE queue = $1 ORDER BY id LIMIT 1",
        )
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("fetch");
        let s: String = row.try_get("s").expect("s");
        let last: Option<DateTime<Utc>> = row.try_get("last_attempted_at").expect("last");
        if s == "awaiting_retry"
            && let Some(last_at) = last
        {
            let run_at: DateTime<Utc> = row.try_get("run_at").expect("run_at");
            let delta = run_at - last_at;
            return delta.num_milliseconds() as f64 / 1000.0;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for awaiting_retry; s={s}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_in_override_beats_builder_default() {
    let (pool, _c) = common::pg18_pool().await;
    push_one(&pool, "rio_override").await;

    let worker = Worker::<Payload, _>::builder()
        .queue("rio_override")
        .pool(pool.clone())
        .max_attempts(3)
        // Builder default short — override should win.
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            Err::<(), _>(JobError::retry_in("override", Duration::from_secs(3)))
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    let delta = observe_first_retry_delay_seconds(&pool, "rio_override").await;
    handle.cancel();
    let _ = handle.join().await;

    // Override = 3s; ±0.5s margin.
    assert!(
        (2.5..=3.5).contains(&delta),
        "expected ~3s override, got {delta}s"
    );
}
