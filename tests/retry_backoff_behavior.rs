#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `BackoffPolicy::fixed(d)` ustala `run_at - last_attempted_at` ≈ `d`.
//! Two-value behavior test: fixed(100ms) vs fixed(2s). DB-observable.
//!
//! PLAN.md test list line 1839: `tests/retry_backoff_behavior.rs — Fixed vs Exponential run_at delta`.

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
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");
}

/// Czeka aż `last_attempted_at IS NOT NULL` AND `status = 'awaiting_retry'`,
/// zwraca `(run_at - last_attempted_at)` w sekundach (f64) z jednego wiersza.
async fn observe_first_retry_delay_seconds(pool: &sqlx::PgPool, queue: &str) -> f64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s, run_at, last_attempted_at, attempts
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
            // chrono::TimeDelta::num_milliseconds may saturate; for our
            // ranges (100ms..a few seconds) it's safe.
            return delta.num_milliseconds() as f64 / 1000.0;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "timeout waiting for awaiting_retry on queue {queue}; status={s}, last={last:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn run_with_backoff(pool: &sqlx::PgPool, queue: &str, backoff: BackoffPolicy) -> f64 {
    push_one(pool, queue).await;
    let worker = Worker::<Payload, _>::builder()
        .queue(queue)
        .pool(pool.clone())
        .max_attempts(3)
        .retry_backoff(backoff)
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            Err::<(), _>(JobError::retry("always_fail"))
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    let delta = observe_first_retry_delay_seconds(pool, queue).await;

    handle.cancel();
    let _ = handle.join().await;
    delta
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_100ms_yields_short_delta() {
    let (pool, _c) = common::pg18_pool().await;
    let delta = run_with_backoff(
        &pool,
        "rb_short",
        BackoffPolicy::fixed(Duration::from_millis(100)),
    )
    .await;
    // 100ms ±50ms margin (DB clock drift + insert→fetch latency).
    assert!(
        (0.05..=0.5).contains(&delta),
        "expected ~0.1s delta, got {delta}s"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_2s_yields_longer_delta() {
    let (pool, _c) = common::pg18_pool().await;
    let delta = run_with_backoff(
        &pool,
        "rb_long",
        BackoffPolicy::fixed(Duration::from_secs(2)),
    )
    .await;
    // 2s ±0.5s margin.
    assert!(
        (1.5..=2.5).contains(&delta),
        "expected ~2.0s delta, got {delta}s"
    );
}
