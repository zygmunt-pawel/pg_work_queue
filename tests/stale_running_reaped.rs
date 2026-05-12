#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Reaper: stale `running` row z `lease_expires_at < now()` powinien zostać
//! flipnięty do `awaiting_retry` z `last_error='lease_expired'`, lease_token
//! NULL, lease_expires_at NULL. attempts < max_attempts → retry, nie dead.
//!
//! To make the assertion deterministic (no race with the poll loop re-claiming
//! the awaiting_retry row), we set poll_interval much longer than the test
//! window so only the reaper ticks within 2.5s.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_running_row_flipped_to_awaiting_retry() {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("stale_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    // Manually force the row into a stale `running` state — lease already
    // expired, attempts < max so reaper should flip to awaiting_retry.
    //
    // We also push `run_at` 1h into the future. The reaper SQL leaves
    // `run_at` untouched (it only flips status/lease_*/last_error/
    // finished_at), so after the reaper's awaiting_retry transition the
    // row is NOT eligible for re-claim by the poll loop (claim filters on
    // `run_at <= now()`). That makes the post-reaper state stable to
    // assert without racing the worker's poll cycle.
    let n = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second',
             attempts = 1,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now(),
             run_at = now() + interval '1 hour'
         WHERE queue = $1",
    )
    .bind("stale_q")
    .execute(&pool)
    .await
    .expect("manual update")
    .rows_affected();
    assert_eq!(n, 1);

    let worker = Worker::<Payload, _>::builder()
        .queue("stale_q")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_secs(1))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    handle.cancel();
    let _ = handle.join().await;

    let row = sqlx::query(
        "SELECT status::text AS status, last_error, lease_token, lease_expires_at, attempts
         FROM pgwq.jobs WHERE queue = $1",
    )
    .bind("stale_q")
    .fetch_one(&pool)
    .await
    .expect("fetch");

    let status: String = row.try_get("status").expect("status");
    let last_error: Option<String> = row.try_get("last_error").expect("last_error");
    let lease_token: Option<uuid::Uuid> = row.try_get("lease_token").expect("lease_token");
    let lease_expires_at: Option<chrono::DateTime<chrono::Utc>> =
        row.try_get("lease_expires_at").expect("lease_expires_at");

    assert_eq!(
        status, "awaiting_retry",
        "reaper must flip stale row → awaiting_retry"
    );
    assert_eq!(
        last_error.as_deref(),
        Some("lease_expired"),
        "reaper must overwrite last_error"
    );
    assert!(lease_token.is_none(), "reaper must clear lease_token");
    assert!(
        lease_expires_at.is_none(),
        "reaper must clear lease_expires_at"
    );
}
