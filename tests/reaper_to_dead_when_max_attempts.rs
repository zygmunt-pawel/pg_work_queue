#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Reaper: stale `running` row z `attempts >= max_attempts` → `dead` (NOT
//! `awaiting_retry`). last_error='lease_expired_max_attempts', finished_at
//! set, lease_token NULL.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reaper_flips_max_attempts_row_to_dead() {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("dead_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    // Stale running row with attempts == max_attempts.
    let n = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second',
             attempts = 3,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now()
         WHERE queue = $1",
    )
    .bind("dead_q")
    .execute(&pool)
    .await
    .expect("manual update")
    .rows_affected();
    assert_eq!(n, 1);

    let worker = Worker::<Payload, _>::builder()
        .queue("dead_q")
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
        "SELECT status::text AS status, last_error, lease_token, finished_at
         FROM pgwq.jobs WHERE queue = $1",
    )
    .bind("dead_q")
    .fetch_one(&pool)
    .await
    .expect("fetch");

    let status: String = row.try_get("status").expect("status");
    let last_error: Option<String> = row.try_get("last_error").expect("last_error");
    let lease_token: Option<uuid::Uuid> = row.try_get("lease_token").expect("lease_token");
    let finished_at: Option<chrono::DateTime<chrono::Utc>> =
        row.try_get("finished_at").expect("finished_at");

    assert_eq!(
        status, "dead",
        "reaper must flip max-attempts stale row to dead"
    );
    assert_eq!(last_error.as_deref(), Some("lease_expired_max_attempts"));
    assert!(lease_token.is_none(), "reaper must clear lease_token");
    assert!(finished_at.is_some(), "dead row must have finished_at set");
}
