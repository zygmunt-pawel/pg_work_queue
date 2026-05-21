#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Per-queue isolation: Worker A na queue='alpha' nie może reapować stale
//! row'a w queue='beta'. SQL filter `queue = $1` jest krytyczny — Anti-pattern
//! #14 (cross-queue reap przejmie długo-running joba innego service'u).

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_on_alpha_does_not_reap_beta_queue() {
    let (pool, _c) = common::pg18_pool().await;

    // Push 1 job to queue='beta'.
    let pusher = Pusher::new("beta");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    // Force the beta row into a stale `running` state.
    let n = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second',
             attempts = 1,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now()
         WHERE queue = 'beta'",
    )
    .execute(&pool)
    .await
    .expect("manual update")
    .rows_affected();
    assert_eq!(n, 1);

    // Start ONLY worker A on queue='alpha'.
    let worker_a = Worker::<Payload, _>::builder()
        .queue("alpha")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_secs(1))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build alpha");
    let handle = worker_a.start().await.expect("start alpha");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    handle.cancel();
    let _ = handle.join().await;

    // Beta row MUST still be `running` — worker A's reaper saw queue='alpha'
    // only. Anti-pattern #14: cross-queue reap = data corruption.
    let row = sqlx::query(
        "SELECT status::text AS status, lease_token, last_error
         FROM pgwq.jobs WHERE queue = 'beta'",
    )
    .fetch_one(&pool)
    .await
    .expect("fetch beta row");

    let status: String = row.try_get("status").expect("status");
    let lease_token: Option<uuid::Uuid> = row.try_get("lease_token").expect("lease_token");
    let last_error: Option<String> = row.try_get("last_error").expect("last_error");

    assert_eq!(
        status, "running",
        "beta row must still be 'running' — alpha worker MUST NOT reap cross-queue"
    );
    assert!(
        lease_token.is_some(),
        "beta row's lease_token must NOT be cleared by alpha's reaper"
    );
    assert!(
        last_error.is_none(),
        "beta row's last_error must NOT be overwritten with 'lease_expired'"
    );
}
