#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Anti-pattern #14 regression: reaper musi używać `j.max_attempts` (per-row,
//! stamped przy claim) NIE worker-local `state.max_attempts`. Rolling deploy:
//! row stamp'ed z max=3, worker B uruchomiony z max=5 — reaper musi
//! zobaczyć `attempts(3) >= max(3)` → dead, niezależnie od config B.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reaper_uses_per_row_max_attempts_not_worker_state() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("roll");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    // Row was stamped by Worker A (max_attempts=3) and reached attempts=3.
    // Reaper should flip it to `dead` because j.attempts(3) >= j.max_attempts(3),
    // regardless of the running worker's `state.max_attempts` (5 below).
    let n = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second',
             attempts = 3,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now()
         WHERE queue = 'roll'",
    )
    .execute(&pool)
    .await
    .expect("manual update")
    .rows_affected();
    assert_eq!(n, 1);

    // Worker B running with max_attempts=5 (would-be the "new code" in a
    // rolling deploy). If the reaper used `state.max_attempts`, this row
    // would go to awaiting_retry (3 < 5). Correct behavior: dead.
    let worker_b = Worker::<Payload, _>::builder()
        .queue("roll")
        .pool(pool.clone())
        .max_attempts(5)
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_secs(1))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build worker B");
    let handle = worker_b.start().await.expect("start");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    handle.cancel();
    let _ = handle.join().await;

    let row = sqlx::query("SELECT status::text AS status FROM pgwq.jobs WHERE queue = 'roll'")
        .fetch_one(&pool)
        .await
        .expect("fetch");
    let status: String = row.try_get("status").expect("status");
    assert_eq!(
        status, "dead",
        "reaper MUST use per-row max_attempts (3) not worker-local (5); got {status}"
    );
}
