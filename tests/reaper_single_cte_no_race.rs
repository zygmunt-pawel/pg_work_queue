#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Single-CTE reap = brak race window. Każda flipniona row MUSI mieć
//! consistent (status, lease_token, lease_expires_at). W1 z plan v1 (two-step
//! reaper) pozwalał na `status='running' AND lease_token IS NULL` — regression
//! guard.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_row_with_running_and_null_lease_token() {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("cte_q");
    let payloads: Vec<Payload> = (0..100u32).map(|seq| Payload { seq }).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    // Stamp ALL 100 as stale running with assorted attempts/max combinations.
    // Mix attempts 1..=5 and max in {3, 5}. ~half land at attempts >= max
    // → dead; rest → awaiting_retry.
    let n = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - (random() * interval '500 milliseconds'),
             attempts = 1 + (id % 5)::int,
             max_attempts = CASE WHEN id % 2 = 0 THEN 3 ELSE 5 END,
             last_attempted_at = now(),
             first_attempted_at = now()
         WHERE queue = 'cte_q'",
    )
    .execute(&pool)
    .await
    .expect("stale stamp")
    .rows_affected();
    assert_eq!(n, 100);

    // Run worker WITH poll loop active so we exercise BOTH reaper and
    // claim simultaneously — the invariant must hold under concurrent
    // reaper + claim_batch + mark_*.
    let worker = Worker::<Payload, _>::builder()
        .queue("cte_q")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_millis(200))
        .handler_timeout(Duration::from_secs(2))
        .concurrency(4)
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    tokio::time::sleep(Duration::from_secs(3)).await;
    handle.cancel();
    let _ = handle.join().await;

    // Inspect every row. CRITICAL invariants:
    // 1. status ∈ {queued, running, awaiting_retry, done, dead}.
    // 2. status='running' ⇒ lease_token IS NOT NULL AND lease_expires_at IS NOT NULL.
    // 3. status IN ('done','dead','awaiting_retry') ⇒ lease_token IS NULL
    //    AND lease_expires_at IS NULL.
    let rows = sqlx::query(
        "SELECT id, status::text AS status, lease_token, lease_expires_at
         FROM pgwq.jobs WHERE queue = 'cte_q'",
    )
    .fetch_all(&pool)
    .await
    .expect("fetch rows");
    assert_eq!(rows.len(), 100);

    let mut running_count: usize = 0;
    for r in &rows {
        let id: i64 = r.try_get("id").expect("id");
        let status: String = r.try_get("status").expect("status");
        let lease_token: Option<uuid::Uuid> = r.try_get("lease_token").expect("lease_token");
        let lease_expires_at: Option<chrono::DateTime<chrono::Utc>> =
            r.try_get("lease_expires_at").expect("lease_expires_at");

        match status.as_str() {
            "running" => {
                running_count += 1;
                assert!(
                    lease_token.is_some(),
                    "id={id} status=running BUT lease_token IS NULL (W1 race regression)"
                );
                assert!(
                    lease_expires_at.is_some(),
                    "id={id} status=running BUT lease_expires_at IS NULL"
                );
            }
            "done" | "dead" | "awaiting_retry" => {
                assert!(
                    lease_token.is_none(),
                    "id={id} status={status} BUT lease_token IS NOT NULL"
                );
                assert!(
                    lease_expires_at.is_none(),
                    "id={id} status={status} BUT lease_expires_at IS NOT NULL"
                );
            }
            "queued" => {
                assert!(
                    lease_token.is_none(),
                    "id={id} queued row has lease_token set"
                );
            }
            other => panic!("id={id} unexpected status: {other}"),
        }
    }
    // running count must be ≤ concurrency (poll captured some after they
    // were reaper-flipped to awaiting_retry and re-claimed).
    assert!(
        running_count <= 4,
        "running_count {running_count} must be ≤ concurrency=4 at shutdown time"
    );
}
