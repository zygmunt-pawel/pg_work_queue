#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Anti-pattern #13 regression — claim_batch must only return as many rows
//! as we have permits for. The observable invariant: for every completed
//! row, `handler_start - first_attempted_at < 200ms` (no claim-but-wait
//! window where the lease ticks while we wait for a permit).
//!
//! Strategy: 20 rows, concurrency=2, batch_size=10, handler sleeps 500ms.
//! Each handler stamps its actual start-time into a side table via the
//! same pool. We then compare `start_time - first_attempted_at`.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn claim_size_bounded_by_permits_so_no_wait_window() {
    let (pool, _c) = common::pg18_pool().await;

    // Side table to record actual handler-start times.
    sqlx::query("CREATE TABLE handler_starts (job_id bigint primary key, started_at timestamptz)")
        .execute(&pool)
        .await
        .expect("create starts table");

    let pusher = Pusher::new("permits_q");
    let payloads: Vec<(Payload, Option<String>)> =
        (0..20u32).map(|i| (Payload { seq: i }, None)).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    let pool_for_handler = pool.clone();
    let worker = Worker::<Payload, _>::builder()
        .queue("permits_q")
        .pool(pool.clone())
        .concurrency(2)
        .batch_size(10)
        .lease_timeout(Duration::from_secs(30))
        .poll_interval(Duration::from_millis(50))
        .handler(move |_p: Payload, ctx: JobContext| {
            let pool = pool_for_handler.clone();
            async move {
                sqlx::query("INSERT INTO handler_starts (job_id, started_at) VALUES ($1, now())")
                    .bind(ctx.id)
                    .execute(&pool)
                    .await
                    .map_err(|e| JobError::Retry {
                        reason: format!("insert start: {e}"),
                        retry_in: None,
                    })?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok::<(), JobError>(())
            }
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait for all 20 to complete (~5s with concurrency=2 × 500ms × 10 rounds).
    let start = std::time::Instant::now();
    loop {
        let row = sqlx::query(
            "SELECT count(*) AS c FROM pgwq.jobs WHERE queue = 'permits_q' AND status = 'done'",
        )
        .fetch_one(&pool)
        .await
        .expect("count");
        let c: i64 = row.try_get("c").unwrap();
        if c == 20 {
            break;
        }
        if start.elapsed() > Duration::from_secs(20) {
            panic!("timeout waiting for 20 completions; have {c}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    // For each row, compute delta = started_at - first_attempted_at.
    // Anti-pattern #13 would manifest as some delta > 500ms (rows claimed
    // but waiting for a permit while a previous handler runs).
    let rows = sqlx::query(
        "SELECT j.id, (EXTRACT(EPOCH FROM (s.started_at - j.first_attempted_at)) * 1000.0)::float8 AS delta_ms
         FROM pgwq.jobs j
         JOIN handler_starts s ON s.job_id = j.id
         WHERE j.queue = 'permits_q'",
    )
    .fetch_all(&pool)
    .await
    .expect("delta query");
    assert_eq!(rows.len(), 20);
    for r in &rows {
        let id: i64 = r.try_get("id").unwrap();
        let delta: f64 = r.try_get("delta_ms").unwrap();
        assert!(
            delta < 200.0,
            "row {id} had delta {delta:.1}ms (>= 200ms) — claim-but-wait-for-permit window detected (Anti-pattern #13)"
        );
    }
}
