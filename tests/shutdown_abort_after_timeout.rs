#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Phase 7: handler nie kończy się przed `shutdown(timeout)` — handlery
//! są aborowane przez `JoinSet::abort_all`, `stats.aborted` rośnie,
//! `stats.pending_recovery` powinien być >= 1 (rzędy zostają `'running'`
//! aż reaper je odzyska po lease expiry).

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shutdown_timeout_aborts_inflight_handlers() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("abort_q");
    let payloads: Vec<(Payload, Option<String>)> = (0..3u32).map(|i| (Payload { seq: i }, None)).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    // Handler trwa "forever" (well, 60s). Worker'owi damy concurrency=4 +
    // batch_size=10, więc wszystkie 3 ruszą równolegle.
    let worker = Worker::<Payload, _>::builder()
        .queue("abort_q")
        .pool(pool.clone())
        .concurrency(4)
        .batch_size(10)
        .lease_timeout(Duration::from_secs(30))
        // handler_timeout musi być < lease_timeout - 1s. 25s > shutdown deadline,
        // więc to abort cascade nas wybudza, nie handler_timeout.
        .handler_timeout(Duration::from_secs(25))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Daj poll loopowi szansę zaclamować i wystartować handlery.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let t0 = std::time::Instant::now();
    let stats = handle
        .shutdown(Duration::from_millis(500))
        .await
        .expect("shutdown returns Ok with abort stats");
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_millis(1500),
        "shutdown z timeout=500ms powinien wrócić w <1.5s (got {elapsed:?})"
    );
    assert!(
        stats.aborted >= 1,
        "at least one in-flight handler should be aborted; got aborted={}",
        stats.aborted
    );
    assert!(
        stats.pending_recovery >= 1,
        "aborted rows should be 'running' awaiting reaper recovery; got pending_recovery={}",
        stats.pending_recovery
    );

    // DB-observable: rzędy są w status='running' z aktualnym lease_token
    // (reaper'a celem).
    let rows =
        sqlx::query("SELECT status::text AS s, lease_token FROM pgwq.jobs WHERE queue = 'abort_q'")
            .fetch_all(&pool)
            .await
            .expect("fetch");
    assert_eq!(rows.len(), 3);
    let mut running = 0u32;
    for r in &rows {
        let s: String = r.try_get("s").unwrap();
        if s == "running" {
            // running row MUSI mieć lease_token (fencing); reaper na tej
            // podstawie sweep'uje.
            let lt: Option<uuid::Uuid> = r.try_get("lease_token").unwrap();
            assert!(lt.is_some(), "running row must carry lease_token");
            running += 1;
        }
    }
    assert!(
        u64::from(running) == stats.pending_recovery,
        "DB running count ({running}) must match pending_recovery ({})",
        stats.pending_recovery
    );
}
