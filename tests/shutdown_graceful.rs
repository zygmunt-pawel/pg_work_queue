#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Phase 7: `shutdown(timeout)` happy path.
//!
//! Push 5 jobów; każdy handler `Ok(())` po 50ms sleep. Worker'a tworzymy z
//! ograniczoną concurrency (=2) tak żeby tylko część batch'a faktycznie
//! ruszyła zanim wystrzelimy `shutdown`. Asserujemy:
//! - `shutdown(5s)` zwraca `Ok(stats)`.
//! - `stats.completed >= 1` — przynajmniej jeden handler dobiegł do końca.
//! - `stats.pending_recovery == 0` — żadna jobowa nie została `'running'`.
//! - W DB: wszystkie wiersze są `'done'` lub `'queued'` (nigdy `'running'`).

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_returns_stats_no_pending_recovery() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("graceful_q");
    let payloads: Vec<Payload> = (0..5u32).map(|i| Payload { seq: i }).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    let worker = Worker::<Payload, _>::builder()
        .queue("graceful_q")
        .pool(pool.clone())
        .concurrency(2)
        .batch_size(2)
        .lease_timeout(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Pozwalamy poll loopowi zaclamować pierwszy batch, ale w żadnym
    // sensownym scenariuszu nie zdąży on przetworzyć wszystkich 5 jobów.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let stats = handle
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown should succeed");

    assert!(
        stats.completed >= 1,
        "at least one handler should have completed under graceful shutdown; got {}",
        stats.completed
    );
    assert_eq!(
        stats.pending_recovery, 0,
        "no rows should be left running after graceful shutdown (got pending_recovery={})",
        stats.pending_recovery
    );

    // DB-observable: żaden row nie zostaje 'running'.
    let rows = sqlx::query("SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'graceful_q'")
        .fetch_all(&pool)
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 5);
    let mut done_count = 0u32;
    let mut queued_count = 0u32;
    for r in &rows {
        let s: String = r.try_get("s").unwrap();
        match s.as_str() {
            "done" => done_count += 1,
            "queued" => queued_count += 1,
            other => panic!("unexpected row status under graceful shutdown: {other}"),
        }
    }
    assert_eq!(
        u64::from(done_count),
        stats.completed,
        "DB 'done' count should match stats.completed"
    );
    assert!(done_count + queued_count == 5);
}
