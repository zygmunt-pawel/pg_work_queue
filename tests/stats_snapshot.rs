#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Phase 7: kanoniczna weryfikacja że `Stats` (zwracane z `shutdown(timeout)`)
//! ma sensownie napełnione liczniki. Pchamy mieszany batch: kilka jobów które
//! zwracają `Ok`, kilka które zwracają `Err(JobError::abort(...))` (mark_dead
//! direct, bypass retry budget) — i sprawdzamy że `completed` + `failed`
//! odzwierciedlają DB-observable terminal states.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stats_reflect_terminal_state_distribution() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("stats_q");
    // 5 jobów: seq 0..=2 → Ok; seq 3..=4 → Abort (mark_dead direct).
    let payloads: Vec<Payload> = (0..5u32).map(|i| Payload { seq: i }).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    // Counter handler-side dla deterministycznego mapowania payload→outcome.
    let invocations = Arc::new(AtomicU32::new(0));
    let invocations_h = invocations.clone();

    let worker = Worker::<Payload, _>::builder()
        .queue("stats_q")
        .pool(pool.clone())
        .concurrency(4)
        .batch_size(10)
        .lease_timeout(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(50))
        .handler(move |p: Payload, _ctx: JobContext| {
            let inv = invocations_h.clone();
            async move {
                inv.fetch_add(1, Ordering::Relaxed);
                if p.seq < 3 {
                    Ok::<(), JobError>(())
                } else {
                    Err(JobError::abort("permanent failure (test)"))
                }
            }
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Czekaj aż wszystkie 5 zostanie obsłużone — poll_interval=50ms, więc
    // 500ms to z dużym zapasem.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let stats = handle
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown returns Ok");

    assert_eq!(
        invocations.load(Ordering::Relaxed),
        5,
        "all 5 payloads should have been invoked exactly once"
    );
    assert_eq!(
        stats.completed, 3,
        "3 Ok-returning handlers should be counted as completed (got {})",
        stats.completed
    );
    assert_eq!(
        stats.failed, 2,
        "2 Abort-returning handlers should be counted as failed (got {})",
        stats.failed
    );
    assert_eq!(
        stats.pending_recovery, 0,
        "no rows should be running post-shutdown (got pending_recovery={})",
        stats.pending_recovery
    );
    assert_eq!(stats.aborted, 0, "no handlers should be cascade-aborted");
    assert_eq!(stats.timed_out, 0, "no handlers should have timed out");

    // DB cross-check: 3× 'done', 2× 'dead'.
    let rows = sqlx::query("SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'stats_q'")
        .fetch_all(&pool)
        .await
        .expect("fetch");
    let mut done = 0u32;
    let mut dead = 0u32;
    for r in &rows {
        let s: String = r.try_get("s").unwrap();
        match s.as_str() {
            "done" => done += 1,
            "dead" => dead += 1,
            other => panic!("unexpected status: {other}"),
        }
    }
    assert_eq!(done, 3);
    assert_eq!(dead, 2);
}
