#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Phase 7: `shutdown(timeout)` mid-poll-sleep wychodzi natychmiast.
//! Komplementarne do `shutdown_immediate_with_pool_starvation.rs` (który
//! testuje pool-starved acquire). Tutaj zwykły idle poll loop: brak jobów,
//! poll sleepuje, `shutdown` powinien wybudzić poprzez cancellation token.

mod common;

use std::time::Duration;

use pg_work_queue::{JobContext, JobError, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_mid_idle_poll_exits_quickly() {
    let (pool, _c) = common::pg18_pool().await;

    let worker = Worker::<Payload, _>::builder()
        .queue("cancel_q")
        .pool(pool.clone())
        .concurrency(1)
        // 5 × poll_interval = 50s, więc lease musi być ≥ 50s.
        .lease_timeout(Duration::from_secs(60))
        // reaper_interval default = lease/4 = 15s; ceiling lease/2 = 30s.
        .reaper_interval(Duration::from_secs(15))
        // Długi poll_interval — wybudzenie MUSI iść przez cancel, nie tick.
        .poll_interval(Duration::from_secs(10))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Daj poll loopowi szansę wejść w ticker.tick() sleep.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let t0 = std::time::Instant::now();
    let res = tokio::time::timeout(
        Duration::from_secs(2),
        handle.shutdown(Duration::from_secs(5)),
    )
    .await;
    let elapsed = t0.elapsed();

    let inner = res.expect("shutdown must complete (not hang)");
    let _stats = inner.expect("shutdown returns Ok stats");
    assert!(
        elapsed < Duration::from_millis(500),
        "shutdown w idle worker'ze powinien wrócić w <500ms (got {elapsed:?})"
    );
}
