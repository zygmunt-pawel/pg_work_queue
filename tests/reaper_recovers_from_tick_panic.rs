#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Reaper panic recovery (Anti-pattern #11): per-tick tokio::spawn isolation
//! + K=3 consecutive panic threshold → worker shutdown.
//!
//! Inject 3 consecutive panics via the test-only `REAPER_PANIC_INJECTIONS`
//! static. After 3 ticks fail, the reaper cancels shutdown — the worker
//! self-shuts. `WorkerHandle::join()` must complete within a bounded time.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use pg_work_queue::__test_exports::REAPER_PANIC_INJECTIONS;
use pg_work_queue::{JobContext, JobError, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_consecutive_reaper_panics_trigger_self_shutdown() {
    let (pool, _c) = common::pg18_pool().await;

    // Arm 3 pending panic injections.
    REAPER_PANIC_INJECTIONS.store(3, Ordering::Relaxed);

    let worker = Worker::<Payload, _>::builder()
        .queue("panic_q")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_secs(1))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");

    let handle = worker.start().await.expect("start");

    // After 3 panic ticks at 1s each → reaper shuts down worker. join()
    // must return within ~8s. Bound the wait so a hang fails loud.
    let join_result = tokio::time::timeout(Duration::from_secs(8), handle.join()).await;

    // Snapshot the counter BEFORE resetting it, so the assertion below
    // verifies the reaper actually consumed the injections (rather than
    // re-asserting the store we just did).
    let remaining_after = REAPER_PANIC_INJECTIONS.load(Ordering::Relaxed);
    REAPER_PANIC_INJECTIONS.store(0, Ordering::Relaxed);

    assert!(
        join_result.is_ok(),
        "WorkerHandle::join() must complete within 8s after reaper panic escalation"
    );
    // join() itself returns Ok(()) — panic threshold cancels shutdown but
    // does NOT set last_fatal (no sqlx::Error available). Faza 7 may
    // surface this through a dedicated `ShutdownError::ReaperPanicEscalation`
    // variant; for now operators detect it via tracing::error.
    let inner = join_result.expect("join completed in time");
    assert!(
        inner.is_ok(),
        "join() must return Ok — panic escalation triggers a clean shutdown, not Fatal"
    );

    // All 3 injections must have been consumed by the reaper before
    // escalation fired — otherwise either the test hook is broken or the
    // tick loop short-circuited.
    assert_eq!(
        remaining_after, 0,
        "reaper must have consumed all 3 injected panics before self-shutdown (observed {remaining_after} remaining)"
    );
}
