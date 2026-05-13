#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Phase 7: `WorkerHandle::shutdown(timeout)` surfaces reaper panic
//! escalation jako `ShutdownError::ReaperPanicEscalation { consecutive_panics }`.
//! Wcześniej (Faza 5) escalation był widoczny tylko przez tracing::error!.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use pg_work_queue::__test_exports::REAPER_PANIC_INJECTIONS;
use pg_work_queue::{JobContext, JobError, ShutdownError, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_surfaces_reaper_panic_escalation() {
    let (pool, _c) = common::pg18_pool().await;

    REAPER_PANIC_INJECTIONS.store(3, Ordering::Relaxed);

    let worker = Worker::<Payload, _>::builder()
        .queue("panic_shutdown_q")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_secs(1))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");

    let handle = worker.start().await.expect("start");

    // Czekaj aż reaper zużyje wszystkie 3 injections (~3s @ 1s tick) i wystawi
    // `last_panic_escalation`. Bez tego `shutdown` zacancelluje reaper'a zanim
    // ten zdąży się self-escalate'ować.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let res = tokio::time::timeout(
        Duration::from_secs(10),
        handle.shutdown(Duration::from_secs(10)),
    )
    .await;

    let remaining = REAPER_PANIC_INJECTIONS.load(Ordering::Relaxed);
    REAPER_PANIC_INJECTIONS.store(0, Ordering::Relaxed);
    assert_eq!(
        remaining, 0,
        "reaper must consume all 3 injected panics (observed {remaining} remaining)"
    );

    let inner = res.expect("shutdown must complete in time");
    match inner {
        Err(ShutdownError::ReaperPanicEscalation { consecutive_panics }) => {
            assert_eq!(
                consecutive_panics, 3,
                "expected exactly 3 consecutive panics surfaced (got {consecutive_panics})"
            );
        }
        other => panic!("expected ShutdownError::ReaperPanicEscalation, got {other:?}"),
    }
}
