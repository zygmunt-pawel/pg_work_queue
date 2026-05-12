#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `pool.close()` mid-flight → poll loop sees `PoolClosed` (fatal) →
//! self-shutdown via the cancellation token. `handle.join()` returns
//! `ShutdownError::Fatal(_)`.

mod common;

use std::time::Duration;

use pg_work_queue::{JobContext, JobError, ShutdownError, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_close_triggers_fatal_self_shutdown() {
    let (pool, _c) = common::pg18_pool().await;

    let worker = Worker::<Payload, _>::builder()
        .queue("fatal_q")
        .pool(pool.clone())
        .concurrency(2)
        .lease_timeout(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Let it run one tick or two, then murder the pool.
    tokio::time::sleep(Duration::from_millis(200)).await;
    pool.close().await;

    // join must surface Fatal within a couple seconds.
    let res = tokio::time::timeout(Duration::from_secs(5), handle.join())
        .await
        .expect("join should not hang");
    match res {
        Err(ShutdownError::Fatal(arc_err)) => {
            let msg = arc_err.to_string();
            assert!(
                msg.to_lowercase().contains("closed") || msg.to_lowercase().contains("pool"),
                "fatal error should mention pool/closed; got {msg}"
            );
        }
        other => panic!("expected ShutdownError::Fatal, got {other:?}"),
    }
}
