#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! PLAN.md test #21 — regresja Anti-pattern #12.
//!
//! Handler robi `tokio::time::sleep(60s)` + post-sleep side-effect counter.
//! `Worker::shutdown(timeout=1s)` musi cascade-abortować handler:
//! - Inner `JoinSet` w `handle_job` dropped → inner task aborted.
//! - Handler future dropped at `.await` point → sleep cancelled.
//! - Post-sleep counter increment NIE jest osiągnięte.
//!
//! Gdyby ktoś kiedyś zamienił lokalny `JoinSet` na `tokio::spawn`
//! (Anti-pattern #12: JoinHandle::drop detach'uje zamiast abortować),
//! handler żyłby dalej w runtime'ie aż do natural completion, counter
//! by inkrementował się po ~60s. Test wait'uje 3s post-shutdown i
//! sprawdza counter — fail jeśli wzrósł.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pg_work_queue::{BackoffPolicy, JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_cascade_aborts_handler_no_post_sleep_side_effect() {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("no_leak_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    // Side-effect counter — handler bumps it POST-sleep. If sleep is
    // properly cancelled by cascade-abort, counter stays at 0.
    let post_sleep_counter = Arc::new(AtomicU64::new(0));
    let pre_sleep_counter = Arc::new(AtomicU64::new(0));
    let post = post_sleep_counter.clone();
    let pre = pre_sleep_counter.clone();

    let worker = Worker::<Payload, _>::builder()
        .queue("no_leak_q")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(120))
        .handler_timeout(Duration::from_secs(90))
        .reaper_interval(Duration::from_secs(30))
        .poll_interval(Duration::from_millis(50))
        .retry_backoff(BackoffPolicy::fixed(Duration::from_secs(1)))
        .handler(move |_p: Payload, _ctx: JobContext| {
            let pre = pre.clone();
            let post = post.clone();
            async move {
                pre.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(60)).await;
                // Reached ONLY if sleep wasn't cascade-aborted — would
                // signal an Anti-pattern #12 regression.
                post.fetch_add(1, Ordering::Relaxed);
                Ok::<(), JobError>(())
            }
        })
        .build()
        .expect("build");

    let handle = worker.start().await.expect("start");

    // Wait until handler actually started its sleep (pre-counter incremented).
    let pre_deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < pre_deadline {
        if pre_sleep_counter.load(Ordering::Relaxed) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        pre_sleep_counter.load(Ordering::Relaxed),
        1,
        "handler must have started its sleep before shutdown"
    );

    // Shutdown z 1s timeout → cascade-abort (handler nigdy sam się nie skończy).
    let stats = handle
        .shutdown(Duration::from_secs(1))
        .await
        .expect("shutdown ok");
    assert!(
        stats.aborted >= 1,
        "stats.aborted must be ≥ 1 (got {}); shutdown should have cascade-aborted the sleeping handler",
        stats.aborted
    );

    // CRITICAL: wait 3s post-shutdown. Pod Anti-pattern #12 (gdyby `tokio::spawn`
    // zamiast lokalnego JoinSet), handler żyłby dalej i counter by się zwiększał.
    // Z cascade-abort: sleep dropped at .await point, post-sleep code unreachable.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        post_sleep_counter.load(Ordering::Relaxed),
        0,
        "post-sleep side-effect MUST NOT execute — would indicate handler was detached, not cascade-aborted (Anti-pattern #12 regression)"
    );
}
