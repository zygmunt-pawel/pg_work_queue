#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Handler-visible `JobContext::max_attempts` reflects the builder's
//! `.max_attempts(N)` at claim time — paired at two distinct N values so
//! the field isn't accidentally hardcoded to a const.

mod common;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use pg_work_queue::{JobContext, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Payload {
    seq: u32,
}

async fn run_with_max_attempts(expected_max: u32, queue: &'static str) {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new(queue);
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let seen: Arc<Mutex<Option<u32>>> = Arc::new(Mutex::new(None));
    let seen_in_handler = Arc::clone(&seen);

    let worker = Worker::<Payload, _>::builder()
        .queue(queue)
        .pool(pool.clone())
        .max_attempts(expected_max)
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .handler(move |_p: Payload, ctx: JobContext| {
            let seen = Arc::clone(&seen_in_handler);
            async move {
                *seen.lock().await = Some(ctx.max_attempts);
                Ok(())
            }
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Spin-wait for the handler to record what it saw.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let observed = loop {
        if let Some(v) = *seen.lock().await {
            break v;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for handler invocation");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };

    handle.cancel();
    let _ = handle.join().await;

    assert_eq!(
        observed, expected_max,
        "handler must see ctx.max_attempts == builder's .max_attempts(N); got {observed}, want {expected_max}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_sees_max_attempts_3() {
    run_with_max_attempts(3, "ctx_ma_3_q").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_sees_max_attempts_7() {
    run_with_max_attempts(7, "ctx_ma_7_q").await;
}
