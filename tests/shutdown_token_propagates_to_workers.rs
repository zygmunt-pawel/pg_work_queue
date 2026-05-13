#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! §5 regression — `WorkerBuilder::shutdown_token(parent.child_token())`
//! plumbs an external `CancellationToken` so `parent.cancel()` triggers
//! shutdown across N workers simultaneously.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parent_token_cancels_all_child_workers() {
    let (pool, _c) = common::pg18_pool().await;

    // Two queues, two workers — each gets parent.child_token().
    for q in ["tok_q_a", "tok_q_b"] {
        let pusher = Pusher::new(q);
        let mut tx = pool.begin().await.expect("tx");
        pusher
            .push(&mut tx, &Payload { seq: 0 })
            .await
            .expect("push");
        tx.commit().await.expect("commit");
    }

    let parent = CancellationToken::new();
    let claimed = Arc::new(AtomicU32::new(0));

    let mk_handler = |claimed: Arc<AtomicU32>| {
        move |_p: Payload, _ctx: JobContext| {
            let claimed = claimed.clone();
            async move {
                claimed.fetch_add(1, Ordering::Relaxed);
                // Long enough that the test cancels mid-handler.
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok::<(), JobError>(())
            }
        }
    };

    let h_a = Worker::<Payload, _>::builder()
        .queue("tok_q_a")
        .pool(pool.clone())
        .concurrency(1)
        .poll_interval(Duration::from_millis(50))
        .shutdown_token(parent.child_token())
        .handler(mk_handler(claimed.clone()))
        .build()
        .expect("build a")
        .start()
        .await
        .expect("start a");

    let h_b = Worker::<Payload, _>::builder()
        .queue("tok_q_b")
        .pool(pool.clone())
        .concurrency(1)
        .poll_interval(Duration::from_millis(50))
        .shutdown_token(parent.child_token())
        .handler(mk_handler(claimed.clone()))
        .build()
        .expect("build b")
        .start()
        .await
        .expect("start b");

    // Wait for both handlers to enter the long sleep (both claimed).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while claimed.load(Ordering::Relaxed) < 2 {
        if tokio::time::Instant::now() >= deadline {
            panic!("handlers never claimed both jobs");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Single `parent.cancel()` must drain BOTH workers within bounded time.
    parent.cancel();
    let stats_a = h_a
        .shutdown(Duration::from_secs(2))
        .await
        .expect("shutdown a");
    let stats_b = h_b
        .shutdown(Duration::from_secs(2))
        .await
        .expect("shutdown b");

    // At least one in-flight handler aborted per worker (cascade abort).
    assert!(
        stats_a.aborted >= 1,
        "worker A must have aborted in-flight handler, got {stats_a:?}"
    );
    assert!(
        stats_b.aborted >= 1,
        "worker B must have aborted in-flight handler, got {stats_b:?}"
    );
}
