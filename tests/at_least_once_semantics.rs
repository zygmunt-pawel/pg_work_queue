#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! At-least-once semantics: gdy handler zwróci `JobError::retry`, job musi
//! być invoked at-least-twice (jeden retry). Counter ≥ 2 + final status='done'
//! to dowód że retry działa end-to-end.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{BackoffPolicy, JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_invoked_at_least_twice_when_first_retry() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("alo_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let counter = Arc::new(AtomicU32::new(0));
    let counter_h = counter.clone();

    let worker = Worker::<Payload, _>::builder()
        .queue("alo_q")
        .pool(pool.clone())
        .max_attempts(3)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(100))
        .handler(move |_p: Payload, _ctx: JobContext| {
            let c = counter_h.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    Err::<(), _>(JobError::retry("first_attempt_fails"))
                } else {
                    Ok(())
                }
            }
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait until done.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = sqlx::query("SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'alo_q'")
            .fetch_one(&pool)
            .await
            .expect("fetch");
        let s: String = row.try_get("s").expect("s");
        if s == "done" {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "timeout waiting for done; counter={}",
                counter.load(Ordering::SeqCst)
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    let final_count = counter.load(Ordering::SeqCst);
    assert!(
        final_count >= 2,
        "handler must be invoked at least twice (retry); got {final_count}"
    );
}
