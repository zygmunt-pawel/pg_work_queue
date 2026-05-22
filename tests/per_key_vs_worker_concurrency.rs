#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Worker-wide `concurrency` cap binds before the per-key limit when it is
//! the tighter constraint.
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T {
    n: u32,
}

/// When `concurrency(2)` is smaller than `concurrency_limits([("k", 10)])`,
/// the worker-wide cap must bind first and limit actual in-flight jobs to 2.
///
/// The per-key limit (10) is deliberately larger than the worker-wide
/// `concurrency` (2). A concurrency-tracking handler records the peak
/// simultaneous in-flight count. After all 6 jobs complete the peak must be
/// `<= 2`, proving the worker-wide cap — not the per-key limit — was the
/// binding constraint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_concurrency_caps_below_per_key_limit() {
    let (pool, _c) = pg18_pool().await;

    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = (0..6)
        .map(|i| (T { n: i }, Some("k".to_string())))
        .collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let now = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));
    let (now2, peak2) = (now.clone(), peak.clone());

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .concurrency(2)
        .concurrency_limits([("k".to_string(), 10u32)])
        .handler(move |_t: T, _c: JobContext| {
            let (now, peak) = (now2.clone(), peak2.clone());
            async move {
                let current = now.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                now.fetch_sub(1, Ordering::SeqCst);
                Ok::<(), JobError>(())
            }
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    let done: i64 = sqlx::query_scalar("SELECT count(*) FROM pgwq.jobs WHERE status = 'done'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(done, 6, "all 6 keyed jobs completed");
    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "peak concurrency {} exceeded worker-wide limit 2",
        peak.load(Ordering::SeqCst),
    );
}
