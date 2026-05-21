#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! A saturated key does not block other keys; the limit is enforced
//! end-to-end through Worker::start.
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T { key: String }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_enforced_and_other_keys_progress() {
    let (pool, _c) = pg18_pool().await;

    let mut tx = pool.begin().await.unwrap();
    let mut items: Vec<(T, Option<String>)> = Vec::new();
    for _ in 0..6 {
        items.push((T { key: "slow".into() }, Some("slow".to_string())));
    }
    for _ in 0..3 {
        items.push((T { key: "fast".into() }, Some("fast".to_string())));
    }
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let slow_peak = Arc::new(AtomicU32::new(0));
    let slow_now = Arc::new(AtomicU32::new(0));
    let fast_done = Arc::new(AtomicU32::new(0));
    let (sp, sn, fd) = (slow_peak.clone(), slow_now.clone(), fast_done.clone());

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .concurrency(8)
        .concurrency_limits([("slow".to_string(), 2u32)])
        .handler(move |t: T, _c: JobContext| {
            let (sp, sn, fd) = (sp.clone(), sn.clone(), fd.clone());
            async move {
                if t.key == "slow" {
                    let now = sn.fetch_add(1, Ordering::SeqCst) + 1;
                    sp.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    sn.fetch_sub(1, Ordering::SeqCst);
                } else {
                    fd.fetch_add(1, Ordering::SeqCst);
                }
                Ok::<(), JobError>(())
            }
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(fast_done.load(Ordering::SeqCst), 3, "fast key not head-of-line blocked");

    let _ = handle.shutdown(Duration::from_secs(10)).await;
    assert!(slow_peak.load(Ordering::SeqCst) <= 2, "slow key exceeded limit 2");
}
