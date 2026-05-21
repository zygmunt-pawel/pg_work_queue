#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! A fresh Worker does not seed its counter from `running` rows left by a
//! crashed process — it claims up to the full limit immediately.
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_worker_ignores_ghost_running_rows() {
    let (pool, _c) = pg18_pool().await;

    sqlx::query(
        "INSERT INTO pgwq.jobs
             (queue, payload, status, concurrency_key, attempts, max_attempts,
              last_attempted_at, first_attempted_at, lease_token, lease_expires_at)
         VALUES ('q', '\\x00', 'running', 'k', 1, 3,
                 now(), now(), gen_random_uuid(), now() + interval '1 hour')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..2).map(|i| (T { n: i }, Some("k".to_string()))).collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let now_counter = Arc::new(AtomicU32::new(0));
    let peak_counter = Arc::new(AtomicU32::new(0));
    let done_counter = Arc::new(AtomicU32::new(0));
    let (now_c, peak_c, done_c) = (now_counter.clone(), peak_counter.clone(), done_counter.clone());

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .concurrency(4)
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(move |_t: T, _c: JobContext| {
            let (now_c, peak_c, done_c) = (now_c.clone(), peak_c.clone(), done_c.clone());
            async move {
                let now = now_c.fetch_add(1, Ordering::SeqCst) + 1;
                peak_c.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                now_c.fetch_sub(1, Ordering::SeqCst);
                done_c.fetch_add(1, Ordering::SeqCst);
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

    let done: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgwq.jobs WHERE status = 'done'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(done, 2, "ghost running row must not block fresh claims");

    let peak = peak_counter.load(Ordering::SeqCst);
    assert!(
        peak >= 2,
        "peak in-flight for key 'k' was {peak}; expected 2 — ghost row must not consume in-memory headroom"
    );
}
