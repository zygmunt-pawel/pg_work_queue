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
                 now(), now(), gen_random_uuid(), now() - interval '1 hour')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..2).map(|i| (T { n: i }, Some("k".to_string()))).collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async { Ok::<(), JobError>(()) })
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
}
