#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `concurrency=1` (serial) vs `concurrency=4` (parallel) — observable
//! via wall-clock span between min(first_attempted_at) and max(finished_at).
//!
//! Serial: 4 × 500ms ≥ 2s. Parallel: max ~600ms (one round of 4 sleeps).

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

async fn push_n(pool: &sqlx::PgPool, queue: &str, n: u32) {
    let pusher = Pusher::new(queue);
    let payloads: Vec<(Payload, Option<String>)> = (0..n).map(|i| (Payload { seq: i }, None)).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher.push_batch(&mut tx, &payloads).await.expect("batch");
    tx.commit().await.expect("commit");
}

async fn span_ms(pool: &sqlx::PgPool, queue: &str) -> i64 {
    let row = sqlx::query(
        "SELECT (EXTRACT(EPOCH FROM (max(finished_at) - min(first_attempted_at))) * 1000.0)::float8 AS span_ms
         FROM pgwq.jobs WHERE queue = $1",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("span");
    let s: f64 = row.try_get("span_ms").expect("span_ms");
    s as i64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_completes_faster_than_serial() {
    let (pool, _c) = common::pg18_pool().await;

    push_n(&pool, "serial_q", 4).await;
    push_n(&pool, "parallel_q", 4).await;

    let serial = Worker::<Payload, _>::builder()
        .queue("serial_q")
        .pool(pool.clone())
        .concurrency(1)
        .batch_size(10)
        .lease_timeout(Duration::from_secs(30))
        .poll_interval(Duration::from_millis(100))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build serial");
    let h_serial = serial.start().await.expect("start serial");

    let parallel = Worker::<Payload, _>::builder()
        .queue("parallel_q")
        .pool(pool.clone())
        .concurrency(4)
        .batch_size(10)
        .lease_timeout(Duration::from_secs(30))
        .poll_interval(Duration::from_millis(100))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build parallel");
    let h_parallel = parallel.start().await.expect("start parallel");

    // Both should be done within 3s (serial ≈ 2s, parallel ≈ 600ms).
    let deadline = Duration::from_secs(4);
    let start = std::time::Instant::now();
    loop {
        let done_serial: i64 = sqlx::query(
            "SELECT count(*) AS c FROM pgwq.jobs WHERE queue = 'serial_q' AND status = 'done'",
        )
        .fetch_one(&pool)
        .await
        .map(|r| r.try_get::<i64, _>("c").unwrap_or(0))
        .unwrap_or(0);
        let done_parallel: i64 = sqlx::query(
            "SELECT count(*) AS c FROM pgwq.jobs WHERE queue = 'parallel_q' AND status = 'done'",
        )
        .fetch_one(&pool)
        .await
        .map(|r| r.try_get::<i64, _>("c").unwrap_or(0))
        .unwrap_or(0);
        if done_serial == 4 && done_parallel == 4 {
            break;
        }
        if start.elapsed() > deadline {
            panic!(
                "timeout waiting for completion; serial={done_serial}, parallel={done_parallel}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    h_serial.cancel();
    h_parallel.cancel();
    let _ = h_serial.join().await;
    let _ = h_parallel.join().await;

    let serial_span = span_ms(&pool, "serial_q").await;
    let parallel_span = span_ms(&pool, "parallel_q").await;

    assert!(
        serial_span >= 1800,
        "serial span should be >= 1800ms (4 × 500ms with overhead); got {serial_span}ms"
    );
    assert!(
        parallel_span <= 1500,
        "parallel span should be <= 1500ms (1 round of 4 sleeps); got {parallel_span}ms"
    );
    assert!(
        parallel_span < serial_span,
        "parallel ({parallel_span}ms) must beat serial ({serial_span}ms)"
    );

    let _: DateTime<Utc> = Utc::now();
}
