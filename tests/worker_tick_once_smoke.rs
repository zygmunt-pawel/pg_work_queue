#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! End-to-end smoke: push 3 jobs, `Worker::tick_once` claims them, handler
//! runs sequentially, mark_done flips all rows.
//!
//! Mierzymy observable behavior: `TickStats`, recorded handler invocations,
//! row state w DB.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::Row;
use tokio::sync::Mutex;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    id: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tick_once_happy_path_marks_all_done() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("smoke_q");

    let payloads: Vec<(Payload, Option<String>)> =
        (0..3u32).map(|id| (Payload { id }, None)).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    let seen: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_handler = seen.clone();

    let worker = Worker::<Payload, _>::builder()
        .queue("smoke_q")
        .max_attempts(3)
        .lease_timeout(Duration::from_secs(10))
        .batch_size(10)
        .pool(pool.clone())
        .handler(move |p: Payload, _ctx: JobContext| {
            let seen = seen_handler.clone();
            async move {
                seen.lock().await.push(p.id);
                Ok::<(), JobError>(())
            }
        })
        .build()
        .expect("build worker");

    let stats = worker.tick_once().await.expect("tick_once ok");
    assert_eq!(stats.claimed, 3, "should claim 3 rows");
    assert_eq!(stats.completed, 3, "should complete 3 rows");
    assert_eq!(stats.failed, 0, "no failures");
    assert_eq!(stats.fenced_out, 0, "no fence-outs");

    let seen_ids: HashSet<u32> = seen.lock().await.iter().copied().collect();
    assert_eq!(seen_ids, (0..3).collect::<HashSet<u32>>());

    // DB state: all 3 rows status='done', lease_token NULL, finished_at set.
    let rows = sqlx::query(
        "SELECT status::text AS status, lease_token, finished_at, last_error
         FROM pgwq.jobs WHERE queue = $1",
    )
    .bind("smoke_q")
    .fetch_all(&pool)
    .await
    .expect("select rows");
    assert_eq!(rows.len(), 3);
    for r in &rows {
        let status: String = r.try_get("status").expect("status");
        let lease_token: Option<uuid::Uuid> = r.try_get("lease_token").expect("lease_token");
        let finished_at: Option<DateTime<Utc>> = r.try_get("finished_at").expect("finished_at");
        let last_error: Option<String> = r.try_get("last_error").expect("last_error");
        assert_eq!(status, "done");
        assert!(lease_token.is_none(), "lease_token must be cleared");
        assert!(finished_at.is_some(), "finished_at must be set");
        assert!(last_error.is_none(), "last_error must be cleared");
    }
}
