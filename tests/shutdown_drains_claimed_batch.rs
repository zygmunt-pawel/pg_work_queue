#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! W9 regression — once a batch is claimed, all rows in that batch must
//! reach a terminal state even if `cancel()` fires mid-batch. We push 5
//! rows, start a worker with `concurrency=5` (so all 5 handlers spawn at
//! once), and `cancel` after 100ms — well before any handler finishes
//! (each sleeps 200ms). After `join()` no row may be left `running`.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_after_claim_still_drains_batch_to_terminal() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("drain_q");
    let payloads: Vec<Payload> = (0..5u32).map(|i| Payload { seq: i }).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    let worker = Worker::<Payload, _>::builder()
        .queue("drain_q")
        .pool(pool.clone())
        .concurrency(5)
        .batch_size(10)
        .lease_timeout(Duration::from_secs(30))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait long enough for the first poll tick + claim to fire (50ms)
    // but well before the 200ms handler sleeps complete.
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.cancel();
    let _ = handle.join().await;

    // No row should be left 'running' — all 5 must be 'done' (handler
    // completed inside its local JoinSet even though the poll loop exited).
    let rows = sqlx::query("SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'drain_q'")
        .fetch_all(&pool)
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 5);
    for r in &rows {
        let s: String = r.try_get("s").unwrap();
        assert_eq!(
            s, "done",
            "no row may remain in 'running' after join (got {s})"
        );
    }
}
