#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Adaptive ticking: gdy reaper tick zwróci pełny `REAPER_BATCH_SIZE` (1024)
//! batch, następny tick pomija `ticker.tick()` i jedzie at-SQL-speed dopóki
//! drain. Test: 5000 stale rows + `reaper_interval=10s`. Bez adaptive: drain
//! wymagałby ~50s (5×10s). Z adaptive: drain w kilka sekund (limit = SQL).

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reaper_drains_5k_backlog_within_8s() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("drain_q");

    // Push 5000 rows via push_batch (single call within MAX_BATCH_SIZE=10k).
    let payloads: Vec<Payload> = (0..5000u32).map(|seq| Payload { seq }).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    // Mark every row as stale running. Half max=3 attempts=3 → dead;
    // half max=3 attempts=1 → awaiting_retry. Use `run_at` far future so
    // post-reap awaiting_retry rows aren't picked up by the poll loop
    // (which would interfere with our drain count).
    let n = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second',
             attempts = CASE WHEN id % 2 = 0 THEN 3 ELSE 1 END,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now(),
             run_at = now() + interval '1 hour'
         WHERE queue = 'drain_q'",
    )
    .execute(&pool)
    .await
    .expect("stale stamp 5k")
    .rows_affected();
    assert_eq!(n, 5000);

    // reaper_interval=10s. Without adaptive: 5000/1024 ≈ 5 ticks × 10s = 50s.
    // With adaptive: first tick at t=0 reaps 1024 + drain skips ticker →
    // ~5 batches in ms, drain in seconds.
    // lease=30s satisfies `reaper_interval <= lease/2` (10 <= 15).
    let worker = Worker::<Payload, _>::builder()
        .queue("drain_q")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(30))
        .reaper_interval(Duration::from_secs(10))
        .poll_interval(Duration::from_secs(5))
        .concurrency(4)
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    let mut last_running: i64;
    loop {
        let row = sqlx::query(
            "SELECT count(*)::bigint AS c FROM pgwq.jobs WHERE queue = 'drain_q' AND status = 'running'",
        )
        .fetch_one(&pool)
        .await
        .expect("count running");
        last_running = row.try_get("c").expect("c");
        if last_running == 0 {
            break;
        }
        if std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    assert_eq!(
        last_running, 0,
        "adaptive reaper MUST drain 5000 stale rows within 8s (without adaptive backlog \
         skip-tick logic, this would need ~50s); still running: {last_running}"
    );
}
