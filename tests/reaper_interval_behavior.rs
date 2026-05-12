#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `reaper_interval` knob: two-value behavioral test. Fast reaper (1s) reaps
//! a stale row within ~1.5s; slow reaper (3s) leaves the row stale at t=2s
//! but reaps by t=4s.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

async fn fetch_status(pool: &sqlx::PgPool, queue: &str) -> String {
    let row = sqlx::query("SELECT status::text AS status FROM pgwq.jobs WHERE queue = $1")
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("fetch status");
    row.try_get("status").expect("status")
}

async fn stale_stamp(pool: &sqlx::PgPool, queue: &str) {
    let n = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '500 milliseconds',
             attempts = 1,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now(),
             run_at = now() + interval '1 hour'
         WHERE queue = $1",
    )
    .bind(queue)
    .execute(pool)
    .await
    .expect("stale stamp")
    .rows_affected();
    assert_eq!(n, 1, "must stale-stamp 1 row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fast_reaper_reaps_quickly() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("fast_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");
    stale_stamp(&pool, "fast_q").await;

    let worker = Worker::<Payload, _>::builder()
        .queue("fast_q")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_secs(1))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Fast reaper: within 1.5s status MUST be awaiting_retry.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let status = fetch_status(&pool, "fast_q").await;

    handle.cancel();
    let _ = handle.join().await;

    assert_eq!(
        status, "awaiting_retry",
        "fast (1s) reaper must have flipped row within 1.5s; got {status}"
    );
}

async fn fetch_status_by_id(pool: &sqlx::PgPool, public_id: uuid::Uuid) -> String {
    let row = sqlx::query("SELECT status::text AS status FROM pgwq.jobs WHERE public_id = $1")
        .bind(public_id)
        .fetch_one(pool)
        .await
        .expect("fetch status by id");
    row.try_get("status").expect("status")
}

async fn stale_stamp_by_id(pool: &sqlx::PgPool, public_id: uuid::Uuid) {
    let n = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '500 milliseconds',
             attempts = 1,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now(),
             run_at = now() + interval '1 hour'
         WHERE public_id = $1",
    )
    .bind(public_id)
    .execute(pool)
    .await
    .expect("stale stamp by id")
    .rows_affected();
    assert_eq!(n, 1, "must stale-stamp 1 row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_tick_latency_respects_reaper_interval() {
    // Real two-value behavioral test of reaper_interval: after the first
    // (immediate) tick, the SECOND tick MUST wait ~reaper_interval before
    // firing. We stamp row B AFTER the first tick has consumed row A, then
    // measure how long until row B is reaped. Latency ≈ reaper_interval -
    // (time-since-first-tick).
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("itl_q");
    let mut tx = pool.begin().await.expect("tx");
    let id_a = pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push a");
    let id_b = pusher
        .push(&mut tx, &Payload { seq: 2 })
        .await
        .expect("push b");
    tx.commit().await.expect("commit");

    // Stamp ONLY row A as stale; row B stays queued (run_at = far future).
    stale_stamp_by_id(&pool, id_a).await;
    // Park row B so the poll loop never claims it.
    let _ =
        sqlx::query("UPDATE pgwq.jobs SET run_at = now() + interval '1 hour' WHERE public_id = $1")
            .bind(id_b)
            .execute(&pool)
            .await
            .expect("park b");

    let worker = Worker::<Payload, _>::builder()
        .queue("itl_q")
        .pool(pool.clone())
        // lease >= 6s required for reaper_interval=3s (3 <= lease/2);
        // lease >= 5 * poll_interval is the existing cross-knob floor.
        .lease_timeout(Duration::from_secs(20))
        .reaper_interval(Duration::from_secs(3))
        .poll_interval(Duration::from_secs(2))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait for the first (immediate) tick to flip row A.
    let first_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < first_deadline {
        if fetch_status_by_id(&pool, id_a).await == "awaiting_retry" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        fetch_status_by_id(&pool, id_a).await,
        "awaiting_retry",
        "row A must flip on first (immediate) reaper tick"
    );

    // Now stamp row B as stale and time the second-tick latency.
    let t_stamp = std::time::Instant::now();
    stale_stamp_by_id(&pool, id_b).await;

    let mut flipped_at: Option<std::time::Instant> = None;
    let deadline = t_stamp + Duration::from_secs(6);
    while std::time::Instant::now() < deadline {
        if fetch_status_by_id(&pool, id_b).await == "awaiting_retry" {
            flipped_at = Some(std::time::Instant::now());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    let observed = flipped_at.expect("row B must flip within 6s of stamp");
    let latency = observed.duration_since(t_stamp);

    // With reaper_interval=3s and an immediate-first-tick: the second tick
    // fires at t = 3s from worker start. If we stamp <3s after start, the
    // second tick will see B and reap it; latency = remaining-time-until-second-tick.
    // The lower bound rules out the failure mode where the interval is
    // ignored (latency would be ≪ 1s). The upper bound catches a hang.
    assert!(
        latency >= Duration::from_secs(1),
        "second-tick latency {latency:?} must respect reaper_interval=3s (lower bound 1s)"
    );
    assert!(
        latency <= Duration::from_secs(5),
        "second-tick latency {latency:?} unreasonable for reaper_interval=3s (upper bound 5s)"
    );
}
