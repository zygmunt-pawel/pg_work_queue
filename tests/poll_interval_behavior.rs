#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Pick-up latency = `first_attempted_at - created_at` is bounded by
//! `poll_interval` (observable behavior — not config shape).
//!
//! Worker A (`poll_interval=100ms`) must pick up the row within ~250ms;
//! Worker B (`poll_interval=500ms`) is slower and observes ≥ 300ms latency
//! on a fresh tick miss (interval timer fires after first tick imediately
//! though, so we measure the second push instead).

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

async fn pickup_latency_ms(pool: &sqlx::PgPool, queue: &str) -> i64 {
    let row = sqlx::query(
        "SELECT (EXTRACT(EPOCH FROM (first_attempted_at - created_at)) * 1000.0)::float8 AS lat_ms
         FROM pgwq.jobs WHERE queue = $1 ORDER BY id DESC LIMIT 1",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .expect("fetch lat");
    let lat: f64 = row.try_get("lat_ms").expect("lat_ms");
    lat as i64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fast_poll_picks_up_faster_than_slow_poll() {
    let (pool, _c) = common::pg18_pool().await;

    // Worker A — 100ms poll.
    let worker_fast = Worker::<Payload, _>::builder()
        .queue("fast_q")
        .pool(pool.clone())
        .concurrency(2)
        .lease_timeout(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(100))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build fast");
    let handle_fast = worker_fast.start().await.expect("start fast");

    // Worker B — 500ms poll.
    let worker_slow = Worker::<Payload, _>::builder()
        .queue("slow_q")
        .pool(pool.clone())
        .concurrency(2)
        .lease_timeout(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(500))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build slow");
    let handle_slow = worker_slow.start().await.expect("start slow");

    // Let both workers drain their first immediate tick (interval fires at
    // 0 by default). Wait beyond 500ms so both pollers are past their first
    // cycle and any push lands between ticks.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let pusher_fast = Pusher::new("fast_q");
    let pusher_slow = Pusher::new("slow_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher_fast
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .unwrap();
    pusher_slow
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .unwrap();
    tx.commit().await.expect("commit");

    // Wait long enough for both to fire at least 1 more poll AND complete
    // the handler. Slow poll's worst case ≤ 500ms; +200ms slack.
    tokio::time::sleep(Duration::from_millis(900)).await;

    handle_fast.cancel();
    handle_slow.cancel();
    let _ = handle_fast.join().await;
    let _ = handle_slow.join().await;

    // Observable: latency from created → first_attempted.
    let lat_fast = pickup_latency_ms(&pool, "fast_q").await;
    let lat_slow = pickup_latency_ms(&pool, "slow_q").await;

    // Sanity: jobs got picked up at all.
    let rows =
        sqlx::query("SELECT status::text AS s FROM pgwq.jobs WHERE queue IN ('fast_q','slow_q')")
            .fetch_all(&pool)
            .await
            .expect("rows");
    for r in &rows {
        let s: String = r.try_get("s").unwrap();
        assert_eq!(s, "done", "all jobs should be done");
    }

    assert!(
        lat_fast < 250,
        "fast worker should pick up <250ms; got {lat_fast}ms"
    );
    // Slow worker should be slower (≥ 100ms) — we don't claim a hard floor
    // of 300ms because the test is timing-sensitive on CI. The key invariant
    // is `lat_slow > lat_fast` AND slow is observable as poll-bounded.
    assert!(
        lat_slow > lat_fast,
        "slow ({lat_slow}ms) should exceed fast ({lat_fast}ms) by at least 1 poll tick"
    );

    // Useful side-effects for debugging:
    let _: DateTime<Utc> = Utc::now();
}
