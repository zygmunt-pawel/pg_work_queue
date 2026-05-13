#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `purge_done(0s)` na 50_000 `done` wierszach — usuwa wszystkie chunkami,
//! liczba w wyniku zgadza się z DB count'em, post-purge `count(done) = 0`.
//!
//! Wstaw bezpośrednio przez SQL `generate_series(...)`, żeby trzymać test
//! szybki (single statement → ~1s) i równocześnie respektować
//! `jobs_status_invariants` CHECK (PLAN.md migration 0001):
//! `status='done'` wymaga `finished_at NOT NULL`, `attempts > 0`,
//! `last_attempted_at NOT NULL`, `first_attempted_at NOT NULL`.

mod common;

use std::time::Duration;

use pg_work_queue::{purge_done, queue_stats};

const N: i64 = 50_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_done_kasuje_wszystkie_done_chunkami() {
    let (pool, _c) = common::pg18_pool().await;

    // Bulk insert N done rows via generate_series. Single statement -> fast.
    // All terminal fields satisfy jobs_status_invariants:
    //   status='done', attempts=1, first_/last_attempted_at, finished_at NOT NULL.
    // finished_at = -30 days ensures we're well past any `age` threshold.
    // jobs_temporal CHECK requires created_at <= first_attempted_at <= ... <=
    // finished_at AND run_at >= created_at AND updated_at >= created_at,
    // so we also push created_at / run_at / updated_at to -30 days.
    let inserted = sqlx::query(
        "INSERT INTO pgwq.jobs (
            queue, payload, public_id, status,
            attempts, max_attempts,
            first_attempted_at, last_attempted_at, finished_at,
            created_at, run_at, updated_at
        )
        SELECT
            'q', '{}'::bytea, uuidv7(), 'done'::pgwq.job_status,
            1, 1,
            now() - interval '30 days',
            now() - interval '30 days',
            now() - interval '30 days',
            now() - interval '30 days',
            now() - interval '30 days',
            now() - interval '30 days'
        FROM generate_series(1, $1)",
    )
    .bind(N)
    .execute(&pool)
    .await
    .expect("seed 50k done rows");
    assert_eq!(inserted.rows_affected(), N as u64, "seed row count");

    // Sanity: queue_stats sees them all.
    let before = queue_stats(&pool, "q").await.expect("stats before");
    assert_eq!(before.done, N as u64, "stats: done before purge");

    // age=0s -> wszystko 'done' starsze niż now() znika.
    let deleted = purge_done(&pool, Duration::from_secs(0))
        .await
        .expect("purge_done");
    assert_eq!(
        deleted, N as u64,
        "purge_done usunie dokładnie wszystkie {N} done rows"
    );

    // Post-purge: 0 done rows. Każdy inny status nieraportowany (brak insertów).
    let after = queue_stats(&pool, "q").await.expect("stats after");
    assert_eq!(after.done, 0, "post-purge done count = 0");
    assert_eq!(after.queued, 0);
    assert_eq!(after.running, 0);
    assert_eq!(after.awaiting_retry, 0);
    assert_eq!(after.dead, 0);

    // Drugi run -> 0 (chunk loop break path).
    let again = purge_done(&pool, Duration::from_secs(0))
        .await
        .expect("purge_done second");
    assert_eq!(again, 0, "drugi purge_done na pustym = 0");
}
