#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Age knob jest behawioralny: `purge_done(age)` deletuje tylko rzędy
//! `finished_at < now() - age`. Świeższe rzędy zostają.

mod common;

use std::time::Duration;

use pg_work_queue::{purge_done, queue_stats};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_done_age_threshold_filtruje_swieze() {
    let (pool, _c) = common::pg18_pool().await;

    // 5 done rzędów sprzed 1h (świeże).
    // jobs_temporal: created_at <= first_attempted_at -> ustawiamy 1h wstecz.
    sqlx::query(
        "INSERT INTO pgwq.jobs (
            queue, payload, public_id, status,
            attempts, max_attempts,
            first_attempted_at, last_attempted_at, finished_at,
            created_at, run_at, updated_at
        )
        SELECT
            'q', '{}'::bytea, uuidv7(), 'done'::pgwq.job_status,
            1, 1,
            now() - interval '1 hour',
            now() - interval '1 hour',
            now() - interval '1 hour',
            now() - interval '1 hour',
            now() - interval '1 hour',
            now() - interval '1 hour'
        FROM generate_series(1, 5)",
    )
    .execute(&pool)
    .await
    .expect("seed fresh");

    // 5 done rzędów sprzed 1 dnia (stare).
    sqlx::query(
        "INSERT INTO pgwq.jobs (
            queue, payload, public_id, status,
            attempts, max_attempts,
            first_attempted_at, last_attempted_at, finished_at,
            created_at, run_at, updated_at
        )
        SELECT
            'q', '{}'::bytea, uuidv7(), 'done'::pgwq.job_status,
            1, 1,
            now() - interval '1 day',
            now() - interval '1 day',
            now() - interval '1 day',
            now() - interval '1 day',
            now() - interval '1 day',
            now() - interval '1 day'
        FROM generate_series(1, 5)",
    )
    .execute(&pool)
    .await
    .expect("seed old");

    let pre = queue_stats(&pool, "q").await.expect("stats pre");
    assert_eq!(pre.done, 10, "10 done rows pre-purge");

    // age = 12h: świeże (1h) przeżywają, stare (1d) lecą.
    let deleted = purge_done(&pool, Duration::from_secs(12 * 3600))
        .await
        .expect("purge_done with age");
    assert_eq!(
        deleted, 5,
        "purge usuwa tylko 5 rzędów sprzed 1d (1h-old za młode)"
    );

    let post = queue_stats(&pool, "q").await.expect("stats post");
    assert_eq!(post.done, 5, "5 fresh-1h done pozostają");
}
