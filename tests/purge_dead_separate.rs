#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `purge_done` nie tyka `dead` rows, `purge_dead` nie tyka `done` rows
//! (PLAN.md test #33: "purge_done nie tyka dead, vice versa").

mod common;

use std::time::Duration;

use pg_work_queue::{purge_dead, purge_done, queue_stats};

const N: i64 = 100;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_done_i_purge_dead_dzialaja_niezaleznie() {
    let (pool, _c) = common::pg18_pool().await;

    // Seed N done + N dead rows, wszystkie 30 dni stare. jobs_temporal CHECK
    // wymaga created_at <= first_attempted_at i run_at/updated_at >= created_at,
    // więc wszystkie 6 timestampów są przesunięte w przeszłość.
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
    .expect("seed done");

    sqlx::query(
        "INSERT INTO pgwq.jobs (
            queue, payload, public_id, status,
            attempts, max_attempts,
            first_attempted_at, last_attempted_at, finished_at,
            last_error,
            created_at, run_at, updated_at
        )
        SELECT
            'q', '{}'::bytea, uuidv7(), 'dead'::pgwq.job_status,
            3, 3,
            now() - interval '30 days',
            now() - interval '30 days',
            now() - interval '30 days',
            'max_attempts',
            now() - interval '30 days',
            now() - interval '30 days',
            now() - interval '30 days'
        FROM generate_series(1, $1)",
    )
    .bind(N)
    .execute(&pool)
    .await
    .expect("seed dead");

    let pre = queue_stats(&pool, "q").await.expect("stats pre");
    assert_eq!(pre.done, N as u64);
    assert_eq!(pre.dead, N as u64);

    // 1) purge_done -> deletuje 100 done, dead nietknięte.
    let d1 = purge_done(&pool, Duration::from_secs(0))
        .await
        .expect("purge_done");
    assert_eq!(d1, N as u64, "purge_done usunie tylko done");

    let mid = queue_stats(&pool, "q").await.expect("stats mid");
    assert_eq!(mid.done, 0, "post purge_done: done=0");
    assert_eq!(
        mid.dead, N as u64,
        "post purge_done: dead nietknięte (separation)"
    );

    // 2) purge_dead -> deletuje 100 dead.
    let d2 = purge_dead(&pool, Duration::from_secs(0))
        .await
        .expect("purge_dead");
    assert_eq!(d2, N as u64, "purge_dead usunie pozostałe dead");

    let post = queue_stats(&pool, "q").await.expect("stats post");
    assert_eq!(post.done, 0);
    assert_eq!(post.dead, 0);

    // 3) Vice versa sanity: re-seed done, sprawdź że purge_dead niczego
    //    nie usuwa gdy istnieją tylko done.
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
    .expect("re-seed done");

    let d3 = purge_dead(&pool, Duration::from_secs(0))
        .await
        .expect("purge_dead empty");
    assert_eq!(d3, 0, "purge_dead na samych done = 0");
    let final_stats = queue_stats(&pool, "q").await.expect("stats final");
    assert_eq!(final_stats.done, N as u64, "done przeżyło purge_dead");
}
