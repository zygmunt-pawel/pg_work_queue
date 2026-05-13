#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `queue_stats(pool, queue)` zwraca 5 liczników (queued / awaiting_retry /
//! running / done / dead) tylko dla wskazanej kolejki — rzędy w innej
//! kolejce nie wliczają się (PLAN.md decision #16).
//!
//! Rzędy seedujemy bezpośrednio przez SQL z polami zgodnymi z
//! `jobs_status_invariants` CHECK + `jobs_temporal` CHECK (nie tworzymy
//! real worker'a — to test odczytu statystyk, nie state machine'y). Gdy
//! ustawiamy `first_attempted_at` w przeszłość, musimy też przesunąć
//! `created_at` aby `created_at <= first_attempted_at` było prawdziwe.

mod common;

use pg_work_queue::queue_stats;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_stats_zwraca_per_status_per_queue() {
    let (pool, _c) = common::pg18_pool().await;

    // -------- queue 'q' --------
    // 3 queued — DB defaults wystarczą (created_at/run_at/updated_at = now()).
    sqlx::query(
        "INSERT INTO pgwq.jobs (queue, payload, public_id)
         SELECT 'q', '{}'::bytea, uuidv7() FROM generate_series(1, 3)",
    )
    .execute(&pool)
    .await
    .expect("seed queued");

    // 2 awaiting_retry. CHECK: attempts > 0, first_/last_attempted_at NOT NULL,
    // finished_at NULL, lease_* NULL. created_at <= -1min.
    sqlx::query(
        "INSERT INTO pgwq.jobs (
            queue, payload, public_id, status,
            attempts, max_attempts,
            first_attempted_at, last_attempted_at,
            created_at, run_at, updated_at
        )
        SELECT
            'q', '{}'::bytea, uuidv7(), 'awaiting_retry'::pgwq.job_status,
            1, 3,
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute'
        FROM generate_series(1, 2)",
    )
    .execute(&pool)
    .await
    .expect("seed awaiting_retry");

    // 1 running. CHECK: attempts > 0, lease_token NOT NULL, lease_expires_at NOT NULL.
    // created_at <= -1s.
    sqlx::query(
        "INSERT INTO pgwq.jobs (
            queue, payload, public_id, status,
            attempts, max_attempts,
            first_attempted_at, last_attempted_at,
            lease_token, lease_expires_at,
            created_at, run_at, updated_at
        )
        VALUES (
            'q', '{}'::bytea, uuidv7(), 'running'::pgwq.job_status,
            1, 3,
            now() - interval '1 second',
            now() - interval '1 second',
            gen_random_uuid(), now() + interval '30 seconds',
            now() - interval '1 second',
            now() - interval '1 second',
            now() - interval '1 second'
        )",
    )
    .execute(&pool)
    .await
    .expect("seed running");

    // 4 done. created_at + finished_at = -1min.
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
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute'
        FROM generate_series(1, 4)",
    )
    .execute(&pool)
    .await
    .expect("seed done");

    // 5 dead.
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
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            'max_attempts',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute'
        FROM generate_series(1, 5)",
    )
    .execute(&pool)
    .await
    .expect("seed dead");

    // -------- inna kolejka 'other' — NIE powinna się liczyć dla 'q' --------
    sqlx::query(
        "INSERT INTO pgwq.jobs (queue, payload, public_id)
         SELECT 'other', '{}'::bytea, uuidv7() FROM generate_series(1, 7)",
    )
    .execute(&pool)
    .await
    .expect("seed other.queued");

    sqlx::query(
        "INSERT INTO pgwq.jobs (
            queue, payload, public_id, status,
            attempts, max_attempts,
            first_attempted_at, last_attempted_at, finished_at,
            created_at, run_at, updated_at
        )
        SELECT
            'other', '{}'::bytea, uuidv7(), 'done'::pgwq.job_status,
            1, 1,
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute',
            now() - interval '1 minute'
        FROM generate_series(1, 9)",
    )
    .execute(&pool)
    .await
    .expect("seed other.done");

    // -------- assert: tylko 'q' --------
    let stats = queue_stats(&pool, "q").await.expect("queue_stats q");
    assert_eq!(stats.queued, 3, "queued count for q");
    assert_eq!(stats.awaiting_retry, 2, "awaiting_retry count for q");
    assert_eq!(stats.running, 1, "running count for q");
    assert_eq!(stats.done, 4, "done count for q");
    assert_eq!(stats.dead, 5, "dead count for q");

    // assert: 'other' też się liczy oddzielnie
    let other = queue_stats(&pool, "other")
        .await
        .expect("queue_stats other");
    assert_eq!(other.queued, 7);
    assert_eq!(other.done, 9);
    assert_eq!(other.awaiting_retry, 0);
    assert_eq!(other.running, 0);
    assert_eq!(other.dead, 0);

    // assert: queue nieistniejąca = wszystkie zera, nie błąd.
    let empty = queue_stats(&pool, "nonexistent")
        .await
        .expect("queue_stats empty");
    assert_eq!(empty.queued, 0);
    assert_eq!(empty.awaiting_retry, 0);
    assert_eq!(empty.running, 0);
    assert_eq!(empty.done, 0);
    assert_eq!(empty.dead, 0);
}
