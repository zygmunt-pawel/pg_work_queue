#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! After all jobs of a key complete (or are aborted at shutdown), the key's
//! headroom is fully restored — the in-memory counter does not leak.
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T {
    n: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_restored_after_all_jobs_complete() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = (0..5)
        .map(|i| (T { n: i }, Some("k".to_string())))
        .collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async { Ok::<(), JobError>(()) })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    let done: i64 = sqlx::query_scalar("SELECT count(*) FROM pgwq.jobs WHERE status = 'done'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(done, 5, "all keyed jobs completed -> counter did not wedge");
}

/// A shutdown with keyed in-flight handlers aborts them cleanly.
///
/// This does NOT observe the in-memory per-key counter directly (it lives in
/// process memory and dies with the worker). What it *does* prove: the abort
/// path runs `KeySlotGuard::drop` for every aborted keyed handler without
/// panicking. A panicking `Drop` on the counter would abort the whole test
/// process, so simply *reaching* the `aborted >= 1` assertion is the
/// observable evidence that the keyed guards dropped cleanly on the abort
/// path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_decrements_on_shutdown_abort() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = (0..2)
        .map(|i| (T { n: i }, Some("k".to_string())))
        .collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .lease_timeout(Duration::from_secs(60))
        .handler_timeout(Duration::from_secs(30))
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async {
            tokio::time::sleep(Duration::from_secs(120)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    // Reaching this assertion without a process abort proves KeySlotGuard::drop
    // ran cleanly for the aborted keyed handlers.
    let stats = handle.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(stats.aborted >= 1, "expected aborted handlers");
}

/// The keyed counter decrements on the `Err`/`mark_dead` exit path, not just
/// on `Ok`/`mark_done`.
///
/// With `concurrency_limits([("k", 2)])` and `max_attempts(1)`, a handler that
/// always returns `Err` exhausts its retry budget on attempt 1, so each job
/// goes straight to `dead`. If `KeySlotGuard` did NOT decrement on the non-`Ok`
/// exit, headroom for key `k` would wedge at 0 after the first 2 jobs and the
/// remaining 3 would stay `queued` forever. Reaching `dead == 5` therefore
/// proves the decrement fires on the `Err` exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_decrements_on_err_dead_exit() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = (0..5)
        .map(|i| (T { n: i }, Some("k".to_string())))
        .collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .max_attempts(1)
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async { Err::<(), JobError>(JobError::retry("x")) })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    let dead: i64 = sqlx::query_scalar("SELECT count(*) FROM pgwq.jobs WHERE status = 'dead'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        dead, 5,
        "all keyed jobs reached dead -> counter decremented on the Err exit",
    );
}

/// The keyed counter decrements on the handler-timeout exit path.
///
/// With `concurrency_limits([("k", 2)])` and `max_attempts(1)`, a handler that
/// sleeps forever gets cancelled by the 1-second `handler_timeout`. The
/// cancelled future counts as a failed attempt; with `max_attempts=1` the job
/// goes straight to `dead`. If `KeySlotGuard` did NOT decrement on the timeout
/// exit, headroom for key `k` would wedge at 0 after the first 2 jobs and the
/// remaining 3 would stay `queued`. Reaching `dead == 5` therefore proves the
/// decrement fires on the timeout exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_decrements_on_handler_timeout() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = (0..5)
        .map(|i| (T { n: i }, Some("k".to_string())))
        .collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .max_attempts(1)
        .handler_timeout(Duration::from_secs(1))
        .lease_timeout(Duration::from_secs(3))
        .poll_interval(Duration::from_millis(200))
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(15)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    let dead: i64 = sqlx::query_scalar("SELECT count(*) FROM pgwq.jobs WHERE status = 'dead'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        dead, 5,
        "all keyed jobs reached dead -> counter decremented on the timeout exit",
    );
}

/// The keyed counter decrements on the handler-panic exit path.
///
/// With `concurrency_limits([("k", 2)])` and `max_attempts(1)`, a handler that
/// panics immediately triggers `PanicPolicy::Retry` (the default), which
/// synthesises a retry attempt. With `max_attempts=1` that exhausts the budget
/// and the job goes to `dead`. If `KeySlotGuard` did NOT decrement on the panic
/// exit, headroom for key `k` would wedge at 0 after the first 2 jobs and the
/// remaining 3 would stay `queued`. Reaching `dead == 5` therefore proves the
/// decrement fires on the panic exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_decrements_on_handler_panic() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = (0..5)
        .map(|i| (T { n: i }, Some("k".to_string())))
        .collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .max_attempts(1)
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async {
            panic!("boom");
            #[allow(unreachable_code)]
            Ok::<(), JobError>(())
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    let dead: i64 = sqlx::query_scalar("SELECT count(*) FROM pgwq.jobs WHERE status = 'dead'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        dead, 5,
        "all keyed jobs reached dead -> counter decremented on the panic exit",
    );
}

/// The keyed counter decrements across the full retry cycle
/// (`running → awaiting_retry → running → dead`).
///
/// With `concurrency_limits([("k", 2)])` and `max_attempts(2)`, a handler that
/// always returns `Err(JobError::retry_in(…))` causes each job to transition
/// `running → awaiting_retry` on attempt 1, then `awaiting_retry → running →
/// dead` on attempt 2. If the slot were NOT freed when the job entered
/// `awaiting_retry`, headroom for key `k` would wedge at 0 after the first 2
/// jobs' first failures and nothing would ever reach `dead`. Reaching
/// `dead == 4` therefore proves: (a) the slot is released on the `mark_retry`
/// exit, and (b) `awaiting_retry` rows do not consume a key slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_decrements_across_retry_cycle() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = (0..4)
        .map(|i| (T { n: i }, Some("k".to_string())))
        .collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .max_attempts(2)
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async {
            Err::<(), JobError>(JobError::retry_in(
                "always-fails",
                Duration::from_millis(100),
            ))
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(20)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    let dead: i64 = sqlx::query_scalar("SELECT count(*) FROM pgwq.jobs WHERE status = 'dead'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        dead, 4,
        "all keyed jobs reached dead -> counter released across retry cycle",
    );
}
