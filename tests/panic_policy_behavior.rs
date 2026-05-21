#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `PanicPolicy::Retry` vs `PanicPolicy::Dead`:
//!
//! - Retry (default): handler panic → synthesized `JobError::Retry`,
//!   row goes through retry budget; eventually `dead` po max_attempts.
//!   `attempts == max_attempts`, `last_error` zaczyna się od `panic: `.
//! - Dead: handler panic → natychmiast `mark_dead` (bypass budget).
//!   `attempts == 1`, `last_error` to `panic: boom` (lub `panic: <msg>`).
//!
//! PLAN.md test list line 1841: `panic_policy_behavior.rs`.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{BackoffPolicy, JobContext, JobError, PanicPolicy, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

async fn push_one(pool: &sqlx::PgPool, queue: &str) {
    let pusher = Pusher::new(queue);
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");
}

async fn await_dead(pool: &sqlx::PgPool, queue: &str) -> (i32, Option<String>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s, attempts, last_error
             FROM pgwq.jobs WHERE queue = $1 ORDER BY id LIMIT 1",
        )
        .bind(queue)
        .fetch_one(pool)
        .await
        .expect("fetch");
        let s: String = row.try_get("s").expect("s");
        if s == "dead" {
            let attempts: i32 = row.try_get("attempts").expect("attempts");
            let last_error: Option<String> = row.try_get("last_error").expect("last_error");
            return (attempts, last_error);
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for dead on {queue}; current = {s}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_retry_burns_attempts_then_dies() {
    let (pool, _c) = common::pg18_pool().await;
    push_one(&pool, "pp_retry").await;

    let worker = Worker::<Payload, _>::builder()
        .queue("pp_retry")
        .pool(pool.clone())
        .max_attempts(3)
        .panic_policy(PanicPolicy::Retry)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            panic!("boom");
            #[allow(unreachable_code)]
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    let (attempts, last_error) = await_dead(&pool, "pp_retry").await;
    handle.cancel();
    let _ = handle.join().await;

    assert_eq!(
        attempts, 3,
        "PanicPolicy::Retry: must burn full budget (max_attempts=3)"
    );
    let last = last_error.expect("last_error must be set");
    assert!(
        last.starts_with("panic: "),
        "last_error must start with 'panic: '; got {last:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_dead_bypasses_retry_budget() {
    let (pool, _c) = common::pg18_pool().await;
    push_one(&pool, "pp_dead").await;

    let worker = Worker::<Payload, _>::builder()
        .queue("pp_dead")
        .pool(pool.clone())
        .max_attempts(3)
        .panic_policy(PanicPolicy::Dead)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            panic!("boom");
            #[allow(unreachable_code)]
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    let (attempts, last_error) = await_dead(&pool, "pp_dead").await;
    handle.cancel();
    let _ = handle.join().await;

    assert_eq!(
        attempts, 1,
        "PanicPolicy::Dead: must bypass budget; attempts == 1"
    );
    let last = last_error.expect("last_error must be set");
    assert!(
        last.contains("boom"),
        "last_error must carry the panic message; got {last:?}"
    );
    assert!(
        last.starts_with("panic: "),
        "last_error must start with 'panic: '; got {last:?}"
    );
}
