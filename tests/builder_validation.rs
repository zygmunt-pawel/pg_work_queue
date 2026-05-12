#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `WorkerBuilder::build()` validation — checks `BuildError` variants
//! implemented w Fazie 3:
//! - `QueueNameInvalid` (empty, > 64 chars)
//! - `MaxAttemptsZero`
//! - `LeaseTimeoutBelowFloor` (< 1s)
//! - `BatchSizeOutOfRange` (0 or > 1000)
//! - `HandlerMissing`
//! - Happy path: defaults + queue + handler → Ok.
//!
//! NOTE: testy NIE wymagają działającej DB — builder validation jest
//! czysto-synchroniczne. Pool jest required (builder demand'uje), ale
//! `PgPool` można zbudować bez połączenia przez `PgPoolOptions::new().connect_lazy(...)`.

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

use pg_work_queue::{BuildError, JobContext, JobError, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

fn dummy_pool() -> sqlx::PgPool {
    dummy_pool_with(64)
}

fn dummy_pool_with(max_conn: u32) -> sqlx::PgPool {
    // connect_lazy: no actual TCP connection until first query. Builder
    // validation never queries — pool is only stored. We size the lazy
    // pool large enough to satisfy `concurrency × 2 + 2` for any sensible
    // default concurrency (≤ #CPUs); tests that want to probe `PoolTooSmall`
    // pass a small max via `dummy_pool_with`.
    PgPoolOptions::new()
        .max_connections(max_conn)
        .connect_lazy("postgres://nobody:nobody@127.0.0.1:1/none")
        .expect("connect_lazy ok")
}

fn handler_ok() -> impl Fn(
    Payload,
    JobContext,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(), JobError>> + Send>,
> + Send
+ Sync
+ Clone
+ 'static {
    |_p: Payload, _ctx: JobContext| Box::pin(async move { Ok::<(), JobError>(()) })
}

#[tokio::test]
async fn empty_queue_name_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("")
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    match res {
        Err(BuildError::QueueNameInvalid(s)) => assert_eq!(s, ""),
        other => panic!("expected QueueNameInvalid, got {other:?}"),
    }
}

#[tokio::test]
async fn overlong_queue_name_rejected() {
    let too_long: String = "a".repeat(65);
    let res = Worker::<Payload, _>::builder()
        .queue(too_long.clone())
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    match res {
        Err(BuildError::QueueNameInvalid(s)) => assert_eq!(s, too_long),
        other => panic!("expected QueueNameInvalid, got {other:?}"),
    }
}

#[tokio::test]
async fn max_attempts_zero_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .max_attempts(0)
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    assert!(
        matches!(res, Err(BuildError::MaxAttemptsZero)),
        "expected MaxAttemptsZero, got {res:?}"
    );
}

#[tokio::test]
async fn lease_timeout_below_floor_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .lease_timeout(Duration::from_millis(500))
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    assert!(
        matches!(res, Err(BuildError::LeaseTimeoutBelowFloor)),
        "expected LeaseTimeoutBelowFloor, got {res:?}"
    );
}

#[tokio::test]
async fn batch_size_zero_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .batch_size(0)
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    match res {
        Err(BuildError::BatchSizeOutOfRange { actual, min, max }) => {
            assert_eq!(actual, 0);
            assert_eq!(min, 1);
            assert_eq!(max, 1000);
        }
        other => panic!("expected BatchSizeOutOfRange, got {other:?}"),
    }
}

#[tokio::test]
async fn batch_size_too_large_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .batch_size(1001)
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    match res {
        Err(BuildError::BatchSizeOutOfRange { actual, min, max }) => {
            assert_eq!(actual, 1001);
            assert_eq!(min, 1);
            assert_eq!(max, 1000);
        }
        other => panic!("expected BatchSizeOutOfRange, got {other:?}"),
    }
}

#[tokio::test]
async fn handler_missing_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .pool(dummy_pool())
        .build();
    assert!(
        matches!(res, Err(BuildError::HandlerMissing)),
        "expected HandlerMissing, got {res:?}"
    );
}

#[tokio::test]
async fn defaults_plus_queue_and_handler_ok() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    assert!(res.is_ok(), "valid config must build; got {res:?}");
}

// ---- Faza 4 BuildError variants ----------------------------------------

#[tokio::test]
async fn poll_interval_too_short_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .poll_interval(Duration::from_millis(1))
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    match res {
        Err(BuildError::PollIntervalTooShort { min }) => {
            assert_eq!(min, pg_work_queue::limits::MIN_POLL_INTERVAL);
        }
        other => panic!("expected PollIntervalTooShort, got {other:?}"),
    }
}

#[tokio::test]
async fn concurrency_zero_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .concurrency(0)
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    assert!(
        matches!(res, Err(BuildError::ConcurrencyZero)),
        "expected ConcurrencyZero, got {res:?}"
    );
}

#[tokio::test]
async fn pool_too_small_rejected() {
    // pool.max_connections = 4 but concurrency = 4 requires 4*2+2 = 10.
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .concurrency(4)
        .pool(dummy_pool_with(4))
        .handler(handler_ok())
        .build();
    match res {
        Err(BuildError::PoolTooSmall { actual, required }) => {
            assert_eq!(actual, 4);
            assert_eq!(required, 10);
        }
        other => panic!("expected PoolTooSmall, got {other:?}"),
    }
}

#[tokio::test]
async fn lease_timeout_too_short_for_poll_rejected() {
    // poll_interval=1s → lease must be >= 5s. 3s falls short.
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .poll_interval(Duration::from_secs(1))
        .lease_timeout(Duration::from_secs(3))
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    assert!(
        matches!(res, Err(BuildError::LeaseTimeoutTooShort)),
        "expected LeaseTimeoutTooShort, got {res:?}"
    );
}

#[tokio::test]
async fn handler_timeout_below_floor_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .lease_timeout(Duration::from_secs(60))
        .handler_timeout(Duration::from_millis(500))
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    match res {
        Err(BuildError::HandlerTimeoutBelowFloor { min }) => {
            assert_eq!(min, pg_work_queue::limits::MIN_HANDLER_TIMEOUT);
        }
        other => panic!("expected HandlerTimeoutBelowFloor, got {other:?}"),
    }
}

#[tokio::test]
async fn handler_timeout_too_long_rejected() {
    // lease=10s, handler=10s → 10+1 > 10, must reject.
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .lease_timeout(Duration::from_secs(10))
        .handler_timeout(Duration::from_secs(10))
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    assert!(
        matches!(res, Err(BuildError::HandlerTimeoutTooLong { .. })),
        "expected HandlerTimeoutTooLong, got {res:?}"
    );
}

#[tokio::test]
async fn mark_timeout_too_short_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(20))
        .mark_timeout(Duration::from_millis(50))
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    assert!(
        matches!(res, Err(BuildError::MarkTimeoutTooShort)),
        "expected MarkTimeoutTooShort, got {res:?}"
    );
}

#[tokio::test]
async fn mark_timeout_too_long_rejected() {
    // lease=30s, handler=20s, budget=10s, request mark=15s.
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(20))
        .mark_timeout(Duration::from_secs(15))
        .pool(dummy_pool())
        .handler(handler_ok())
        .build();
    assert!(
        matches!(res, Err(BuildError::MarkTimeoutTooLong { .. })),
        "expected MarkTimeoutTooLong, got {res:?}"
    );
}

#[tokio::test]
async fn pool_missing_rejected() {
    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .handler(handler_ok())
        .build();
    assert!(
        matches!(res, Err(BuildError::PoolMissing)),
        "expected PoolMissing, got {res:?}"
    );
}

/// Regression #44 v3.5 — fresh `connect_lazy` pool has `pool.size() == 0`
/// (no connections opened yet). Builder MUST use
/// `pool.options().get_max_connections()`, not `pool.size()`.
#[tokio::test]
async fn fresh_pool_max_connections_validated_not_size() {
    // size() = 0 on a lazy pool; get_max_connections() = 64 here.
    let pool = dummy_pool_with(64);
    assert_eq!(pool.size(), 0, "lazy pool has no opened conns yet");

    let res = Worker::<Payload, _>::builder()
        .queue("ok_q")
        .concurrency(4)
        .pool(pool)
        .handler(handler_ok())
        .build();
    assert!(
        res.is_ok(),
        "valid config with fresh lazy pool must build; got {res:?}"
    );
}
