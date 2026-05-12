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
    // connect_lazy: no actual TCP connection until first query. Builder
    // validation never queries — pool is only stored.
    PgPoolOptions::new()
        .max_connections(2)
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
