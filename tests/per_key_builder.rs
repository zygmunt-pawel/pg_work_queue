#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! WorkerBuilder::concurrency_limits validation.
//!
//! NOTE: testy NIE wymagają działającej DB — builder validation jest
//! czysto-synchroniczne. Pool jest required (builder demand'uje), ale
//! `PgPool` można zbudować bez połączenia przez `PgPoolOptions::new().connect_lazy(...)`.

use sqlx::postgres::PgPoolOptions;

use pg_work_queue::{BuildError, JobContext, JobError, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct T {
    n: u32,
}

fn dummy_pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(64)
        .connect_lazy("postgres://nobody:nobody@127.0.0.1:1/none")
        .expect("connect_lazy ok")
}

fn builder(pool: sqlx::PgPool) -> pg_work_queue::WorkerBuilder<T, pg_work_queue::JsonCodec, ()> {
    Worker::<T>::builder().pool(pool).queue("q")
}

async fn ok_handler(_t: T, _c: JobContext) -> Result<(), JobError> {
    Ok(())
}

#[tokio::test]
async fn concurrency_limit_zero_rejected() {
    let err = builder(dummy_pool())
        .concurrency_limits([("k".to_string(), 0u32)])
        .handler(ok_handler)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::ConcurrencyLimitInvalid { .. }));
}

#[tokio::test]
async fn concurrency_key_empty_rejected() {
    let err = builder(dummy_pool())
        .concurrency_limits([(String::new(), 2u32)])
        .handler(ok_handler)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::ConcurrencyKeyInvalid(_)));
}

#[tokio::test]
async fn valid_concurrency_limits_build_ok() {
    let built = builder(dummy_pool())
        .concurrency_limits([("a".to_string(), 2u32), ("b".to_string(), 5u32)])
        .handler(ok_handler)
        .build();
    assert!(built.is_ok());
}

#[tokio::test]
async fn concurrency_key_too_long_rejected() {
    let err = builder(dummy_pool())
        .concurrency_limits([("a".repeat(129), 2u32)])
        .handler(ok_handler)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::ConcurrencyKeyInvalid(_)));
}

#[tokio::test]
async fn concurrency_limit_too_large_rejected() {
    let err = builder(dummy_pool())
        .concurrency_limits([("k".to_string(), u32::MAX)])
        .handler(ok_handler)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::ConcurrencyLimitInvalid { .. }));
}
