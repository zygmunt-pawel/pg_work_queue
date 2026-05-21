#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! WorkerBuilder::concurrency_limits validation.
mod common;

use common::pg18_pool;
use pg_work_queue::{BuildError, JobContext, JobError, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct T {
    n: u32,
}

fn builder(pool: sqlx::PgPool) -> pg_work_queue::WorkerBuilder<T, pg_work_queue::JsonCodec, ()> {
    Worker::<T>::builder().pool(pool).queue("q")
}

async fn ok_handler(_t: T, _c: JobContext) -> Result<(), JobError> {
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_limit_zero_rejected() {
    let (pool, _c) = pg18_pool().await;
    let err = builder(pool)
        .concurrency_limits([("k".to_string(), 0u32)])
        .handler(ok_handler)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::ConcurrencyLimitInvalid { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_key_empty_rejected() {
    let (pool, _c) = pg18_pool().await;
    let err = builder(pool)
        .concurrency_limits([(String::new(), 2u32)])
        .handler(ok_handler)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::ConcurrencyKeyInvalid(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_concurrency_limits_build_ok() {
    let (pool, _c) = pg18_pool().await;
    let built = builder(pool)
        .concurrency_limits([("a".to_string(), 2u32), ("b".to_string(), 5u32)])
        .handler(ok_handler)
        .build();
    assert!(built.is_ok());
}
