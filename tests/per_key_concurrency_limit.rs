#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! The keyed claim SQL claims at most `headroom` rows per key.
mod common;

use common::pg18_pool;
use pg_work_queue::{JsonCodec, Pusher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

async fn push_n_keyed(pool: &sqlx::PgPool, queue: &str, key: &str, n: u32) {
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..n).map(|i| (T { n: i }, Some(key.to_string()))).collect();
    Pusher::new(queue).push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_claim_respects_headroom_limit_2() {
    let (pool, _c) = pg18_pool().await;
    push_n_keyed(&pool, "q", "k", 10).await;

    let headroom: HashMap<String, u32> = [("k".to_string(), 2u32)].into();
    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool, &JsonCodec, "q", 32, Duration::from_secs(30), 3, &headroom,
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_claim_saturated_key_claims_zero() {
    let (pool, _c) = pg18_pool().await;
    push_n_keyed(&pool, "q", "k", 10).await;

    let headroom: HashMap<String, u32> = [("k".to_string(), 0u32)].into();
    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool, &JsonCodec, "q", 32, Duration::from_secs(30), 3, &headroom,
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_claim_unconfigured_key_is_unlimited() {
    let (pool, _c) = pg18_pool().await;
    push_n_keyed(&pool, "q", "other", 10).await;

    let headroom: HashMap<String, u32> = [("k".to_string(), 1u32)].into();
    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool, &JsonCodec, "q", 32, Duration::from_secs(30), 3, &headroom,
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_claim_null_key_is_unlimited() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..10).map(|i| (T { n: i }, None)).collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let headroom: HashMap<String, u32> = [("k".to_string(), 1u32)].into();
    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool, &JsonCodec, "q", 32, Duration::from_secs(30), 3, &headroom,
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 10);
}
