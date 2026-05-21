#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! The immutability trigger rejects any UPDATE that changes concurrency_key,
//! and the normal claim/mark lifecycle preserves it.
mod common;

use common::pg18_pool;
use pg_work_queue::Pusher;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct T {
    n: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_update_of_concurrency_key_is_rejected() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("q").push(&mut tx, &T { n: 1 }, Some("k")).await.unwrap();
    tx.commit().await.unwrap();

    let err = sqlx::query("UPDATE pgwq.jobs SET concurrency_key = 'other' WHERE public_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("immutable"), "expected immutability error, got: {msg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setting_concurrency_key_from_null_is_rejected() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("q").push(&mut tx, &T { n: 1 }, None).await.unwrap();
    tx.commit().await.unwrap();

    let err = sqlx::query("UPDATE pgwq.jobs SET concurrency_key = 'x' WHERE public_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("immutable"));
}
