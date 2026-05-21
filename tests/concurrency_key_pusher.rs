#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Pusher carries concurrency_key through to the row; validation rejects
//! out-of-range keys.
mod common;

use common::pg18_pool;
use pg_work_queue::{PushError, Pusher};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct T {
    n: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_with_key_persists_it() {
    let (pool, _c) = pg18_pool().await;
    let pusher = Pusher::new("q");
    let mut tx = pool.begin().await.expect("tx");
    let id = pusher
        .push(&mut tx, &T { n: 1 }, Some("handler-x"))
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let key: Option<String> =
        sqlx::query_scalar("SELECT concurrency_key FROM pgwq.jobs WHERE public_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("select");
    assert_eq!(key.as_deref(), Some("handler-x"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_with_none_key_is_null() {
    let (pool, _c) = pg18_pool().await;
    let pusher = Pusher::new("q");
    let mut tx = pool.begin().await.expect("tx");
    let id = pusher
        .push(&mut tx, &T { n: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let key: Option<String> =
        sqlx::query_scalar("SELECT concurrency_key FROM pgwq.jobs WHERE public_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("select");
    assert!(key.is_none(), "expected NULL, got {key:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_with_empty_key_rejected() {
    let (pool, _c) = pg18_pool().await;
    let pusher = Pusher::new("q");
    let mut tx = pool.begin().await.expect("tx");
    let err = pusher
        .push(&mut tx, &T { n: 1 }, Some(""))
        .await
        .expect_err("must fail");
    assert!(
        matches!(err, PushError::ConcurrencyKeyInvalid(_)),
        "expected ConcurrencyKeyInvalid, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_with_oversize_key_rejected() {
    let (pool, _c) = pg18_pool().await;
    let pusher = Pusher::new("q");
    let mut tx = pool.begin().await.expect("tx");
    let long_key = "a".repeat(129);
    let err = pusher
        .push(&mut tx, &T { n: 1 }, Some(&long_key))
        .await
        .expect_err("must fail");
    assert!(
        matches!(err, PushError::ConcurrencyKeyInvalid(_)),
        "expected ConcurrencyKeyInvalid, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_with_multibyte_key_at_limit_accepted() {
    let (pool, _c) = pg18_pool().await;
    let pusher = Pusher::new("q");
    let mut tx = pool.begin().await.expect("tx");
    // "ą" is 2 bytes in UTF-8; 128 chars = 256 bytes — exactly at the limit.
    let key_128_chars = "ą".repeat(128);
    pusher
        .push(&mut tx, &T { n: 1 }, Some(&key_128_chars))
        .await
        .expect("push at char limit should succeed");
    tx.commit().await.expect("commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_batch_carries_per_item_keys_in_order() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = vec![
        (T { n: 0 }, Some("a".to_string())),
        (T { n: 1 }, None),
        (T { n: 2 }, Some("b".to_string())),
    ];
    let ids = Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(ids.len(), 3);

    for (id, expected) in ids.iter().zip(["a", "", "b"]) {
        let key: Option<String> = sqlx::query_scalar(
            "SELECT concurrency_key FROM pgwq.jobs WHERE public_id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let want = if expected.is_empty() { None } else { Some(expected.to_string()) };
        assert_eq!(key, want);
    }
}

// Un-commented in Task 6 (needs Task 5's claim_and_decode signature).
//
// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// #[ignore = "needs Task 5 claim_and_decode signature"]
// async fn claimed_job_exposes_concurrency_key() {
//     let (pool, _c) = pg18_pool().await;
//     let mut tx = pool.begin().await.unwrap();
//     Pusher::new("q").push(&mut tx, &T { n: 1 }, Some("k")).await.unwrap();
//     tx.commit().await.unwrap();
//
//     let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
//         &pool,
//         &pg_work_queue::JsonCodec,
//         "q",
//         10,
//         std::time::Duration::from_secs(30),
//         3,
//         &std::collections::HashMap::new(),
//     )
//     .await
//     .unwrap();
//     assert_eq!(jobs.len(), 1);
//     assert_eq!(jobs[0].concurrency_key.as_deref(), Some("k"));
// }
