#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Asserts the per-key concurrency migration applied the expected schema.
mod common;

use common::pg18_pool;

#[tokio::test]
async fn concurrency_key_column_and_objects_present() {
    let (pool, _container) = pg18_pool().await;

    // Column exists, nullable, type text.
    let (data_type, is_nullable): (String, String) = sqlx::query_as(
        "SELECT data_type, is_nullable FROM information_schema.columns
         WHERE table_schema = 'pgwq' AND table_name = 'jobs'
           AND column_name = 'concurrency_key'",
    )
    .fetch_one(&pool)
    .await
    .expect("concurrency_key column must exist");
    assert_eq!(data_type, "text");
    assert_eq!(is_nullable, "YES");

    // Both claim indexes exist.
    for idx in ["jobs_claim_idx", "jobs_claim_conc_idx"] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes
             WHERE schemaname = 'pgwq' AND indexname = $1)",
        )
        .bind(idx)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exists, "index {idx} must exist");
    }

    // Length CHECK constraint exists.
    let check_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_constraint
         WHERE conname = 'jobs_concurrency_key_len')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(check_exists, "jobs_concurrency_key_len CHECK must exist");

    // Immutability trigger exists.
    let trigger_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_trigger
         WHERE tgname = 'assert_concurrency_key_immutable')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(trigger_exists, "immutability trigger must exist");
}
