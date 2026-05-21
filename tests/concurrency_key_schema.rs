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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        .expect("index existence query must succeed");
        assert!(exists, "index {idx} must exist");
    }

    // Length CHECK constraint exists.
    let check_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_constraint
         WHERE conname = 'jobs_concurrency_key_len')",
    )
    .fetch_one(&pool)
    .await
    .expect("constraint existence query must succeed");
    assert!(check_exists, "jobs_concurrency_key_len CHECK must exist");

    // Immutability trigger exists.
    let trigger_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_trigger
         WHERE tgname = 'assert_concurrency_key_immutable')",
    )
    .fetch_one(&pool)
    .await
    .expect("trigger existence query must succeed");
    assert!(trigger_exists, "immutability trigger must exist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_concurrency_key_rejected_by_check() {
    let (pool, _container) = pg18_pool().await;
    let res = sqlx::query(
        "INSERT INTO pgwq.jobs (queue, payload, concurrency_key) VALUES ('q', $1, $2)",
    )
    .bind(b"\x00" as &[u8])
    .bind("")
    .execute(&pool)
    .await;
    let err = res.expect_err("empty concurrency_key must be rejected");
    assert_sqlstate_23(&err);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_key_over_128_chars_rejected_by_check() {
    let (pool, _container) = pg18_pool().await;
    let long_key: String = "x".repeat(129);
    let res = sqlx::query(
        "INSERT INTO pgwq.jobs (queue, payload, concurrency_key) VALUES ('q', $1, $2)",
    )
    .bind(b"\x00" as &[u8])
    .bind(&long_key)
    .execute(&pool)
    .await;
    let err = res.expect_err("129-char concurrency_key must be rejected");
    assert_sqlstate_23(&err);
}

fn assert_sqlstate_23(err: &sqlx::Error) {
    let sqlx::Error::Database(db) = err else {
        panic!("expected database error, got {err:?}");
    };
    let code = db.code().expect("sqlstate present");
    assert!(
        code.starts_with("23"),
        "expected SQLSTATE 23xxx (integrity constraint), got {code}: {err:?}"
    );
}
