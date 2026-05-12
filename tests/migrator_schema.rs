#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Migrator on PG18 — schema/table/columns/enum/constraints/indexes weryfikacja.
//!
//! Sprawdza wszystkie elementy schema z PLAN.md §"Schema (DB layout)":
//! - schema `pgwq` istnieje
//! - tabela `pgwq.jobs` ma expected columns (incl. `max_attempts`)
//! - enum `pgwq.job_status` ma 5 wariantów
//! - INSERT płatne payload > 1 MiB → SQLSTATE 23xxx (CHECK violation)
//! - INSERT z queue='' → CHECK violation
//! - INSERT z queue dłuższym niż 64 znaki → CHECK violation
//! - reloptions zawierają fillfactor=90 + obie autovacuum scale factors
//! - 3 partial indexes istnieją (jobs_claim_idx, jobs_reap_idx, jobs_terminal_idx)

mod common;

use sqlx::Row;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwq_schema_table_enum_columns_present() {
    let (pool, _c) = common::pg18_pool().await;

    // Schema
    let row: (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'pgwq')")
            .fetch_one(&pool)
            .await
            .expect("query schema");
    assert!(row.0, "pgwq schema must exist");

    // Table
    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables
                       WHERE table_schema = 'pgwq' AND table_name = 'jobs')",
    )
    .fetch_one(&pool)
    .await
    .expect("query table");
    assert!(row.0, "pgwq.jobs table must exist");

    // Columns sample
    let cols: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'pgwq' AND table_name = 'jobs'",
    )
    .fetch_all(&pool)
    .await
    .expect("query columns");
    for required in [
        "id",
        "public_id",
        "queue",
        "payload",
        "status",
        "attempts",
        "max_attempts",
        "lease_token",
        "lease_expires_at",
        "last_error",
        "run_at",
    ] {
        assert!(
            cols.iter().any(|c| c == required),
            "missing column `{required}` in pgwq.jobs; got: {cols:?}"
        );
    }

    // Enum
    let variants: Vec<String> = sqlx::query_scalar(
        "SELECT e.enumlabel
         FROM pg_type t
         JOIN pg_enum e ON e.enumtypid = t.oid
         JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE n.nspname = 'pgwq' AND t.typname = 'job_status'
         ORDER BY e.enumsortorder",
    )
    .fetch_all(&pool)
    .await
    .expect("query enum");
    assert_eq!(
        variants.len(),
        5,
        "enum should have 5 variants; got {variants:?}"
    );
    for v in ["queued", "running", "awaiting_retry", "done", "dead"] {
        assert!(
            variants.iter().any(|x| x == v),
            "missing enum variant {v}; got {variants:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_over_1_mib_rejected_by_check() {
    let (pool, _c) = common::pg18_pool().await;
    let big = vec![0u8; 1_048_577]; // 1 MiB + 1
    let res = sqlx::query("INSERT INTO pgwq.jobs (queue, payload, public_id) VALUES ($1, $2, $3)")
        .bind("q1")
        .bind(&big)
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
    let err = res.expect_err("must fail");
    assert_sqlstate_23(&err);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_queue_name_rejected_by_check() {
    let (pool, _c) = common::pg18_pool().await;
    let res = sqlx::query("INSERT INTO pgwq.jobs (queue, payload, public_id) VALUES ($1, $2, $3)")
        .bind("")
        .bind(b"x" as &[u8])
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
    let err = res.expect_err("must fail");
    assert_sqlstate_23(&err);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_name_over_64_chars_rejected_by_check() {
    let (pool, _c) = common::pg18_pool().await;
    let long_q: String = "x".repeat(65);
    let res = sqlx::query("INSERT INTO pgwq.jobs (queue, payload, public_id) VALUES ($1, $2, $3)")
        .bind(&long_q)
        .bind(b"x" as &[u8])
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await;
    let err = res.expect_err("must fail");
    assert_sqlstate_23(&err);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reloptions_contain_fillfactor_and_autovacuum_scale() {
    let (pool, _c) = common::pg18_pool().await;
    let row = sqlx::query(
        "SELECT c.reloptions
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'pgwq' AND c.relname = 'jobs'",
    )
    .fetch_one(&pool)
    .await
    .expect("reloptions");
    let reloptions: Option<Vec<String>> = row.try_get("reloptions").expect("col");
    let opts = reloptions.expect("reloptions must be set");
    let joined = opts.join(",");
    assert!(
        joined.contains("fillfactor=90"),
        "missing fillfactor=90: {joined}"
    );
    assert!(
        joined.contains("autovacuum_vacuum_scale_factor=0.05"),
        "missing autovacuum_vacuum_scale_factor=0.05: {joined}"
    );
    assert!(
        joined.contains("autovacuum_analyze_scale_factor=0.05"),
        "missing autovacuum_analyze_scale_factor=0.05: {joined}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_partial_indexes_present() {
    let (pool, _c) = common::pg18_pool().await;
    let idxs: Vec<String> = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes
         WHERE schemaname = 'pgwq' AND tablename = 'jobs'",
    )
    .fetch_all(&pool)
    .await
    .expect("indexes");
    for expected in ["jobs_claim_idx", "jobs_reap_idx", "jobs_terminal_idx"] {
        assert!(
            idxs.iter().any(|i| i == expected),
            "missing index {expected}; got {idxs:?}"
        );
    }
    // Confirm partiality — each has WHERE in pg_indexes.indexdef.
    for expected in ["jobs_claim_idx", "jobs_reap_idx", "jobs_terminal_idx"] {
        let def: String = sqlx::query_scalar(
            "SELECT indexdef FROM pg_indexes
             WHERE schemaname='pgwq' AND tablename='jobs' AND indexname=$1",
        )
        .bind(expected)
        .fetch_one(&pool)
        .await
        .expect("indexdef");
        assert!(
            def.to_uppercase().contains("WHERE"),
            "{expected} not partial: {def}"
        );
    }
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
