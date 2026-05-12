//! Shared test fixtures — spin up a PG18 container, run the embedded migrator,
//! return a connected `PgPool` + the container handle (drop = stop).
//!
//! Each integration test file gets its own module instance (Cargo compiles
//! each `tests/*.rs` into its own crate), so we re-init tracing per test —
//! cheap & idempotent via `try_init`.

#![allow(dead_code)] // helpers used by a subset of test files

use std::sync::Once;

use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};

static TRACING_INIT: Once = Once::new();

pub(crate) fn init_tracing() {
    TRACING_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,sqlx=warn")),
            )
            .with_test_writer()
            .try_init();
    });
}

/// Boot a fresh PG18 container, connect a small pool, run the embedded
/// migrator. Returns both the pool AND the container so test code can keep
/// the container alive for the whole test (dropping the handle stops it).
pub(crate) async fn pg18_pool() -> (PgPool, ContainerAsync<Postgres>) {
    init_tracing();
    let container = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .expect("start pg18 container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect pg18 pool");
    pg_work_queue::migrator()
        .run(&pool)
        .await
        .expect("run migrator");
    (pool, container)
}

/// Variant: PG17 — used by the loud-fail test. Migrator should bail.
pub(crate) async fn pg17_container() -> (String, ContainerAsync<Postgres>) {
    init_tracing();
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .expect("start pg17 container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}
