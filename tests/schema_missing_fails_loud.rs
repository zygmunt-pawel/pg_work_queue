#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `Worker::start()` MUST loud-fail when `pgwq` schema is missing — operator
//! forgot to run the migrator. PLAN.md §"Faza 4 schema check" (line 1997).
//!
//! Anti-pattern: Apalis silently infinite-warn-loops on missing schema.
//! We surface [`StartError::SchemaMissing`] immediately.

mod common;

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt, runners::AsyncRunner};

use pg_work_queue::{JobContext, JobError, StartError, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_fails_with_schema_missing_when_migrator_not_run() {
    common::init_tracing();

    // Boot PG18 but DO NOT run the migrator.
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
        .max_connections(16)
        .connect(&url)
        .await
        .expect("connect pg18 pool");

    let worker = Worker::<Payload, _>::builder()
        .queue("missing_q")
        .pool(pool)
        .concurrency(2)
        .lease_timeout(Duration::from_secs(30))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build worker");

    let res = worker.start().await;
    match res {
        Err(StartError::SchemaMissing(e)) => {
            // Must be a SQLSTATE 42P01 (undefined table) or 3F000.
            let msg = e.to_string();
            assert!(
                msg.contains("relation")
                    || msg.contains("schema")
                    || msg.contains("does not exist"),
                "unexpected schema-missing error body: {msg}"
            );
        }
        other => panic!("expected SchemaMissing, got {other:?}"),
    }
}
