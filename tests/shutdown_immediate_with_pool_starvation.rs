#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Regression #2 v3.5 (PLAN.md lines 1923-1928) — when the pool is fully
//! starved (user holds all connections), `handle.cancel()` MUST be
//! intercepted via `tokio::select!` on the SQL await, not block waiting
//! for `acquire_timeout` (sqlx default 30s).
//!
//! Strategy:
//! 1. Build worker with a tiny pool (max=4, concurrency=1 → 1*2+2=4).
//! 2. Acquire ALL 4 connections OUTSIDE the worker — hold them.
//! 3. Start the worker. Its first claim await blocks on `pool.acquire()`.
//! 4. Cancel. join() must return within 500ms (NOT 30s default acquire).

mod common;

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt, runners::AsyncRunner};

use pg_work_queue::{JobContext, JobError, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_intercepts_pool_acquire_within_500ms() {
    common::init_tracing();

    let container = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .expect("start pg18");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    // max=4, concurrency=1 → 1*2+2 = 4 OK. acquire_timeout default 30s.
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect");

    pg_work_queue::migrator()
        .run(&pool)
        .await
        .expect("migrator");

    let worker = Worker::<Payload, _>::builder()
        .queue("starve_q")
        .pool(pool.clone())
        .concurrency(1)
        .lease_timeout(Duration::from_secs(30))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Starve the pool. We need to hold the 4 conns even after `start()`
    // (which used one for the schema probe — but released it). Grab 4 now.
    let c1 = pool.acquire().await.expect("c1");
    let c2 = pool.acquire().await.expect("c2");
    let c3 = pool.acquire().await.expect("c3");
    let c4 = pool.acquire().await.expect("c4");

    // Let the worker tick at least once so it hits the pool-starved claim.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel + measure join time.
    let t0 = std::time::Instant::now();
    handle.cancel();
    let res = tokio::time::timeout(Duration::from_secs(2), handle.join()).await;
    let elapsed = t0.elapsed();
    assert!(res.is_ok(), "join must complete (not hang on acquire)");
    assert!(
        elapsed < Duration::from_millis(500),
        "join must return within 500ms after cancel under pool starvation; got {elapsed:?}"
    );

    drop((c1, c2, c3, c4));
}
