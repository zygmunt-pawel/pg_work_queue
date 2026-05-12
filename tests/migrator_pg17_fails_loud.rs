#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! S5: migrator MUSI bombnąć głośnie na PG < 18 (uuidv7() jest natywne dopiero w 18).
//!
//! Test: spin up PG17, run migrator → expect sqlx::Error::Migrate (or wrapping)
//! z message zawierającym "PostgreSQL 18+" (zgodne z RAISE EXCEPTION w SQL).

mod common;

use sqlx::postgres::PgPoolOptions;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migrator_on_pg17_emits_pg18_required_message() {
    let (url, _c) = common::pg17_container().await;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect pg17");
    let err = pg_work_queue::migrator()
        .run(&pool)
        .await
        .expect_err("migrator must fail on PG17");
    let msg = format!("{err}");
    let chained = chain_messages(&err);
    let full = format!("{msg} | {chained}");
    assert!(
        full.contains("PostgreSQL 18+"),
        "migrator error should mention PG18 requirement; got: {full}"
    );
}

fn chain_messages(err: &(dyn std::error::Error + 'static)) -> String {
    let mut s = String::new();
    let mut src = err.source();
    while let Some(e) = src {
        s.push_str(" -> ");
        s.push_str(&e.to_string());
        src = e.source();
    }
    s
}
