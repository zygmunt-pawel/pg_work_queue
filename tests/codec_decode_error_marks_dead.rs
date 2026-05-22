#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Codec decode error na claim time → row jest mark_dead'em z reason
//! `"codec decode: ..."`. Niskopoziomowy `INSERT` z invalid-JSON bytes,
//! `claim_and_decode` dla typu `SomeStruct` musi zrobic mark_dead i NIE
//! emit'owac `Job<T>` w wyniku.

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::Row;

use pg_work_queue::__test_exports::claim_and_decode;
use pg_work_queue::JsonCodec;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct SomeStruct {
    n: i32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decode_error_marks_row_dead_and_drops_it_from_output() {
    let (pool, _c) = common::pg18_pool().await;
    // "not-json" raw bytes — valid bytea, invalid serde_json.
    sqlx::query("INSERT INTO pgwq.jobs (queue, payload) VALUES ($1, $2::bytea)")
        .bind("dec_q")
        .bind(b"not-json".to_vec())
        .execute(&pool)
        .await
        .expect("insert raw");

    let codec = JsonCodec;
    let claimed = claim_and_decode::<SomeStruct, _>(
        &pool,
        &codec,
        "dec_q",
        10,
        Duration::from_secs(10),
        3,
        &std::collections::HashMap::new(),
    )
    .await
    .expect("claim ok");
    assert!(
        claimed.is_empty(),
        "decode failure must drop the row from output"
    );

    let row = sqlx::query(
        "SELECT status::text AS status, last_error, lease_token, lease_expires_at, finished_at
         FROM pgwq.jobs WHERE queue = $1",
    )
    .bind("dec_q")
    .fetch_one(&pool)
    .await
    .expect("fetch row");
    let status: String = row.try_get("status").expect("status");
    let last_error: Option<String> = row.try_get("last_error").expect("last_error");
    let lease_token: Option<uuid::Uuid> = row.try_get("lease_token").expect("lease_token");
    let lease_expires_at: Option<DateTime<Utc>> =
        row.try_get("lease_expires_at").expect("lease_expires_at");
    let finished_at: Option<DateTime<Utc>> = row.try_get("finished_at").expect("finished_at");

    assert_eq!(status, "dead");
    let le = last_error.expect("last_error set");
    assert!(
        le.starts_with("codec decode:"),
        "expected last_error to start with 'codec decode:', got: {le}"
    );
    assert!(lease_token.is_none(), "lease_token must be cleared");
    assert!(
        lease_expires_at.is_none(),
        "lease_expires_at must be cleared"
    );
    assert!(
        finished_at.is_some(),
        "finished_at must be set on terminal status"
    );
}
