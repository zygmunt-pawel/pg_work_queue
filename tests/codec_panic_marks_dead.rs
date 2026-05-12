#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Custom `Codec` ktorego `decode` panic'uje. `claim_and_decode` musi
//! odzyskac przez `std::panic::catch_unwind`, mark_dead'em ten konkretny
//! row (z reason `"codec panic: ..."`), a worker survive — inne rowsy
//! NIE dotkniete.

mod common;

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::Row;

use pg_work_queue::__test_exports::claim_and_decode;
use pg_work_queue::{Codec, JsonCodec, Pusher};

#[derive(Debug)]
struct DummyErr;
impl std::fmt::Display for DummyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("dummy")
    }
}
impl std::error::Error for DummyErr {}

/// Codec that intentionally panics on `decode`. Used to assert
/// `catch_unwind` isolation.
struct PanickyCodec;
impl Codec for PanickyCodec {
    type Error = DummyErr;
    fn encode<T: Serialize>(&self, _value: &T) -> Result<Vec<u8>, Self::Error> {
        Ok(vec![])
    }
    fn decode<T: DeserializeOwned>(&self, _bytes: &[u8]) -> Result<T, Self::Error> {
        panic!("intentional codec panic");
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Payload {
    n: i32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_in_codec_decode_is_isolated_and_marks_only_that_row_dead() {
    let (pool, _c) = common::pg18_pool().await;

    // Push 3 jobs via real JsonCodec (so payload bytes are valid JSON).
    let pusher = Pusher::new("panic_q");
    let payloads = vec![Payload { n: 1 }, Payload { n: 2 }, Payload { n: 3 }];
    let mut tx = pool.begin().await.expect("tx");
    pusher.push_batch(&mut tx, &payloads).await.expect("push");
    tx.commit().await.expect("commit");

    // Now claim with PanickyCodec — every decode panics. Library MUST mark
    // every claimed row as dead (one at a time, no propagated panic).
    let res = claim_and_decode::<Payload, _>(
        &pool,
        &PanickyCodec,
        "panic_q",
        10,
        Duration::from_secs(10),
        3,
    )
    .await
    .expect("claim_and_decode survived panics");
    assert!(
        res.is_empty(),
        "all rows should have been dropped (mark_dead)"
    );

    // All three rows must be 'dead' with codec panic reason.
    let rows = sqlx::query(
        "SELECT id, status::text AS status, last_error, lease_token, finished_at
         FROM pgwq.jobs WHERE queue = $1 ORDER BY id",
    )
    .bind("panic_q")
    .fetch_all(&pool)
    .await
    .expect("select");
    assert_eq!(rows.len(), 3);
    for r in &rows {
        let status: String = r.try_get("status").expect("status");
        let last_error: Option<String> = r.try_get("last_error").expect("last_error");
        let lease_token: Option<uuid::Uuid> = r.try_get("lease_token").expect("lease_token");
        let finished_at: Option<chrono::DateTime<chrono::Utc>> =
            r.try_get("finished_at").expect("finished_at");
        assert_eq!(status, "dead");
        let le = last_error.expect("last_error set");
        assert!(
            le.starts_with("codec panic:"),
            "expected last_error to start with 'codec panic:', got: {le}"
        );
        assert!(
            le.contains("intentional codec panic"),
            "panic body must surface: {le}"
        );
        assert!(lease_token.is_none());
        assert!(finished_at.is_some());
    }

    // Sanity: an unrelated queue with valid jobs is unaffected.
    let pusher_ok = Pusher::new("ok_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher_ok
        .push(&mut tx, &Payload { n: 42 })
        .await
        .expect("push ok");
    tx.commit().await.expect("commit");

    let claimed_ok =
        claim_and_decode::<Payload, _>(&pool, &JsonCodec, "ok_q", 10, Duration::from_secs(10), 3)
            .await
            .expect("ok_q claim");
    assert_eq!(claimed_ok.len(), 1);
    assert_eq!(claimed_ok[0].payload.n, 42);
}
