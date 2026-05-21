#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `claim_batch` honors `FOR UPDATE SKIP LOCKED` — dwa rownolegle wywolania
//! claim'ow na tej samej kolejce nie zdouble-claim'uja zadnego row'a i razem
//! consume'uja caly zestaw push'owanych jobow.
//!
//! Strategia:
//! 1. Push 100 jobs do `q1`.
//! 2. Pusc `claim_batch(50)` x2 concurrently w `tokio::join!`.
//! 3. Assert: union = 100 distinct rows, intersection = empty, all DB rows
//!    `status='running'`, `attempts=1`, distinct `lease_token`s.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use sqlx::Row;
use uuid::Uuid;

use pg_work_queue::__test_exports::claim_and_decode;
use pg_work_queue::{JsonCodec, Pusher};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claim_batch_szanuje_skip_locked_pod_zrownoleglonym_dostepem() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("q1");
    let payloads: Vec<(Payload, Option<String>)> = (0..100u32).map(|seq| (Payload { seq }, None)).collect();

    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    let codec = JsonCodec;
    let headroom = std::collections::HashMap::new();
    let (left, right) = tokio::join!(
        claim_and_decode::<Payload, _>(&pool, &codec, "q1", 50, Duration::from_secs(10), 3, &headroom),
        claim_and_decode::<Payload, _>(&pool, &codec, "q1", 50, Duration::from_secs(10), 3, &headroom),
    );
    let left = left.expect("left claim ok");
    let right = right.expect("right claim ok");

    assert_eq!(
        left.len() + right.len(),
        100,
        "combined claimed count must equal pushed count; left={}, right={}",
        left.len(),
        right.len(),
    );

    let left_ids: HashSet<i64> = left.iter().map(|j| j.id).collect();
    let right_ids: HashSet<i64> = right.iter().map(|j| j.id).collect();
    let overlap: Vec<&i64> = left_ids.intersection(&right_ids).collect();
    assert!(
        overlap.is_empty(),
        "no row may be claimed twice; overlap={overlap:?}"
    );

    // All rows in DB end up running with attempts=1.
    let rows = sqlx::query(
        "SELECT id, status::text AS status, attempts, lease_token FROM pgwq.jobs WHERE queue = $1",
    )
    .bind("q1")
    .fetch_all(&pool)
    .await
    .expect("select all");
    assert_eq!(rows.len(), 100);
    let mut tokens: HashSet<Uuid> = HashSet::new();
    for r in &rows {
        let status: String = r.try_get("status").expect("status");
        let attempts: i32 = r.try_get("attempts").expect("attempts");
        let token: Uuid = r.try_get("lease_token").expect("lease_token");
        assert_eq!(status, "running");
        assert_eq!(attempts, 1);
        tokens.insert(token);
    }
    // `gen_random_uuid()` jest evaluated **per-row** w klauzuli SET (nie per-statement),
    // więc każdy z 100 wierszy musi mieć unikalny lease_token. Regresja byłaby gdy ktoś
    // przeniósł `gen_random_uuid()` poza UPDATE (np. do CTE w `SELECT … gen_random_uuid()`),
    // co dałoby per-batch token zamiast per-row → fence-out test by się posypał.
    assert_eq!(
        tokens.len(),
        100,
        "lease_token musi być per-row unikalny (gen_random_uuid() in SET); got {} distinct of 100",
        tokens.len()
    );
}
