#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `claim_batch` respektuje `LIMIT $2` (batch_size).
//!
//! Strategia:
//! 1. Push 100 jobow.
//! 2. claim(10) → exactly 10 rows.
//! 3. claim(50) → exactly 50 rows (90 claimed in total).
//! 4. claim(50) → only 10 left → exactly 10 rows.
//! 5. claim(50) → 0 rows (empty queue).
//! 6. Total claimed ids = 100 distinct, set-equal to id-set in DB.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use pg_work_queue::__test_exports::claim_and_decode;
use pg_work_queue::{JsonCodec, Pusher};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_batch_respektuje_limit() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("bs_q");
    let payloads: Vec<Payload> = (0..100u32).map(|seq| Payload { seq }).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher.push_batch(&mut tx, &payloads).await.expect("push");
    tx.commit().await.expect("commit");

    let codec = JsonCodec;
    let lease = Duration::from_secs(30);

    let c1 = claim_and_decode::<Payload, _>(&pool, &codec, "bs_q", 10, lease, 3)
        .await
        .expect("c1");
    assert_eq!(c1.len(), 10, "first claim must return exactly batch_size");

    let c2 = claim_and_decode::<Payload, _>(&pool, &codec, "bs_q", 50, lease, 3)
        .await
        .expect("c2");
    assert_eq!(c2.len(), 50, "second claim must return exactly batch_size");

    let c3 = claim_and_decode::<Payload, _>(&pool, &codec, "bs_q", 50, lease, 3)
        .await
        .expect("c3");
    assert_eq!(c3.len(), 40, "third claim takes whatever remains (40)");

    let c4 = claim_and_decode::<Payload, _>(&pool, &codec, "bs_q", 50, lease, 3)
        .await
        .expect("c4");
    assert!(c4.is_empty(), "queue drained; fourth claim must be empty");

    let mut ids: HashSet<i64> = HashSet::new();
    for batch in [&c1, &c2, &c3] {
        for j in batch.iter() {
            assert!(
                ids.insert(j.id),
                "row id={} claimed twice across batches",
                j.id
            );
        }
    }
    assert_eq!(ids.len(), 100);
}
