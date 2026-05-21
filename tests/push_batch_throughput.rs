#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Throughput: `push_batch` of N items vs N × `push()` (każdy w własnej tx).
//!
//! Twierdzenie z PLAN.md: batch musi być ≥ 5× szybszy (mniej roundtripów →
//! lower wall-clock). Konserwatywny guard — pod normal devbox + idle Docker
//! batch jest typowo 20-50× szybszy.

mod common;

use std::time::Instant;

use serde::{Deserialize, Serialize};

use pg_work_queue::Pusher;

#[derive(Serialize, Deserialize)]
struct Tiny {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_push_is_at_least_5x_faster_than_per_row_push() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("throughput");

    const N: usize = 1_000;

    // -------- per-row push (N transactions) --------
    let t0 = Instant::now();
    for i in 0..N {
        let mut tx = pool.begin().await.expect("tx");
        pusher
            .push(&mut tx, &Tiny { seq: i as u32 }, None)
            .await
            .expect("push");
        tx.commit().await.expect("commit");
    }
    let per_row = t0.elapsed();

    // -------- batch push (1 transaction) --------
    let payloads: Vec<(Tiny, Option<String>)> = (0..N).map(|i| (Tiny { seq: i as u32 }, None)).collect();
    let t1 = Instant::now();
    let mut tx = pool.begin().await.expect("tx");
    let ids = pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");
    let batch = t1.elapsed();
    assert_eq!(ids.len(), N);

    let ratio = per_row.as_secs_f64() / batch.as_secs_f64();
    eprintln!("throughput: per_row={per_row:?}, batch={batch:?}, ratio={ratio:.2}x");
    assert!(
        ratio >= 5.0,
        "batch push must be >=5x faster; per_row={per_row:?}, batch={batch:?}, ratio={ratio:.2}x"
    );
}
