#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `lease_timeout` knob: claim_batch musi stamp'ować `lease_expires_at = now() +
//! lease_timeout` per-row. Two-value behavioral test (Anti-pattern #1):
//! Worker A z lease=10s, Worker B z lease=30s — róznica obserwowalna jako
//! `lease_expires_at - now()` po claim'ie.

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sqlx::Row;
use tokio::sync::Notify;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_timeout_knob_writes_per_row_lease_expires_at() {
    let (pool, _c) = common::pg18_pool().await;

    // 1 row per queue.
    let pa = Pusher::new("ls_a");
    let pb = Pusher::new("ls_b");
    let mut tx = pool.begin().await.expect("tx");
    pa.push(&mut tx, &Payload { seq: 1 }, None).await.expect("push a");
    pb.push(&mut tx, &Payload { seq: 2 }, None).await.expect("push b");
    tx.commit().await.expect("commit");

    // Both handlers block forever on a Notify — we never wake them, we just
    // need the row to be claimed so we can read its lease_expires_at.
    let block_a = Arc::new(Notify::new());
    let block_b = Arc::new(Notify::new());
    let block_a_h = block_a.clone();
    let block_b_h = block_b.clone();

    let worker_a = Worker::<Payload, _>::builder()
        .queue("ls_a")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(100))
        .handler_timeout(Duration::from_secs(8))
        .handler(move |_p: Payload, _ctx: JobContext| {
            let n = block_a_h.clone();
            async move {
                n.notified().await;
                Ok::<(), JobError>(())
            }
        })
        .build()
        .expect("build a");
    let worker_b = Worker::<Payload, _>::builder()
        .queue("ls_b")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(30))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(100))
        .handler_timeout(Duration::from_secs(25))
        .handler(move |_p: Payload, _ctx: JobContext| {
            let n = block_b_h.clone();
            async move {
                n.notified().await;
                Ok::<(), JobError>(())
            }
        })
        .build()
        .expect("build b");

    let handle_a = worker_a.start().await.expect("start a");
    let handle_b = worker_b.start().await.expect("start b");

    // Give both workers a moment to claim.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let now = Utc::now();

    let lease_a: chrono::DateTime<chrono::Utc> = sqlx::query(
        "SELECT lease_expires_at FROM pgwq.jobs WHERE queue = 'ls_a' AND status = 'running'",
    )
    .fetch_one(&pool)
    .await
    .expect("fetch a")
    .try_get("lease_expires_at")
    .expect("lease_expires_at a");
    let lease_b: chrono::DateTime<chrono::Utc> = sqlx::query(
        "SELECT lease_expires_at FROM pgwq.jobs WHERE queue = 'ls_b' AND status = 'running'",
    )
    .fetch_one(&pool)
    .await
    .expect("fetch b")
    .try_get("lease_expires_at")
    .expect("lease_expires_at b");

    let remaining_a = (lease_a - now).to_std().unwrap_or(Duration::ZERO);
    let remaining_b = (lease_b - now).to_std().unwrap_or(Duration::ZERO);

    // Worker A: lease was set to now+10s at claim time (~500ms ago) so
    // remaining ≤ 10s (with small margin). And > 7s realistically.
    assert!(
        remaining_a <= Duration::from_secs(10),
        "worker A: lease must be ≤ 10s, got {remaining_a:?}"
    );
    // Worker B: lease was set to now+30s, so remaining > 5s (well above
    // Worker A's max of 10s).
    assert!(
        remaining_b > Duration::from_secs(15),
        "worker B: lease must be > 15s (was 30s set ~500ms ago), got {remaining_b:?}"
    );
    // Sanity: B is strictly longer than A.
    assert!(
        remaining_b > remaining_a,
        "lease B ({remaining_b:?}) must exceed lease A ({remaining_a:?})"
    );

    // Wake handlers BEFORE cancelling so drain_handlers() doesn't have to
    // wait for handler_timeout (25s for B) to elapse. Both handlers return
    // Ok(()) once notified; mark_done flips the row → drain completes.
    block_a.notify_one();
    block_b.notify_one();
    handle_a.cancel();
    handle_b.cancel();
    let _ = handle_a.join().await;
    let _ = handle_b.join().await;
}
