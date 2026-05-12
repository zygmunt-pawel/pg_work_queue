#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `tick_once` policzy fenced_out gdy mark_done przegra wyścig z reaperem.
//!
//! Scenariusz:
//! 1. Push 1 job.
//! 2. Worker handler czeka na `Notify` — w międzyczasie test code robi manual
//!    UPDATE row'a do `awaiting_retry` (clear lease_token) — symulacja reaper'a.
//! 3. Handler dostaje notify, zwraca Ok(()) — Worker próbuje mark_done.
//! 4. mark_done WHERE clause już nie matchuje (lease_token != stary, status != running)
//!    → rows_affected == 0 → `fenced_out += 1`, `completed` zostaje 0.
//! 5. Assert: TickStats { fenced_out: 1, completed: 0 }, row w DB nadal
//!    `awaiting_retry` (nie flipnięty do done — w tym sedno fencing'u).

mod common;

use std::sync::Arc;
use std::time::Duration;

use sqlx::Row;
use tokio::sync::Notify;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mark_done_lost_race_counted_as_fenced_out() {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("race_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 7 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    // Handler-side coordination: handler signals "I'm running" then waits for
    // "ok, proceed". Test code w międzyczasie robi reaper-simulation UPDATE.
    let handler_started: Arc<Notify> = Arc::new(Notify::new());
    let proceed: Arc<Notify> = Arc::new(Notify::new());

    let handler_started_h = handler_started.clone();
    let proceed_h = proceed.clone();

    let worker = Worker::<Payload, _>::builder()
        .queue("race_q")
        .max_attempts(3)
        .lease_timeout(Duration::from_secs(30))
        .batch_size(10)
        .pool(pool.clone())
        .handler(move |_p: Payload, _ctx: JobContext| {
            let started = handler_started_h.clone();
            let proceed = proceed_h.clone();
            async move {
                started.notify_one();
                proceed.notified().await;
                Ok::<(), JobError>(())
            }
        })
        .build()
        .expect("build");

    // Run tick_once + the "reaper simulator" concurrently.
    let pool_for_sim = pool.clone();
    let handler_started_sim = handler_started.clone();
    let proceed_sim = proceed.clone();
    let sim = tokio::spawn(async move {
        // Czekamy aż handler rozpocznie.
        handler_started_sim.notified().await;
        // Symulujemy: reaper flipnął row do awaiting_retry, lease_token NULL.
        let n = sqlx::query(
            "UPDATE pgwq.jobs
             SET status = 'awaiting_retry', lease_token = NULL, lease_expires_at = NULL
             WHERE queue = $1 AND status = 'running'",
        )
        .bind("race_q")
        .execute(&pool_for_sim)
        .await
        .expect("reaper sim update")
        .rows_affected();
        assert_eq!(n, 1, "reaper sim must flip exactly 1 row");
        // Teraz pozwalamy handler'owi dokończyć → mark_done dostanie 0 rows.
        proceed_sim.notify_one();
    });

    let stats = worker.tick_once().await.expect("tick_once ok");
    sim.await.expect("sim task");

    assert_eq!(stats.claimed, 1, "claimed exactly 1");
    assert_eq!(
        stats.completed, 0,
        "completed must be 0 — mark_done fenced out"
    );
    assert_eq!(stats.failed, 0, "no handler failures");
    assert_eq!(stats.fenced_out, 1, "must count exactly 1 fence-out");

    // Row nadal awaiting_retry — mark_done nie przeszło.
    let r = sqlx::query(
        "SELECT status::text AS status, lease_token, finished_at
         FROM pgwq.jobs WHERE queue = $1",
    )
    .bind("race_q")
    .fetch_one(&pool)
    .await
    .expect("fetch");
    let status: String = r.try_get("status").expect("status");
    let lease_token: Option<uuid::Uuid> = r.try_get("lease_token").expect("lease_token");
    let finished_at: Option<chrono::DateTime<chrono::Utc>> =
        r.try_get("finished_at").expect("finished_at");
    assert_eq!(status, "awaiting_retry", "row musi zostać awaiting_retry");
    assert!(lease_token.is_none(), "lease_token cleared by sim");
    assert!(
        finished_at.is_none(),
        "finished_at musi być NULL — mark_done fenced out, nigdy nie zapisał"
    );
}
