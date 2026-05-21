#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Fencing-token guard: mark_done/mark_retry/mark_dead z STALE token muszą
//! zwrócić `rows_affected == 0` (worker stracił wyścig z reaper'em).
//!
//! Strategia (low-level — wołamy mark_* przez `__test_exports`):
//! 1. Push 1 job.
//! 2. claim_and_decode → capturujemy OLD lease_token.
//! 3. Manualnie `UPDATE … SET lease_token = gen_random_uuid()` w DB,
//!    aby udawać że reaper przejął row'a (nowy token).
//! 4. Każde z mark_done/mark_retry/mark_dead z OLD lease_token musi zwrócić 0
//!    (defense-in-depth: nawet jeśli SQL przepuści inne checki, fencing-token
//!    is the single canonical guard).

mod common;

use std::time::Duration;

use sqlx::Row;
use uuid::Uuid;

use pg_work_queue::__test_exports::{claim_and_decode, mark_dead, mark_done, mark_retry};
use pg_work_queue::JsonCodec;
use pg_work_queue::Pusher;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mark_queries_fenced_out_when_lease_token_drifted() {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("fence_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let codec = JsonCodec;
    let claimed =
        claim_and_decode::<Payload, _>(&pool, &codec, "fence_q", 10, Duration::from_secs(30), 3)
            .await
            .expect("claim ok");
    assert_eq!(claimed.len(), 1);
    let job = &claimed[0];
    let old_token: Uuid = job.lease_token;

    // Symulujemy reaper / inny worker który przejął row'a: zmieniamy lease_token
    // w DB na nową wartość. Row zostaje 'running' (nie chcemy testować innych
    // pól WHERE — tylko fencing token).
    let n = sqlx::query(
        "UPDATE pgwq.jobs SET lease_token = gen_random_uuid() WHERE id = $1 AND status = 'running'",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .expect("rotate lease_token")
    .rows_affected();
    assert_eq!(n, 1, "lease_token rotation must hit exactly 1 row");

    // mark_done z OLD token → 0 (fenced out).
    let rows = mark_done(&pool, job.id, old_token)
        .await
        .expect("mark_done sql");
    assert_eq!(
        rows, 0,
        "mark_done with stale lease_token must affect 0 rows"
    );

    // mark_retry z OLD token → 0.
    let rows = mark_retry(
        &pool,
        job.id,
        old_token,
        "retry reason",
        std::time::Duration::from_secs(60),
    )
    .await
    .expect("mark_retry sql");
    assert_eq!(
        rows, 0,
        "mark_retry with stale lease_token must affect 0 rows"
    );

    // mark_dead z OLD token → 0.
    let rows = mark_dead(&pool, job.id, old_token, "dead reason")
        .await
        .expect("mark_dead sql");
    assert_eq!(
        rows, 0,
        "mark_dead with stale lease_token must affect 0 rows"
    );

    // Status row'a zostaje 'running' — żadne z mark_* nie przeszło.
    let r = sqlx::query("SELECT status::text AS status FROM pgwq.jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .expect("fetch status");
    let status: String = r.try_get("status").expect("status");
    assert_eq!(
        status, "running",
        "row musi pozostać w 'running' — wszystkie mark_* fenced out"
    );
}
