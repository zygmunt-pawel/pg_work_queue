#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `push_batch` zwraca `Vec<Uuid>` w **input order**.
//!
//! Strategia: 50 payloads z distinct `seq`. Po commit:
//! 1. `SELECT public_id, payload FROM pgwq.jobs WHERE queue=$1 ORDER BY id`
//! 2. Sprawdzamy, że DB-order public_id == zwrócony Vec<Uuid> (input order).
//! 3. Sprawdzamy, że decoded payload.seq w DB-order == 0,1,...,49 (input order).
//!
//! Cel: NIE polegamy na RETURNING — Uuid::now_v7() na client-side gwarantuje
//! monotonic order matching input. DB row insert order == array order pod
//! unnest, więc public_id-w-DB-orderze == zwrócony Vec.

mod common;

use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use pg_work_queue::Codec;
use pg_work_queue::{JsonCodec, Pusher};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct WithSeq {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_batch_preserves_input_order_in_db_and_in_returned_uuids() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("order_q");
    let payloads: Vec<(WithSeq, Option<String>)> =
        (0..50u32).map(|seq| (WithSeq { seq }, None)).collect();

    let mut tx = pool.begin().await.expect("tx");
    let returned_ids = pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    assert_eq!(returned_ids.len(), 50);

    let rows = sqlx::query("SELECT public_id, payload FROM pgwq.jobs WHERE queue = $1 ORDER BY id")
        .bind("order_q")
        .fetch_all(&pool)
        .await
        .expect("select all");

    assert_eq!(rows.len(), 50);

    let codec = JsonCodec;
    for (i, row) in rows.iter().enumerate() {
        let db_pubid: Uuid = row.try_get("public_id").expect("public_id");
        assert_eq!(
            db_pubid, returned_ids[i],
            "row {i}: DB ordering must match input ordering (client-side uuidv7)"
        );

        let bytes: Vec<u8> = row.try_get("payload").expect("payload");
        let decoded: WithSeq = codec.decode(&bytes).expect("decode");
        assert_eq!(
            decoded.seq, i as u32,
            "row {i}: payload.seq must match input position"
        );
    }
}
