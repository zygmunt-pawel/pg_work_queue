#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Resource limits — client-side validation w Pusher (PLAN.md §"Resource limits").
//!
//! Sprawdza wszystkie limity narzucone przez `pgwq::limits`:
//! - PayloadTooLarge (single push, index = 0)
//! - BatchEmpty
//! - BatchTooLarge
//! - PayloadTooLarge w batch (poprawny index)
//! - BatchPayloadTooLarge (aggregate cap)
//! - QueueNameInvalid (empty + > 64 chars) — fail-late
//! - Pusher::new("") nie panikuje (fail-late kontrakt)

mod common;

use serde::{Deserialize, Serialize};

use pg_work_queue::limits::{MAX_BATCH_SIZE, MAX_PAYLOAD_BYTES, MAX_QUEUE_LEN};
use pg_work_queue::{PushError, Pusher};

#[derive(Serialize, Deserialize)]
struct Bag {
    /// Use a base64-like blob so encoded JSON size ≈ bytes.len() + overhead.
    blob: Vec<u8>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_push_oversize_payload_returns_payload_too_large_index_zero() {
    let (pool, _c) = common::pg18_pool().await;
    // Build a struct whose JSON-encoded size is strictly > MAX_PAYLOAD_BYTES.
    // Serializing a Vec<u8> with serde_json produces "[1,2,3,...]"; size
    // scales > bytes.len(). Use raw size > MAX_PAYLOAD_BYTES to be sure.
    let payload = Bag {
        blob: vec![0u8; MAX_PAYLOAD_BYTES + 1024],
    };
    let pusher = Pusher::new("q");
    let mut tx = pool.begin().await.expect("tx");
    let err = pusher
        .push(&mut tx, &payload, None)
        .await
        .expect_err("must fail");
    match err {
        PushError::PayloadTooLarge { index, size, max } => {
            assert_eq!(index, 0);
            assert_eq!(max, MAX_PAYLOAD_BYTES);
            assert!(size > MAX_PAYLOAD_BYTES);
        }
        e => panic!("expected PayloadTooLarge, got {e:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_batch_returns_batch_empty() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("q");
    let mut tx = pool.begin().await.expect("tx");
    let empty: Vec<(u32, Option<String>)> = vec![];
    let err = pusher
        .push_batch(&mut tx, &empty)
        .await
        .expect_err("must fail");
    assert!(matches!(err, PushError::BatchEmpty), "got {err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_over_max_size_returns_batch_too_large() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("q");
    // 10_001 tiny payloads — fails on size check BEFORE encode loop.
    let payloads: Vec<(u32, Option<String>)> = vec![(0u32, None); MAX_BATCH_SIZE + 1];
    let mut tx = pool.begin().await.expect("tx");
    let err = pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect_err("must fail");
    match err {
        PushError::BatchTooLarge { size, max } => {
            assert_eq!(size, MAX_BATCH_SIZE + 1);
            assert_eq!(max, MAX_BATCH_SIZE);
        }
        e => panic!("expected BatchTooLarge, got {e:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_item_index_three_oversize_reports_index_three() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("q");
    // Items 0..3 small, item 3 oversize.
    let mut items: Vec<(Bag, Option<String>)> = (0..3)
        .map(|_| {
            (
                Bag {
                    blob: vec![0u8; 100],
                },
                None,
            )
        })
        .collect();
    items.push((
        Bag {
            blob: vec![0u8; MAX_PAYLOAD_BYTES + 1024],
        },
        None,
    ));
    let mut tx = pool.begin().await.expect("tx");
    let err = pusher
        .push_batch(&mut tx, &items)
        .await
        .expect_err("must fail");
    match err {
        PushError::PayloadTooLarge { index, max, .. } => {
            assert_eq!(index, 3);
            assert_eq!(max, MAX_PAYLOAD_BYTES);
        }
        e => panic!("expected PayloadTooLarge{{index:3}}, got {e:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_aggregate_over_max_bytes_returns_batch_payload_too_large() {
    let (pool, _c) = common::pg18_pool().await;
    let pusher = Pusher::new("q");
    // 65 items × ~1 MiB raw → JSON-encoded ≈ ~3 MiB each (Vec<u8> serializes
    // as `[N,N,...]`). 65 × 3 MiB ≫ 64 MiB aggregate cap, dobrze trippuje
    // BatchPayloadTooLarge.
    //
    // BUT individual encoded size also exceeds MAX_PAYLOAD_BYTES, więc
    // dostaniemy PayloadTooLarge zamiast BatchPayloadTooLarge. Trzeba zrobić
    // items, których KAŻDY pojedynczo mieści się ≤ MAX_PAYLOAD_BYTES, ale
    // SUMA > MAX_BATCH_BYTES. Encoded size dla [N×u8] wynosi ~3-4 bajty na u8
    // (worst case "255," = 4 znaki + brackets). Maksymalny safe raw size, by
    // encoded ≤ 1 MiB to ~250 KiB. 250 KiB × ~700 items > 64 MiB encoded.
    //
    // Prościej: items są dużych ale-mieszczących-się Strings ≈ 800 KiB JSON
    // encoded (raw String + quoting ~ raw + 2 bytes). 90 × 800 KiB = 72 MiB > 64 MiB.
    let one_item = "x".repeat(800 * 1024);
    let items: Vec<(String, Option<String>)> = (0..90).map(|_| (one_item.clone(), None)).collect();
    let mut tx = pool.begin().await.expect("tx");
    let err = pusher
        .push_batch(&mut tx, &items)
        .await
        .expect_err("must fail");
    match err {
        PushError::BatchPayloadTooLarge { total_bytes, max } => {
            assert!(total_bytes > max, "total_bytes={total_bytes}, max={max}");
            assert_eq!(max, pg_work_queue::limits::MAX_BATCH_BYTES);
        }
        e => panic!("expected BatchPayloadTooLarge, got {e:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pusher_new_empty_queue_is_infallible_push_returns_queue_name_invalid() {
    let (pool, _c) = common::pg18_pool().await;
    // Per PLAN.md / API sketch: Pusher::new infallible; queue validation on push.
    let pusher = Pusher::new(""); // does NOT panic
    let mut tx = pool.begin().await.expect("tx");
    let err = pusher
        .push(&mut tx, &42u32, None)
        .await
        .expect_err("must fail");
    match err {
        PushError::QueueNameInvalid(name) => assert_eq!(name, ""),
        e => panic!("expected QueueNameInvalid, got {e:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_name_over_max_len_returns_queue_name_invalid_on_push() {
    let (pool, _c) = common::pg18_pool().await;
    let too_long = "x".repeat(MAX_QUEUE_LEN + 1);
    let pusher = Pusher::new(&too_long);
    let mut tx = pool.begin().await.expect("tx");
    let err = pusher
        .push(&mut tx, &42u32, None)
        .await
        .expect_err("must fail");
    match err {
        PushError::QueueNameInvalid(name) => assert_eq!(name.len(), MAX_QUEUE_LEN + 1),
        e => panic!("expected QueueNameInvalid, got {e:?}"),
    }
}
