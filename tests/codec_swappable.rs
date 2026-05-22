#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Codec is swappable — push przez `Pusher::with_codec(custom)` musi użyć
//! podanego codec'a, nie default JsonCodec. Weryfikujemy raw `payload` z DB:
//! bytes muszą zaczynać się od custom prefix "PWQ1:" (dowód że JsonCodec NIE
//! został wywołany).

mod common;

use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::Row;

use pg_work_queue::{Codec, Pusher};

#[derive(Debug)]
struct CodecError(String);
impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for CodecError {}

/// Minimal "custom" codec — prefixes the JSON bytes z "PWQ1:" tag.
/// Nie potrzeba importować bincode / cbor — wystarczy że to NIE jest default
/// JSON bytes (dowodzi, że Pusher użył podanego codec'a).
#[derive(Debug, Clone, Copy)]
struct PrefixedJsonCodec;

const PREFIX: &[u8] = b"PWQ1:";

impl Codec for PrefixedJsonCodec {
    type Error = CodecError;
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Self::Error> {
        let inner = serde_json::to_vec(value).map_err(|e| CodecError(e.to_string()))?;
        let mut out = Vec::with_capacity(PREFIX.len() + inner.len());
        out.extend_from_slice(PREFIX);
        out.extend_from_slice(&inner);
        Ok(out)
    }
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
        let rest = bytes
            .strip_prefix(PREFIX)
            .ok_or_else(|| CodecError("missing PWQ1 prefix".into()))?;
        serde_json::from_slice(rest).map_err(|e| CodecError(e.to_string()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pusher_with_custom_codec_writes_custom_bytes_to_db() {
    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("custom_q").with_codec(PrefixedJsonCodec);
    let mut tx = pool.begin().await.expect("tx");
    let id = pusher
        .push(&mut tx, &"hello world", None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let row = sqlx::query("SELECT payload FROM pgwq.jobs WHERE public_id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("fetch");
    let bytes: Vec<u8> = row.try_get("payload").expect("payload");
    assert!(
        bytes.starts_with(PREFIX),
        "payload must use custom codec; got bytes starting with: {:?}",
        &bytes[..PREFIX.len().min(bytes.len())]
    );
    // Sanity decode.
    let decoded = PrefixedJsonCodec.decode::<String>(&bytes).expect("decode");
    assert_eq!(decoded, "hello world");
}
