#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Codec — round-trip JSON encode/decode na różnych typach. No-DB.
//!
//! Sprawdza:
//! - JsonCodec.encode -> decode zwraca tę samą wartość (struct, prymitywy, Vec, opcje).
//! - Implementacja Codec dla JsonCodec używa serde_json (bytes parsable jako JSON).

use pg_work_queue::{Codec, JsonCodec};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Email {
    to: String,
    body: String,
    priority: u32,
}

#[test]
fn json_codec_roundtrip_struct() {
    let codec = JsonCodec;
    let value = Email {
        to: "a@b".into(),
        body: "hello".into(),
        priority: 3,
    };
    let bytes = codec.encode(&value).expect("encode ok");
    let back: Email = codec.decode(&bytes).expect("decode ok");
    assert_eq!(value, back);
}

#[test]
fn json_codec_roundtrip_primitives() {
    let codec = JsonCodec;
    let n: i64 = 12345;
    let bytes = codec.encode(&n).expect("encode i64");
    let back: i64 = codec.decode(&bytes).expect("decode i64");
    assert_eq!(n, back);

    let s = String::from("zażółć gęślą jaźń");
    let bytes = codec.encode(&s).expect("encode str");
    let back: String = codec.decode(&bytes).expect("decode str");
    assert_eq!(s, back);
}

#[test]
fn json_codec_roundtrip_vec_and_option() {
    let codec = JsonCodec;
    let v: Vec<Option<u32>> = vec![Some(1), None, Some(3)];
    let bytes = codec.encode(&v).expect("encode vec");
    let back: Vec<Option<u32>> = codec.decode(&bytes).expect("decode vec");
    assert_eq!(v, back);
}

#[test]
fn json_codec_emits_valid_json_bytes() {
    let codec = JsonCodec;
    let value = Email {
        to: "x".into(),
        body: "y".into(),
        priority: 1,
    };
    let bytes = codec.encode(&value).expect("encode");
    // JSON-readable: re-parse jako serde_json::Value.
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
    assert_eq!(v["to"], "x");
    assert_eq!(v["priority"], 1);
}
