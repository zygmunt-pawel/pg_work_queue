#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! `fmt_err_trimmed` — UTF-8 char-safe truncate na granicy `MAX_LAST_ERROR_LEN` chars.
//!
//! Critical: cięcie po `chars().count()`, nie po bajtach — Postgres `length(TEXT)`
//! liczy chars; DB CHECK to backstop. Truncacja mid-codepoint == panic w runtime,
//! czego `panic = "deny"` w lints nie łapie. Test guard: każdy output zawsze
//! valid UTF-8 (trivially true dla `String`) + `chars().count() <= MAX_LAST_ERROR_LEN`.

use pg_work_queue::__test_exports::fmt_err_trimmed;
use pg_work_queue::limits::MAX_LAST_ERROR_LEN;

#[derive(Debug)]
struct StrErr(String);

impl std::fmt::Display for StrErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StrErr {}

fn err(s: impl Into<String>) -> StrErr {
    StrErr(s.into())
}

#[test]
fn ascii_longer_than_limit_truncated_to_exactly_max_chars() {
    let long = "a".repeat(MAX_LAST_ERROR_LEN + 1000);
    let e = err(long);
    let out = fmt_err_trimmed(&e);
    assert_eq!(out.chars().count(), MAX_LAST_ERROR_LEN);
}

#[test]
fn polish_diacritics_truncated_to_max_chars_not_bytes() {
    // "żółć" — każdy char to 2 bajty w UTF-8. 4 chars × 5000 = 20_000 chars,
    // 40_000 bajtów. Limit jest na chars, więc output ma dokładnie MAX_LAST_ERROR_LEN
    // chars (8192), nie ~8192 bajtów (które dałoby ~4000 chars).
    let long = "żółć".repeat(5000);
    let e = err(long);
    let out = fmt_err_trimmed(&e);
    assert_eq!(out.chars().count(), MAX_LAST_ERROR_LEN);
    // UTF-8 safety: nie cięte mid-codepoint.
    assert!(std::str::from_utf8(out.as_bytes()).is_ok());
}

#[test]
fn emoji_truncated_to_max_chars() {
    // Każdy crab to 4 bajty. 3 chars × 5000 = 15_000 chars, 60_000 bajtów.
    let long = "🦀🦀🦀".repeat(5000);
    let e = err(long);
    let out = fmt_err_trimmed(&e);
    assert_eq!(out.chars().count(), MAX_LAST_ERROR_LEN);
    assert!(std::str::from_utf8(out.as_bytes()).is_ok());
}

#[test]
fn short_string_returned_unchanged() {
    let s = "kept as-is";
    let e = err(s);
    let out = fmt_err_trimmed(&e);
    assert_eq!(out, s);
}

#[test]
fn never_lands_mid_codepoint_property() {
    // Sweep różnych szerokości i offset'ów wokół granicy.
    for n in [
        MAX_LAST_ERROR_LEN - 1,
        MAX_LAST_ERROR_LEN,
        MAX_LAST_ERROR_LEN + 1,
        MAX_LAST_ERROR_LEN * 2,
    ] {
        let long = "🦀ą".repeat(n);
        let e = err(long);
        let out = fmt_err_trimmed(&e);
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
        assert!(out.chars().count() <= MAX_LAST_ERROR_LEN);
    }
}
