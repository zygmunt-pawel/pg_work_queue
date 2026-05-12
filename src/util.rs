//! Internal utilities. `pub(crate)` only — public re-export under
//! `pg_work_queue::__test_exports` is `#[doc(hidden)]` for test access.

use crate::limits::MAX_LAST_ERROR_LEN;

/// Format `e.to_string()` and truncate to at most `MAX_LAST_ERROR_LEN` *chars*
/// (not bytes). Centralizes the truncation used by mark_retry / mark_dead /
/// codec-decode-error / panic-message extraction in later phases.
///
/// Postgres `length(TEXT)` counts characters; DB CHECK is the backstop.
/// `chars().take(N)` walks UTF-8 boundaries — panic-safe by construction.
#[must_use]
#[doc(hidden)]
pub fn fmt_err_trimmed(e: &dyn std::error::Error) -> String {
    let s = e.to_string();
    // Avoid materializing a new String when the input already fits.
    // `chars().nth(N)` short-circuits at index N+1.
    if s.chars().nth(MAX_LAST_ERROR_LEN).is_none() {
        return s;
    }
    s.chars().take(MAX_LAST_ERROR_LEN).collect()
}
