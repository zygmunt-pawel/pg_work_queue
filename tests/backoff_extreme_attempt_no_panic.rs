#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! v3.5 regresja #4: extreme `attempt` values do NOT panic.
//!
//! `factor.powi(attempt as i32)` przy factor=10, attempt=400 daje f64::INFINITY;
//! konwersja `Duration::from_secs_f64(INFINITY)` panicuje w stdlib. Library musi
//! clamp'ować przed konwersją.
//!
//! PLAN.md §"Retry backoff policy" lines 1293-1318; tests/backoff_extreme_attempt_no_panic
//! line 1934-1936.

use std::time::Duration;

use pg_work_queue::BackoffPolicy;

#[test]
fn next_400_with_factor_10_returns_cap_no_panic() {
    let p = BackoffPolicy::exponential(
        Duration::from_secs(1),
        10.0,
        Duration::from_secs(24 * 3600),
        0.0,
    );
    let d = p.next(400);
    assert_eq!(
        d,
        Duration::from_secs(24 * 3600),
        "extreme attempt must clamp to cap, not panic"
    );
}

#[test]
fn next_u32_max_returns_cap_no_panic() {
    let p = BackoffPolicy::exponential(
        Duration::from_secs(1),
        10.0,
        Duration::from_secs(24 * 3600),
        0.0,
    );
    let d = p.next(u32::MAX);
    assert_eq!(
        d,
        Duration::from_secs(24 * 3600),
        "u32::MAX attempt must clamp to cap, not panic"
    );
}

#[test]
fn next_extreme_with_jitter_no_panic_still_capped() {
    let p = BackoffPolicy::exponential(
        Duration::from_secs(1),
        10.0,
        Duration::from_secs(24 * 3600),
        0.2,
    );
    // Jitter ratio ≤ 1.0 → post-jitter ≤ 2× cap; library re-clamps po jitterze.
    let d = p.next(u32::MAX);
    assert!(
        d <= Duration::from_secs(24 * 3600),
        "must not exceed cap even after jitter; got {d:?}"
    );
}
