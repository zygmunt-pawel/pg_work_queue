#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Pure unit tests dla `BackoffPolicy::next(attempt)`. Bez DB — quick smoke
//! dla matematyki + jitter band + capped invariant.
//!
//! Pokrycie:
//! 1. Fixed (degenerate Linear): stała wartość niezależna od attempt.
//! 2. Linear: arithmetic progression + cap.
//! 3. Exponential bez jittera: geometric progression + cap.
//! 4. Exponential z jitterem: band assertion + ≤ cap invariant.

use std::time::Duration;

use pg_work_queue::BackoffPolicy;

#[test]
fn fixed_returns_constant_regardless_of_attempt() {
    let p = BackoffPolicy::fixed(Duration::from_secs(2));
    assert_eq!(p.next(0), Duration::from_secs(2));
    assert_eq!(p.next(1), Duration::from_secs(2));
    assert_eq!(p.next(5), Duration::from_secs(2));
    assert_eq!(p.next(1_000), Duration::from_secs(2));
}

#[test]
fn linear_arithmetic_progression_and_cap() {
    // base=1s, increment=1s, cap=10s
    let p = BackoffPolicy::linear(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(10),
    );
    assert_eq!(p.next(0), Duration::from_secs(1));
    assert_eq!(p.next(1), Duration::from_secs(2));
    assert_eq!(p.next(5), Duration::from_secs(6));
    assert_eq!(p.next(9), Duration::from_secs(10));
    // Cap kicks in.
    assert_eq!(p.next(100), Duration::from_secs(10));
    assert_eq!(p.next(u32::MAX), Duration::from_secs(10));
}

#[test]
fn exponential_no_jitter_geometric_progression_and_cap() {
    // base=1s, factor=2.0, cap=30s, jitter=0.0
    let p = BackoffPolicy::exponential(Duration::from_secs(1), 2.0, Duration::from_secs(30), 0.0);
    assert_eq!(p.next(0), Duration::from_secs(1));
    assert_eq!(p.next(1), Duration::from_secs(2));
    assert_eq!(p.next(2), Duration::from_secs(4));
    assert_eq!(p.next(3), Duration::from_secs(8));
    assert_eq!(p.next(4), Duration::from_secs(16));
    // 32s > cap(30s) → clamp.
    assert_eq!(p.next(5), Duration::from_secs(30));
    assert_eq!(p.next(10), Duration::from_secs(30));
}

#[test]
fn exponential_with_jitter_stays_in_band_and_under_cap() {
    // base=1s, factor=2.0, cap=10s, jitter=0.5 (±50%).
    let p = BackoffPolicy::exponential(Duration::from_secs(1), 2.0, Duration::from_secs(10), 0.5);
    // attempt=2 → raw = 4s; jittered ∈ [2s, 6s]. Loop 100× by zobaczyć
    // distribution (deterministic test → assert band, nie konkretna wartość).
    for _ in 0..100 {
        let d = p.next(2);
        assert!(
            d >= Duration::from_secs(2) && d <= Duration::from_secs(6),
            "expected ∈ [2s, 6s], got {d:?}"
        );
    }
    // attempt=10 → raw = 1024s, clamped do 10s; jittered ∈ [5s, 15s] PRE clamp,
    // post-clamp ≤ 10s. Invariant: result ≤ cap (inclusive jitter).
    for _ in 0..100 {
        let d = p.next(10);
        assert!(
            d <= Duration::from_secs(10),
            "result must be ≤ cap; got {d:?}"
        );
        // Min: 5s (= 10s × (1 - 0.5)). Floor floor jitter ratio applied to
        // clamped value.
        assert!(
            d >= Duration::from_secs(5),
            "result must be ≥ 5s (cap × (1-jitter)); got {d:?}"
        );
    }
}

#[test]
fn exponential_default_constants() {
    // Default: Exponential { 1s, 2.0, 5min, 0.2 }. Sprawdza tylko że default
    // istnieje i ma sensowne wartości pierwszego retry'a (~1s ±20% = [0.8s, 1.2s]).
    let p = BackoffPolicy::default();
    for _ in 0..50 {
        let d = p.next(0);
        assert!(
            d >= Duration::from_millis(800) && d <= Duration::from_millis(1_200),
            "first retry must be ≈ 1s ±20%; got {d:?}"
        );
    }
}

#[test]
fn cap_invariant_holds_for_all_attempts() {
    let p = BackoffPolicy::exponential(Duration::from_secs(1), 2.0, Duration::from_secs(60), 0.3);
    let cap = Duration::from_secs(60);
    for attempt in [0_u32, 1, 5, 20, 100, 10_000] {
        for _ in 0..10 {
            let d = p.next(attempt);
            assert!(d <= cap, "attempt={attempt}: d={d:?} > cap={cap:?}");
        }
    }
}
