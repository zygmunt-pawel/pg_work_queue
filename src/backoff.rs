//! Retry backoff policy — `Linear`, `Exponential` z jitter.
//!
//! Single source of truth: PLAN.md §"Retry backoff policy" (lines 1273-1336).
//!
//! Two variants: `Linear { base, increment, cap }` i `Exponential { base,
//! factor, cap, jitter }`. `BackoffPolicy::fixed(d)` jest convenience-form
//! degenerate `Linear { base: d, increment: 0, cap: d }`. Default:
//! `Exponential { 1s, 2.0, 5min, 0.2 }`.
//!
//! ## Overflow safety (v3.5 regression #4)
//!
//! `factor.powi(attempt as i32)` może overflow'ować do `f64::INFINITY`
//! przy dużych `attempt` (np. `factor=10, attempt=400`). Konwersja
//! `Duration::from_secs_f64(INFINITY)` PANICUJE w stdlib. Library
//! sprawdza `is_finite()` i clamp'uje do `cap` przed jitterem; jitter
//! też jest re-clamped (jitter ratio ≤ 1.0 nie wyjdzie poza 2× cap, ale
//! defensywnie clamp'ujemy ponownie). `Duration::from_secs_f64(0.0)` jest
//! safe; `NaN`/negative są też re-routed do `cap` przez `is_finite` check.
//!
//! ## Jitter
//!
//! Ważny przy thundering herd (10 jobs failuje równocześnie → bez jittera
//! wszystkie wracają w tym samym ticku → DB hammering). Per PLAN.md
//! `Exponential.jitter` jest ratio `[0.0, 1.0]` — wartość docelową
//! mnożymy przez `1 + uniform(-jitter, +jitter)` (a w `Linear` jitter
//! nie istnieje — degenerate-fixed semantics użytkownika).

use std::time::Duration;

use rand::RngExt;

use crate::error::BuildError;

/// Hard lower clamp for `base` w `validate()`. Below this any `factor>1.0` daje
/// pierwsze attempt'y poniżej `MIN_POLL_INTERVAL` → DB hammering w tight
/// retry loops.
const MIN_BACKOFF_BASE: Duration = Duration::from_millis(100);

/// Hard upper clamp for `cap` w `validate()` (24h). Większy cap to footgun —
/// retry "za rok" zwykle jest bugiem (lepszy `JobError::abort`).
const MAX_BACKOFF_CAP: Duration = Duration::from_secs(24 * 3600);

/// Retry backoff policy used by the worker when a `JobError::Retry` carries
/// no per-call `retry_in` override.
///
/// Per-call `JobError::retry_in(...)` ma priorytet (z osobnym clampem
/// `[max(poll_interval, 100ms), 24h]`).
///
/// # Examples
///
/// ```ignore
/// // Fixed 2s delay between retries.
/// let fixed = pg_work_queue::BackoffPolicy::fixed(std::time::Duration::from_secs(2));
///
/// // Linear: 1s, 2s, 3s, ..., capped at 30s.
/// let linear = pg_work_queue::BackoffPolicy::linear(
///     std::time::Duration::from_secs(1),
///     std::time::Duration::from_secs(1),
///     std::time::Duration::from_secs(30),
/// );
///
/// // Exponential: 1s, 2s, 4s, 8s, ..., capped at 5min, ±20% jitter.
/// let exp = pg_work_queue::BackoffPolicy::exponential(
///     std::time::Duration::from_secs(1),
///     2.0,
///     std::time::Duration::from_secs(300),
///     0.2,
/// );
/// ```
#[derive(Debug, Clone, Copy)]
pub enum BackoffPolicy {
    /// Linear progression: `next(n) = base + increment × n`, capped at `cap`.
    /// `fixed(d)` builds `Linear { base: d, increment: 0, cap: d }`.
    Linear {
        /// First retry delay.
        base: Duration,
        /// Per-attempt step increase.
        increment: Duration,
        /// Hard ceiling (clamp).
        cap: Duration,
    },
    /// Exponential progression: `next(n) = base × factor.powi(n)`, capped at
    /// `cap`. `factor` musi być > 1.0. `jitter ∈ [0.0, 1.0]` to ratio — wynik
    /// mnożony przez `1 + uniform(-jitter, +jitter)` po clampie.
    Exponential {
        /// First retry delay.
        base: Duration,
        /// Geometric factor (must be > 1.0).
        factor: f64,
        /// Hard ceiling (clamp).
        cap: Duration,
        /// Jitter ratio `[0.0, 1.0]`.
        jitter: f64,
    },
}

impl Default for BackoffPolicy {
    /// Library default — `Exponential { 1s, 2.0, 5min, 0.2 }`.
    /// Sequence: ~1s, 2s, 4s, 8s, ... 5min (±20% jitter).
    fn default() -> Self {
        Self::Exponential {
            base: Duration::from_secs(1),
            factor: 2.0,
            cap: Duration::from_secs(5 * 60),
            jitter: 0.2,
        }
    }
}

impl BackoffPolicy {
    /// Construct an `Exponential` policy with the supplied parameters.
    /// The builder validates parameters internally on `build()`.
    #[must_use]
    pub const fn exponential(base: Duration, factor: f64, cap: Duration, jitter: f64) -> Self {
        Self::Exponential {
            base,
            factor,
            cap,
            jitter,
        }
    }

    /// Construct a `Linear` policy.
    #[must_use]
    pub const fn linear(base: Duration, increment: Duration, cap: Duration) -> Self {
        Self::Linear {
            base,
            increment,
            cap,
        }
    }

    /// Construct a fixed-delay policy: every retry waits exactly `d`. Implemented
    /// as `Linear { base: d, increment: 0, cap: d }` so there is no separate
    /// `Fixed` variant.
    #[must_use]
    pub const fn fixed(d: Duration) -> Self {
        Self::Linear {
            base: d,
            increment: Duration::ZERO,
            cap: d,
        }
    }

    /// Cap of the policy (used by callers to clamp `retry_in` overrides).
    #[must_use]
    pub(crate) const fn cap(&self) -> Duration {
        match *self {
            Self::Linear { cap, .. } | Self::Exponential { cap, .. } => cap,
        }
    }

    /// Compute the retry delay for the given (1-indexed) `attempt` count.
    ///
    /// **Never panics** — internal `f64` math is overflow-safe via
    /// `is_finite()` + clamp-to-`cap`. See v3.5 regression #4.
    #[must_use]
    pub fn next(&self, attempt: u32) -> Duration {
        let cap_secs = self.cap().as_secs_f64();
        let raw_secs = match *self {
            Self::Linear {
                base, increment, ..
            } => {
                // `increment.as_secs_f64() * attempt as f64` może też być
                // INFINITY przy gigantycznym increment + max attempt — przejmie
                // to is_finite() guard poniżej. `mul_add` to clippy::suboptimal_flops
                // rekomendacja (jeden zaokr. zamiast dwóch).
                increment
                    .as_secs_f64()
                    .mul_add(f64::from(attempt), base.as_secs_f64())
            }
            Self::Exponential { base, factor, .. } => {
                // `i32::try_from(u32)` może fail dla u32 > i32::MAX. Wtedy
                // sygnalizujemy "saturated" przez f64::INFINITY → wpadnie
                // w `is_finite` else-branch i da `cap_secs`.
                let exp = i32::try_from(attempt).unwrap_or(i32::MAX);
                base.as_secs_f64() * factor.powi(exp)
            }
        };
        // is_finite() filters NaN i ±INF. Negative się też nie zdarzy
        // (base ≥ 0, factor > 1.0), ale defensywnie .max(0.0).
        let clamped_secs = if raw_secs.is_finite() {
            raw_secs.min(cap_secs).max(0.0)
        } else {
            cap_secs
        };
        let jittered = self.apply_jitter(clamped_secs);
        // Re-clamp po jitterze: jitter ratio ≤ 1.0 nie wyjdzie poza 2× cap,
        // ale formally `min(cap_secs)`.
        let final_secs = jittered.min(cap_secs).max(0.0);
        // `Duration::from_secs_f64` panic'uje na NaN/INF/negative. Wszystkie
        // trzy wykluczone przez powyższy clamp + is_finite branch (cap_secs
        // pochodzi z Duration więc jest finite i ≥ 0).
        Duration::from_secs_f64(final_secs)
    }

    /// Apply jitter ratio to a clamped seconds value. `Linear` doesn't carry
    /// jitter — passthrough. `Exponential` mnoży przez `1 + uniform(-j, +j)`.
    fn apply_jitter(&self, secs: f64) -> f64 {
        match *self {
            Self::Linear { .. } => secs,
            Self::Exponential { jitter, .. } => {
                if jitter <= 0.0 {
                    return secs;
                }
                // rand 0.10: `rng()` zwraca ThreadRng; `random_range`
                // pochodzi z `Rng` trait.
                let j = jitter.clamp(0.0, 1.0);
                let mut rng = rand::rng();
                let delta: f64 = rng.random_range(-j..=j);
                secs * (1.0 + delta)
            }
        }
    }

    /// Validate policy parameters. Called from `WorkerBuilder::build`. See
    /// `BuildError::BackoffInvalid` for the surfaced reason strings.
    ///
    /// # Errors
    /// `BuildError::BackoffInvalid { reason }` when one of:
    /// - `factor` (Exponential) is not `> 1.0` or not finite.
    /// - `jitter` (Exponential) not in `[0.0, 1.0]` or not finite.
    /// - `cap == Duration::ZERO`.
    /// - `cap > 24h`.
    /// - `base < 100ms`.
    pub(crate) fn validate(&self) -> Result<(), BuildError> {
        let (base, cap) = match *self {
            Self::Linear { base, cap, .. } => (base, cap),
            Self::Exponential {
                base,
                factor,
                cap,
                jitter,
            } => {
                if !factor.is_finite() || factor <= 1.0 {
                    return Err(BuildError::BackoffInvalid {
                        reason: "Exponential.factor must be finite and > 1.0",
                    });
                }
                if !jitter.is_finite() || !(0.0..=1.0).contains(&jitter) {
                    return Err(BuildError::BackoffInvalid {
                        reason: "Exponential.jitter must be in [0.0, 1.0]",
                    });
                }
                (base, cap)
            }
        };
        if cap == Duration::ZERO {
            return Err(BuildError::BackoffInvalid {
                reason: "cap must be > 0",
            });
        }
        if cap > MAX_BACKOFF_CAP {
            return Err(BuildError::BackoffInvalid {
                reason: "cap must be <= 24h",
            });
        }
        if base < MIN_BACKOFF_BASE {
            return Err(BuildError::BackoffInvalid {
                reason: "base must be >= 100ms",
            });
        }
        Ok(())
    }
}

/// Panic policy: decides what library does when a handler `panic!`s.
///
/// Default `Retry` matches the at-least-once contract — bug in user code
/// shouldn't take a row hostage; `mark_retry` lets the next worker (or
/// next deploy) try again with the same `idempotency_key`.
///
/// Use `Dead` when handlers are pure code review'd flows and any panic
/// indicates an unrecoverable issue (e.g. logic bug w finalnym pre-prod
/// pipeline) — bypass retry budget, surface w `dead` queue immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PanicPolicy {
    /// Treat panic as transient — synthesize `JobError::Retry` with the
    /// backoff policy delay. Default.
    #[default]
    Retry,
    /// Treat panic as fatal — `mark_dead` with `reason = "panic: <msg>"`.
    Dead,
}
