//! Public error enums for the push side, handler side and builder side.
//!
//! See PLAN.md §"Error semantics & handling". `PushError` distinguishes
//! caller-bug variants (deterministic — fix the input, don't loop) from
//! transient infrastructure failures (`is_retriable() == true`).
//!
//! `JobError` is the handler-side error type; `BuildError` is returned by
//! `WorkerBuilder::build()` for invalid configurations.

use std::time::Duration;

use thiserror::Error;

/// Boxed `std::error::Error` used to wrap codec failures without leaking
/// the concrete codec error type through `PushError`.
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors produced by `Pusher::push*` family. Mix of caller-bug (deterministic)
/// and transient DB faults. Use [`PushError::is_retriable`] to decide whether
/// a retry loop is justified.
#[derive(Error, Debug)]
pub enum PushError {
    /// One payload exceeds `limits::MAX_PAYLOAD_BYTES`. `index` is the
    /// position within a batch (0 for single push).
    #[error("payload too large at index {index}: {size} bytes > {max}")]
    PayloadTooLarge {
        /// Index within the batch (0 for `push`/`push_at`).
        index: usize,
        /// Encoded size in bytes.
        size: usize,
        /// Configured `MAX_PAYLOAD_BYTES`.
        max: usize,
    },

    /// `push_batch` was called with more than `MAX_BATCH_SIZE` items.
    #[error("batch too large: {size} > {max}")]
    BatchTooLarge {
        /// Number of items supplied.
        size: usize,
        /// Configured `MAX_BATCH_SIZE`.
        max: usize,
    },

    /// Sum of encoded payload bytes exceeds `MAX_BATCH_BYTES`.
    /// Short-circuits encode loop so we don't accumulate a multi-GB
    /// transient buffer before failing.
    #[error("batch aggregate payload exceeds {max} bytes (got {total_bytes})")]
    BatchPayloadTooLarge {
        /// Encoded bytes accumulated when the limit tripped.
        total_bytes: usize,
        /// Configured `MAX_BATCH_BYTES`.
        max: usize,
    },

    /// `push_batch` was called with an empty slice.
    #[error("batch is empty")]
    BatchEmpty,

    /// Queue name violates `MAX_QUEUE_LEN` or is empty.
    #[error("queue name invalid: {0:?}")]
    QueueNameInvalid(String),

    /// Codec failed on a single `push`/`push_at`.
    #[error("codec error: {0}")]
    Codec(#[source] BoxError),

    /// Codec failed on item `index` within `push_batch`.
    #[error("codec error at batch index {index}: {source}")]
    BatchCodec {
        /// Index of the failing item.
        index: usize,
        /// Underlying codec error.
        #[source]
        source: BoxError,
    },

    /// Deterministic DB error (SQLSTATE class 23 — integrity constraint
    /// violation). Caller bug; retrying without changing the input is futile.
    #[error("database constraint violation: {0}")]
    Constraint(#[source] sqlx::Error),

    /// Transient DB error (connection, IO, pool). Caller may retry.
    #[error("database error (transient): {0}")]
    Transient(#[source] sqlx::Error),

    /// Defense-in-depth: `rows_affected` mismatched the expected count.
    /// Under current single-statement INSERT...SELECT...unnest, CHECK
    /// violations roll back the whole statement, so this is unreachable
    /// in v0.1 — reserved for future `ON CONFLICT DO NOTHING` push-side
    /// dedup paths.
    #[error("batch partial: inserted {inserted} of {expected} expected rows")]
    BatchPartial {
        /// Rows actually persisted.
        inserted: usize,
        /// Rows we attempted to insert.
        expected: usize,
    },
}

impl PushError {
    /// `true` iff calling code may retry without changing the input — i.e.
    /// the failure is transient infrastructure noise, not a caller bug.
    #[must_use]
    pub const fn is_retriable(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

impl From<sqlx::Error> for PushError {
    fn from(e: sqlx::Error) -> Self {
        // Classify SQLSTATE class 23 (integrity constraint violation) as
        // deterministic. Everything else is treated as transient — the
        // worker-side classifier in later phases distinguishes finer-grained
        // fatal variants; for the push side, transient-or-constraint is the
        // axis users care about.
        if let sqlx::Error::Database(db) = &e {
            if db.code().as_deref().is_some_and(|c| c.starts_with("23")) {
                return Self::Constraint(e);
            }
        }
        Self::Transient(e)
    }
}

/// Handler-side error: signals retry-vs-abort intent for the library.
///
/// See PLAN.md §"Handler signature & error semantics" (lines 1442-1478).
/// Use [`JobError::retry`], [`JobError::retry_in`], [`JobError::abort`]
/// ctor helpers. `reason` strings are truncated library-side via
/// `fmt_err_trimmed` before being persisted to `last_error`.
#[derive(Error, Debug)]
pub enum JobError {
    /// Job miał transient issue — library decyduje retry-vs-dead na
    /// bazie `attempts < max_attempts`. Optional `retry_in` override
    /// backoff policy dla tego konkretnego retry. W Fazie 3 backoff
    /// jest stałą (`default_retry_delay`); per-job `retry_in` ma priorytet.
    #[error("retry: {reason}")]
    Retry {
        /// Operator-visible category for the retry (do NOT include PII).
        reason: String,
        /// Optional override of the per-builder default retry delay.
        retry_in: Option<Duration>,
    },
    /// Job permanently can't proceed — `mark_dead` direct (bypass retry
    /// budget). Użyj gdy retry nigdy nie pomoże (invalid input,
    /// permissions denied, etc.).
    #[error("abort: {reason}")]
    Abort {
        /// Operator-visible category for the abort (do NOT include PII).
        reason: String,
    },
}

impl JobError {
    /// Construct a `Retry` variant with no explicit delay; library applies
    /// `default_retry_delay` from the worker builder.
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry {
            reason: reason.into(),
            retry_in: None,
        }
    }

    /// Construct a `Retry` variant with an explicit delay override.
    pub fn retry_in(reason: impl Into<String>, retry_in: Duration) -> Self {
        Self::Retry {
            reason: reason.into(),
            retry_in: Some(retry_in),
        }
    }

    /// Construct an `Abort` variant — bypass retry budget, mark dead directly.
    pub fn abort(reason: impl Into<String>) -> Self {
        Self::Abort {
            reason: reason.into(),
        }
    }
}

/// Errors produced by `WorkerBuilder::build()` for invalid configuration.
///
/// In Faza 3 only the variants relevant to Phase-3 knobs are implemented.
/// Variants for `poll_interval`, `concurrency`, `handler_timeout`,
/// `mark_timeout`, `reaper_interval` land in Fazach 4-7 along with the
/// corresponding builder knobs (PLAN.md lines 1395-1425).
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BuildError {
    /// `max_attempts == 0` — no attempt would ever fire.
    #[error("max_attempts must be >= 1")]
    MaxAttemptsZero,

    /// `lease_timeout < 1s` — too tight to safely commit `mark_*` before
    /// the reaper claws back the row.
    #[error("lease_timeout must be >= 1s (MIN_LEASE_TIMEOUT)")]
    LeaseTimeoutBelowFloor,

    /// Queue name empty or exceeds `limits::MAX_QUEUE_LEN`.
    #[error("queue name invalid: {0:?}")]
    QueueNameInvalid(String),

    /// `.handler()` was not called before `.build()`.
    #[error("handler not set")]
    HandlerMissing,

    /// `batch_size` outside the supported range `1..=1000`.
    #[error("batch_size {actual} out of range [{min}, {max}]")]
    BatchSizeOutOfRange {
        /// Configured value that failed validation.
        actual: u32,
        /// Inclusive minimum.
        min: u32,
        /// Inclusive maximum.
        max: u32,
    },
}
