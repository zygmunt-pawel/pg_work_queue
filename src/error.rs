//! Public error enums for the push side.
//!
//! See PLAN.md §"Error semantics & handling". `PushError` distinguishes
//! caller-bug variants (deterministic — fix the input, don't loop) from
//! transient infrastructure failures (`is_retriable() == true`).

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
