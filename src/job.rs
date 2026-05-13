//! `Job<T>` (decoded claimed row) and `JobContext` (handler argument).
//!
//! See PLAN.md §"`JobContext` (handler argument)" and §"Faza 2".
//!
//! `JobContext` is constructed by `Worker` from a `Job<T>` before invoking
//! the user handler — see Faza 3+. In Faza 2 only the type defs land;
//! `Worker` is still a stub.

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A claimed row, payload already decoded into `T` by `claim_and_decode`.
///
/// Internal pipeline element passed from claim/decode → handler invocation.
/// `payload` is owned (decoded `T`), not bytes — codec ran successfully.
#[derive(Debug, Clone)]
pub struct Job<T> {
    /// Internal BIGINT PK; used by `mark_*` SQL and reaper.
    pub id: i64,
    /// External job handle (uuidv7) — returned from `Pusher::push`.
    pub public_id: Uuid,
    /// Source queue name (matches `Pusher`'s queue).
    pub queue: String,
    /// Decoded payload. `claim_and_decode` produced this from `payload BYTEA`
    /// via the configured `Codec`.
    pub payload: T,
    /// 1-indexed current attempt. `claim_batch` did `attempts = attempts + 1`,
    /// so a freshly claimed row has `attempts = 1`.
    pub attempts: u32,
    /// Per-row `max_attempts` stamped by the *claiming* worker (rolling-deploy
    /// safe; see schema comment on `max_attempts`).
    pub max_attempts: u32,
    /// Timestamp of the very first attempt (COALESCE'd at claim time);
    /// stable across retries.
    pub first_attempted_at: DateTime<Utc>,
    /// Fencing token for this claim — required by all `mark_*` WHERE clauses.
    pub lease_token: Uuid,
    /// Per-row lease deadline (`now() + lease_timeout` at claim time).
    pub lease_expires_at: DateTime<Utc>,
}

/// Handler argument — read-only view of the claimed row's metadata.
///
/// `public_id == idempotency_key`. The two field names are aliases for
/// readability; choose whichever fits the call site:
/// * `public_id` matches the DB column name and the `Pusher::push` return.
/// * `idempotency_key` matches the handler intent at external-API call sites
///   (e.g. `stripe.charge(amount, &ctx.idempotency_key.to_string())`).
///
/// The struct is `#[non_exhaustive]`: future additive fields are not a
/// breaking change. Handlers should access fields by name (`ctx.attempt`)
/// rather than destructuring exhaustively.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobContext {
    /// Internal BIGINT PK.
    pub id: i64,
    /// External job handle (uuidv7); same value as `idempotency_key`.
    pub public_id: Uuid,
    /// Same value as `public_id` — stable across retries, suitable for
    /// `Idempotency-Key` headers on external API calls.
    pub idempotency_key: Uuid,
    /// Source queue name.
    pub queue: String,
    /// 1-indexed current attempt.
    pub attempt: u32,
    /// Per-row `max_attempts` stamped by the *claiming* worker. Compare with
    /// `attempt` to branch handler-side on remaining retry budget — e.g.
    /// warn-and-alert on `attempt == max_attempts`, or run a once-per-job
    /// DLQ side-effect on the final retry. Rolling-deploy safe: the value
    /// is whatever the claiming worker stamped at claim time, *not* the
    /// current builder's `.max_attempts(...)` (see schema docs on
    /// `max_attempts`).
    pub max_attempts: u32,
    /// Timestamp of the very first attempt.
    pub first_attempted_at: DateTime<Utc>,
    /// Fencing token for the current claim.
    pub lease_token: Uuid,
}

impl<T> Job<T> {
    /// Build a `JobContext` from this job for handler invocation.
    ///
    /// Performs a shallow clone of `queue` (small string) and copies all
    /// `Copy` scalar fields. The decoded `payload` is consumed separately
    /// by the handler — not part of `JobContext`.
    #[must_use]
    pub fn context(&self) -> JobContext {
        JobContext {
            id: self.id,
            public_id: self.public_id,
            idempotency_key: self.public_id,
            queue: self.queue.clone(),
            attempt: self.attempts,
            max_attempts: self.max_attempts,
            first_attempted_at: self.first_attempted_at,
            lease_token: self.lease_token,
        }
    }
}
