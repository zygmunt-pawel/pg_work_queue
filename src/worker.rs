//! Worker — single-shot `tick_once` (Faza 3). Poll loop, reaper, retention
//! sweeper land w Fazach 4+.
//!
//! `Worker<T, C>` is generic over the payload type and the codec. The
//! builder uses the type-state pattern (`H` for handler) so a missing
//! handler is caught at runtime via `BuildError::HandlerMissing` rather
//! than at type level — the latter would force users to track type
//! parameters across construction points.
//!
//! `tick_once` is a single-shot: `claim_batch` → run handlers **sequential**
//! → `mark_done` / `mark_retry` / `mark_dead` z fencing token w WHERE. No
//! poll loop, no cancellation token, no spawn. Handler panic w Fazie 3
//! propaguje up — explicit doc note. Faza 4 dodaje `JoinSet` z
//! `handler_timeout` i panic policy.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde::de::DeserializeOwned;
use sqlx::PgPool;

use crate::codec::{Codec, JsonCodec};
use crate::error::{BuildError, JobError};
use crate::job::JobContext;
use crate::limits::MAX_QUEUE_LEN;
use crate::mark::{mark_dead, mark_done, mark_retry};
use crate::util::fmt_err_trimmed;

/// Minimum `lease_timeout`. Below this the handler cannot reliably finish
/// trivial work + `mark_*` commit before the reaper claws back the row.
const MIN_LEASE_TIMEOUT: Duration = Duration::from_secs(1);

/// Default `lease_timeout` when builder doesn't override.
const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default `default_retry_delay` when builder doesn't override.
const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Default `max_attempts` (1-indexed; 3 attempts total).
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Default `batch_size`.
const DEFAULT_BATCH_SIZE: u32 = 32;

/// Hard min/max for `batch_size` validation.
const BATCH_SIZE_MIN: u32 = 1;
const BATCH_SIZE_MAX: u32 = 1000;

/// Outcome counters returned by [`Worker::tick_once`].
///
/// All fields are observable post-state — `claimed` is the # of rows the
/// SQL `claim_batch` actually returned; `completed` / `failed` / `fenced_out`
/// sum to ≤ `claimed` (rows whose `mark_*` itself failed with a transient
/// DB error are NOT counted in any bucket — they remain `running` and the
/// reaper recovers them after lease expiry).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TickStats {
    /// Rows returned by `claim_batch`.
    pub claimed: u64,
    /// Rows whose handler returned `Ok(())` AND `mark_done` flipped 1 row.
    pub completed: u64,
    /// Rows whose handler returned `Err(JobError::*)` AND the corresponding
    /// `mark_*` flipped 1 row. Includes both retry-able and aborted failures.
    pub failed: u64,
    /// Rows whose `mark_*` returned 0 `rows_affected` — lease drifted (reaper
    /// or another worker beat us). Worker continues; row is in a consistent
    /// state managed by the conflicting writer.
    pub fenced_out: u64,
}

/// Handler trait object signature: `Fn(T, JobContext) -> impl Future<Output = Result<(), JobError>>`.
///
/// We boxed the future into `Pin<Box<dyn Future + Send>>` so the handler
/// can be stored in a `dyn` trait object without leaking type parameters
/// into `Worker`.
type BoxedHandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<(), JobError>> + Send + 'a>>;

/// Object-safe handler trait. Public solely so it can appear in the type
/// parameter of `WorkerBuilder<T, C, Arc<dyn JobHandler<T>>>`; users do not
/// implement it directly — any `Fn(T, JobContext) -> impl Future<Output =
/// Result<(), JobError>>` automatically qualifies via the blanket impl
/// below.
#[doc(hidden)]
pub trait JobHandler<T>: Send + Sync {
    /// Invoke the handler, boxing the future for object-safety.
    fn call(&self, payload: T, ctx: JobContext) -> BoxedHandlerFuture<'static>;
}

impl<T, F, Fut> JobHandler<T> for F
where
    F: Fn(T, JobContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), JobError>> + Send + 'static,
{
    fn call(&self, payload: T, ctx: JobContext) -> BoxedHandlerFuture<'static> {
        Box::pin((self)(payload, ctx))
    }
}

/// Builder for [`Worker`]. Use [`Worker::builder()`] to create one.
///
/// The `H` type parameter encodes whether `.handler()` has been called —
/// `()` means "not yet"; `Arc<dyn JobHandler<T>>` means "set". The runtime
/// `HandlerMissing` check is redundant w obecnym type-state, ale zachowane
/// dla future-proof gdy dodamy `.from_env()` / similar non-typed builder
/// entry points.
pub struct WorkerBuilder<T, C = JsonCodec, H = ()> {
    pool: Option<PgPool>,
    queue: Option<String>,
    max_attempts: u32,
    lease_timeout: Duration,
    batch_size: u32,
    default_retry_delay: Duration,
    codec: C,
    handler: H,
    _payload: PhantomData<fn() -> T>,
}

impl<T> WorkerBuilder<T, JsonCodec, ()> {
    /// Internal constructor used by [`Worker::builder`].
    fn new() -> Self {
        Self {
            pool: None,
            queue: None,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            lease_timeout: DEFAULT_LEASE_TIMEOUT,
            batch_size: DEFAULT_BATCH_SIZE,
            default_retry_delay: DEFAULT_RETRY_DELAY,
            codec: JsonCodec,
            handler: (),
            _payload: PhantomData,
        }
    }
}

impl<T, C, H> WorkerBuilder<T, C, H> {
    /// Set the source queue name. Required.
    #[must_use]
    pub fn queue(mut self, q: impl Into<String>) -> Self {
        self.queue = Some(q.into());
        self
    }

    /// Set the `sqlx::PgPool`. Required.
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Set the per-row max attempts (≥ 1). Default: 3.
    #[must_use]
    pub const fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }

    /// Set the lease timeout (≥ 1s). Default: 30s.
    #[must_use]
    pub const fn lease_timeout(mut self, d: Duration) -> Self {
        self.lease_timeout = d;
        self
    }

    /// Set the per-tick batch size (1..=1000). Default: 32.
    #[must_use]
    pub const fn batch_size(mut self, n: u32) -> Self {
        self.batch_size = n;
        self
    }

    /// Set the default retry delay used when a `JobError::Retry` carries no
    /// `retry_in`. Default: 1s. Faza 6 introduces full `BackoffPolicy`.
    #[must_use]
    pub const fn default_retry_delay(mut self, d: Duration) -> Self {
        self.default_retry_delay = d;
        self
    }

    /// Swap the codec. Default: [`JsonCodec`].
    pub fn codec<C2: Codec>(self, codec: C2) -> WorkerBuilder<T, C2, H> {
        WorkerBuilder {
            pool: self.pool,
            queue: self.queue,
            max_attempts: self.max_attempts,
            lease_timeout: self.lease_timeout,
            batch_size: self.batch_size,
            default_retry_delay: self.default_retry_delay,
            codec,
            handler: self.handler,
            _payload: PhantomData,
        }
    }
}

impl<T, C, H> WorkerBuilder<T, C, H>
where
    T: 'static,
{
    /// Set the handler. Required. Accepts any
    /// `Fn(T, JobContext) -> impl Future<Output = Result<(), JobError>>`
    /// that's `Send + Sync + 'static`.
    pub fn handler<F, Fut>(self, f: F) -> WorkerBuilder<T, C, Arc<dyn JobHandler<T>>>
    where
        F: Fn(T, JobContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), JobError>> + Send + 'static,
    {
        WorkerBuilder {
            pool: self.pool,
            queue: self.queue,
            max_attempts: self.max_attempts,
            lease_timeout: self.lease_timeout,
            batch_size: self.batch_size,
            default_retry_delay: self.default_retry_delay,
            codec: self.codec,
            handler: Arc::new(f) as Arc<dyn JobHandler<T>>,
            _payload: PhantomData,
        }
    }
}

impl<T, C> WorkerBuilder<T, C, Arc<dyn JobHandler<T>>>
where
    T: DeserializeOwned + Send + 'static,
    C: Codec,
{
    /// Validate config and build the `Worker`.
    ///
    /// # Errors
    /// Returns [`BuildError`] when any knob is out of range or required
    /// fields are missing.
    pub fn build(self) -> Result<Worker<T, C>, BuildError> {
        let queue = self.queue.unwrap_or_default();
        if queue.is_empty() || queue.len() > MAX_QUEUE_LEN {
            return Err(BuildError::QueueNameInvalid(queue));
        }

        if self.max_attempts == 0 {
            return Err(BuildError::MaxAttemptsZero);
        }

        if self.lease_timeout < MIN_LEASE_TIMEOUT {
            return Err(BuildError::LeaseTimeoutBelowFloor);
        }

        if self.batch_size < BATCH_SIZE_MIN || self.batch_size > BATCH_SIZE_MAX {
            return Err(BuildError::BatchSizeOutOfRange {
                actual: self.batch_size,
                min: BATCH_SIZE_MIN,
                max: BATCH_SIZE_MAX,
            });
        }

        // Pool: `BuildError` is `#[non_exhaustive]`, so a dedicated
        // `PoolMissing` variant lands in Faza 4 alongside the schema-check
        // / pool-too-small validations. For Faza 3 we route missing pool
        // through `HandlerMissing` — the closest existing "required field
        // absent" semantic. Tests always supply pool; this branch is
        // belt-and-suspenders.
        let pool = self.pool.ok_or(BuildError::HandlerMissing)?;

        Ok(Worker {
            pool,
            queue,
            max_attempts: self.max_attempts,
            lease_timeout: self.lease_timeout,
            batch_size: self.batch_size,
            default_retry_delay: self.default_retry_delay,
            codec: self.codec,
            handler: self.handler,
            _payload: PhantomData,
        })
    }
}

// Separate `build()` impl for the type-state where `.handler()` was NOT
// called — surfaces `HandlerMissing` at runtime.
impl<T, C> WorkerBuilder<T, C, ()>
where
    T: DeserializeOwned + Send + 'static,
    C: Codec,
{
    /// Always returns `Err(HandlerMissing)` — `.handler()` was not called.
    ///
    /// # Errors
    /// Always returns [`BuildError::HandlerMissing`].
    #[allow(clippy::unused_self)]
    pub fn build(self) -> Result<Worker<T, C>, BuildError> {
        Err(BuildError::HandlerMissing)
    }
}

/// Single-queue worker with a configured handler. Build via
/// [`Worker::builder()`]; drive via [`Worker::tick_once`] (Faza 3).
///
/// Faza 4+ adds `start()` for the poll loop + `WorkerHandle::shutdown()`.
pub struct Worker<T, C = JsonCodec> {
    pool: PgPool,
    queue: String,
    max_attempts: u32,
    lease_timeout: Duration,
    batch_size: u32,
    default_retry_delay: Duration,
    codec: C,
    handler: Arc<dyn JobHandler<T>>,
    _payload: PhantomData<fn() -> T>,
}

impl<T, C> std::fmt::Debug for Worker<T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("queue", &self.queue)
            .field("max_attempts", &self.max_attempts)
            .field("lease_timeout", &self.lease_timeout)
            .field("batch_size", &self.batch_size)
            .field("default_retry_delay", &self.default_retry_delay)
            .finish_non_exhaustive()
    }
}

impl<T> Worker<T, JsonCodec>
where
    T: DeserializeOwned + Send + 'static,
{
    /// Start building a worker with the default [`JsonCodec`].
    #[must_use]
    pub fn builder() -> WorkerBuilder<T, JsonCodec, ()> {
        WorkerBuilder::new()
    }
}

impl<T, C> Worker<T, C>
where
    T: DeserializeOwned + Send + 'static,
    C: Codec,
{
    /// Run a single tick: claim a batch, run each handler sequentially,
    /// flip terminal/transitional state with fencing token in WHERE.
    ///
    /// # Errors
    /// Propagates only the `sqlx::Error` from `claim_batch`. Per-row
    /// `mark_*` errors are logged at `warn!` and swallowed (reaper recovers
    /// after lease expiry).
    ///
    /// # Panics
    /// Faza 3: handler panic propagates up through `.await`. Phase 4
    /// introduces `JoinSet` + panic policy.
    #[tracing::instrument(
        name = "pgwq.tick_once",
        skip(self),
        fields(
            queue = %self.queue,
            batch_size = self.batch_size,
            claimed = tracing::field::Empty,
            completed = tracing::field::Empty,
            failed = tracing::field::Empty,
            fenced_out = tracing::field::Empty,
        )
    )]
    pub async fn tick_once(&self) -> Result<TickStats, sqlx::Error> {
        let claimed = crate::claim::claim_and_decode::<T, C>(
            &self.pool,
            &self.codec,
            &self.queue,
            self.batch_size,
            self.lease_timeout,
            self.max_attempts,
        )
        .await?;

        let mut stats = TickStats {
            claimed: claimed.len() as u64,
            ..TickStats::default()
        };

        for job in claimed {
            let ctx = job.context();
            let id = job.id;
            let lease_token = job.lease_token;
            let attempts = job.attempts;
            let max_attempts = job.max_attempts;

            let result = self.handler.call(job.payload, ctx).await;
            self.apply_handler_result(id, lease_token, attempts, max_attempts, result, &mut stats)
                .await;
        }

        let span = tracing::Span::current();
        span.record("claimed", stats.claimed);
        span.record("completed", stats.completed);
        span.record("failed", stats.failed);
        span.record("fenced_out", stats.fenced_out);

        Ok(stats)
    }

    /// Dispatch the per-row terminal/transitional `mark_*` based on
    /// `handler` result. Updates `stats` in place.
    async fn apply_handler_result(
        &self,
        id: i64,
        lease_token: uuid::Uuid,
        attempts: u32,
        max_attempts: u32,
        result: Result<(), JobError>,
        stats: &mut TickStats,
    ) {
        match result {
            Ok(()) => self.flip_done(id, lease_token, stats).await,
            Err(JobError::Retry { reason, retry_in }) => {
                let reason = fmt_err_trimmed(&DisplayStr(reason));
                if attempts >= max_attempts {
                    self.flip_dead(id, lease_token, &reason, "mark_dead (retry-upgrade)", stats)
                        .await;
                } else {
                    let delay = retry_in.unwrap_or(self.default_retry_delay);
                    self.flip_retry(id, lease_token, &reason, delay, stats)
                        .await;
                }
            }
            Err(JobError::Abort { reason }) => {
                let reason = fmt_err_trimmed(&DisplayStr(reason));
                self.flip_dead(id, lease_token, &reason, "mark_dead (abort)", stats)
                    .await;
            }
        }
    }

    async fn flip_done(&self, id: i64, lease_token: uuid::Uuid, stats: &mut TickStats) {
        match mark_done(&self.pool, id, lease_token).await {
            Ok(1) => stats.completed += 1,
            Ok(_) => {
                tracing::warn!(
                    job.id = id,
                    "mark_done lost race; reaper may have flipped row"
                );
                stats.fenced_out += 1;
            }
            Err(e) => {
                tracing::warn!(
                    job.id = id,
                    error = %e,
                    "mark_done failed; reaper will recover after lease expiry"
                );
            }
        }
    }

    async fn flip_retry(
        &self,
        id: i64,
        lease_token: uuid::Uuid,
        reason: &str,
        delay: Duration,
        stats: &mut TickStats,
    ) {
        let run_at = Utc::now()
            + chrono::Duration::from_std(delay)
                .unwrap_or_else(|_| chrono::Duration::milliseconds(i64::MAX));
        match mark_retry(&self.pool, id, lease_token, reason, run_at).await {
            Ok(1) => stats.failed += 1,
            Ok(_) => {
                tracing::warn!(
                    job.id = id,
                    "mark_retry lost race; reaper may have flipped row"
                );
                stats.fenced_out += 1;
            }
            Err(e) => {
                tracing::warn!(
                    job.id = id,
                    error = %e,
                    "mark_retry failed; reaper will recover after lease expiry"
                );
            }
        }
    }

    async fn flip_dead(
        &self,
        id: i64,
        lease_token: uuid::Uuid,
        reason: &str,
        race_log_label: &str,
        stats: &mut TickStats,
    ) {
        match mark_dead(&self.pool, id, lease_token, reason).await {
            Ok(1) => stats.failed += 1,
            Ok(_) => {
                tracing::warn!(
                    job.id = id,
                    label = race_log_label,
                    "mark_dead lost race; reaper may have flipped row"
                );
                stats.fenced_out += 1;
            }
            Err(e) => {
                tracing::warn!(
                    job.id = id,
                    label = race_log_label,
                    error = %e,
                    "mark_dead failed; reaper will recover after lease expiry"
                );
            }
        }
    }
}

/// Tiny adapter so we can route an owned `String` through `fmt_err_trimmed`
/// (which wants `&dyn std::error::Error`). Trimming is char-safe per
/// `util::fmt_err_trimmed`'s contract.
struct DisplayStr(String);
impl std::fmt::Display for DisplayStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::fmt::Debug for DisplayStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for DisplayStr {}
