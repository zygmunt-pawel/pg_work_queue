//! Worker — `tick_once` (Faza 3) + `start()` poll loop (Faza 4).
//!
//! `Worker<T, C>` is generic over the payload type and the codec. The
//! builder uses the type-state pattern (`H` for handler) so a missing
//! handler is caught at runtime via `BuildError::HandlerMissing` rather
//! than at type level — the latter would force users to track type
//! parameters across construction points.
//!
//! `tick_once` is a single-shot: `claim_batch` → run handlers **sequential**
//! → `mark_done` / `mark_retry` / `mark_dead` z fencing token w WHERE.
//!
//! `start()` (Faza 4) spawns the poll loop + a `JoinSet` of in-flight
//! handlers; returns a [`WorkerHandle`] for cancel/join. Architectural rule:
//! **acquire permits FIRST, then claim only what permits allow** (PLAN.md
//! lines 573-685, Anti-pattern #13).

// `significant_drop_tightening`: clippy warns that Arc-clones held across a
// `select!` could be dropped earlier. The clones live for the duration of an
// acquire await (microseconds) and `select!` cancellation drops them
// correctly. The warning is noisy and not actionable.
// `redundant_else` / `redundant_continue`: stylistic only; we keep explicit
// `continue` statements at the end of match arms to make poll-loop branching
// readable.
#![allow(
    clippy::significant_drop_tightening,
    clippy::needless_continue,
    clippy::too_many_lines
)]

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::Utc;
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::codec::{Codec, JsonCodec};
use crate::error::{BuildError, JobError, ShutdownError, StartError};
use crate::job::{Job, JobContext};
use crate::limits::{MAX_QUEUE_LEN, MIN_HANDLER_TIMEOUT, MIN_MARK_TIMEOUT, MIN_POLL_INTERVAL};
use crate::mark::{mark_dead, mark_done, mark_retry};
use crate::transition::{TransitionCtx, TransitionSource, WorkerIdentity, emit_transition};
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

/// Default `poll_interval` when builder doesn't override.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

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

/// Atomic counterpart of [`TickStats`] plus the Faza 4 timeout-related
/// counters. Each field is monotonically incremented; readers snapshot via
/// `load(Relaxed)`. Faza 7 grows this with `aborted` and `pending_recovery`.
#[derive(Debug, Default)]
pub(crate) struct AtomicStats {
    pub(crate) completed: AtomicU64,
    pub(crate) failed: AtomicU64,
    pub(crate) fenced_out: AtomicU64,
    pub(crate) timed_out: AtomicU64,
    pub(crate) mark_timed_out: AtomicU64,
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
    poll_interval: Duration,
    concurrency: Option<usize>,
    handler_timeout: Option<Duration>,
    mark_timeout: Option<Duration>,
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
            poll_interval: DEFAULT_POLL_INTERVAL,
            concurrency: None,
            handler_timeout: None,
            mark_timeout: None,
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

    /// Poll interval — how often the poll loop wakes to call `claim_batch`.
    /// Default: 1s. Floor: [`crate::limits::MIN_POLL_INTERVAL`] (10ms).
    #[must_use]
    pub const fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Maximum number of concurrent handlers. Default: number of CPU cores
    /// (clamped to a sane bound by the validated pool size).
    #[must_use]
    pub const fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }

    /// Per-handler timeout. Default: `lease_timeout × 80%` (clamped to
    /// [`crate::limits::MIN_HANDLER_TIMEOUT`]).
    #[must_use]
    pub const fn handler_timeout(mut self, d: Duration) -> Self {
        self.handler_timeout = Some(d);
        self
    }

    /// Per-mark SQL timeout. Default: `lease_timeout - handler_timeout - 1s`.
    /// Floor: [`crate::limits::MIN_MARK_TIMEOUT`] (100ms).
    #[must_use]
    pub const fn mark_timeout(mut self, d: Duration) -> Self {
        self.mark_timeout = Some(d);
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
            poll_interval: self.poll_interval,
            concurrency: self.concurrency,
            handler_timeout: self.handler_timeout,
            mark_timeout: self.mark_timeout,
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
            poll_interval: self.poll_interval,
            concurrency: self.concurrency,
            handler_timeout: self.handler_timeout,
            mark_timeout: self.mark_timeout,
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

        // poll_interval floor.
        if self.poll_interval < MIN_POLL_INTERVAL {
            return Err(BuildError::PollIntervalTooShort {
                min: MIN_POLL_INTERVAL,
            });
        }
        // lease_timeout >= 5 × poll_interval — give the worker at least 5
        // poll cycles to complete mark_* before the reaper claws back.
        if let Some(min_lease) = self.poll_interval.checked_mul(5) {
            if self.lease_timeout < min_lease {
                return Err(BuildError::LeaseTimeoutTooShort);
            }
        }

        // Pool: dedicated `PoolMissing` variant (replaces Faza 3's
        // `HandlerMissing` hack).
        let pool = self.pool.ok_or(BuildError::PoolMissing)?;

        // Concurrency default = #CPUs; clamp by pool size validation.
        let concurrency = self.concurrency.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map_or(4, std::num::NonZero::get)
        });
        if concurrency == 0 {
            return Err(BuildError::ConcurrencyZero);
        }
        // Pool check: read max_connections from PoolOptions (NOT pool.size()
        // which is lazy and reports 0 for fresh pools — v3.5 regression #6).
        let max_conn = pool.options().get_max_connections();
        let conc_u32 = u32::try_from(concurrency).unwrap_or(u32::MAX);
        let required = conc_u32.saturating_mul(2).saturating_add(2);
        if max_conn < required {
            return Err(BuildError::PoolTooSmall {
                actual: max_conn,
                required,
            });
        }

        // handler_timeout: default = lease_timeout × 80% (clamped to floor).
        let handler_timeout = self.handler_timeout.unwrap_or_else(|| {
            let candidate = self.lease_timeout.mul_f64(0.8);
            candidate.max(MIN_HANDLER_TIMEOUT)
        });
        if handler_timeout < MIN_HANDLER_TIMEOUT {
            return Err(BuildError::HandlerTimeoutBelowFloor {
                min: MIN_HANDLER_TIMEOUT,
            });
        }
        // handler_timeout + 1s ≤ lease_timeout.
        let one_sec = Duration::from_secs(1);
        if handler_timeout
            .checked_add(one_sec)
            .is_none_or(|sum| sum > self.lease_timeout)
        {
            return Err(BuildError::HandlerTimeoutTooLong {
                handler: handler_timeout,
                lease: self.lease_timeout,
            });
        }

        // mark_timeout: default = lease_timeout - handler_timeout - 1s.
        let budget = self
            .lease_timeout
            .checked_sub(handler_timeout)
            .unwrap_or(Duration::ZERO);
        let mark_timeout = self
            .mark_timeout
            .unwrap_or_else(|| budget.checked_sub(one_sec).unwrap_or(MIN_MARK_TIMEOUT));
        if mark_timeout < MIN_MARK_TIMEOUT {
            return Err(BuildError::MarkTimeoutTooShort);
        }
        if mark_timeout > budget {
            return Err(BuildError::MarkTimeoutTooLong {
                mark: mark_timeout,
                budget,
            });
        }

        Ok(Worker {
            pool,
            queue,
            max_attempts: self.max_attempts,
            lease_timeout: self.lease_timeout,
            batch_size: self.batch_size,
            default_retry_delay: self.default_retry_delay,
            poll_interval: self.poll_interval,
            concurrency,
            handler_timeout,
            mark_timeout,
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
/// [`Worker::builder()`]; drive via [`Worker::tick_once`] (Faza 3) or
/// [`Worker::start`] (Faza 4).
pub struct Worker<T, C = JsonCodec> {
    pool: PgPool,
    queue: String,
    max_attempts: u32,
    lease_timeout: Duration,
    batch_size: u32,
    default_retry_delay: Duration,
    poll_interval: Duration,
    concurrency: usize,
    handler_timeout: Duration,
    mark_timeout: Duration,
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
            .field("poll_interval", &self.poll_interval)
            .field("concurrency", &self.concurrency)
            .field("handler_timeout", &self.handler_timeout)
            .field("mark_timeout", &self.mark_timeout)
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
    C: Codec + Clone + 'static,
{
    /// Run a single tick: claim a batch, run each handler sequentially,
    /// flip terminal/transitional state with fencing token in WHERE.
    ///
    /// `tick_once` does NOT use the Faza 4 `handler_timeout` / `mark_timeout`
    /// wrappers — it is a deliberately simple single-shot. Use [`Worker::start`]
    /// for the production poll loop.
    ///
    /// # Errors
    /// Propagates only the `sqlx::Error` from `claim_batch`. Per-row
    /// `mark_*` errors are logged at `warn!` and swallowed (reaper recovers
    /// after lease expiry).
    ///
    /// # Panics
    /// Faza 3: handler panic propagates up through `.await`. Use `start()`
    /// for `JoinSet` panic isolation.
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

        // Build a transient WorkerState-shaped helper for `apply_handler_result`.
        // tick_once doesn't expose `worker.id` (no `start()`), so transitions
        // emit with `worker.id = None`.
        for job in claimed {
            let public_id = job.public_id;
            let queue = job.queue.clone();
            let attempts = job.attempts;
            let max_attempts = job.max_attempts;
            let id = job.id;
            let lease_token = job.lease_token;

            let ctx = job.context();
            let result = self.handler.call(job.payload, ctx).await;
            apply_handler_result(
                &self.pool,
                None,
                &queue,
                public_id,
                id,
                lease_token,
                attempts,
                max_attempts,
                self.default_retry_delay,
                result,
                &mut stats,
            )
            .await;
        }

        let span = tracing::Span::current();
        span.record("claimed", stats.claimed);
        span.record("completed", stats.completed);
        span.record("failed", stats.failed);
        span.record("fenced_out", stats.fenced_out);

        Ok(stats)
    }

    /// Spawn the poll loop and return a [`WorkerHandle`] for cancel/join.
    ///
    /// Runs a one-shot schema probe (`SELECT 1 FROM pgwq.jobs LIMIT 0`)
    /// before any spawn so missing-schema mistakes surface immediately as
    /// [`StartError::SchemaMissing`] (loud-fail; Apalis anti-pattern is the
    /// silent infinite warn loop).
    ///
    /// # Errors
    /// Returns [`StartError::SchemaMissing`] if the probe fails with a
    /// SQLSTATE `42P01` (`undefined_table`) / `3F000` (`invalid_schema_name`)
    /// error. Any other sqlx error is surfaced via [`StartError::Database`].
    pub async fn start(self) -> Result<WorkerHandle, StartError> {
        // Schema probe: a no-op SELECT that touches `pgwq.jobs`. If the
        // schema or table is missing, this returns a sqlx::Database error
        // with SQLSTATE 42P01 / 3F000 — we classify both as
        // `SchemaMissing`.
        if let Err(e) = sqlx::query("SELECT 1 FROM pgwq.jobs LIMIT 0")
            .execute(&self.pool)
            .await
        {
            if let sqlx::Error::Database(db) = &e {
                let code = db.code();
                if matches!(code.as_deref(), Some("42P01" | "3F000")) {
                    return Err(StartError::SchemaMissing(e));
                }
            }
            return Err(StartError::Database(e));
        }

        let worker_id = Uuid::now_v7();
        let batch_size_usize = self.batch_size as usize;

        let state: Arc<WorkerState<T, C>> = Arc::new(WorkerState {
            pool: self.pool,
            queue: self.queue,
            worker_id,
            codec: self.codec,
            handler: self.handler,
            max_attempts: self.max_attempts,
            lease_timeout: self.lease_timeout,
            handler_timeout: self.handler_timeout,
            mark_timeout: self.mark_timeout,
            default_retry_delay: self.default_retry_delay,
            batch_size: batch_size_usize,
            poll_interval: self.poll_interval,
            semaphore: Arc::new(Semaphore::new(self.concurrency)),
            shutdown: CancellationToken::new(),
            tasks: Mutex::new(JoinSet::new()),
            stats: AtomicStats::default(),
            last_fatal: OnceLock::new(),
        });

        let state_for_loop = state.clone();
        let poll_join: JoinHandle<()> = tokio::spawn(poll_loop(state_for_loop));
        let poll_abort = poll_join.abort_handle();

        Ok(WorkerHandle {
            state,
            poll_join,
            poll_abort,
        })
    }
}

/// Shared state owned by the poll loop, handler tasks, and `WorkerHandle`.
/// `pub(crate)` only — never crosses the public API boundary.
pub(crate) struct WorkerState<T, C> {
    pub(crate) pool: PgPool,
    pub(crate) queue: String,
    pub(crate) worker_id: Uuid,
    pub(crate) codec: C,
    pub(crate) handler: Arc<dyn JobHandler<T>>,
    pub(crate) max_attempts: u32,
    pub(crate) lease_timeout: Duration,
    pub(crate) handler_timeout: Duration,
    pub(crate) mark_timeout: Duration,
    pub(crate) default_retry_delay: Duration,
    pub(crate) batch_size: usize,
    pub(crate) poll_interval: Duration,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) tasks: Mutex<JoinSet<()>>,
    pub(crate) stats: AtomicStats,
    pub(crate) last_fatal: OnceLock<Arc<sqlx::Error>>,
}

/// Handle returned by [`Worker::start`]. Faza 4 surface is minimal — `cancel`
/// + `join`. Faza 7 replaces this with `shutdown(timeout) -> Result<Stats, _>`.
pub struct WorkerHandle {
    state: Arc<dyn WorkerStateOps>,
    poll_join: JoinHandle<()>,
    #[allow(dead_code)] // retained for Faza 7 hard-abort path
    poll_abort: AbortHandle,
}

impl std::fmt::Debug for WorkerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHandle").finish_non_exhaustive()
    }
}

impl WorkerHandle {
    /// Trigger shutdown signal. Poll loop exits at the next `.await` point
    /// (cancellation is intercepted via `tokio::select!`). Idempotent.
    ///
    /// Faza 4 minimal — Faza 7 replaces with
    /// `shutdown(timeout) -> Result<Stats, _>`.
    pub fn cancel(&self) {
        self.state.cancel_shutdown();
    }

    /// Await poll loop completion + drain in-flight handlers.
    ///
    /// Drains the local handler `JoinSet` until empty. Returns
    /// [`ShutdownError::Fatal`] if the poll loop self-shutdown after a
    /// fatal sqlx error (see [`crate::error::ShutdownError`]).
    ///
    /// # Errors
    /// Returns [`ShutdownError::Fatal`] iff the poll loop classified a
    /// `sqlx::Error` as fatal via `is_fatal_sqlx` before exiting.
    pub async fn join(self) -> Result<(), ShutdownError> {
        // 1) Await poll loop natural exit (already cancelled or self-shut).
        let _ = self.poll_join.await;

        // 2) Drain handler JoinSet. Each handle_job task is fire-and-forget;
        //    we wait until JoinSet is empty so all mark_* commits have a
        //    chance to land.
        self.state.drain_handlers().await;

        // 3) Surface fatal poll-loop error if any.
        if let Some(fatal) = self.state.last_fatal_snapshot() {
            return Err(ShutdownError::Fatal(fatal));
        }
        Ok(())
    }
}

/// Type-erased view of `WorkerState<T, C>` used by `WorkerHandle` so the
/// handle itself doesn't need the `T, C` generics.
trait WorkerStateOps: Send + Sync {
    fn cancel_shutdown(&self);
    fn last_fatal_snapshot(&self) -> Option<Arc<sqlx::Error>>;
    fn drain_handlers<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

impl<T, C> WorkerStateOps for WorkerState<T, C>
where
    T: Send + 'static,
    C: Send + Sync + 'static,
{
    fn cancel_shutdown(&self) {
        self.shutdown.cancel();
    }

    fn last_fatal_snapshot(&self) -> Option<Arc<sqlx::Error>> {
        self.last_fatal.get().cloned()
    }

    fn drain_handlers<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Drain JoinSet until empty. We hold the Mutex across awaits —
            // OK because once `cancel` was issued, the poll loop has exited
            // and no new tasks are being spawned.
            let mut guard = self.tasks.lock().await;
            while let Some(_res) = guard.join_next().await {
                // Per-task panic/cancel already handled inside handle_job
                // (panics caught via JoinError::is_panic in the inner JoinSet).
            }
        })
    }
}

/// Poll loop body — see PLAN.md §"Poll loop (heart)" (lines 573-685).
///
/// Architectural rule: acquire permits FIRST, then claim only what permits
/// allow (Anti-pattern #13). Permits are owned by the spawned `handle_job`
/// task and freed on its completion via `Drop`.
async fn poll_loop<T, C>(state: Arc<WorkerState<T, C>>)
where
    T: DeserializeOwned + Send + 'static,
    C: Codec + Clone + Send + Sync + 'static,
{
    let mut ticker = tokio::time::interval(state.poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            () = state.shutdown.cancelled() => break,
        }

        // Gate on capacity: blocking acquire of the first permit, then
        // greedy try_acquire up to batch_size. Both interleaved with
        // shutdown so cancel doesn't wait for permits. The `Arc` clones
        // live just long enough for the acquire call; we bind them to
        // named locals to give clippy a clear lifetime.
        let sema_first = state.semaphore.clone();
        let p1 = tokio::select! {
            r = sema_first.acquire_owned() => r,
            () = state.shutdown.cancelled() => break,
        };
        let Ok(p1) = p1 else { break }; // semaphore closed
        let mut permits = vec![p1];
        while permits.len() < state.batch_size {
            let sema = state.semaphore.clone();
            match sema.try_acquire_owned() {
                Ok(p) => permits.push(p),
                Err(TryAcquireError::NoPermits) => break,
                Err(TryAcquireError::Closed) => return,
            }
        }
        let want = permits.len();
        let want_u32 = u32::try_from(want).unwrap_or(u32::MAX);

        let tick_span = tracing::info_span!(
            "pgwq.poll_tick",
            worker.id = %state.worker_id,
            queue = %state.queue,
            batch_size = want_u32,
            claimed = tracing::field::Empty,
        );
        let _enter = tick_span.enter();

        // Wrap the claim await in select! — otherwise pool starvation
        // (sqlx acquire_timeout 30s default) blocks shutdown beyond cancel.
        let claim_result = tokio::select! {
            r = crate::claim::claim_and_decode::<T, C>(
                &state.pool,
                &state.codec,
                &state.queue,
                want_u32,
                state.lease_timeout,
                state.max_attempts,
            ) => r,
            () = state.shutdown.cancelled() => break,
        };

        match claim_result {
            Ok(rows) if rows.is_empty() => {
                tick_span.record("claimed", 0u64);
                // Permits drop at end of iteration — return slots.
            }
            Ok(rows) => {
                let n = rows.len();
                tick_span.record("claimed", n as u64);
                tracing::debug!(claimed = n, wanted = want, "batch claimed");
                // CRITICAL invariant: rows.len() ≤ permits.len() (claim
                // respects LIMIT). Each row pairs with a permit; surplus
                // permits drop at end of scope. `into_iter()` on permits
                // (not `drain`) — the Vec is consumed entirely here.
                let mut tasks = state.tasks.lock().await;
                for (row, permit) in rows.into_iter().zip(permits.into_iter()) {
                    let s = state.clone();
                    tasks.spawn(handle_job(row, s, permit));
                }
                // `permits` cannot be reused after `into_iter()`. We
                // explicitly bind a fresh empty Vec for the next iteration
                // implicit at top-of-loop reset.
                continue;
            }
            Err(e) if is_fatal_sqlx(&e) => {
                tracing::error!(
                    worker.id = %state.worker_id,
                    error = %e,
                    "fatal DB error in claim_batch; shutting down worker"
                );
                let _ = state.last_fatal.set(Arc::new(e));
                state.shutdown.cancel();
                break;
            }
            Err(e) => {
                tracing::warn!(
                    worker.id = %state.worker_id,
                    error = %e,
                    "claim batch failed; will retry next tick"
                );
                // Permits drop at scope end — other tasks/ticks reclaim.
            }
        }
    }
}

/// Handler invocation — wraps the handler in `tokio::time::timeout` inside
/// a **local `JoinSet`** (panic isolation + cascade abort; see PLAN.md
/// §"Handler invocation (`handle_job`)" lines 930-1052).
///
/// `tokio::spawn` would leak the inner handler future under outer
/// `abort_all` (Anti-pattern #12) — `JoinSet::drop` aborts pending tasks
/// so cascade works correctly.
async fn handle_job<T, C>(job: Job<T>, state: Arc<WorkerState<T, C>>, _permit: OwnedSemaphorePermit)
where
    T: Send + 'static,
    C: Send + Sync + 'static,
{
    let id = job.id;
    let public_id = job.public_id;
    let queue = job.queue.clone();
    let attempts = job.attempts;
    let max_attempts = job.max_attempts;
    let lease_token = job.lease_token;

    let span = tracing::info_span!(
        "pgwq.handle_job",
        worker.id = %state.worker_id,
        queue = %queue,
        job.id = id,
        job.public_id = %public_id,
        job.attempt = attempts,
        timeout_ms = u64::try_from(state.handler_timeout.as_millis()).unwrap_or(u64::MAX),
    );
    let _enter = span.enter();

    let ctx = job.context();
    let handler = state.handler.clone();
    let handler_fut = handler.call(job.payload, ctx);
    let handler_timeout = state.handler_timeout;

    // Local JoinSet — Drop aborts pending tasks (cascade), unlike JoinHandle
    // which detaches (Anti-pattern #12).
    let mut set: JoinSet<Result<Result<(), JobError>, tokio::time::error::Elapsed>> =
        JoinSet::new();
    set.spawn(tokio::time::timeout(handler_timeout, handler_fut));

    let outcome = set.join_next().await;

    let identity = WorkerIdentity(state.worker_id);

    match outcome {
        // Handler returned normally.
        Some(Ok(Ok(result))) => {
            apply_handler_result_state(
                &state,
                identity,
                &queue,
                public_id,
                id,
                lease_token,
                attempts,
                max_attempts,
                result,
            )
            .await;
        }
        // Timeout fired.
        Some(Ok(Err(_elapsed))) => {
            state.stats.timed_out.fetch_add(1, Ordering::Relaxed);
            tracing::event!(
                target: "pgwq.handler.timeout_elapsed",
                tracing::Level::WARN,
                worker.id = %state.worker_id,
                job.id = id,
                job.public_id = %public_id,
                job.attempt = attempts,
                timeout_ms = u64::try_from(handler_timeout.as_millis()).unwrap_or(u64::MAX),
            );
            // Synthesize a Retry { reason: "handler_timeout" } — same path
            // as JobError::Retry, including the max_attempts upgrade.
            let synthesized: Result<(), JobError> = Err(JobError::Retry {
                reason: "handler_timeout".to_string(),
                retry_in: None,
            });
            apply_handler_result_state(
                &state,
                identity,
                &queue,
                public_id,
                id,
                lease_token,
                attempts,
                max_attempts,
                synthesized,
            )
            .await;
        }
        // Inner task panic or cancellation. Faza 4 routes panic through
        // `mark_retry` (current default; Faza 6 introduces PanicPolicy).
        Some(Err(je)) if je.is_panic() => {
            let msg = "handler panic";
            let synthesized: Result<(), JobError> = Err(JobError::Retry {
                reason: format!("panic: {msg}"),
                retry_in: None,
            });
            tracing::error!(
                worker.id = %state.worker_id,
                job.id = id,
                job.public_id = %public_id,
                "handler panicked; routing through mark_retry (Faza 6 adds PanicPolicy)"
            );
            apply_handler_result_state(
                &state,
                identity,
                &queue,
                public_id,
                id,
                lease_token,
                attempts,
                max_attempts,
                synthesized,
            )
            .await;
        }
        Some(Err(_cancelled)) => {
            // Inner task cancelled before join_next() — theoretical only;
            // we never abort the inner set from inside handle_job. Leave
            // row 'running' for the reaper.
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                "inner handler task cancelled unexpectedly; leaving row 'running' for reaper"
            );
        }
        None => {
            // Set had exactly one spawned task; join_next() returning None
            // would mean an empty set. Unreachable in practice; emit a warn
            // and bail rather than crash (panic = deny w src/).
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                "handle_job: inner JoinSet returned None unexpectedly"
            );
        }
    }
    // _permit drops here — semaphore slot freed.
}

/// Apply a handler `Result<(), JobError>` to the DB via mark_* with the
/// Faza-4 `mark_timeout` wrapper + stats counters + transition events.
#[allow(clippy::too_many_arguments)]
async fn apply_handler_result_state<T, C>(
    state: &WorkerState<T, C>,
    identity: WorkerIdentity,
    queue: &str,
    public_id: Uuid,
    id: i64,
    lease_token: Uuid,
    attempts: u32,
    max_attempts: u32,
    result: Result<(), JobError>,
) where
    T: Send + 'static,
    C: Send + Sync + 'static,
{
    match result {
        Ok(()) => {
            flip_done_state(state, identity, queue, public_id, id, lease_token, attempts).await;
        }
        Err(JobError::Retry { reason, retry_in }) => {
            let reason = fmt_err_trimmed(&DisplayStr(reason));
            if attempts >= max_attempts {
                flip_dead_state(
                    state,
                    identity,
                    queue,
                    public_id,
                    id,
                    lease_token,
                    attempts,
                    &reason,
                )
                .await;
            } else {
                let delay = retry_in.unwrap_or(state.default_retry_delay);
                flip_retry_state(
                    state,
                    identity,
                    queue,
                    public_id,
                    id,
                    lease_token,
                    attempts,
                    &reason,
                    delay,
                )
                .await;
            }
        }
        Err(JobError::Abort { reason }) => {
            let reason = fmt_err_trimmed(&DisplayStr(reason));
            flip_dead_state(
                state,
                identity,
                queue,
                public_id,
                id,
                lease_token,
                attempts,
                &reason,
            )
            .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn flip_done_state<T, C>(
    state: &WorkerState<T, C>,
    identity: WorkerIdentity,
    queue: &str,
    public_id: Uuid,
    id: i64,
    lease_token: Uuid,
    attempts: u32,
) where
    T: Send + 'static,
    C: Send + Sync + 'static,
{
    let res =
        tokio::time::timeout(state.mark_timeout, mark_done(&state.pool, id, lease_token)).await;
    match res {
        Ok(Ok(1)) => {
            state.stats.completed.fetch_add(1, Ordering::Relaxed);
            emit_transition(
                Some("running"),
                "done",
                Some(identity),
                TransitionCtx {
                    job_id: id,
                    public_id,
                    queue,
                    attempts,
                    source: TransitionSource::Worker,
                    reason: None,
                    lost_race: false,
                },
            );
        }
        Ok(Ok(_)) => {
            state.stats.fenced_out.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                "mark_done lost race; reaper may have flipped row"
            );
        }
        Ok(Err(e)) => {
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                error = %e,
                "mark_done failed; reaper will recover after lease expiry"
            );
        }
        Err(_elapsed) => {
            state.stats.mark_timed_out.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                mark_timeout_ms = u64::try_from(state.mark_timeout.as_millis()).unwrap_or(u64::MAX),
                "mark_done timed out under pool pressure; leaving row 'running' for reaper"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn flip_retry_state<T, C>(
    state: &WorkerState<T, C>,
    identity: WorkerIdentity,
    queue: &str,
    public_id: Uuid,
    id: i64,
    lease_token: Uuid,
    attempts: u32,
    reason: &str,
    delay: Duration,
) where
    T: Send + 'static,
    C: Send + Sync + 'static,
{
    let run_at = Utc::now()
        + chrono::Duration::from_std(delay)
            .unwrap_or_else(|_| chrono::Duration::milliseconds(i64::MAX));
    let res = tokio::time::timeout(
        state.mark_timeout,
        mark_retry(&state.pool, id, lease_token, reason, run_at),
    )
    .await;
    match res {
        Ok(Ok(1)) => {
            state.stats.failed.fetch_add(1, Ordering::Relaxed);
            emit_transition(
                Some("running"),
                "awaiting_retry",
                Some(identity),
                TransitionCtx {
                    job_id: id,
                    public_id,
                    queue,
                    attempts,
                    source: TransitionSource::Worker,
                    reason: Some(reason),
                    lost_race: false,
                },
            );
        }
        Ok(Ok(_)) => {
            state.stats.fenced_out.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                "mark_retry lost race; reaper may have flipped row"
            );
        }
        Ok(Err(e)) => {
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                error = %e,
                "mark_retry failed; reaper will recover after lease expiry"
            );
        }
        Err(_elapsed) => {
            state.stats.mark_timed_out.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                mark_timeout_ms = u64::try_from(state.mark_timeout.as_millis()).unwrap_or(u64::MAX),
                "mark_retry timed out under pool pressure; leaving row 'running' for reaper"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn flip_dead_state<T, C>(
    state: &WorkerState<T, C>,
    identity: WorkerIdentity,
    queue: &str,
    public_id: Uuid,
    id: i64,
    lease_token: Uuid,
    attempts: u32,
    reason: &str,
) where
    T: Send + 'static,
    C: Send + Sync + 'static,
{
    let res = tokio::time::timeout(
        state.mark_timeout,
        mark_dead(&state.pool, id, lease_token, reason),
    )
    .await;
    match res {
        Ok(Ok(1)) => {
            state.stats.failed.fetch_add(1, Ordering::Relaxed);
            emit_transition(
                Some("running"),
                "dead",
                Some(identity),
                TransitionCtx {
                    job_id: id,
                    public_id,
                    queue,
                    attempts,
                    source: TransitionSource::Worker,
                    reason: Some(reason),
                    lost_race: false,
                },
            );
        }
        Ok(Ok(_)) => {
            state.stats.fenced_out.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                "mark_dead lost race; reaper may have flipped row"
            );
        }
        Ok(Err(e)) => {
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                error = %e,
                "mark_dead failed; reaper will recover after lease expiry"
            );
        }
        Err(_elapsed) => {
            state.stats.mark_timed_out.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                worker.id = %state.worker_id,
                job.id = id,
                mark_timeout_ms = u64::try_from(state.mark_timeout.as_millis()).unwrap_or(u64::MAX),
                "mark_dead timed out under pool pressure; leaving row 'running' for reaper"
            );
        }
    }
}

/// `tick_once` flavor of `apply_handler_result` — uses `TickStats` and
/// does NOT apply `mark_timeout` (intentional simplicity of single-shot).
#[allow(clippy::too_many_arguments)]
async fn apply_handler_result(
    pool: &PgPool,
    worker: Option<WorkerIdentity>,
    queue: &str,
    public_id: Uuid,
    id: i64,
    lease_token: Uuid,
    attempts: u32,
    max_attempts: u32,
    default_retry_delay: Duration,
    result: Result<(), JobError>,
    stats: &mut TickStats,
) {
    match result {
        Ok(()) => {
            flip_done_tick(
                pool,
                worker,
                queue,
                public_id,
                id,
                lease_token,
                attempts,
                stats,
            )
            .await;
        }
        Err(JobError::Retry { reason, retry_in }) => {
            let reason = fmt_err_trimmed(&DisplayStr(reason));
            if attempts >= max_attempts {
                flip_dead_tick(
                    pool,
                    worker,
                    queue,
                    public_id,
                    id,
                    lease_token,
                    attempts,
                    &reason,
                    stats,
                )
                .await;
            } else {
                let delay = retry_in.unwrap_or(default_retry_delay);
                flip_retry_tick(
                    pool,
                    worker,
                    queue,
                    public_id,
                    id,
                    lease_token,
                    attempts,
                    &reason,
                    delay,
                    stats,
                )
                .await;
            }
        }
        Err(JobError::Abort { reason }) => {
            let reason = fmt_err_trimmed(&DisplayStr(reason));
            flip_dead_tick(
                pool,
                worker,
                queue,
                public_id,
                id,
                lease_token,
                attempts,
                &reason,
                stats,
            )
            .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn flip_done_tick(
    pool: &PgPool,
    worker: Option<WorkerIdentity>,
    queue: &str,
    public_id: Uuid,
    id: i64,
    lease_token: Uuid,
    attempts: u32,
    stats: &mut TickStats,
) {
    match mark_done(pool, id, lease_token).await {
        Ok(1) => {
            stats.completed += 1;
            emit_transition(
                Some("running"),
                "done",
                worker,
                TransitionCtx {
                    job_id: id,
                    public_id,
                    queue,
                    attempts,
                    source: TransitionSource::Worker,
                    reason: None,
                    lost_race: false,
                },
            );
        }
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

#[allow(clippy::too_many_arguments)]
async fn flip_retry_tick(
    pool: &PgPool,
    worker: Option<WorkerIdentity>,
    queue: &str,
    public_id: Uuid,
    id: i64,
    lease_token: Uuid,
    attempts: u32,
    reason: &str,
    delay: Duration,
    stats: &mut TickStats,
) {
    let run_at = Utc::now()
        + chrono::Duration::from_std(delay)
            .unwrap_or_else(|_| chrono::Duration::milliseconds(i64::MAX));
    match mark_retry(pool, id, lease_token, reason, run_at).await {
        Ok(1) => {
            stats.failed += 1;
            emit_transition(
                Some("running"),
                "awaiting_retry",
                worker,
                TransitionCtx {
                    job_id: id,
                    public_id,
                    queue,
                    attempts,
                    source: TransitionSource::Worker,
                    reason: Some(reason),
                    lost_race: false,
                },
            );
        }
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

#[allow(clippy::too_many_arguments)]
async fn flip_dead_tick(
    pool: &PgPool,
    worker: Option<WorkerIdentity>,
    queue: &str,
    public_id: Uuid,
    id: i64,
    lease_token: Uuid,
    attempts: u32,
    reason: &str,
    stats: &mut TickStats,
) {
    match mark_dead(pool, id, lease_token, reason).await {
        Ok(1) => {
            stats.failed += 1;
            emit_transition(
                Some("running"),
                "dead",
                worker,
                TransitionCtx {
                    job_id: id,
                    public_id,
                    queue,
                    attempts,
                    source: TransitionSource::Worker,
                    reason: Some(reason),
                    lost_race: false,
                },
            );
        }
        Ok(_) => {
            tracing::warn!(
                job.id = id,
                "mark_dead lost race; reaper may have flipped row"
            );
            stats.fenced_out += 1;
        }
        Err(e) => {
            tracing::warn!(
                job.id = id,
                error = %e,
                "mark_dead failed; reaper will recover after lease expiry"
            );
        }
    }
}

/// Classify a `sqlx::Error` as fatal (worker self-shuts) vs transient
/// (logged at warn, retry next tick).
///
/// PLAN.md §"Sqlx error classification" (lines 1574-1604).
///
/// Fatal: `PoolClosed`, `WorkerCrashed`, `Configuration(_)`, `Migrate(_)`,
/// `ColumnDecode { .. }`, `Decode(_)`, `TypeNotFound { .. }`,
/// `ColumnNotFound(_)`, `Protocol(_)`.
///
/// Transient (returns false): `Database(_)`, `Io(_)`, `Tls(_)`,
/// `PoolTimedOut`.
#[doc(hidden)]
#[must_use]
pub const fn is_fatal_sqlx(e: &sqlx::Error) -> bool {
    matches!(
        e,
        sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed
            | sqlx::Error::Configuration(_)
            | sqlx::Error::Migrate(_)
            | sqlx::Error::ColumnDecode { .. }
            | sqlx::Error::Decode(_)
            | sqlx::Error::TypeNotFound { .. }
            | sqlx::Error::ColumnNotFound(_)
            | sqlx::Error::Protocol(_)
    )
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
