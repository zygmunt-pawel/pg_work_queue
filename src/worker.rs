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

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::de::DeserializeOwned;
use sqlx::PgPool;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::backoff::{BackoffPolicy, PanicPolicy};
use crate::codec::{Codec, JsonCodec};
use crate::error::{BuildError, JobError, ShutdownError, StartError};
use crate::job::{Job, JobContext};
use crate::limits::{
    MAX_CONCURRENCY_KEY_LEN, MAX_QUEUE_LEN, MIN_HANDLER_TIMEOUT, MIN_MARK_TIMEOUT,
    MIN_POLL_INTERVAL,
};
use crate::mark::{mark_dead, mark_done, mark_retry};
use crate::reaper::MIN_REAPER_INTERVAL;
use crate::transition::{TransitionCtx, TransitionSource, WorkerIdentity, emit_transition};
use crate::util::fmt_err_trimmed;

/// Hard floor for `retry_in` (handler-supplied per-call override). Below this
/// a tight retry loop (`retry_in: Duration::ZERO`) hammer'uje DB. Per PLAN.md
/// §"Retry backoff policy" (lines 1331-1336): `[max(poll_interval, 100ms), 24h]`.
const MIN_RETRY_IN: Duration = Duration::from_millis(100);

/// Hard ceiling for `retry_in` (handler-supplied per-call override) — same as
/// the `BackoffPolicy::cap` ceiling. Larger values usually indicate user bug.
const MAX_RETRY_IN: Duration = Duration::from_secs(24 * 3600);

/// Minimum `lease_timeout`. Below this the handler cannot reliably finish
/// trivial work + `mark_*` commit before the reaper claws back the row.
const MIN_LEASE_TIMEOUT: Duration = Duration::from_secs(1);

/// Default `lease_timeout` when builder doesn't override.
const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Atomic counterpart of [`Stats`] used internally by the running worker.
/// Each field is monotonically incremented from at most one writer (handler
/// task or shutdown drain); readers snapshot via [`AtomicStats::snapshot`].
///
/// `aborted` is bumped by `WorkerHandle::shutdown` for handlers that were
/// still in-flight when `JoinSet::abort_all` fired. `pending_recovery` is NOT
/// stored here — `shutdown` queries the DB once at the end of the drain
/// (under a small timeout) and folds the result into the returned `Stats`.
#[derive(Debug, Default)]
pub(crate) struct AtomicStats {
    pub(crate) completed: AtomicU64,
    pub(crate) failed: AtomicU64,
    pub(crate) fenced_out: AtomicU64,
    pub(crate) timed_out: AtomicU64,
    pub(crate) mark_timed_out: AtomicU64,
    pub(crate) aborted: AtomicU64,
}

impl AtomicStats {
    /// Snapshot the running counters into a `Stats` value. `pending_recovery`
    /// is left at `0` — the shutdown path fills it in after a bounded DB query.
    pub(crate) fn snapshot(&self) -> Stats {
        Stats {
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            mark_timed_out: self.mark_timed_out.load(Ordering::Relaxed),
            aborted: self.aborted.load(Ordering::Relaxed),
            fenced_out: self.fenced_out.load(Ordering::Relaxed),
            pending_recovery: 0,
        }
    }
}

/// Stats snapshot returned by [`WorkerHandle::shutdown`]. All counters are
/// monotonic since worker start.
///
/// Field semantics (PLAN.md §"Shutdown semantics"):
/// - `completed` — handlers that returned `Ok(())` AND whose `mark_done`
///   flipped one row (commit observed).
/// - `failed` — handlers that returned `Err(JobError::Retry|Abort)` OR
///   panicked, AND whose `mark_retry` / `mark_dead` flipped one row.
/// - `timed_out` — handlers cancelled via the per-handler `handler_timeout`.
///   Routed through `mark_retry` with `reason = "handler_timeout"`.
/// - `mark_timed_out` — `mark_*` SQL itself exceeded `mark_timeout` (pool
///   pressure). Row left `'running'`; reaper recovers after lease expiry.
/// - `aborted` — handlers aborted via `JoinSet::abort_all` during
///   `WorkerHandle::shutdown` (NOT via `handler_timeout`). Row left
///   `'running'`; reaper recovers.
/// - `fenced_out` — `mark_*` returned `0 rows_affected` — lease lost to the
///   reaper or a competing worker. State is consistent under the conflicting
///   writer.
/// - `pending_recovery` — rows still in `status = 'running'` for our queue
///   at the moment `shutdown` returned. Best-effort: if the lookup query
///   fails or times out (e.g., dead pool), this field reads `0` and a
///   `tracing::warn!` event is emitted.
///
/// Edge case (documented as accepted): a handler whose `mark_done` UPDATE
/// committed server-side but whose future was cancelled before returning to
/// the worker → row is `'done'` in the DB but `completed` does NOT count it.
/// Stats may be off by 1-2 under aggressive shutdown. See PLAN.md
/// §"Aborted handlers semantics".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Handlers `Ok` AND `mark_done` flipped 1 row.
    pub completed: u64,
    /// Handlers `Err(JobError::*)` (or panic-routed) AND `mark_*` flipped 1 row.
    pub failed: u64,
    /// Handlers cancelled via `handler_timeout`.
    pub timed_out: u64,
    /// `mark_*` SQL exceeded `mark_timeout`.
    pub mark_timed_out: u64,
    /// Handlers aborted by `WorkerHandle::shutdown` cascade.
    pub aborted: u64,
    /// `mark_*` returned 0 `rows_affected` — lease drift.
    pub fenced_out: u64,
    /// Rows still `'running'` for our queue at shutdown return.
    pub pending_recovery: u64,
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
    retry_backoff: Option<BackoffPolicy>,
    panic_policy: PanicPolicy,
    poll_interval: Duration,
    concurrency: Option<usize>,
    concurrency_limits: HashMap<String, u32>,
    handler_timeout: Option<Duration>,
    mark_timeout: Option<Duration>,
    reaper_interval: Option<Duration>,
    shutdown_token: Option<CancellationToken>,
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
            retry_backoff: None,
            panic_policy: PanicPolicy::default(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            concurrency: None,
            concurrency_limits: HashMap::new(),
            handler_timeout: None,
            mark_timeout: None,
            reaper_interval: None,
            shutdown_token: None,
            codec: JsonCodec,
            handler: (),
            _payload: PhantomData,
        }
    }
}

impl<T, C, H> WorkerBuilder<T, C, H> {
    /// Source queue name. **Required**.
    ///
    /// Validated on `build()`: non-empty and `≤ MAX_QUEUE_LEN` (64) chars.
    /// Empty / oversize → [`BuildError::QueueNameInvalid`].
    ///
    /// **Observable effect**: the worker only claims rows whose `queue`
    /// column matches this value (per-queue isolation; multiple workers
    /// can share one DB with different queue names).
    ///
    /// See `tests/builder_validation.rs` and
    /// `tests/reaper_per_queue_isolation.rs`.
    #[must_use]
    pub fn queue(mut self, q: impl Into<String>) -> Self {
        self.queue = Some(q.into());
        self
    }

    /// `sqlx::PgPool`. **Required**.
    ///
    /// The pool's `max_connections` must satisfy
    /// `max_connections >= concurrency × 2 + 2` (each handler may hold
    /// 1 conn for work + 1 for `mark_*`; +2 for the poll loop and reaper).
    /// Read via `pool.options().get_max_connections()` (NOT `pool.size()`,
    /// which is lazy and reports 0 for fresh pools). Failing this surfaces
    /// [`BuildError::PoolTooSmall`].
    ///
    /// **Observable effect**: connection capacity for the whole worker.
    /// Pool starvation manifests as `Stats::mark_timed_out` increments
    /// (the `mark_*` SQL wrapper trips its timeout while waiting for an
    /// acquire) and rows left in `'running'` that the reaper recovers.
    ///
    /// **Multi-worker pool sharing (operator responsibility, not validated)**:
    /// The `concurrency × 2 + 2` rule is a *per-worker* lower bound. If
    /// `N` workers share the **same** `PgPool`, the joint floor is
    /// `Σᵢ (concurrencyᵢ × 2) + 2N`. The builder cannot enforce this
    /// because each `Worker::start` only sees its own pool handle and
    /// has no view of sibling workers. Under-provisioning manifests
    /// exactly like single-worker pool starvation (mark timeouts, reaper
    /// falling behind, growing `pending_recovery`) but is harder to
    /// diagnose because no single worker hit `PoolTooSmall` at build.
    /// **Recommendation**: either size the shared pool to the joint
    /// floor, or give each worker its own pool.
    ///
    /// See `tests/builder_validation.rs` (`PoolTooSmall`) and
    /// `tests/shutdown_immediate_with_pool_starvation.rs`.
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Per-row max attempts (`≥ 1`). Default: 3.
    ///
    /// The value is stamped on each row at claim time, so a rolling
    /// deploy that changes `max_attempts` only affects newly-claimed
    /// rows; in-flight rows keep the value the *claiming* worker stamped.
    ///
    /// **Observable effect**: number of retries before a failing job
    /// transitions to `dead`. `attempts >= max_attempts` on a retry path
    /// upgrades the transition to `mark_dead` (or, on the reaper path,
    /// to `lease_expired_max_attempts`).
    ///
    /// `max_attempts == 0` → [`BuildError::MaxAttemptsZero`].
    ///
    /// See `tests/max_attempts_behavior.rs` and
    /// `tests/max_attempts_rolling_deploy.rs`.
    #[must_use]
    pub const fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }

    /// Per-row lease deadline written to `lease_expires_at` at claim time.
    /// Default: 30s. Floor: 1s.
    ///
    /// Cross-knob constraints (enforced by `build()`):
    /// - `lease_timeout >= 5 × poll_interval` ([`BuildError::LeaseTimeoutTooShort`]).
    /// - `lease_timeout >= 2s` so a valid `reaper_interval` exists
    ///   ([`BuildError::LeaseTimeoutTooShortForReaper`]).
    /// - `handler_timeout + 1s <= lease_timeout` ([`BuildError::HandlerTimeoutTooLong`]).
    ///
    /// **Observable effect**: process-death recovery threshold. If a
    /// worker crashes mid-handler, the reaper flips the stale `running`
    /// row no earlier than `now + lease_timeout`. Handler-level
    /// cancellation goes through `handler_timeout`, **not** this knob.
    ///
    /// See `tests/lease_timeout_behavior.rs` and
    /// `tests/stale_running_reaped.rs`.
    #[must_use]
    pub const fn lease_timeout(mut self, d: Duration) -> Self {
        self.lease_timeout = d;
        self
    }

    /// Per-tick batch size (`1..=1000`). Default: 32.
    ///
    /// Out-of-range → [`BuildError::BatchSizeOutOfRange`].
    ///
    /// **Observable effect**: rows pulled per `claim_batch` invocation
    /// (and hence per `TickStats::claimed`). Larger batches amortize
    /// claim overhead at the cost of tail latency when a slow handler
    /// stalls the per-tick sequential loop in `tick_once` (the
    /// production poll loop is concurrent, so batching matters less
    /// there).
    ///
    /// See `tests/batch_size_behavior.rs`.
    #[must_use]
    pub const fn batch_size(mut self, n: u32) -> Self {
        self.batch_size = n;
        self
    }

    /// Retry backoff policy used when `JobError::Retry` carries no
    /// `retry_in` override. Default: [`BackoffPolicy::default()`] —
    /// `Exponential { 1s, 2.0, 5min, 0.2 }`.
    ///
    /// Validated on `build()`; invalid params → [`BuildError::BackoffInvalid`].
    ///
    /// **Observable effect**: the value written to `run_at` on the
    /// retry path. Per-call `JobError::retry_in(...)` has priority and
    /// is clamped to `[max(poll_interval, 100ms), 24h]`.
    ///
    /// See `tests/retry_backoff_behavior.rs`,
    /// `tests/backoff_policy_unit.rs` and
    /// `tests/retry_in_override.rs`.
    #[must_use]
    pub const fn retry_backoff(mut self, policy: BackoffPolicy) -> Self {
        self.retry_backoff = Some(policy);
        self
    }

    /// What the library does when a handler panics. Default:
    /// [`PanicPolicy::Retry`] — synthesize a `JobError::Retry` and route
    /// through the configured backoff; [`PanicPolicy::Dead`] bypasses
    /// the retry budget and goes straight to `mark_dead`.
    ///
    /// **Observable effect**: the terminal status of a panicking handler.
    /// With `Retry`, the row reaches `dead` only after `attempts >=
    /// max_attempts`; with `Dead`, the row is `dead` after the very
    /// first panic.
    ///
    /// See `tests/panic_policy_behavior.rs`.
    #[must_use]
    pub const fn panic_policy(mut self, policy: PanicPolicy) -> Self {
        self.panic_policy = policy;
        self
    }

    /// Time between poll-loop ticks (`claim_batch` invocations).
    /// Default: 1s. Floor: [`crate::limits::MIN_POLL_INTERVAL`] (10ms).
    ///
    /// Below the floor → [`BuildError::PollIntervalTooShort`].
    ///
    /// **Observable effect**: pickup latency. A push committed at
    /// `t = 0` is observable as `status = 'running'` no later than
    /// `t + poll_interval` (modulo permit availability and the
    /// "permits-first" rule — see `Worker::start`).
    ///
    /// See `tests/poll_interval_behavior.rs`.
    #[must_use]
    pub const fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Maximum number of concurrent handlers. Default: number of CPU cores.
    ///
    /// `0` → [`BuildError::ConcurrencyZero`]. Cross-knob: pool
    /// `max_connections >= concurrency × 2 + 2` ([`BuildError::PoolTooSmall`]).
    ///
    /// **Observable effect**: the semaphore capacity gating handler
    /// spawn. The poll loop acquires permits *before* `claim_batch` so
    /// claim count never exceeds free permits (no spawn backlog).
    ///
    /// See `tests/concurrency_behavior.rs` and
    /// `tests/poll_acquires_permits_before_claim.rs`.
    #[must_use]
    pub const fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }

    /// Per-key concurrency limits — `key → max concurrently-running jobs`.
    ///
    /// A job carrying a `concurrency_key` (set via `Pusher::push`) present in
    /// this map is limited to at most `limit` concurrently-running handler
    /// tasks. A `None` key, or a key absent from this map, is unlimited.
    ///
    /// Accumulates across calls; a duplicate key takes the last value (a
    /// `tracing::warn!` is emitted on overwrite immediately, at the point of
    /// the `.concurrency_limits()` call).
    ///
    /// Validated on `build()`: each key `1..=MAX_CONCURRENCY_KEY_LEN`
    /// characters → [`BuildError::ConcurrencyKeyInvalid`]; each limit
    /// `1..=i32::MAX` → [`BuildError::ConcurrencyLimitInvalid`].
    ///
    /// **Single-instance only** — the limit is enforced via an in-process
    /// counter. See the README "Per-key concurrency" section.
    #[must_use]
    pub fn concurrency_limits(
        mut self,
        limits: impl IntoIterator<Item = (String, u32)>,
    ) -> Self {
        for (k, v) in limits {
            if let Some(prev) = self.concurrency_limits.insert(k.clone(), v) {
                tracing::warn!(
                    target: "pgwq.builder",
                    key = %k,
                    previous = prev,
                    replacement = v,
                    "concurrency_limits: duplicate key overwritten",
                );
            }
        }
        self
    }

    /// Per-handler wall-clock timeout. Default: `lease_timeout × 80%`,
    /// clamped to [`crate::limits::MIN_HANDLER_TIMEOUT`] (1s).
    ///
    /// Cross-knob: `handler_timeout + 1s <= lease_timeout`
    /// ([`BuildError::HandlerTimeoutTooLong`]) — `mark_retry` needs
    /// margin to commit before the lease expires.
    ///
    /// **Observable effect**: the deadline at which the library drops
    /// the handler future and routes the row to `mark_retry` with
    /// `reason = "handler_timeout"`. `Stats::timed_out` counts these.
    /// **Cancellation only fires at `.await` points** — CPU-bound work
    /// without yields will not be cancelled; use `spawn_blocking` or
    /// `tokio::task::yield_now()`.
    ///
    /// See `tests/handler_timeout_behavior.rs`.
    #[must_use]
    pub const fn handler_timeout(mut self, d: Duration) -> Self {
        self.handler_timeout = Some(d);
        self
    }

    /// Per-`mark_*` SQL timeout. Default:
    /// `lease_timeout - handler_timeout - 1s`. Floor:
    /// [`crate::limits::MIN_MARK_TIMEOUT`] (100ms).
    /// Ceiling: `lease_timeout - handler_timeout`
    /// ([`BuildError::MarkTimeoutTooLong`]).
    ///
    /// **Observable effect**: under pool starvation a `mark_*` may wait
    /// seconds for a connection; this timeout caps that wait so the
    /// lease can not expire mid-mark (which would fence the worker out
    /// and cause a duplicate side-effect). Trip → `Stats::mark_timed_out`
    /// increments, the row stays `'running'`, and the reaper recovers
    /// it after lease expiry.
    ///
    /// See `tests/shutdown_immediate_with_pool_starvation.rs` for the
    /// pool-starvation regression and `tests/builder_validation.rs` for
    /// the cross-knob validation.
    #[must_use]
    pub const fn mark_timeout(mut self, d: Duration) -> Self {
        self.mark_timeout = Some(d);
        self
    }

    /// Reaper sweep interval — how often the reaper task wakes to flip
    /// stale `running` rows. Default: `lease_timeout / 4`.
    ///
    /// Floor: 1s ([`BuildError::ReaperIntervalTooShort`]).
    /// Ceiling: `lease_timeout / 2` so the reaper ticks at least twice
    /// per lease ([`BuildError::ReaperIntervalTooLong`]).
    ///
    /// **Observable effect**: the worst-case latency between a worker
    /// process crash and the orphaned row becoming claimable again.
    /// The reaper is **only** for process-death recovery (crash, OOM,
    /// network partition); handler-level cancellation goes through
    /// `handler_timeout`, not this knob.
    ///
    /// See `tests/reaper_interval_behavior.rs` and
    /// `tests/reaper_drains_backlog_adaptive.rs`.
    #[must_use]
    pub const fn reaper_interval(mut self, d: Duration) -> Self {
        self.reaper_interval = Some(d);
        self
    }

    /// Plumb an external [`CancellationToken`] so the worker's poll loop,
    /// reaper, and in-flight handlers can be cancelled by a token owned
    /// outside the worker. Default: `None` (a fresh token is created in
    /// [`Worker::start`]).
    ///
    /// **Use case**: spawn `N` workers under one parent token via
    /// `.shutdown_token(parent.child_token())`; a single `parent.cancel()`
    /// then triggers graceful drain across all of them simultaneously.
    ///
    /// **Semantics**: cancellation propagates *down* the `tokio-util`
    /// parent→child tree. [`WorkerHandle::cancel`] /
    /// [`WorkerHandle::shutdown`] still cancel only this worker (firing
    /// the child token does not climb up to siblings). Dropping the
    /// user-side parent does NOT abort this worker — `WorkerState` clones
    /// the token on construction.
    ///
    /// See `tests/shutdown_token_propagates_to_workers.rs`.
    #[must_use]
    pub fn shutdown_token(mut self, token: CancellationToken) -> Self {
        self.shutdown_token = Some(token);
        self
    }

    /// Swap the payload codec. Default: [`JsonCodec`].
    ///
    /// **Observable effect**: how `payload BYTEA` is decoded before
    /// being handed to the handler (and how it is encoded on the push
    /// side via [`crate::Pusher::with_codec`]). The codec must be
    /// symmetric with the pusher's codec for any given queue.
    ///
    /// See `tests/codec_swappable.rs` and
    /// `tests/codec_decode_error_marks_dead.rs`.
    pub fn codec<C2: Codec>(self, codec: C2) -> WorkerBuilder<T, C2, H> {
        WorkerBuilder {
            shutdown_token: self.shutdown_token,
            pool: self.pool,
            queue: self.queue,
            max_attempts: self.max_attempts,
            lease_timeout: self.lease_timeout,
            batch_size: self.batch_size,
            retry_backoff: self.retry_backoff,
            panic_policy: self.panic_policy,
            poll_interval: self.poll_interval,
            concurrency: self.concurrency,
            concurrency_limits: self.concurrency_limits,
            handler_timeout: self.handler_timeout,
            mark_timeout: self.mark_timeout,
            reaper_interval: self.reaper_interval,
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
    /// Job handler. **Required**.
    ///
    /// Accepts any
    /// `Fn(T, JobContext) -> impl Future<Output = Result<(), JobError>>`
    /// that is `Send + Sync + 'static`. The library wraps each call in
    /// `tokio::time::timeout(handler_timeout, _)` and runs it inside a
    /// per-worker `JoinSet` for panic isolation and clean shutdown
    /// cascade.
    ///
    /// **Observable effect**: `Ok(())` routes to `mark_done`;
    /// `Err(JobError::Retry { .. })` routes to `mark_retry` (after
    /// `attempts >= max_attempts` upgrade to `mark_dead`);
    /// `Err(JobError::Abort { .. })` routes to `mark_dead` directly.
    /// A panic is treated according to [`PanicPolicy`].
    ///
    /// `JobContext::idempotency_key` (a `uuidv7`) is **stable across
    /// retries** for the same logical job — use it as the dedup key on
    /// any external side-effect.
    ///
    /// See `tests/worker_tick_once_smoke.rs`,
    /// `tests/at_least_once_semantics.rs` and
    /// `tests/idempotency_key_stable_across_retries.rs`.
    pub fn handler<F, Fut>(self, f: F) -> WorkerBuilder<T, C, Arc<dyn JobHandler<T>>>
    where
        F: Fn(T, JobContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), JobError>> + Send + 'static,
    {
        WorkerBuilder {
            shutdown_token: self.shutdown_token,
            pool: self.pool,
            queue: self.queue,
            max_attempts: self.max_attempts,
            lease_timeout: self.lease_timeout,
            batch_size: self.batch_size,
            retry_backoff: self.retry_backoff,
            panic_policy: self.panic_policy,
            poll_interval: self.poll_interval,
            concurrency: self.concurrency,
            concurrency_limits: self.concurrency_limits,
            handler_timeout: self.handler_timeout,
            mark_timeout: self.mark_timeout,
            reaper_interval: self.reaper_interval,
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
        if queue.is_empty() || queue.chars().count() > MAX_QUEUE_LEN {
            return Err(BuildError::QueueNameInvalid(queue));
        }

        if self.max_attempts == 0 {
            return Err(BuildError::MaxAttemptsZero);
        }

        if self.lease_timeout < MIN_LEASE_TIMEOUT {
            return Err(BuildError::LeaseTimeoutBelowFloor);
        }

        // Reaper floor on lease: with MIN_REAPER_INTERVAL = 1s and the
        // `reaper_interval <= lease_timeout / 2` rule, the lease must be
        // ≥ 2s for ANY valid reaper_interval to exist. Placed AFTER
        // `LeaseTimeoutBelowFloor` so lease < 1s still surfaces the
        // original variant (the existing `lease_timeout_below_floor_rejected`
        // test stays green).
        if self.lease_timeout < MIN_REAPER_INTERVAL.saturating_mul(2) {
            return Err(BuildError::LeaseTimeoutTooShortForReaper {
                lease: self.lease_timeout,
            });
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
        if let Some(min_lease) = self.poll_interval.checked_mul(5)
            && self.lease_timeout < min_lease
        {
            return Err(BuildError::LeaseTimeoutTooShort);
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

        // reaper_interval: default = lease_timeout / 4 (clamped to floor).
        // Constraint: floor MIN_REAPER_INTERVAL (1s) and ceiling
        // lease_timeout / 2 so the reaper ticks at least twice per lease.
        let lease_half = self.lease_timeout / 2;
        let reaper_interval = self.reaper_interval.unwrap_or_else(|| {
            let candidate = self.lease_timeout / 4;
            candidate.max(MIN_REAPER_INTERVAL)
        });
        if reaper_interval < MIN_REAPER_INTERVAL {
            return Err(BuildError::ReaperIntervalTooShort);
        }
        if reaper_interval > lease_half {
            return Err(BuildError::ReaperIntervalTooLong {
                actual: reaper_interval,
                max: lease_half,
            });
        }

        // Backoff policy: validate user-supplied policy or fall back to the
        // library default (already valid). Default is never invalid — but call
        // `validate()` for safety in case someone changes the default impl.
        let retry_backoff = self.retry_backoff.unwrap_or_default();
        retry_backoff.validate()?;

        // Per-key concurrency limits validation.
        for (key, &limit) in &self.concurrency_limits {
            let key_len = key.chars().count();
            if key_len == 0 || key_len > MAX_CONCURRENCY_KEY_LEN {
                return Err(BuildError::ConcurrencyKeyInvalid(key.clone()));
            }
            if limit == 0 || limit > i32::MAX as u32 {
                return Err(BuildError::ConcurrencyLimitInvalid {
                    key: key.clone(),
                    limit,
                });
            }
        }

        Ok(Worker {
            pool,
            queue,
            max_attempts: self.max_attempts,
            lease_timeout: self.lease_timeout,
            batch_size: self.batch_size,
            retry_backoff,
            panic_policy: self.panic_policy,
            poll_interval: self.poll_interval,
            concurrency,
            concurrency_limits: self.concurrency_limits,
            handler_timeout,
            mark_timeout,
            reaper_interval,
            shutdown_token: self.shutdown_token,
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
    retry_backoff: BackoffPolicy,
    panic_policy: PanicPolicy,
    poll_interval: Duration,
    concurrency: usize,
    concurrency_limits: HashMap<String, u32>,
    handler_timeout: Duration,
    mark_timeout: Duration,
    reaper_interval: Duration,
    shutdown_token: Option<CancellationToken>,
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
            .field("retry_backoff", &self.retry_backoff)
            .field("panic_policy", &self.panic_policy)
            .field("poll_interval", &self.poll_interval)
            .field("concurrency", &self.concurrency)
            .field("handler_timeout", &self.handler_timeout)
            .field("mark_timeout", &self.mark_timeout)
            .field("reaper_interval", &self.reaper_interval)
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
            &std::collections::HashMap::new(),
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
                self.retry_backoff,
                self.poll_interval,
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
            retry_backoff: self.retry_backoff,
            panic_policy: self.panic_policy,
            batch_size: batch_size_usize,
            poll_interval: self.poll_interval,
            reaper_interval: self.reaper_interval,
            semaphore: Arc::new(Semaphore::new(self.concurrency)),
            concurrency_running: {
                // No DB seed — the counter tracks THIS process's live handler
                // tasks, which is zero at startup. `running` rows left by a
                // crashed prior process are dead ghosts that consume nothing;
                // the reaper reclaims them. Every configured key is
                // initialized to 0 so headroom/acquire never hit a missing key.
                let initial: HashMap<String, u32> =
                    self.concurrency_limits.keys().map(|k| (k.clone(), 0)).collect();
                Arc::new(std::sync::Mutex::new(initial))
            },
            concurrency_limits: self.concurrency_limits,
            shutdown: self.shutdown_token.unwrap_or_default(),
            tasks: Mutex::new(JoinSet::new()),
            stats: AtomicStats::default(),
            last_fatal: OnceLock::new(),
            last_panic_escalation: OnceLock::new(),
        });

        let state_for_loop = state.clone();
        let poll_join: JoinHandle<()> = tokio::spawn(poll_loop(state_for_loop));
        let poll_abort = poll_join.abort_handle();

        let state_for_reaper = state.clone();
        let reaper_join: JoinHandle<()> =
            tokio::spawn(crate::reaper::reaper_loop(state_for_reaper));
        let reaper_abort = reaper_join.abort_handle();

        Ok(WorkerHandle {
            inner: Some(WorkerHandleInner {
                state,
                poll_join,
                poll_abort,
                reaper_join,
                reaper_abort,
            }),
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
    pub(crate) retry_backoff: BackoffPolicy,
    pub(crate) panic_policy: PanicPolicy,
    pub(crate) batch_size: usize,
    pub(crate) poll_interval: Duration,
    pub(crate) reaper_interval: Duration,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) concurrency_limits: HashMap<String, u32>,
    pub(crate) concurrency_running: crate::key_slot::ConcurrencyCounter,
    pub(crate) shutdown: CancellationToken,
    pub(crate) tasks: Mutex<JoinSet<()>>,
    pub(crate) stats: AtomicStats,
    pub(crate) last_fatal: OnceLock<Arc<sqlx::Error>>,
    /// Set by the reaper once `REAPER_PANIC_ESCALATION_THRESHOLD` consecutive
    /// panic ticks have fired and shutdown was forced. Used by
    /// `WorkerHandle::{join, shutdown}` to surface
    /// `ShutdownError::ReaperPanicEscalation`. `Fatal` takes precedence if
    /// both are set (e.g., a fatal SQL error landed inside the same loop).
    pub(crate) last_panic_escalation: OnceLock<u32>,
}

/// Handle returned by [`Worker::start`].
///
/// Two terminal once-only methods are exposed. Both consume `self`, so
/// double-shutdown is statically prevented (the `ShutdownError::AlreadyShutdown`
/// variant is reserved for future API revisions).
///
/// - [`WorkerHandle::join`] — wait for natural exit (cancel + drain). No
///   timeout; no Stats. Kept for back-compat with Faza 4 surface.
/// - [`WorkerHandle::shutdown`] — preferred for production. Bounded shutdown
///   with hard-abort cascade + best-effort `Stats` snapshot.
///
/// **Drop safety**: if the handle is dropped *without* calling `shutdown` /
/// `join` / `cancel`, the `Drop` impl fires the cancellation token and
/// aborts the poll / reaper tasks. Without that defense a dropped handle
/// would silently leak both background loops — the classic tokio footgun
/// where dropping a `JoinHandle` *detaches* rather than aborting. Each
/// leaked loop holds an `Arc<WorkerState>` and a pool connection. Explicit
/// `shutdown` is still the production-recommended path because `Drop` is
/// necessarily best-effort (sync, no `await`, no Stats).
pub struct WorkerHandle {
    // Option<_> wrap: `shutdown` / `join` consume the inner via `.take()`
    // so they can move owned `JoinHandle`s into local locals (Rust forbids
    // partial moves out of a `&mut self` that has a `Drop` impl). After a
    // method consumed inner, `Drop` runs on an `inner = None` and short-
    // -circuits — see the `Drop` impl below.
    inner: Option<WorkerHandleInner>,
}

struct WorkerHandleInner {
    state: Arc<dyn WorkerStateOps>,
    poll_join: JoinHandle<()>,
    poll_abort: AbortHandle,
    reaper_join: JoinHandle<()>,
    reaper_abort: AbortHandle,
}

impl std::fmt::Debug for WorkerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHandle").finish_non_exhaustive()
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // Reached only when the handle was dropped without calling
            // `shutdown` / `join` (those `.take()` `inner` themselves and
            // leave `None` here). Best-effort: cancel the shared token and
            // abort both background tasks. JoinHandles drop unawaited
            // (detach), but the tasks were already told to stop, so they
            // exit at their next `.await` or via the `abort()` we just
            // fired. No drain, no Stats — that's what explicit `shutdown`
            // is for.
            inner.state.cancel_shutdown();
            inner.poll_abort.abort();
            inner.reaper_abort.abort();
        }
    }
}

impl WorkerHandle {
    /// Trigger shutdown signal. Poll loop exits at the next `.await` point
    /// (cancellation is intercepted via `tokio::select!`). Idempotent.
    ///
    /// Pairs with [`WorkerHandle::join`] for the no-timeout, no-Stats path.
    /// For production, prefer [`WorkerHandle::shutdown`] (atomic
    /// cancel + bounded drain + Stats).
    pub fn cancel(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.state.cancel_shutdown();
        }
    }

    /// Await poll loop + reaper loop completion, then drain in-flight handlers.
    ///
    /// Drains the local handler `JoinSet` until empty. Faza 7 adds two
    /// new error variants:
    ///   - [`ShutdownError::Fatal`] — fatal `sqlx::Error` in poll or reaper.
    ///   - [`ShutdownError::ReaperPanicEscalation`] — reaper hit the panic
    ///     threshold (Faza 5 surface gap is closed here).
    ///
    /// `Fatal` takes precedence over `ReaperPanicEscalation` if both fire.
    ///
    /// # Errors
    /// See above.
    pub async fn join(mut self) -> Result<(), ShutdownError> {
        // `inner` is always `Some` for a fresh handle. If a future refactor
        // ever splits ownership and reaches here twice, the defensive `else`
        // returns `Ok(())` rather than panicking (CLAUDE.md: panic-deny).
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };

        // 1) Await poll loop natural exit (already cancelled or self-shut).
        let _ = inner.poll_join.await;

        // 2) Await reaper loop natural exit. Single shared CancellationToken
        //    in WorkerState wakes both — by the time poll_join completes the
        //    reaper has either also seen the cancel, or will see it now.
        let _ = inner.reaper_join.await;

        // 3) Drain handler JoinSet. Each handle_job task is fire-and-forget;
        //    we wait until JoinSet is empty so all mark_* commits have a
        //    chance to land.
        inner.state.drain_handlers().await;

        // 4) Surface fatal poll-loop / reaper error if any.
        if let Some(fatal) = inner.state.last_fatal_snapshot() {
            return Err(ShutdownError::Fatal(fatal));
        }
        // 5) Reaper panic escalation — Phase 5 surface gap closure.
        if let Some(consecutive_panics) = inner.state.last_panic_escalation_snapshot() {
            return Err(ShutdownError::ReaperPanicEscalation { consecutive_panics });
        }
        Ok(())
    }

    /// Bounded shutdown — preferred for production. Returns a [`Stats`]
    /// snapshot covering the worker's lifetime.
    ///
    /// Sequence (PLAN.md §"Shutdown semantics"):
    /// 1. **Soft cancel** — fire the cancellation token. Poll / reaper
    ///    `select!` branches exit at their next `.await` point.
    /// 2. **Soft drain (`timeout/2`)** — await poll + reaper `JoinHandle`s.
    ///    Under normal load both exit in single-digit ms.
    /// 3. **Hard abort poll / reaper** — `AbortHandle::abort()` defends
    ///    against a hung SQL future under pool starvation.
    /// 4. **Handler drain (remaining timeout)** — wait for in-flight
    ///    handlers to finish naturally up to the deadline.
    /// 5. **Hard abort handlers** — `JoinSet::abort_all` + short bounded
    ///    drain (cancellation cascade per Anti-pattern #12). Each in-flight
    ///    handler at this point is counted in `stats.aborted`.
    /// 6. **`pending_recovery` count** — `SELECT count(*) WHERE status =
    ///    'running'` for our queue, under a 500ms timeout (best-effort).
    /// 7. **Build Stats** + surface terminal errors.
    ///
    /// # Errors
    /// - [`ShutdownError::Fatal`] — fatal `sqlx::Error` in poll or reaper.
    /// - [`ShutdownError::ReaperPanicEscalation`] — reaper hit the panic
    ///   threshold.
    ///
    /// `Fatal` wins over `ReaperPanicEscalation` if both are set. On the
    /// happy path Stats is returned even if the soft-drain stage exhausted
    /// the timeout — Step 5 always completes synchronously after `abort_all`.
    pub async fn shutdown(mut self, timeout: Duration) -> Result<Stats, ShutdownError> {
        // Take inner so we can move owned `JoinHandle`s into the soft-drain
        // block below. After this point `self.inner` is `None`; the `Drop`
        // impl short-circuits when the handle is finally dropped at end of
        // scope.
        let Some(inner) = self.inner.take() else {
            return Ok(Stats::default());
        };

        // Capture absolute deadline up-front so each phase honors a single
        // wall-clock budget.
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;

        // 1) Soft cancel.
        inner.state.cancel_shutdown();

        // 2) Soft drain (timeout/2) on poll + reaper.
        let half = timeout / 2;
        let _ = tokio::time::timeout(half, async {
            let _ = inner.poll_join.await;
            let _ = inner.reaper_join.await;
        })
        .await;

        // 3) Hard abort poll + reaper. Cheap idempotent — both ignore the
        //    abort if the task already exited.
        inner.poll_abort.abort();
        inner.reaper_abort.abort();

        // 4) Handler drain up to overall deadline (may be Instant::now()
        //    if step 2 burned the whole budget — that's fine; the call
        //    short-circuits via `timeout_at`).
        let _drained_clean = inner.state.drain_handlers_until(deadline).await;

        // 5) Hard abort remaining handlers — cascade via JoinSet drop +
        //    abort_all. We attribute each remaining task to `aborted`.
        //    Cap the post-abort wait at 1s so a non-cooperating handler
        //    (theoretical: blocking syscall) can't extend shutdown.
        let _aborted_now = inner
            .state
            .abort_remaining_handlers(Duration::from_secs(1))
            .await;

        // 6) pending_recovery — best-effort DB lookup under a small timeout.
        let pending_recovery = inner
            .state
            .count_pending_recovery(Duration::from_millis(500))
            .await;

        // 7) Build Stats + classify terminal errors.
        let mut stats = inner.state.snapshot_stats();
        stats.pending_recovery = pending_recovery;

        tracing::info!(
            target: "pgwq.shutdown",
            completed = stats.completed,
            failed = stats.failed,
            timed_out = stats.timed_out,
            mark_timed_out = stats.mark_timed_out,
            aborted = stats.aborted,
            fenced_out = stats.fenced_out,
            pending_recovery = stats.pending_recovery,
            elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            "worker shutdown complete"
        );

        if let Some(fatal) = inner.state.last_fatal_snapshot() {
            return Err(ShutdownError::Fatal(fatal));
        }
        if let Some(consecutive_panics) = inner.state.last_panic_escalation_snapshot() {
            return Err(ShutdownError::ReaperPanicEscalation { consecutive_panics });
        }
        Ok(stats)
    }
}

/// Type-erased view of `WorkerState<T, C>` used by `WorkerHandle` so the
/// handle itself doesn't need the `T, C` generics.
trait WorkerStateOps: Send + Sync {
    fn cancel_shutdown(&self);
    fn last_fatal_snapshot(&self) -> Option<Arc<sqlx::Error>>;
    fn last_panic_escalation_snapshot(&self) -> Option<u32>;
    fn drain_handlers<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    /// Drain `state.tasks` `JoinSet` via `tokio::time::timeout_at(deadline, _)`,
    /// returning `true` if the `JoinSet` was fully drained before the deadline
    /// fired. Each successfully drained handler is already handled inside
    /// `handle_job` (panic / cancel branches) so no per-task counting is done
    /// here.
    fn drain_handlers_until<'a>(
        &'a self,
        deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
    /// Bump `aborted` by the remaining in-flight handler count, then
    /// `abort_all` and drain (each abort-cancelled task is one already
    /// counted). Returns the number of aborts attributed.
    fn abort_remaining_handlers<'a>(
        &'a self,
        grace: Duration,
    ) -> Pin<Box<dyn Future<Output = u64> + Send + 'a>>;
    /// Snapshot the running counters into a `Stats`. `pending_recovery` is
    /// left at `0` — the caller fills it in via [`Self::count_pending_recovery`].
    fn snapshot_stats(&self) -> Stats;
    /// Count rows still in `status = 'running'` for our queue at this moment.
    /// Wrapped in `tokio::time::timeout(query_timeout, _)` so a dead pool can
    /// not extend shutdown indefinitely. On error / timeout returns `Ok(0)`
    /// and logs a warn — Stats is best-effort per PLAN.md §"Shutdown semantics".
    fn count_pending_recovery<'a>(
        &'a self,
        query_timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = u64> + Send + 'a>>;
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

    fn last_panic_escalation_snapshot(&self) -> Option<u32> {
        self.last_panic_escalation.get().copied()
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

    fn drain_handlers_until<'a>(
        &'a self,
        deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            let mut guard = self.tasks.lock().await;
            loop {
                if guard.is_empty() {
                    return true;
                }
                match tokio::time::timeout_at(deadline, guard.join_next()).await {
                    Ok(Some(_res)) => continue,
                    // JoinSet returned None — set is empty.
                    Ok(None) => return true,
                    Err(_elapsed) => return false,
                }
            }
        })
    }

    fn abort_remaining_handlers<'a>(
        &'a self,
        grace: Duration,
    ) -> Pin<Box<dyn Future<Output = u64> + Send + 'a>> {
        Box::pin(async move {
            let mut guard = self.tasks.lock().await;
            if guard.is_empty() {
                return 0;
            }
            // First, opportunistically drain any handlers that finished BETWEEN
            // the soft-drain's deadline elapse and now (mutex was released
            // briefly during the deadline check). These already-finished tasks
            // updated their own `completed`/`failed`/etc. counters and must
            // NOT be folded into `aborted` (double-count race; reviewer I1).
            // try_join_next() is non-blocking: it returns ready completions
            // without yielding to a not-yet-finished task.
            while guard.try_join_next().is_some() {}
            // Now the remaining set is exactly the in-flight tasks. abort_all
            // them and count cancelled outcomes during the bounded drain.
            let still_pending_before_abort = guard.len() as u64;
            if still_pending_before_abort == 0 {
                return 0;
            }
            guard.abort_all();
            let mut aborted_count: u64 = 0;
            let grace_deadline = tokio::time::Instant::now() + grace;
            loop {
                if guard.is_empty() {
                    break;
                }
                match tokio::time::timeout_at(grace_deadline, guard.join_next()).await {
                    Ok(Some(Err(je))) if je.is_cancelled() => {
                        aborted_count = aborted_count.saturating_add(1);
                    }
                    // Other outcomes (panic, or rare Ok — task happened to
                    // finish exactly when abort fired): NOT counted as
                    // aborted. Their per-handler completion path already
                    // bumped the appropriate counter (panic → routed via
                    // PanicPolicy; Ok → completed via flip_done_state).
                    Ok(Some(_)) => continue,
                    Ok(None) | Err(_) => break,
                }
            }
            if aborted_count > 0 {
                self.stats
                    .aborted
                    .fetch_add(aborted_count, Ordering::Relaxed);
            }
            aborted_count
        })
    }

    fn snapshot_stats(&self) -> Stats {
        self.stats.snapshot()
    }

    fn count_pending_recovery<'a>(
        &'a self,
        query_timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = u64> + Send + 'a>> {
        Box::pin(async move {
            let q = sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pgwq.jobs WHERE queue = $1 AND status = 'running'",
            )
            .bind(&self.queue)
            .fetch_one(&self.pool);
            match tokio::time::timeout(query_timeout, q).await {
                Ok(Ok(n)) => u64::try_from(n).unwrap_or(0),
                Ok(Err(e)) => {
                    tracing::event!(
                        target: "pgwq.shutdown.pending_recovery_failed",
                        tracing::Level::WARN,
                        worker.id = %self.worker_id,
                        queue = %self.queue,
                        error = %e,
                        "pending_recovery query failed; reporting 0 (best-effort)"
                    );
                    0
                }
                Err(_elapsed) => {
                    tracing::event!(
                        target: "pgwq.shutdown.pending_recovery_timed_out",
                        tracing::Level::WARN,
                        worker.id = %self.worker_id,
                        queue = %self.queue,
                        timeout_ms = u64::try_from(query_timeout.as_millis()).unwrap_or(u64::MAX),
                        "pending_recovery query timed out; reporting 0 (best-effort)"
                    );
                    0
                }
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

    // W#5 — consecutive non-fatal claim errors. After `MAX_CONSECUTIVE_CLAIM_
    // ERRORS` ticks fail in a row, the worker self-shuts with the most recent
    // error as `last_fatal`, surfacing through `WorkerHandle::{join, shutdown}`
    // as `ShutdownError::Fatal`. Reset on any `Ok(_)` from `claim_and_decode`.
    let mut consecutive_claim_errors: u32 = 0;

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
        // `headroom` is bound here (not inline) so the temporary outlives the
        // `select!` future.
        //
        // Per-tick headroom snapshot: limit − live-task count, per configured
        // key. The map always contains every configured key (headroom >= 0).
        // Poisoned mutex (unreachable in practice — counter critical sections are
        // panic-free, so the mutex cannot actually be poisoned): fall back to
        // empty headroom map, which skips per-key limit enforcement for this tick
        // (jobs may be claimed past their per-key concurrency limit) and lets
        // claim_batch proceed on the global semaphore headroom alone.
        let headroom: std::collections::HashMap<String, u32> = state
            .concurrency_running
            .lock()
            .map_or_else(
                |_| std::collections::HashMap::new(),
                |counts| {
                    state
                        .concurrency_limits
                        .iter()
                        .map(|(k, &limit)| {
                            let used = counts.get(k).copied().unwrap_or(0);
                            (k.clone(), limit.saturating_sub(used))
                        })
                        .collect()
                },
            );
        let claim_result = tokio::select! {
            r = crate::claim::claim_and_decode::<T, C>(
                &state.pool,
                &state.codec,
                &state.queue,
                want_u32,
                state.lease_timeout,
                state.max_attempts,
                &headroom,
            ) => r,
            () = state.shutdown.cancelled() => break,
        };

        match claim_result {
            Ok(rows) if rows.is_empty() => {
                consecutive_claim_errors = 0;
                tick_span.record("claimed", 0u64);
                // Permits drop at end of iteration — return slots.
            }
            Ok(rows) => {
                consecutive_claim_errors = 0;
                let n = rows.len();
                tick_span.record("claimed", n as u64);
                tracing::debug!(claimed = n, wanted = want, "batch claimed");
                // CRITICAL invariant: rows.len() ≤ permits.len() (claim
                // respects LIMIT). Each row pairs with a permit; surplus
                // permits drop at end of scope. `into_iter()` on permits
                // (not `drain`) — the Vec is consumed entirely here.
                let mut tasks = state.tasks.lock().await;
                for (row, permit) in rows.into_iter().zip(permits) {
                    let s = state.clone();
                    let slot = match &row.concurrency_key {
                        Some(k) if state.concurrency_limits.contains_key(k) => {
                            crate::key_slot::KeySlotGuard::acquire(
                                state.concurrency_running.clone(),
                                k.clone(),
                            )
                        }
                        _ => crate::key_slot::KeySlotGuard::none(),
                    };
                    tasks.spawn(handle_job(row, s, permit, slot));
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
                consecutive_claim_errors = consecutive_claim_errors.saturating_add(1);
                if consecutive_claim_errors >= crate::limits::MAX_CONSECUTIVE_CLAIM_ERRORS {
                    tracing::error!(
                        worker.id = %state.worker_id,
                        consecutive_errors = consecutive_claim_errors,
                        threshold = crate::limits::MAX_CONSECUTIVE_CLAIM_ERRORS,
                        error = %e,
                        "claim batch hit consecutive-error threshold; escalating to fatal"
                    );
                    let _ = state.last_fatal.set(Arc::new(e));
                    state.shutdown.cancel();
                    break;
                }
                tracing::warn!(
                    worker.id = %state.worker_id,
                    consecutive_errors = consecutive_claim_errors,
                    threshold = crate::limits::MAX_CONSECUTIVE_CLAIM_ERRORS,
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
async fn handle_job<T, C>(
    job: Job<T>,
    state: Arc<WorkerState<T, C>>,
    _permit: OwnedSemaphorePermit,
    // _slot: held for the task's lifetime; its Drop decrements the per-key concurrency counter on
    // every exit (normal return, panic, or shutdown abort).
    _slot: crate::key_slot::KeySlotGuard,
) where
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
        // Inner task panic or cancellation. Faza 6 routes via PanicPolicy:
        // - PanicPolicy::Retry → synthesize `JobError::Retry` + backoff;
        // - PanicPolicy::Dead → bypass retry budget; flip dead immediately.
        Some(Err(je)) if je.is_panic() => {
            // Trim panic reason ONCE at the top — both Retry and Dead
            // branches use the same value. apply_handler_result_state will
            // re-trim downstream (no-op since the value is already at or
            // below the truncation limit).
            let msg = crate::reaper::extract_panic_message(je);
            let trimmed_reason = fmt_err_trimmed(&DisplayStr(format!("panic: {msg}")));
            match state.panic_policy {
                PanicPolicy::Retry => {
                    tracing::error!(
                        worker.id = %state.worker_id,
                        job.id = id,
                        job.public_id = %public_id,
                        panic = %msg,
                        policy = "retry",
                        "handler panicked; routing through mark_retry"
                    );
                    let synthesized: Result<(), JobError> = Err(JobError::Retry {
                        reason: trimmed_reason,
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
                PanicPolicy::Dead => {
                    tracing::error!(
                        worker.id = %state.worker_id,
                        job.id = id,
                        job.public_id = %public_id,
                        panic = %msg,
                        policy = "dead",
                        "handler panicked; flipping row to dead (bypass retry budget)"
                    );
                    flip_dead_state(
                        &state,
                        identity,
                        &queue,
                        public_id,
                        id,
                        lease_token,
                        attempts,
                        &trimmed_reason,
                    )
                    .await;
                }
            }
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
                let delay = compute_retry_delay(
                    retry_in,
                    state.retry_backoff,
                    state.poll_interval,
                    attempts,
                );
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
    let res = tokio::time::timeout(
        state.mark_timeout,
        mark_retry(&state.pool, id, lease_token, reason, delay),
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
    retry_backoff: BackoffPolicy,
    poll_interval: Duration,
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
                let delay = compute_retry_delay(retry_in, retry_backoff, poll_interval, attempts);
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

/// Compute the actual `mark_retry` delay given (optional) user override,
/// the worker's `BackoffPolicy`, current `poll_interval`, and current
/// `attempts` count.
///
/// If `retry_in.is_some()` → clamp to `[max(poll_interval, MIN_RETRY_IN), MAX_RETRY_IN]`;
/// emit `tracing::warn!` (target = `"pgwq.retry_in.clamped"`) when the input
/// is outside the band and we had to clamp.
///
/// If `retry_in.is_none()` → defer to `backoff.next(attempts)`.
fn compute_retry_delay(
    retry_in: Option<Duration>,
    backoff: BackoffPolicy,
    poll_interval: Duration,
    attempts: u32,
) -> Duration {
    retry_in.map_or_else(
        || backoff.next(attempts),
        |requested| {
            let floor = poll_interval.max(MIN_RETRY_IN);
            let ceiling = MAX_RETRY_IN;
            let clamped = requested.clamp(floor, ceiling);
            if clamped != requested {
                tracing::warn!(
                    target: "pgwq.retry_in.clamped",
                    requested_ms = u64::try_from(requested.as_millis()).unwrap_or(u64::MAX),
                    applied_ms = u64::try_from(clamped.as_millis()).unwrap_or(u64::MAX),
                    "retry_in clamped"
                );
            }
            clamped
        },
    )
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
    match mark_retry(pool, id, lease_token, reason, delay).await {
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
/// Fatal:
/// - sqlx-level: `PoolClosed`, `WorkerCrashed`, `Configuration(_)`,
///   `Migrate(_)`, `ColumnDecode { .. }`, `Decode(_)`, `TypeNotFound { .. }`,
///   `ColumnNotFound(_)`, `Protocol(_)`.
/// - `Database(_)` with SQLSTATE that signals a server-side state the
///   worker cannot make progress against by retrying:
///   - class `42` — syntax / access rule (missing table/column, missing
///     function, undefined object). Schema drift or a buggy library
///     query; retrying will loop forever.
///   - class `0A` — feature not supported.
///   - class `23` — integrity constraint violation (CHECK, FK, NOT
///     NULL, unique). Our state-machine CHECK is the canonical
///     trigger; library bug, not transient.
///   - `57P01` / `57P02` / `57P03` — admin/crash shutdown,
///     `cannot_connect_now`. The session is gone; pool will rebuild,
///     but treating these as transient masks operator-initiated
///     shutdowns. Caller should self-shut and let the supervisor
///     decide restart.
///
/// Transient (returns false): everything else under `Database(_)`,
/// `Io(_)`, `Tls(_)`, `PoolTimedOut`.
#[doc(hidden)]
#[must_use]
pub fn is_fatal_sqlx(e: &sqlx::Error) -> bool {
    if matches!(
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
    ) {
        return true;
    }
    if let sqlx::Error::Database(db) = e
        && let Some(code) = db.code()
    {
        let c: &str = code.as_ref();
        // Class prefixes that signal fatal state.
        if c.starts_with("42") || c.starts_with("0A") || c.starts_with("23") {
            return true;
        }
        // Specific admin/crash shutdown SQLSTATEs.
        if matches!(c, "57P01" | "57P02" | "57P03") {
            return true;
        }
    }
    false
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
