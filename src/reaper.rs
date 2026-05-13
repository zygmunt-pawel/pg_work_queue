//! Reaper — sweeps stale `running` rows whose `lease_expires_at < now()` and
//! flips them back into `awaiting_retry` (or `dead` if `attempts >=
//! max_attempts`). Single-CTE SQL, no race window. See PLAN.md §"Reaper
//! (single-CTE, no race window)" (lines 722-852) and §"Reaper panic recovery"
//! (lines 854-928).
//!
//! Spawn'ed parallel z poll loop w `Worker::start`. Sharing one `WorkerState`
//! with `poll`/`handle_job`: shutdown signal, pool, queue name. Per-tick
//! panic isolation via `tokio::spawn` + `JoinError::is_panic()` + threshold
//! escalation (PLAN.md Anti-pattern #11).
//
// `unreachable_pub` is a false-positive here: the module itself is
// `pub(crate)`, so any `pub` items inside are crate-internal.
// `redundant_pub_crate` fires on `pub(crate)` inside that same module —
// switch to `pub` and silence `unreachable_pub` locally (matches transition.rs).
#![allow(unreachable_pub)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sqlx::{PgPool, Row};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::limits::REAPER_BATCH_SIZE;
use crate::transition::{TransitionCtx, TransitionSource, WorkerIdentity, emit_transition};
use crate::worker::WorkerState;

/// Threshold of consecutive reaper tick panics before the worker
/// self-shuts. Loud failure path — surfaces via `WorkerHandle::join`'s
/// `ShutdownError::Fatal` is NOT triggered here (no `last_fatal` set, since
/// no `sqlx::Error` is available). Threshold reached → shutdown is cancelled
/// and `reaper_loop` returns; `WorkerHandle::join()` will complete normally
/// but the worker is no longer processing.
pub const REAPER_PANIC_ESCALATION_THRESHOLD: u32 = 3;

/// Minimum builder-allowed `reaper_interval`. Reaper hammering DB faster than
/// this gives no operational value and competes with normal traffic.
pub const MIN_REAPER_INTERVAL: Duration = Duration::from_secs(1);

/// Test-only counter — when > 0, the next reap tick panics and decrements.
/// Tests bump this to exercise panic recovery in a deterministic way.
/// In production callers never touch this (it stays 0 forever); the cost
/// is one `Relaxed` load per tick.
#[doc(hidden)]
pub static REAPER_PANIC_INJECTIONS: AtomicU32 = AtomicU32::new(0);

/// One reaped row's pre-transition shape. `status` is the **post-update**
/// destination (`"awaiting_retry"` or `"dead"`) so callers can route the
/// transition event without re-reading the row.
#[derive(Debug, Clone)]
pub struct ReapedRow {
    /// Internal BIGINT PK.
    pub id: i64,
    /// External job handle (uuidv7).
    pub public_id: Uuid,
    /// Post-update destination status (`"awaiting_retry"` or `"dead"`).
    pub status: String,
    /// 1-indexed attempt count at the time of reaping.
    pub attempts: u32,
}

/// Run the reaper SQL once. Returns the rows that were flipped from
/// `running` → `awaiting_retry` / `dead`. Empty `Vec` means "no stale rows".
///
/// SQL verbatim per PLAN.md §"Reaper (single-CTE, no race window)".
///
/// # Errors
/// Propagates `sqlx::Error` from the query (caller decides fatal-vs-warn).
pub async fn reap(
    pool: &PgPool,
    queue: &str,
    batch_limit: usize,
) -> Result<Vec<ReapedRow>, sqlx::Error> {
    // i32 is the wire type for PG INTEGER. Clamp to i32::MAX so an oversized
    // `batch_limit` (impossible in practice — REAPER_BATCH_SIZE is a const)
    // does not panic-via-try_from in src/.
    let i32_max_usize: usize = i32::MAX as usize;
    let limit: i32 = i32::try_from(batch_limit.min(i32_max_usize)).unwrap_or(i32::MAX);

    let rows = sqlx::query(
        "WITH stale AS (
             SELECT id, attempts, max_attempts FROM pgwq.jobs
             WHERE queue = $1
               AND status = 'running'
               AND lease_expires_at < now()
             ORDER BY lease_expires_at
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )
         UPDATE pgwq.jobs j
         SET status = CASE
                 WHEN s.attempts >= s.max_attempts THEN 'dead'::pgwq.job_status
                 ELSE 'awaiting_retry'::pgwq.job_status
             END,
             finished_at = CASE
                 WHEN s.attempts >= s.max_attempts THEN now()
                 ELSE NULL
             END,
             last_error = CASE
                 WHEN s.attempts >= s.max_attempts THEN 'lease_expired_max_attempts'
                 ELSE 'lease_expired'
             END,
             lease_token = NULL,
             lease_expires_at = NULL
         FROM stale s
         WHERE j.id = s.id
           AND j.status = 'running'
         RETURNING j.id, j.public_id, j.status::text AS status, j.attempts",
    )
    .bind(queue)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let attempts_i32: i32 = r.try_get("attempts")?;
        out.push(ReapedRow {
            id: r.try_get("id")?,
            public_id: r.try_get("public_id")?,
            status: r.try_get("status")?,
            // attempts is CHECK >= 0 → non-negative i32 fits in u32.
            attempts: u32::try_from(attempts_i32).unwrap_or(0),
        });
    }
    Ok(out)
}

/// Extract a human-readable message from a `tokio::task::JoinError` raised
/// by a panicking spawned task. `panic!` macro payloads are most commonly
/// `&'static str` (format-less) or `String` (formatted); anything else →
/// fallback string. Per PLAN.md §"`extract_panic_message`".
pub fn extract_panic_message(je: tokio::task::JoinError) -> String {
    je.try_into_panic().map_or_else(
        |_| "<task cancelled before panic>".to_string(),
        |payload| match payload.downcast::<&'static str>() {
            Ok(s) => (*s).to_string(),
            Err(payload) => payload
                .downcast::<String>()
                .map_or_else(|_| "<unknown panic payload>".to_string(), |s| *s),
        },
    )
}

/// Spawned reaper task body. Ticks at `state.reaper_interval` (adaptive:
/// when a tick returns a full [`REAPER_BATCH_SIZE`] batch, the next tick
/// skips the `ticker.tick()` to drain backlog at SQL speed). Per-tick panic
/// isolation via `tokio::spawn`; after K consecutive panics the worker
/// self-shuts (PLAN.md Anti-pattern #11).
#[allow(clippy::too_many_lines)]
pub async fn reaper_loop<T, C>(state: Arc<WorkerState<T, C>>)
where
    T: Send + 'static,
    C: Send + Sync + 'static,
{
    let mut ticker = tokio::time::interval(state.reaper_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut skip_next_tick = false;
    let mut consecutive_panics: u32 = 0;

    loop {
        if !skip_next_tick {
            tokio::select! {
                _ = ticker.tick() => {},
                () = state.shutdown.cancelled() => return,
            }
        }
        skip_next_tick = false;

        // Wrap each tick in tokio::spawn for panic isolation (Anti-pattern
        // #11). The handle is awaited inside a shutdown-aware select so
        // pool starvation doesn't block cancel.
        let pool = state.pool.clone();
        let queue = state.queue.clone();
        let mut tick_task: JoinHandle<Result<Vec<ReapedRow>, sqlx::Error>> =
            tokio::spawn(async move {
                // Test-only panic injection — production callers leave the
                // static at 0 (one Relaxed load per tick).
                #[allow(clippy::panic)]
                {
                    let pending = REAPER_PANIC_INJECTIONS.load(Ordering::Relaxed);
                    if pending > 0 {
                        REAPER_PANIC_INJECTIONS.fetch_sub(1, Ordering::Relaxed);
                        panic!("reaper test panic injection");
                    }
                }
                reap(&pool, &queue, REAPER_BATCH_SIZE).await
            });

        // We poll `&mut tick_task` (not move) so that on the cancel arm
        // we still own the `JoinHandle` and can `.abort()` it. Dropping
        // a `JoinHandle` does NOT cancel the task in tokio — without
        // the explicit abort the spawned `reap()` future would outlive
        // `reaper_loop` and, under pool starvation, could hold a pool
        // connection up to `acquire_timeout` past `WorkerHandle::shutdown`
        // returning. The follow-up `await` waits for the cancellation
        // to actually take effect (yields at the next `pool.acquire()`).
        let outcome = tokio::select! {
            r = &mut tick_task => r,
            () = state.shutdown.cancelled() => {
                tick_task.abort();
                let _ = tick_task.await;
                return;
            }
        };

        match outcome {
            Ok(Ok(reaped)) => {
                consecutive_panics = 0;
                if reaped.len() >= REAPER_BATCH_SIZE {
                    skip_next_tick = true;
                }
                if !reaped.is_empty() {
                    let dead_count = reaped.iter().filter(|r| r.status == "dead").count();
                    let retry_count = reaped.len() - dead_count;
                    tracing::warn!(
                        worker.id = %state.worker_id,
                        queue = %state.queue,
                        reaped_total = reaped.len(),
                        reaped_dead = dead_count,
                        reaped_retry = retry_count,
                        backlog_continues = skip_next_tick,
                        "stale jobs reaped"
                    );
                    let identity = WorkerIdentity(state.worker_id);
                    for row in &reaped {
                        let (to, reason) = if row.status == "dead" {
                            ("dead", "lease_expired_max_attempts")
                        } else {
                            ("awaiting_retry", "lease_expired")
                        };
                        emit_transition(
                            Some("running"),
                            to,
                            Some(identity),
                            TransitionCtx {
                                job_id: row.id,
                                public_id: row.public_id,
                                queue: &state.queue,
                                attempts: row.attempts,
                                source: TransitionSource::Reaper,
                                reason: Some(reason),
                                lost_race: false,
                            },
                        );
                    }
                }
            }
            Ok(Err(e)) if crate::worker::is_fatal_sqlx(&e) => {
                tracing::error!(
                    worker.id = %state.worker_id,
                    error = %e,
                    "fatal DB error in reaper; shutting down worker"
                );
                let _ = state.last_fatal.set(Arc::new(e));
                state.shutdown.cancel();
                return;
            }
            Ok(Err(e)) => {
                consecutive_panics = 0;
                tracing::warn!(
                    worker.id = %state.worker_id,
                    error = %e,
                    "reap tick failed; will retry"
                );
            }
            Err(je) if je.is_panic() => {
                consecutive_panics = consecutive_panics.saturating_add(1);
                let msg = extract_panic_message(je);
                tracing::error!(
                    worker.id = %state.worker_id,
                    panic = %msg,
                    consecutive = consecutive_panics,
                    "reaper tick panicked"
                );
                if consecutive_panics >= REAPER_PANIC_ESCALATION_THRESHOLD {
                    tracing::event!(
                        target: "pgwq.reaper.escalation",
                        tracing::Level::ERROR,
                        worker.id = %state.worker_id,
                        threshold = REAPER_PANIC_ESCALATION_THRESHOLD,
                        consecutive_panics,
                        "reaper exceeded panic threshold; shutting down worker"
                    );
                    // Surface programmatically via WorkerHandle::shutdown /
                    // join → ShutdownError::ReaperPanicEscalation. First
                    // escalation wins; set is idempotent.
                    let _ = state.last_panic_escalation.set(consecutive_panics);
                    state.shutdown.cancel();
                    return;
                }
            }
            Err(_je_cancelled) => return,
        }
    }
}
