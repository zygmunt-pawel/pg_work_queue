//! State-machine transition observability — single-source `emit_transition`
//! helper used by every site that flips a `pgwq.jobs.status`.
//!
//! See PLAN.md §"State transition events" (lines 1628-1672). Every transition
//! (`mark_done` success, `mark_retry`, `mark_dead`, claim transitions, reaper
//! transitions, purge) MUST route through [`emit_transition`] so attrs don't
//! drift between call sites.
//!
//! Tracing event target is `"pgwq.state.transition"`; level is picked by the
//! destination state — `dead` is ERROR (dead-letter), `done` / `awaiting_retry`
//! INFO, anything else (`running`) DEBUG.
//!
//! PII safety: `reason` body is omitted at ERROR level (only `reason_length`
//! and `reason_present` metadata). At INFO/DEBUG the body is included verbatim
//! (already library-side truncated by `util::fmt_err_trimmed`).
//
// `unreachable_pub` is a false-positive here: the module itself is
// `pub(crate)`, so `pub` items inside are crate-internal. `redundant_pub_crate`
// fires on `pub(crate)`. We disable the former locally.
#![allow(unreachable_pub)]

use uuid::Uuid;

/// Origin of a state-machine transition. Surfaced as the `source` attribute
/// on every `pgwq.state.transition` event so operators can attribute the
/// flip to a specific subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // `Purge` lands w Fazie 8 (Reaper jest już used z Fazy 5)
pub enum TransitionSource {
    /// `Worker` claimed → handler executed → `mark_*`.
    Worker,
    /// `Reaper` swept a stale `running` row back into `awaiting_retry` / `dead`.
    Reaper,
    /// `purge_*` deleted a terminal row.
    Purge,
}

/// Context payload threaded through every `emit_transition` call.
///
/// Borrowed so callers can hand-roll the smallest possible owned set
/// (`queue: &str`, `reason: Option<&str>`) without forcing allocations
/// per emit.
#[derive(Debug, Clone, Copy)]
pub struct TransitionCtx<'a> {
    /// Internal BIGINT PK.
    pub job_id: i64,
    /// External job handle (uuidv7).
    pub public_id: Uuid,
    /// Source queue name.
    pub queue: &'a str,
    /// 1-indexed current attempt at the time of the transition.
    pub attempts: u32,
    /// Origin (worker / reaper / purge).
    pub source: TransitionSource,
    /// Optional reason body. Only logged at INFO/DEBUG level (omitted at
    /// ERROR for PII safety; ERROR gets `reason_length` + `reason_present`).
    pub reason: Option<&'a str>,
    /// `true` iff the `mark_*` SQL returned 0 `rows_affected` — caller lost
    /// the race with the reaper. Surfaces as `lost_race` attr.
    pub lost_race: bool,
}

/// Worker identity attached to every transition event so multi-replica
/// operators can correlate "who flipped this row?". Constructed once at
/// `Worker::start` (`Uuid::now_v7()`).
#[derive(Debug, Clone, Copy)]
pub struct WorkerIdentity(pub Uuid);

/// Emit one structured event for a state-machine transition.
///
/// `from` is `None` for transitions whose source state is implicit (push
/// path: `∅ → queued`). For ERROR-level transitions (`dead`), the `reason`
/// body is replaced by metadata-only attrs (`reason_length`, `reason_present`).
pub fn emit_transition(
    from: Option<&str>,
    to: &str,
    worker: Option<WorkerIdentity>,
    ctx: TransitionCtx<'_>,
) {
    let from_attr = from.unwrap_or("none");
    let source_attr = match ctx.source {
        TransitionSource::Worker => "worker",
        TransitionSource::Reaper => "reaper",
        TransitionSource::Purge => "purge",
    };
    let worker_id = worker.map(|w| w.0);

    // PII safety: ERROR level (dead-letter) gets metadata-only — no body.
    // INFO/DEBUG get the body verbatim (already truncated by
    // `util::fmt_err_trimmed`).
    let reason_present = ctx.reason.is_some();
    let reason_length = ctx.reason.map_or(0, str::len);

    match to {
        "dead" => {
            tracing::event!(
                target: "pgwq.state.transition",
                tracing::Level::ERROR,
                worker.id = ?worker_id,
                job.id = ctx.job_id,
                job.public_id = %ctx.public_id,
                queue = ctx.queue,
                job.attempts = ctx.attempts,
                status.from = from_attr,
                status.to = to,
                source = source_attr,
                lost_race = ctx.lost_race,
                reason_length = reason_length,
                reason_present = reason_present,
            );
        }
        "done" | "awaiting_retry" => {
            tracing::event!(
                target: "pgwq.state.transition",
                tracing::Level::INFO,
                worker.id = ?worker_id,
                job.id = ctx.job_id,
                job.public_id = %ctx.public_id,
                queue = ctx.queue,
                job.attempts = ctx.attempts,
                status.from = from_attr,
                status.to = to,
                source = source_attr,
                lost_race = ctx.lost_race,
                reason_length = reason_length,
                reason_present = reason_present,
                reason = ctx.reason,
            );
        }
        _ => {
            tracing::event!(
                target: "pgwq.state.transition",
                tracing::Level::DEBUG,
                worker.id = ?worker_id,
                job.id = ctx.job_id,
                job.public_id = %ctx.public_id,
                queue = ctx.queue,
                job.attempts = ctx.attempts,
                status.from = from_attr,
                status.to = to,
                source = source_attr,
                lost_race = ctx.lost_race,
                reason_length = reason_length,
                reason_present = reason_present,
                reason = ctx.reason,
            );
        }
    }
}
