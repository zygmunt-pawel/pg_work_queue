//! Mark queries — terminal/transitional state-machine transitions for a
//! *claimed* row.
//!
//! Faza 3: pełny zestaw `mark_done` / `mark_retry` / `mark_dead`.
//!
//! Every `mark_*` SQL carries the **fencing-token guard** (`status='running'
//! AND lease_token = $token`) per PLAN.md §"Mark queries (fencing token w
//! WHERE)". Callers MUST run `last_error` (`mark_retry` / `mark_dead`)
//! through `util::fmt_err_trimmed` before passing it here — `mark_*`
//! functions only execute the SQL.

use sqlx::PgPool;
use sqlx::postgres::types::PgInterval;
use std::time::Duration;
use uuid::Uuid;

/// Transition a claimed row to `dead`.
///
/// SQL per PLAN.md §"Mark queries". Returns `rows_affected`:
/// - `1` = success (we owned the lease and flipped the row).
/// - `0` = fenced out (lease expired, reaper or another worker already
///   transitioned the row). Caller logs `warn!` + `fenced_out++` and moves on.
///
/// # Errors
/// Propagates any `sqlx::Error` from the underlying execute (pool starvation,
/// network drop, etc.). Caller decides whether to retry (Faza 2: callers in
/// `claim_and_decode` just warn-and-continue; row stays `running` and the
/// reaper recovers it after lease expiration).
#[tracing::instrument(skip(pool, reason), fields(job.id = id))]
pub async fn mark_dead(
    pool: &PgPool,
    id: i64,
    lease_token: Uuid,
    reason: &str,
) -> Result<u64, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'dead', finished_at = now(), last_error = $3,
             lease_token = NULL, lease_expires_at = NULL
         WHERE id = $1 AND status = 'running' AND lease_token = $2",
    )
    .bind(id)
    .bind(lease_token)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Transition a claimed row to `done`. Success terminal state.
///
/// SQL per PLAN.md §"Mark queries". Returns `rows_affected`:
/// - `1` = success.
/// - `0` = fenced out (lease expired, reaper or another worker already
///   transitioned the row). Caller emits `warn!` + bumps `fenced_out`.
///
/// # Errors
/// Propagates any `sqlx::Error` from the underlying execute.
#[tracing::instrument(skip(pool), fields(job.id = id))]
pub async fn mark_done(pool: &PgPool, id: i64, lease_token: Uuid) -> Result<u64, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'done', finished_at = now(), last_error = NULL,
             lease_token = NULL, lease_expires_at = NULL
         WHERE id = $1 AND status = 'running' AND lease_token = $2",
    )
    .bind(id)
    .bind(lease_token)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Transition a claimed row to `awaiting_retry` with `run_at = now() + delay`
/// computed **server-side** so the row's `run_at` is anchored to the database
/// clock (defends against worker host NTP skew — symmetrical with
/// `claim_batch`, which already uses `now() + interval` for `lease_expires_at`).
///
/// SQL per PLAN.md §"Mark queries". Caller MUST have already truncated
/// `reason` via `util::fmt_err_trimmed`. Returns `rows_affected`:
/// - `1` = success.
/// - `0` = fenced out (caller warns + bumps `fenced_out`).
///
/// # Errors
/// Propagates any `sqlx::Error` from the underlying execute.
#[tracing::instrument(skip(pool, reason), fields(job.id = id))]
pub async fn mark_retry(
    pool: &PgPool,
    id: i64,
    lease_token: Uuid,
    reason: &str,
    delay: Duration,
) -> Result<u64, sqlx::Error> {
    // Saturate to i64::MAX microseconds (~292k years) — any backoff
    // overflow already clamped by BackoffPolicy::next; this is belt-and-
    // -braces against a caller passing a Duration > i64::MAX µs.
    let micros = i64::try_from(delay.as_micros()).unwrap_or(i64::MAX);
    let interval = PgInterval {
        months: 0,
        days: 0,
        microseconds: micros,
    };
    let res = sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'awaiting_retry', last_error = $3, run_at = now() + $4,
             lease_token = NULL, lease_expires_at = NULL
         WHERE id = $1 AND status = 'running' AND lease_token = $2",
    )
    .bind(id)
    .bind(lease_token)
    .bind(reason)
    .bind(interval)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}
