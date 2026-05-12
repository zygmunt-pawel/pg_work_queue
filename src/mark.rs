//! Mark queries — terminal state-machine transitions for a *claimed* row.
//!
//! Faza 2: only `mark_dead` lands (needed for codec-decode-error and
//! codec-panic paths in `claim_and_decode`). The full `mark_done` /
//! `mark_retry` / `mark_dead` trinity formalizes in Faza 3.
//!
//! Every `mark_*` SQL carries the **fencing-token guard** (`status='running'
//! AND lease_token = $token`) per PLAN.md §"Mark queries (fencing token w
//! WHERE)". Callers MUST run `last_error` through `util::fmt_err_trimmed`
//! before passing it here — `mark_dead` only executes the SQL.

use sqlx::PgPool;
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
#[allow(clippy::redundant_pub_crate)] // inside `pub(crate) mod mark`; `pub(crate)` is the intent.
pub(crate) async fn mark_dead(
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
