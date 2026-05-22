//! `claim_batch` SQL wrapper + decode-time codec orchestration.
//!
//! Pipeline:
//! 1. `claim_batch_raw` — runs the CTE+UPDATE+RETURNING SQL and returns
//!    `Vec<RawClaimedRow>` (payload still as bytes, no codec contact yet).
//!    Two paths: empty `headroom` → `SQL_PLAIN` (the original PLAN.md
//!    §"`claim_batch` SQL"); non-empty `headroom` → `SQL_KEYED` (per-key
//!    bounded claim, design spec §5).
//! 2. `claim_and_decode` — for each raw row, runs `codec.decode` inside
//!    `std::panic::catch_unwind(AssertUnwindSafe(...))`:
//!       - Ok(payload)  → produce `Job<T>`.
//!       - Err(decode)  → `mark_dead` with `"codec decode: <err>"`, drop.
//!       - panic        → `mark_dead` with `"codec panic: <msg>"`, drop.
//!
//! Sync `catch_unwind` (not `tokio::spawn`) per PLAN.md §"Reaper panic
//! recovery" — codec decode is CPU-bound and synchronous; spawn would add
//! scheduling overhead and risk JoinHandle leak under outer abort.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::codec::Codec;
use crate::job::Job;
use crate::mark::mark_dead;
use crate::util::fmt_err_trimmed;

/// Raw row returned by `claim_batch` SQL — payload still as bytes.
///
/// Crate-internal: never exposed publicly. `claim_and_decode` turns this
/// into either `Job<T>` (success path) or a `mark_dead` (decode/panic path).
#[derive(Debug)]
pub(crate) struct RawClaimedRow {
    pub(crate) id: i64,
    pub(crate) public_id: Uuid,
    pub(crate) queue: String,
    pub(crate) concurrency_key: Option<String>,
    pub(crate) payload: Vec<u8>,
    pub(crate) attempts: u32,
    pub(crate) max_attempts: u32,
    pub(crate) first_attempted_at: DateTime<Utc>,
    pub(crate) lease_token: Uuid,
    pub(crate) lease_expires_at: DateTime<Utc>,
}

// Plain claim SQL (empty headroom — unbounded, from PLAN.md §"`claim_batch` SQL").
const SQL_PLAIN: &str = "WITH claimed AS (
     SELECT id FROM pgwq.jobs
     WHERE queue = $1
       AND status IN ('queued', 'awaiting_retry')
       AND run_at <= now()
     ORDER BY run_at, id
     LIMIT $2
     FOR UPDATE SKIP LOCKED
 )
 UPDATE pgwq.jobs j
 SET status = 'running', attempts = j.attempts + 1, max_attempts = $4,
     last_attempted_at = now(),
     first_attempted_at = COALESCE(j.first_attempted_at, now()),
     lease_token = gen_random_uuid(),
     lease_expires_at = now() + $3::interval, last_error = NULL
 FROM claimed
 WHERE j.id = claimed.id
 RETURNING j.id, j.public_id, j.queue, j.payload, j.attempts, \
j.max_attempts, j.first_attempted_at, j.lease_token, j.lease_expires_at, \
j.concurrency_key";

// Per-key bounded claim SQL (non-empty headroom — design spec §5).
// Headroom map bound as jsonb ($5); keys without an entry are claimed
// without a per-key cap (eligible_unlimited branch).
//
// CORRECTNESS — the two-stage shape (unlocked `eligible_keyed` /
// `eligible_unlimited` CTEs that rank+LIMIT the candidate set, then a
// SEPARATE `locked` CTE that applies `FOR UPDATE SKIP LOCKED`) is
// deliberate and relies on the crate's single-`Worker`-per-queue
// assumption. The per-key `LIMIT` MUST rank against the true ordered
// candidate rows WITHOUT `SKIP LOCKED`: if `SKIP LOCKED` were pushed
// into the LATERAL, rows locked by an in-flight transaction would be
// skipped during ranking, the LIMIT would admit *further* rows to refill
// the cap, and the per-key concurrency limit would be over-admitted.
// Do NOT "optimize" by folding `FOR UPDATE SKIP LOCKED` into the
// `eligible_*` CTEs — that reintroduces the over-admit bug. The lock is
// applied only afterward, in `locked`.
const SQL_KEYED: &str = "WITH
 hr AS (
     SELECT key AS concurrency_key, value::int AS h
     FROM jsonb_each_text($5::jsonb)
 ),
 eligible_keyed AS (
     SELECT e.id
     FROM hr
     CROSS JOIN LATERAL (
         SELECT j.id FROM pgwq.jobs j
         WHERE j.queue = $1
           AND j.status IN ('queued', 'awaiting_retry')
           AND j.concurrency_key = hr.concurrency_key
           AND j.run_at <= now()
         ORDER BY j.run_at, j.id
         LIMIT LEAST(GREATEST(hr.h, 0), $2)
     ) e
 ),
 eligible_unlimited AS (
     SELECT j.id FROM pgwq.jobs j
     LEFT JOIN hr ON hr.concurrency_key = j.concurrency_key
     WHERE j.queue = $1
       AND j.status IN ('queued', 'awaiting_retry')
       AND j.run_at <= now()
       AND hr.concurrency_key IS NULL
     ORDER BY j.run_at, j.id
     LIMIT $2
 ),
 locked AS (
     SELECT j.id FROM pgwq.jobs j
     WHERE j.id IN (SELECT id FROM eligible_keyed
                    UNION ALL
                    SELECT id FROM eligible_unlimited)
       AND j.status IN ('queued', 'awaiting_retry')
       AND j.run_at <= now()
     ORDER BY j.run_at, j.id
     LIMIT $2
     FOR UPDATE SKIP LOCKED
 )
 UPDATE pgwq.jobs j
 SET status = 'running', attempts = j.attempts + 1, max_attempts = $4,
     last_attempted_at = now(),
     first_attempted_at = COALESCE(j.first_attempted_at, now()),
     lease_token = gen_random_uuid(),
     lease_expires_at = now() + $3::interval, last_error = NULL
 FROM locked
 WHERE j.id = locked.id
 RETURNING j.id, j.public_id, j.queue, j.payload, j.attempts, \
j.max_attempts, j.first_attempted_at, j.lease_token, j.lease_expires_at, \
j.concurrency_key";

/// Run `claim_batch` SQL — claim up to `batch_size` runnable rows from `queue`.
///
/// Returns rows in claim order (by `run_at, id` ASC). Each row is now
/// `status='running'` with `attempts` incremented, `lease_token` set to a
/// fresh `gen_random_uuid()`, and `lease_expires_at = now() + lease_timeout`.
///
/// `headroom` selects the claim path: empty → `SQL_PLAIN` (the PLAN.md
/// `claim_batch` SQL, unbounded); non-empty → `SQL_KEYED` (per-key bounded
/// claim, design spec §5), with the headroom map bound as `jsonb` (`$5`).
async fn claim_batch_raw(
    pool: &PgPool,
    queue: &str,
    batch_size: u32,
    lease_timeout: Duration,
    max_attempts: u32,
    headroom: &HashMap<String, u32>,
) -> Result<Vec<RawClaimedRow>, sqlx::Error> {
    // `i32` for binding: PG INTEGER. `u32::min(i32::MAX as u32)` keeps the
    // values DB-shaped without panic in src/.
    let i32_max_u32: u32 = i32::MAX as u32;
    let batch_size_i32: i32 = i32::try_from(batch_size.min(i32_max_u32)).unwrap_or(i32::MAX);
    let max_attempts_i32: i32 = i32::try_from(max_attempts.min(i32_max_u32)).unwrap_or(i32::MAX);

    let rows = if headroom.is_empty() {
        sqlx::query(SQL_PLAIN)
            .bind(queue)
            .bind(batch_size_i32)
            .bind(lease_timeout)
            .bind(max_attempts_i32)
            .fetch_all(pool)
            .await?
    } else {
        let headroom_json = serde_json::to_string(headroom).unwrap_or_else(|_| "{}".to_string());
        sqlx::query(SQL_KEYED)
            .bind(queue)
            .bind(batch_size_i32)
            .bind(lease_timeout)
            .bind(max_attempts_i32)
            .bind(headroom_json)
            .fetch_all(pool)
            .await?
    };

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let attempts_i32: i32 = r.try_get("attempts")?;
        let max_attempts_i32: i32 = r.try_get("max_attempts")?;
        out.push(RawClaimedRow {
            id: r.try_get("id")?,
            public_id: r.try_get("public_id")?,
            queue: r.try_get("queue")?,
            concurrency_key: r.try_get("concurrency_key")?,
            payload: r.try_get("payload")?,
            // attempts is CHECK >= 0 → non-negative i32 fits in u32 unconditionally.
            attempts: u32::try_from(attempts_i32).unwrap_or(0),
            max_attempts: u32::try_from(max_attempts_i32).unwrap_or(0),
            first_attempted_at: r.try_get("first_attempted_at")?,
            lease_token: r.try_get("lease_token")?,
            lease_expires_at: r.try_get("lease_expires_at")?,
        });
    }
    Ok(out)
}

/// Extract a human-readable message from a `catch_unwind` payload.
///
/// `panic!` macro payloads are most commonly `&'static str` (format-less)
/// or `String` (formatted). Anything else → fallback string. Per PLAN.md
/// §"`extract_panic_message`".
fn extract_panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<unknown panic payload>".to_string()
}

/// Public-to-tests (`#[doc(hidden)]`-re-exported) entry point:
/// claim a batch and decode payloads through `codec`. Rows whose decode
/// fails or panics are marked dead in-place and dropped from the result.
///
/// `headroom` is forwarded verbatim to `claim_batch_raw`: empty map →
/// `SQL_PLAIN` (unbounded claim); non-empty → `SQL_KEYED` (per-key bounded).
///
/// # Errors
/// Returns the underlying `sqlx::Error` if the `claim_batch` SQL itself
/// fails. `mark_dead` failures on individual rows are logged at `warn`
/// and swallowed — the reaper recovers those rows via lease expiration
/// (per PLAN.md §"Error semantics", codec-decode-failure path).
// `implicit_hasher`: the only caller is the Worker poll loop, which builds the
// headroom map with the default hasher; a `BuildHasher` type parameter would
// leak into the crate-internal call sites for no benefit.
#[allow(clippy::implicit_hasher)]
#[doc(hidden)]
pub async fn claim_and_decode<T, C>(
    pool: &PgPool,
    codec: &C,
    queue: &str,
    batch_size: u32,
    lease_timeout: Duration,
    max_attempts: u32,
    headroom: &HashMap<String, u32>,
) -> Result<Vec<Job<T>>, sqlx::Error>
where
    T: serde::de::DeserializeOwned,
    C: Codec,
{
    let raws = claim_batch_raw(
        pool,
        queue,
        batch_size,
        lease_timeout,
        max_attempts,
        headroom,
    )
    .await?;
    let mut out: Vec<Job<T>> = Vec::with_capacity(raws.len());

    for raw in raws {
        // `AssertUnwindSafe` is appropriate: the closure captures only
        // `codec: &C` (`Codec: Send + Sync + 'static` per trait) and
        // `raw.payload: &[u8]` (bytes). No shared mutable state crosses
        // the catch_unwind boundary into observable post-panic territory.
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| codec.decode::<T>(&raw.payload)));
        match result {
            Ok(Ok(payload)) => {
                out.push(Job {
                    id: raw.id,
                    public_id: raw.public_id,
                    queue: raw.queue,
                    concurrency_key: raw.concurrency_key,
                    payload,
                    attempts: raw.attempts,
                    max_attempts: raw.max_attempts,
                    first_attempted_at: raw.first_attempted_at,
                    lease_token: raw.lease_token,
                    lease_expires_at: raw.lease_expires_at,
                });
            }
            Ok(Err(decode_err)) => {
                let reason = fmt_err_trimmed(&DisplayErr(format!("codec decode: {decode_err}")));
                match mark_dead(pool, raw.id, raw.lease_token, &reason).await {
                    Ok(n) => {
                        tracing::error!(
                            job.id = raw.id,
                            error = %decode_err,
                            rows_affected = n,
                            "codec decode failed; marked dead"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            job.id = raw.id,
                            error = %e,
                            "codec decode failed but mark_dead failed too; reaper will recover after lease expiry"
                        );
                    }
                }
            }
            Err(panic_payload) => {
                let msg = extract_panic_message(&*panic_payload);
                let reason = fmt_err_trimmed(&DisplayErr(format!("codec panic: {msg}")));
                match mark_dead(pool, raw.id, raw.lease_token, &reason).await {
                    Ok(n) => {
                        tracing::error!(
                            job.id = raw.id,
                            panic = %msg,
                            rows_affected = n,
                            "codec decode panicked; marked dead"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            job.id = raw.id,
                            error = %e,
                            "codec decode panicked but mark_dead failed too; reaper will recover after lease expiry"
                        );
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Tiny adapter so we can route any `String` through `fmt_err_trimmed`
/// (which wants `&dyn std::error::Error`). Trimming is char-safe per
/// `util::fmt_err_trimmed`'s contract.
struct DisplayErr(String);
impl std::fmt::Display for DisplayErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::fmt::Debug for DisplayErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for DisplayErr {}
