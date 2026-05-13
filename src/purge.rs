//! Manual retention sweepers — [`purge_done`] and [`purge_dead`].
//!
//! The library DOES NOT spawn an automatic cleanup task (PLAN.md anti-features).
//! User invokes [`purge_done`] / [`purge_dead`] on their own schedule (cron,
//! scheduled task, manual ops) when they want to reclaim terminal rows.
//!
//! Each call iterates with `LIMIT = limits::PURGE_CHUNK_SIZE` until a chunk
//! returns 0 rows. Per-chunk SQL uses `FOR UPDATE SKIP LOCKED` so a purge
//! never blocks (and is never blocked by) running workers, and emits ONE
//! aggregate `tracing::info!` per call (PLAN.md §"State transition events":
//! `dead`/`done` → ∅ is logged as 1 event per call with `deleted: count`,
//! not per-row).
//!
//! [`queue_stats`] is a read-only helper (PLAN.md decision #16) returning
//! the row counts per status for one queue. Operator cookbook material.

use std::time::Duration;

use sqlx::{PgPool, Row};

use crate::error::PurgeError;
use crate::limits::PURGE_CHUNK_SIZE;

/// Per-status row counts for a single queue. Returned by [`queue_stats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueStats {
    /// Rows in status `queued`.
    pub queued: u64,
    /// Rows in status `awaiting_retry`.
    pub awaiting_retry: u64,
    /// Rows in status `running`.
    pub running: u64,
    /// Rows in status `done`.
    pub done: u64,
    /// Rows in status `dead`.
    pub dead: u64,
}

/// Delete `done` rows whose `finished_at < now() - age`. Returns total rows
/// deleted across all chunks.
///
/// Chunked with `LIMIT = limits::PURGE_CHUNK_SIZE` and
/// `FOR UPDATE SKIP LOCKED` so a long retention sweep never blocks live
/// workers. Iterates until a chunk affects 0 rows.
///
/// # Errors
/// Returns [`PurgeError::Database`] on any underlying `sqlx::Error`.
pub async fn purge_done(pool: &PgPool, age: Duration) -> Result<u64, PurgeError> {
    purge_terminal(pool, "done", age).await
}

/// Delete `dead` rows whose `finished_at < now() - age`. Returns total rows
/// deleted across all chunks.
///
/// Same chunking + locking semantics as [`purge_done`].
///
/// # Errors
/// Returns [`PurgeError::Database`] on any underlying `sqlx::Error`.
pub async fn purge_dead(pool: &PgPool, age: Duration) -> Result<u64, PurgeError> {
    purge_terminal(pool, "dead", age).await
}

/// Shared chunked-DELETE loop. `status_label` is bound as `pgwq.job_status`
/// via explicit `$1::pgwq.job_status` cast (sqlx binds `&str`).
async fn purge_terminal(
    pool: &PgPool,
    status_label: &'static str,
    age: Duration,
) -> Result<u64, PurgeError> {
    // i64 — Postgres LIMIT parameter type. PURGE_CHUNK_SIZE = 10_000 fits trivially.
    let chunk_limit: i64 = i64::try_from(PURGE_CHUNK_SIZE).unwrap_or(i64::MAX);
    let mut total: u64 = 0;

    loop {
        let result = sqlx::query(
            "WITH victims AS (
                SELECT id FROM pgwq.jobs
                WHERE status = $1::pgwq.job_status
                  AND finished_at < now() - $2::interval
                ORDER BY finished_at
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM pgwq.jobs WHERE id IN (SELECT id FROM victims)",
        )
        .bind(status_label)
        .bind(age)
        .bind(chunk_limit)
        .execute(pool)
        .await?;

        let affected = result.rows_affected();
        total = total.saturating_add(affected);

        if affected == 0 {
            break;
        }
    }

    tracing::info!(
        status = %status_label,
        age_secs = age.as_secs(),
        deleted = total,
        "pgwq.purge complete",
    );

    Ok(total)
}

/// Read-only counts of jobs by status for a single queue. PLAN.md decision
/// #16 — operator cookbook helper (NOT a hot-loop dashboard primitive;
/// requires a full index scan per status filter).
///
/// # Errors
/// Returns [`PurgeError::Database`] on any underlying `sqlx::Error`.
pub async fn queue_stats(pool: &PgPool, queue: &str) -> Result<QueueStats, PurgeError> {
    let row = sqlx::query(
        "SELECT
            count(*) FILTER (WHERE status = 'queued')         AS queued,
            count(*) FILTER (WHERE status = 'awaiting_retry') AS awaiting_retry,
            count(*) FILTER (WHERE status = 'running')        AS running,
            count(*) FILTER (WHERE status = 'done')           AS done,
            count(*) FILTER (WHERE status = 'dead')           AS dead
        FROM pgwq.jobs WHERE queue = $1",
    )
    .bind(queue)
    .fetch_one(pool)
    .await?;

    // `count(*)` returns BIGINT (i64, non-negative). Cast to u64; saturate
    // any defensive surprise to 0 rather than panic on try_from.
    let queued = u64::try_from(row.try_get::<i64, _>("queued")?).unwrap_or(0);
    let awaiting_retry = u64::try_from(row.try_get::<i64, _>("awaiting_retry")?).unwrap_or(0);
    let running = u64::try_from(row.try_get::<i64, _>("running")?).unwrap_or(0);
    let done = u64::try_from(row.try_get::<i64, _>("done")?).unwrap_or(0);
    let dead = u64::try_from(row.try_get::<i64, _>("dead")?).unwrap_or(0);

    Ok(QueueStats {
        queued,
        awaiting_retry,
        running,
        done,
        dead,
    })
}
