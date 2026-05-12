//! Resource limits — central source of truth for size/bound constants.
//!
//! Each constant has a corresponding DB-side CHECK or builder-side validation
//! (defense-in-depth). See PLAN.md §"Resource limits".

use std::time::Duration;

/// Max payload size pushable. Matches `jobs_payload_max_size` DB CHECK.
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024; // 1 MiB

/// Max items per `Pusher::push_batch` call. Larger batches must chunk
/// client-side.
pub const MAX_BATCH_SIZE: usize = 10_000;

/// Max queue name length. Matches `jobs_queue_max_len` DB CHECK.
pub const MAX_QUEUE_LEN: usize = 64;

/// Max length of `last_error` text (character units — Postgres `length(TEXT)`).
/// Library truncates at this; DB CHECK backstops.
pub const MAX_LAST_ERROR_LEN: usize = 8 * 1024; // 8 KiB chars

/// Minimum `poll_interval` allowed in builder.
pub const MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Minimum `handler_timeout`. Below this the handler cannot reliably finish
/// trivial work (parse + 1 DB query) and `mark_retry` commit before being
/// aborted. Lower bound for builder validation.
pub const MIN_HANDLER_TIMEOUT: Duration = Duration::from_secs(1);

/// Minimum `mark_timeout`. Below this the `mark_*` SQL cannot reliably
/// commit under any non-trivial network conditions. Lower bound for
/// builder validation.
pub const MIN_MARK_TIMEOUT: Duration = Duration::from_millis(100);

/// Reaper sweep batch size (rows per tick).
pub const REAPER_BATCH_SIZE: usize = 1024;

/// Aggregate cap na całkowity batch payload bytes — chroni przed
/// 10k items × 1 MiB = 10 GB single transaction.
pub const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

/// Purge function chunk size (rows per `DELETE ... LIMIT N` iteration).
pub const PURGE_CHUNK_SIZE: usize = 10_000;
