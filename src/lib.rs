//! Minimal polling-based Postgres job queue.
//!
//! `pg_work_queue` is a single-table Postgres job queue built around three
//! ideas: **polling** (no `LISTEN/NOTIFY`), **fencing tokens** (every
//! `mark_*` carries a per-claim UUID in its `WHERE`), and **DB-side state
//! invariants** (`pgwq.job_status` ENUM + CHECK constraints, single source
//! of truth for `attempts` / `max_attempts`). Postgres 18+, Tokio, MIT.
//!
//! # Delivery semantics
//!
//! **At-least-once.** A handler may be invoked one *or more* times for a
//! single logical job (handler timeout, worker crash, fence-out on
//! `mark_done`, etc.). Use [`JobContext::idempotency_key`] (stable across
//! retries) as the dedup key for any external side-effect — Stripe,
//! SQS, SMTP, HTTP. See the README "Delivery semantics" section.
//!
//! # Minimum viable example
//!
//! ```no_run
//! use pg_work_queue::{Worker, JobError, JobContext, Pusher};
//! use serde::{Deserialize, Serialize};
//! use std::time::Duration;
//!
//! #[derive(Serialize, Deserialize)]
//! struct EmailTask { to: String, body: String }
//!
//! # async fn run(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! pg_work_queue::migrator().run(&pool).await?;
//!
//! let handle = Worker::<EmailTask>::builder()
//!     .pool(pool.clone())
//!     .queue("email_send")
//!     .handler(|task: EmailTask, ctx: JobContext| async move {
//!         tracing::info!(idempotency_key = %ctx.idempotency_key, "sending");
//!         // send_smtp(&task, &ctx.idempotency_key).await
//!         //     .map_err(|e| JobError::retry(e.to_string()))?;
//!         Ok(())
//!     })
//!     .build()?
//!     .start()
//!     .await?;
//!
//! let mut tx = pool.begin().await?;
//! let _id = Pusher::new("email_send")
//!     .push(&mut tx, &EmailTask { to: "a@b".into(), body: "hi".into() })
//!     .await?;
//! tx.commit().await?;
//!
//! let _stats = handle.shutdown(Duration::from_secs(10)).await?;
//! # Ok(()) }
//! ```
//!
//! # Module map
//!
//! - [`backoff`] — [`BackoffPolicy`] / [`PanicPolicy`] for retry pacing.
//! - [`codec`] — pluggable serialization ([`Codec`] trait, [`JsonCodec`]).
//! - [`error`] — public error enums ([`PushError`], [`JobError`],
//!   [`BuildError`], [`StartError`], [`ShutdownError`], [`PurgeError`]).
//! - [`limits`] — central `pub const` bounds (payload, batch, queue name).
//! - [`mod@migrator`] — embedded schema migrations ([`migrator()`]).
//! - [`purge`] — manual retention sweepers and read-only [`queue_stats`].
//! - [`pusher`] — [`Pusher`] (enqueue inside the caller's own transaction).
//! - [`worker`] — [`Worker`], [`WorkerBuilder`], [`WorkerHandle`], [`Stats`].

pub mod backoff;
pub mod codec;
pub mod error;
pub mod limits;
pub mod migrator;
pub mod purge;
pub mod pusher;
pub mod worker;

#[doc(hidden)]
pub mod claim;
pub(crate) mod job;
#[doc(hidden)]
pub mod mark;
pub(crate) mod reaper;
pub(crate) mod transition;
pub(crate) mod util;

// Public API surface — items defined w Fazach 1-4 są tu re-exported.
// Items dependent on later phases są commented out with `(Faza N)` markers
// and will be re-enabled as those phases land.
pub use crate::backoff::{BackoffPolicy, PanicPolicy};
pub use crate::codec::{Codec, JsonCodec};
pub use crate::error::{BuildError, JobError, PurgeError, PushError, ShutdownError, StartError};
pub use crate::job::{Job, JobContext};
pub use crate::migrator::migrator;
pub use crate::purge::{QueueStats, purge_dead, purge_done, queue_stats};
pub use crate::pusher::Pusher;
pub use crate::worker::{Stats, TickStats, Worker, WorkerBuilder, WorkerHandle};

/// Hidden re-exports used by integration tests under `tests/`. NOT part of
/// the public API; may change or vanish at any time.
#[doc(hidden)]
pub mod __test_exports {
    pub use crate::claim::claim_and_decode;
    pub use crate::mark::{mark_dead, mark_done, mark_retry};
    pub use crate::reaper::REAPER_PANIC_INJECTIONS;
    pub use crate::util::fmt_err_trimmed;
    pub use crate::worker::is_fatal_sqlx;
}
