//! Minimal polling-based Postgres job queue.
//!
//! See `PLAN.md` for design rationale. Public API stabilizuje się w v0.1.0.

pub mod backoff;
pub mod codec;
pub mod error;
pub mod limits;
pub mod migrator;
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
pub use crate::error::{BuildError, JobError, PushError, ShutdownError, StartError};
pub use crate::job::{Job, JobContext};
pub use crate::migrator::migrator;
pub use crate::pusher::Pusher;
pub use crate::worker::{TickStats, Worker, WorkerBuilder, WorkerHandle};

// pub use crate::error::PurgeError;                                         // (Faza 8)
// pub use crate::purge::{purge_dead, purge_done, queue_stats, QueueStats};  // (Faza 8)
// pub use crate::worker::Stats;                                             // (Faza 7)

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
