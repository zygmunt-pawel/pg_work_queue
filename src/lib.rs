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
pub(crate) mod mark;
pub(crate) mod util;

// Public API surface — only items defined in Faza 1 are re-exported here.
// Items dependent on later phases are commented out with `(Faza N)` markers
// and will be re-enabled as those phases land.
pub use crate::codec::{Codec, JsonCodec};
pub use crate::error::PushError;
pub use crate::job::{Job, JobContext};
pub use crate::migrator::migrator;
pub use crate::pusher::Pusher;

// pub use crate::backoff::BackoffPolicy;                                    // (Faza 6)
// pub use crate::error::{BuildError, JobError, PurgeError, ShutdownError};  // (Fazy 3/6/7/8)
// pub use crate::purge::{purge_dead, purge_done, queue_stats, QueueStats};  // (Faza 8)
// pub use crate::worker::{                                                  // (Fazy 3-8)
//     JobContext, PanicPolicy, Stats, Worker, WorkerBuilder, WorkerHandle,
// };

/// Hidden re-exports used by integration tests under `tests/`. NOT part of
/// the public API; may change or vanish at any time.
#[doc(hidden)]
pub mod __test_exports {
    pub use crate::claim::claim_and_decode;
    pub use crate::util::fmt_err_trimmed;
}
