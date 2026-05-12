//! Minimal polling-based Postgres job queue.
//!
//! See `PLAN.md` for design rationale. Public API stabilizuje się w v0.1.0.

pub mod backoff;
pub mod codec;
pub mod migrator;
pub mod pusher;
pub mod worker;
