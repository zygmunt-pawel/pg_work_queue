# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project-specific working rules

- **README.md is the source-of-truth user manual.** Any change to public
  API, builder knob, default, error variant, limit, state machine,
  schema, tracing target, or observable behavior **must** be reflected
  in `README.md` in the same change. The README is self-contained ("a
  working integration from this section alone, without reading the
  source") and silently letting it drift defeats that purpose. Update
  the matching section *before* claiming the task complete.
- The crate is pre-1.0 (`v0.1`); breaking changes are allowed, but the
  README/rustdoc must move in lockstep with them.
- Polish phrasing appears in source comments (`Faza N` = phase N). Match
  the surrounding style when editing a file; don't bulk-rewrite to
  English.

## Build, lint, test

Tests boot a real Postgres 18 container via `testcontainers` — **Docker
must be running** or the suite hangs at container start.

```bash
cargo build
cargo clippy --all-targets -- -D warnings    # lints in Cargo.toml are warn/deny — treat warnings as errors
cargo test --no-fail-fast                    # full integration suite (~120 tests, slow; each test spins up its own PG18)
cargo test --test <file_stem>                # single integration test file, e.g. `cargo test --test shutdown_graceful`
cargo test --test <file_stem> -- <name>      # single test within a file
RUST_LOG=info,sqlx=warn cargo test ...       # tracing-subscriber honors RUST_LOG via the shared `init_tracing()` helper
cargo doc --no-deps --open                   # rustdoc — the public API is documented in-tree, this is the local fallback for docs.rs
```

Lint posture (`Cargo.toml [lints]`): `unsafe_code = forbid`,
`clippy::pedantic` + `clippy::nursery` at warn, and
`unwrap_used`/`expect_used`/`panic = deny`. Don't reach for `.unwrap()`
or `.expect()` in `src/` — production code must propagate via
`Result`/`?`. Tests are exempt by convention.

Pinned exact versions throughout `Cargo.toml` (`=0.8.6`, `=1.52.3`, …)
are intentional — do **not** loosen them in passing edits.

## Big-picture architecture

This is a single-table Postgres job queue. The README's "Architecture"
section is the canonical narrative; this is the orientation map for
navigating the code.

**Five runtime components, all but `Pusher`/`Worker` crate-private:**

1. `pgwq.jobs` table + `pgwq.job_status` ENUM + `jobs_status_invariants`
   CHECK — the FSM is enforced *in the database*, not in Rust. Any code
   change that produces a status/attempts/lease shape outside the CHECK
   will fail loudly at commit, not silently corrupt rows. Schema lives
   in `migrations/20260513000000_v01_init.sql`.
2. `Pusher` (`src/pusher.rs`) — enqueue handle. Takes a
   `&mut PgConnection` so inserts join the caller's own transaction.
   Generates `uuidv7` *client-side* so `Pusher::push` returns the
   `public_id` without a `RETURNING`.
3. `Worker` poll loop (`src/worker.rs`) — `tokio::time::interval` tick.
   **Permits-first**: acquire concurrency semaphore *before*
   `claim_batch`, then claim `min(batch_size, free_permits)` rows.
   Spawns each surviving job into a `JoinSet` wrapped in
   `tokio::time::timeout(handler_timeout, …)`. Every `mark_*` carries
   `WHERE status='running' AND lease_token=$token` — the fencing-token
   guard.
4. Reaper (`src/reaper.rs`) — sibling `tokio::spawn` that sweeps stale
   `running` rows back to `awaiting_retry` (or `dead` if
   `attempts >= max_attempts`). **Adaptive**: full-batch tick (1024
   rows) skips the next `interval.tick()` to drain at SQL speed. This
   is the *only* process-death recovery path; handler-level
   cancellation is `handler_timeout`, which is strictly shorter than
   `lease_timeout`.
5. Retention helpers (`src/purge.rs`) — `purge_done` / `purge_dead`
   chunk-DELETE under `FOR UPDATE SKIP LOCKED`. The library does
   **not** spawn a background sweeper; operators call these from their
   own scheduler.

**Source layout, by concern:**

| file | role |
|---|---|
| `lib.rs` | Module map + crate-root re-exports. Public API surface is whatever appears in the `pub use` block. |
| `worker.rs` | `Worker`, `WorkerBuilder` (type-state for handler), `WorkerHandle`, `Stats`, `TickStats`, the poll loop, the fatal/transient sqlx classifier (`is_fatal_sqlx`). Largest file (~2.2k lines); navigate by section comments. |
| `pusher.rs` | `Pusher::{push, push_at, push_batch}` + pre-INSERT validation (queue name, payload size, batch size). |
| `claim.rs` | `claim_batch` CTE + `claim_and_decode` (decode under `catch_unwind`; codec error/panic → `mark_dead`). |
| `mark.rs` | `mark_done` / `mark_retry` / `mark_dead` SQL. All three include the fencing-token guard. |
| `reaper.rs` | Reaper tick loop + 3-strikes panic escalation. |
| `transition.rs` | `pgwq.state.transition` event emitter (single source of truth for transition tracing). |
| `backoff.rs` | `BackoffPolicy` (Linear/Exponential), `PanicPolicy` (Retry/Dead), `BackoffPolicy::next` (overflow-safe via `is_finite()` + clamp). |
| `codec.rs` | `Codec` trait + `JsonCodec` default. |
| `error.rs` | All public error enums. `#[non_exhaustive]` on all except `PushError` (treat that as `#[non_exhaustive]` for forward-compat). |
| `purge.rs` | `purge_done`, `purge_dead`, `queue_stats`, `QueueStats`. |
| `migrator.rs` | Embedded `sqlx::Migrator` via `sqlx::migrate!()`. |
| `limits.rs` | Public `pub const` resource bounds; each has a matching DB CHECK or builder validation. |
| `util.rs` | UTF-8-safe string truncation for `last_error` etc. |
| `job.rs` | Internal `Job<T>` + public `JobContext` (handler argument). |

**Architectural invariants that must not regress:**

- `lease_token` is v4 UUID (no time leak), stamped at claim, cleared on
  every transition out of `running`. Never persist it past terminal
  status.
- The reaper sees only `(lease_expires_at, lease_token, attempts,
  max_attempts)` per row. **No worker-registration table.**
- `max_attempts` is stamped per row at claim time by the *claiming*
  worker. Reaper and `mark_retry` both consult `j.max_attempts` (not
  worker-local state) for the retry-vs-dead verdict.
- `JobContext::idempotency_key == public_id` (both UUIDv7, stable
  across retries). The dual name is intentional — keep both.
- Cross-knob invariants enforced in `WorkerBuilder::build`
  (`handler_timeout + 1s ≤ lease_timeout`,
  `reaper_interval ≤ lease_timeout / 2`,
  `max_connections ≥ concurrency × 2 + 2`, etc.) — see the builder
  table in README for the full list. Pool capacity reads
  `pool.options().get_max_connections()`, **not** `pool.size()`.
- `WorkerHandle::shutdown` is a 7-step sequence (soft cancel → soft
  drain → hard abort poll/reaper → handler drain → hard abort handlers
  → `pending_recovery` count → build `Stats`). The order matters; the
  hard-abort step exists specifically to defend against pool
  starvation hanging the SQL futures.

**Test conventions:**

- One file per behavior in `tests/`. Each public builder knob has a
  paired behavioral test at **two distinct values** — keep this when
  adding knobs.
- `tests/common/mod.rs` provides `pg18_pool()` which boots a fresh
  PG18 container per test and runs `migrator()`. Container drops with
  the returned handle — keep it bound for the test's lifetime.
- `__test_exports` (in `lib.rs`) is the controlled crack in the
  encapsulation for tests that need crate-internals
  (`claim_and_decode`, `mark_*`, `is_fatal_sqlx`,
  `REAPER_PANIC_INJECTIONS`). Don't add to it casually — prefer
  testing through the public API.

## Known constraint to keep in mind

`sqlx::migrate!()` on `0.8.x` hard-codes the migration tracking table
to `_sqlx_migrations`. If you co-locate this crate's migrations with an
application using its own `sqlx::migrate!()`, the two will collide on
version numbers. The fix waits on `sqlx 0.9`'s
`dangerous_set_table_name`. See README "Known limitations".

## Reference docs in this repo

- `README.md` — user-facing manual; **must stay current**.
- `PLAN.md` — original implementation plan (~105k). Historical
  reference for *why* a design was chosen; the code and README are
  authoritative for *what* the code does today. Don't cite line
  numbers from PLAN in user-facing output.
- `research/` — vendored sources of comparable crates (apalis-*) and
  review notes. Git-ignored except for `review_findings.md`.
- `docs/` — auxiliary docs (currently the superpowers harness).
