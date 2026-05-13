# pg_work_queue

Minimal polling-based Postgres job queue for Rust.

## Status

`v0.1` — pre-publish. Postgres **18+** (uses native `uuidv7()`), Tokio, MIT.

122 integration tests over `testcontainers` PG18. Every public builder knob
has a two-value behavioral test (`tests/<knob>_behavior.rs`).

## Quick start

`Cargo.toml`:

```toml
[dependencies]
pg_work_queue = "0.1"
sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "postgres", "uuid", "chrono", "macros", "migrate"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

```rust
use pg_work_queue::{Worker, JobError, JobContext, Pusher, BackoffPolicy};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct EmailTask {
    to: String,
    body: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;

    // 1) Apply the schema. Library does NOT migrate automatically.
    pg_work_queue::migrator().run(&pool).await?;

    // 2) Build a worker.
    let handle = Worker::<EmailTask>::builder()
        .pool(pool.clone())
        .queue("email_send")
        .poll_interval(Duration::from_millis(500))
        .concurrency(16)
        .max_attempts(5)
        .lease_timeout(Duration::from_secs(300))
        .handler_timeout(Duration::from_secs(240)) // 80% of lease (default)
        .retry_backoff(BackoffPolicy::Exponential {
            base: Duration::from_secs(1),
            factor: 2.0,
            cap: Duration::from_secs(5 * 60),
            jitter: 0.2, // ±20%
        })
        .handler(|task: EmailTask, ctx: JobContext| async move {
            // ctx.idempotency_key is STABLE across retries — use it for
            // any external API that supports Idempotency-Key headers.
            send_smtp(&task, &ctx.idempotency_key.to_string())
                .await
                .map_err(|e| JobError::retry(e.to_string()))?;
            Ok(())
        })
        .build()?
        .start()
        .await?;

    // 3) Push jobs from your own transaction.
    let mut tx = pool.begin().await?;
    let _id = Pusher::new("email_send")
        .push(&mut tx, &EmailTask {
            to: "a@b.example".into(),
            body: "hello".into(),
        })
        .await?;
    tx.commit().await?;

    // 4) Graceful shutdown — bounded drain returns a Stats snapshot.
    tokio::signal::ctrl_c().await?;
    let stats = handle.shutdown(Duration::from_secs(10)).await?;
    tracing::info!(
        completed = stats.completed,
        failed = stats.failed,
        aborted = stats.aborted,
        pending_recovery = stats.pending_recovery,
        "shutdown complete"
    );
    Ok(())
}
#
# async fn send_smtp(_t: &EmailTask, _key: &str) -> Result<(), String> { Ok(()) }
```

## Delivery semantics: at-least-once

`pg_work_queue` provides **at-least-once** delivery. A handler may be
invoked one *or more* times for the same logical job. Common causes:

- `handler_timeout` elapsed while the handler was awaiting an external
  call — the future is dropped, `mark_retry` fires, the next claim
  re-invokes the handler.
- Worker process crashed (OOM kill, network partition, SIGKILL) after
  the handler issued its side-effect but before `mark_done` committed —
  the reaper flips the row to `awaiting_retry` after `lease_timeout`.
- `mark_done` was fenced out (rare; the worker paused long enough that
  the reaper took the row first) — the row is `awaiting_retry`,
  `attempts` is already incremented, and the next worker re-runs the
  handler.

The library cannot give you exactly-once external side-effects — no
polling queue can. What it does give you is `JobContext::idempotency_key`:

- **Stable across retries** — set once at push time, never rewritten.
- **Unique per logical job** — UUIDv7, near-zero collision.
- **Always present** — handed to the handler in every attempt.

### Using `idempotency_key` for external APIs

```rust
use pg_work_queue::{JobContext, JobError};

# async fn dedupe(_k: &str) -> Result<bool, String> { Ok(false) }
# async fn stripe_charge(_a: u64, _k: &str) -> Result<(), String> { Ok(()) }
# async fn record(_k: &str) -> Result<(), String> { Ok(()) }
# #[derive(serde::Deserialize)] struct ChargeTask { amount: u64 }
async fn handle(task: ChargeTask, ctx: JobContext) -> Result<(), JobError> {
    // 1) Check your own dedup store first — cheap, no external call.
    if dedupe(&ctx.idempotency_key.to_string())
        .await
        .map_err(JobError::retry)?
    {
        return Ok(()); // already charged on a previous attempt
    }

    // 2) External API: pass the same key so the provider also dedups.
    stripe_charge(task.amount, &ctx.idempotency_key.to_string())
        .await
        .map_err(JobError::retry)?;

    // 3) Record success so subsequent retries (if any) short-circuit.
    record(&ctx.idempotency_key.to_string())
        .await
        .map_err(JobError::retry)?;

    Ok(())
}
```

Stripe, AWS, and most modern HTTP APIs accept an `Idempotency-Key`
header — `idempotency_key` is a UUIDv7 and a drop-in fit.

### Cancellation gotcha (CPU-bound work)

`handler_timeout` is wired through `tokio::time::timeout`, which only
cancels at `.await` points. A handler that does CPU-bound work (or
blocking I/O) without yielding will **not** be cancelled — and the
lease can expire while the handler is still running. Use
`tokio::task::spawn_blocking` for blocking work, or sprinkle
`tokio::task::yield_now().await` between iterations.

## Architecture

- **One Postgres table**: `pgwq.jobs`. ENUM `pgwq.job_status` plus CHECK
  invariants enforce the state machine DB-side.
- **Polling worker** + a **reaper task**. No `LISTEN/NOTIFY` (commit
  notifications serialize cluster-wide).
- **Fencing tokens**: every `mark_*` carries the claim's `lease_token`
  in its `WHERE`. A stale worker can never overwrite a row reclaimed
  by the reaper.
- **Per-row `max_attempts`**: stamped at claim time so a rolling deploy
  that changes the worker's `max_attempts` only affects newly-claimed
  rows (in-flight rows keep the claiming worker's value).
- **Permits-first poll loop**: the loop acquires concurrency permits
  *before* `claim_batch`, so claim count never exceeds free permits
  (no spawn backlog).
- **Reaper is process-death recovery only**: not the handler cancel
  path. Handler timeouts go through `handler_timeout`, which is much
  shorter than `lease_timeout`.

## Operational knobs

Every knob has a behavioral test that measures observable effect at two
distinct values. The exhaustive list lives in the rustdoc for
[`WorkerBuilder`]; the highlights:

| Method | Default | Effect | Test |
|---|---|---|---|
| `queue` | required | per-queue isolation | `builder_validation.rs` |
| `pool` | required | DB capacity (`>= concurrency × 2 + 2`) | `builder_validation.rs` |
| `max_attempts` | 3 | retries before dead-letter | `max_attempts_behavior.rs` |
| `lease_timeout` | 30s | process-death recovery threshold | `lease_timeout_behavior.rs` |
| `batch_size` | 32 | rows per `claim_batch` (1..=1000) | `batch_size_behavior.rs` |
| `retry_backoff` | `Exponential { 1s, 2.0, 5min, 0.2 }` | retry pacing | `retry_backoff_behavior.rs` |
| `panic_policy` | `Retry` | terminal status on panic | `panic_policy_behavior.rs` |
| `poll_interval` | 1s | pickup latency upper bound | `poll_interval_behavior.rs` |
| `concurrency` | num_cpus | parallel handler slots | `concurrency_behavior.rs` |
| `handler_timeout` | `lease × 0.8` | per-handler wall clock | `handler_timeout_behavior.rs` |
| `mark_timeout` | `lease − handler − 1s` | `mark_*` SQL wait cap | `shutdown_immediate_with_pool_starvation.rs` |
| `reaper_interval` | `lease / 4` | reaper tick cadence | `reaper_interval_behavior.rs` |
| `codec` | `JsonCodec` | payload serialization | `codec_swappable.rs` |

Builder validation rejects out-of-range values at `build()` time —
`BuildError` is `#[non_exhaustive]` so new constraints can land without
breaking callers.

## Manual retention

The library does **not** spawn an automatic cleanup task. Operators
call these helpers from their own scheduler (cron, `tokio::interval`,
manual):

```rust,no_run
use std::time::Duration;

# async fn run(pool: sqlx::PgPool) -> Result<(), pg_work_queue::PurgeError> {
let purged_done = pg_work_queue::purge_done(
    &pool,
    Duration::from_secs(7 * 24 * 3600),
).await?;

let purged_dead = pg_work_queue::purge_dead(
    &pool,
    Duration::from_secs(90 * 24 * 3600),
).await?;

let stats = pg_work_queue::queue_stats(&pool, "email_send").await?;
println!("running = {}, dead = {}", stats.running, stats.dead);
# Ok(()) }
```

Both purge helpers iterate `LIMIT = 10_000` chunks under
`FOR UPDATE SKIP LOCKED` so a long retention sweep never blocks (and
is never blocked by) running workers.

## What this crate deliberately does not do

- **No `LISTEN/NOTIFY`** — commit-NOTIFY serializes cluster-wide.
- **No automatic retention sweeper** — user invokes `purge_done` /
  `purge_dead` on their own schedule.
- **No multi-backend abstraction** — Postgres-only by design.
- **No worker dashboard, GUI, or metrics endpoint** — observability is
  via `tracing` events and direct queries on `pgwq.jobs`.
- **No Tower middleware stack.**
- **No typed retry strategies in the handler API** — only
  `Err(JobError::Retry { reason, retry_in })` or
  `Err(JobError::Abort { reason })`. `Ok(())` means done.
- **No cross-worker priorities or fairness.**
- **No push-side dedup column** in `v0.1`. Use your own
  `INSERT ... ON CONFLICT` before `Pusher::push` if you need it.
- **No worker-registration table** — the reaper sees only
  `jobs.last_attempted_at` + `lease_token`. Avoids the apalis
  "reaper-joined-to-purged-worker-row" race.

See `PLAN.md` for the full anti-features rationale.

## Known limitations

### Shared `_sqlx_migrations` table

`pg_work_queue::migrator()` uses `sqlx::migrate!()` which (in `sqlx`
`0.8.x`) hard-codes the migration tracking table to `_sqlx_migrations`.
If your application also runs `sqlx::migrate!()` against the same
database, `version` collisions are a matter of time — both migrators
write to the same table and treat each other's rows as "missing
migrations".

Workarounds for now:

- Apply this crate's migrations against a database that is **not**
  shared with your application's own sqlx-managed schema, or
- Run the embedded SQL in `migrations/20260513000000_v01_init.sql`
  yourself via your own migration tooling, skipping
  `pg_work_queue::migrator()`.

`sqlx::migrate::Migrator::dangerous_set_table_name` (which would let us
namespace the table to `_pgwq_migrations`) is only available on the
`sqlx` `0.9` line — pinned `0.8.6` here predates it. The fix will land
once `sqlx 0.9` reaches a stable release.

## Testing

Integration tests run against real Postgres via
[`testcontainers`](https://crates.io/crates/testcontainers) (PG18
image). Requires a working Docker daemon.

```bash
cargo test --no-fail-fast
```

Run a single behavioral test:

```bash
cargo test --test poll_interval_behavior
```

## License

MIT.
