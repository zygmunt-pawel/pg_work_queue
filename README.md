# pg_work_queue

Minimal polling-based Postgres job queue for Rust. One table, one CTE per
operation, no `LISTEN/NOTIFY`. Built around fencing tokens, per-row
`max_attempts`, and at-least-once semantics.

```text
+-----------+      +---------+       +-----------+
|  Pusher   | ---> | pgwq.jobs| <--- |  Worker   |
|  (your tx)|      |  (PG 18) |      |  (poll)   |
+-----------+      +----+----+       +-----+-----+
                        ^                  |
                        |                  v
                        |            +-----+-----+
                        +------------+  Reaper   |
                          process-   |  (lease)  |
                          death      +-----------+
                          recovery
```

## Status

`v0.1` — pre-publish. Postgres **18+** (uses native `uuidv7()`), Rust 1.88+,
Tokio, MIT licensed.

The crate has ~120 integration tests running against real Postgres 18 via
[`testcontainers`](https://crates.io/crates/testcontainers). Every public
builder knob has a paired behavioral test that measures observable effect
at two distinct values.

## Table of contents

- [What this crate is](#what-this-crate-is)
- [What this crate is not](#what-this-crate-is-not)
- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Delivery semantics: at-least-once](#delivery-semantics-at-least-once)
- [Per-key concurrency](#per-key-concurrency)
- [State machine and schema](#state-machine-and-schema)
- [API reference](#api-reference)
  - [`migrator()`](#migrator)
  - [`Pusher` — enqueue side](#pusher--enqueue-side)
  - [`Worker` / `WorkerBuilder`](#worker--workerbuilder)
  - [`WorkerHandle` — lifecycle](#workerhandle--lifecycle)
  - [`JobContext` — handler argument](#jobcontext--handler-argument)
  - [`JobError` — handler outcome](#joberror--handler-outcome)
  - [`BackoffPolicy`](#backoffpolicy)
  - [`PanicPolicy`](#panicpolicy)
  - [`Codec` / `JsonCodec`](#codec--jsoncodec)
  - [`Stats` / `TickStats` / `QueueStats`](#stats--tickstats--queuestats)
  - [Retention helpers](#retention-helpers)
  - [Error types](#error-types)
  - [Resource limits](#resource-limits)
- [Tracing / observability](#tracing--observability)
- [Design decisions](#design-decisions)
- [Known limitations](#known-limitations)
- [Testing](#testing)
- [License](#license)

## What this crate is

- A **single-table Postgres job queue**: producers push rows, workers poll
  and run handlers, retries flow back through the same table.
- A **library**, not a framework. You own your `PgPool`, your async runtime
  setup, your migration tooling, your scheduler. The crate just gives you
  `Worker` + `Pusher` + a few helpers.
- **Polling-based**. Each worker runs a `tokio::time::interval` tick and
  issues a `claim_batch` CTE per cycle. No `LISTEN/NOTIFY`, no background
  goroutines wandering around.
- **Process-death safe**. A separate reaper task sweeps stale `running`
  rows back into `awaiting_retry` (or `dead`) based on a per-row lease
  deadline; every `mark_*` carries a fencing token so a paused worker can
  never overwrite a row the reaper already clawed back.
- **At-least-once** by construction. The library guarantees a row is not
  lost; it does *not* guarantee a handler runs exactly once. Use
  `JobContext::idempotency_key` for external-API dedup.

## What this crate is not

- **Not** a multi-backend abstraction. Postgres-only by design — the
  schema, the CTEs, and the locking strategy lean hard on Postgres
  semantics (`FOR UPDATE SKIP LOCKED`, partial indexes, `ENUM`, native
  `uuidv7()`).
- **Not** a `LISTEN/NOTIFY` queue. Commit-NOTIFY serializes cluster-wide
  and breaks under modest write rate; polling at 500ms–1s is the better
  default for v0.1.
- **Not** an exactly-once system. No polling queue can be — see
  [Delivery semantics](#delivery-semantics-at-least-once).
- **Not** an admin dashboard. There is no GUI, no metrics endpoint, no
  HTTP server. Observability is `tracing` events and `pgwq.jobs` queries.
  An `admin` module (list / cancel / requeue / `list_by_correlation`) is
  on the v0.2 roadmap.
- **Not** a Tower middleware stack. The handler is a plain
  `Fn(T, JobContext) -> impl Future<Output = Result<(), JobError>>`.
- **Not** a typed-retry-strategy DSL. Handlers return one of:
  `Ok(())`, `Err(JobError::Retry { reason, retry_in })`,
  `Err(JobError::Abort { reason })`. That is the entire vocabulary.
- **Not** an auto-retention sweeper. The library does **not** spawn a
  background cleanup task. Operators invoke `purge_done` / `purge_dead`
  on their own schedule.
- **Not** a cross-worker priority / fairness scheduler. Within a queue,
  ordering is `(run_at, id)` ASC; across queues, each worker handles
  exactly one queue.
- **Not** a worker-registration directory. There is no `pgwq.workers`
  table — the reaper looks only at `lease_expires_at`. This avoids the
  Apalis-style "reaper joined to purged worker row" race entirely.

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

    // 1) Apply the schema. The library does NOT migrate automatically.
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

    // 3) Push jobs from your own transaction. The trailing `None` is the
    //    optional per-key concurrency key — see "Per-key concurrency".
    let mut tx = pool.begin().await?;
    let _id = Pusher::new("email_send")
        .push(&mut tx, &EmailTask {
            to: "a@b.example".into(),
            body: "hello".into(),
        }, None)
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
# async fn send_smtp(_t: &EmailTask, _key: &str) -> Result<(), String> { Ok(()) }
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          pgwq.jobs                              │
│ id BIGINT PK | public_id UUIDv7 | queue | payload BYTEA         │
│ status ENUM  | attempts | max_attempts | lease_token UUID(v4)   │
│ lease_expires_at | run_at | first_attempted_at | finished_at    │
└─────────────────────────────────────────────────────────────────┘
              ▲                          ▲                  ▲
              │ INSERT (user tx)         │ UPDATE … RETURNING│ UPDATE
              │                          │ (claim_batch)     │ (reaper)
              │                          │                  │
        +-----+-------+        +---------+--------+    +----+------+
        |    Pusher   |        |  Worker poll     |    |  Reaper   |
        |  (own tx)   |        |  loop + handler  |    |  (lease)  |
        +-------------+        |  + mark_*        |    +-----------+
                               +------------------+
```

Five components, all crate-internal except `Pusher` and `Worker`:

1. **One Postgres schema** (`pgwq`) with **one table** (`pgwq.jobs`). State
   transitions enforced DB-side by an ENUM (`pgwq.job_status`) plus a
   `jobs_status_invariants` CHECK that pins `(status, attempts,
   last_attempted_at, lease_token, …)` consistency. A buggy SQL path that
   tried to flip the row into an impossible shape would fail loudly
   instead of corrupting state.
2. **`Pusher`** — your code calls `Pusher::push(&mut tx, &payload)` from
   inside your *own* transaction. Job insert and your business write
   commit atomically. Returns a `Uuid` (`uuidv7`, time-ordered) which is
   stable across retries.
3. **`Worker` poll loop** — `tokio::time::interval(poll_interval)`. Each
   tick:
   - **Acquire permits first** (semaphore, capacity = `concurrency`). The
     loop only claims as many rows as it has free permits → no spawn
     backlog.
   - Run the claim CTE: `FOR UPDATE SKIP LOCKED` on up to
     `min(batch_size, free_permits)` rows whose status is `queued` or
     `awaiting_retry` and `run_at <= now()`, then `UPDATE … status =
     'running'`, increment `attempts`, stamp `lease_token` (v4),
     `lease_expires_at = now() + lease_timeout`, and `max_attempts =
     <worker's max>`. With no `concurrency_limits` configured this is
     the plain `claim_batch` CTE; with limits configured the loop
     instead runs a per-key bounded keyed claim that respects each
     key's remaining headroom (see [Per-key
     concurrency](#per-key-concurrency)).
   - Decode each payload through the configured `Codec` inside
     `catch_unwind` — codec panic or decode error → `mark_dead`
     immediately, row dropped from the batch.
   - Spawn each surviving job into a `JoinSet` under
     `tokio::time::timeout(handler_timeout, handler_future)`.
   - On handler return, fire `mark_done` / `mark_retry` / `mark_dead`
     under `tokio::time::timeout(mark_timeout, _)`. Every `mark_*` SQL
     carries `WHERE status = 'running' AND lease_token = $token` — the
     fencing-token guard.
4. **Reaper task** — a sibling `tokio::spawn` that ticks at
   `reaper_interval` and runs a single CTE that flips stale `running`
   rows (`lease_expires_at < now()`) back to `awaiting_retry`, or to
   `dead` if `attempts >= max_attempts`. The reaper is **only** the
   process-death recovery path — handler-level cancellation goes through
   `handler_timeout`, which is much shorter than `lease_timeout`.
   Adaptive: when a tick reaps a full `REAPER_BATCH_SIZE` (1024) batch,
   the next tick skips its `interval.tick()` to drain backlog at SQL
   speed.
5. **Retention helpers** — `purge_done` and `purge_dead` chunk-DELETE
   terminal rows in `PURGE_CHUNK_SIZE` (10_000) iterations under
   `FOR UPDATE SKIP LOCKED` so a retention sweep never blocks (and is
   never blocked by) live workers.

### Architectural rules baked into the design

- **Fencing tokens, not advisory locks.** A v4 UUID is stamped at claim
  time and cleared on every transition out of `running`. Every `mark_*`
  matches `WHERE … AND lease_token = $token` — a paused worker that
  wakes after the reaper reclaimed the row sees `rows_affected = 0`
  (`fenced_out` in stats) and moves on.
- **Per-row `max_attempts`.** Each worker stamps its own `max_attempts`
  on the row at claim time. A rolling deploy that changes the worker's
  `max_attempts` only affects newly-claimed rows; in-flight rows keep
  the value the *claiming* worker stamped. Reaper / `mark_retry` both
  consult `j.max_attempts` (not worker-local state) for the
  `dead-vs-retry` verdict, so heterogeneous replicas produce
  deterministic outcomes.
- **Permits-first poll loop.** Claim count never exceeds free permits.
  The semaphore is acquired *before* `claim_batch`, never after — no
  spawn backlog regardless of handler latency.
- **No worker registration table.** The reaper sees only
  `lease_expires_at` + `lease_token` per row. This avoids the
  apalis-style "reaper-joined-to-purged-worker-row" race entirely.
- **Two claim paths, one chosen per worker.** With no
  `concurrency_limits` configured the poll loop runs the plain
  `claim_batch` CTE (a single `(queue, run_at, id)` range scan). With
  limits configured it runs a per-key bounded *keyed claim* — the CTE
  takes a per-key headroom map and claims at most `headroom` rows per
  key, so a saturated key contributes nothing. The branch is decided by
  whether the headroom map is empty; `tick_once` always passes an empty
  map. See [Per-key concurrency](#per-key-concurrency).

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
blocking I/O) without yielding will **not** be cancelled — and the lease
can expire while the handler is still running. Use
`tokio::task::spawn_blocking` for blocking work, or sprinkle
`tokio::task::yield_now().await` between iterations.

### Branching on retry budget

`JobContext` exposes both `attempt` (1-indexed current) and `max_attempts`
(per-row value stamped by the claiming worker — *not* the current
builder's setting, so rolling deploys that change `.max_attempts(...)`
do not retroactively rewrite in-flight rows). Use the pair to branch
handler logic on remaining retry budget without trusting an out-of-band
config.

Note: the library already upgrades `JobError::Retry` to `mark_dead`
when `attempts >= max_attempts`, so the handler does **not** need to
detect the last attempt in order to dead-letter. The patterns below
are about what the *handler* should do differently when it knows the
retry budget is about to run out — observability, side effects, perf
tuning — not about controlling the row's terminal state.

**Warn-and-alert on final retry.** Emits a distinct tracing event the
last time around, so oncall can alert on `pgwq.final_attempt` without
false positives for every transient failure:

```rust
# use pg_work_queue::{JobContext, JobError};
# #[derive(serde::Deserialize)] struct Email { to: String }
# async fn send_email(_e: &Email) -> Result<(), String> { Ok(()) }
async fn handle(task: Email, ctx: JobContext) -> Result<(), JobError> {
    let res = send_email(&task).await;
    if res.is_err() && ctx.attempt == ctx.max_attempts {
        tracing::warn!(
            target: "pgwq.final_attempt",     // user-defined; the library does not emit this
            job.id = ctx.id,
            attempt = ctx.attempt,
            "final retry about to be dead-lettered"
        );
    }
    res.map_err(JobError::retry)
}
```

**Branching retry-only fallbacks.** Cheap-and-fast on early attempts,
spend the budget on the last shot:

```rust
# use pg_work_queue::{JobContext, JobError};
# use std::time::Duration;
# #[derive(serde::Deserialize)] struct Fetch { url: String }
# struct Client; impl Client {
#     fn get(&self, _u: &str) -> Self { Client }
#     fn timeout(self, _d: Duration) -> Self { Client }
#     async fn send(self) -> Result<(), String> { Ok(()) }
# }
# let http_client = Client;
async fn handle(task: Fetch, ctx: JobContext) -> Result<(), JobError> {
    let timeout = if ctx.attempt < ctx.max_attempts {
        Duration::from_secs(5)    // fail-fast, retry budget left
    } else {
        Duration::from_secs(60)   // last shot, give it room
    };
    # let http_client = Client;
    http_client.get(&task.url).timeout(timeout).send().await
        .map_err(JobError::retry)
}
```

Same shape works for "try fallback DNS resolver only on last attempt",
"accept stale cache on last attempt instead of regenerating", or
"insert into manual-review table once before dead-lettering".

## Per-key concurrency

`WorkerBuilder::concurrency` is one process-wide cap over the whole
handler. **Per-key concurrency** adds a second, narrower axis: cap how
many jobs of a given *kind* run at once — e.g. one job kind hits a
rate-limited external API, another is heavy and must not be flooded.

### The model

- A job carries an **optional `concurrency_key: Option<&str>`**, stamped
  once at enqueue (`Pusher::push` / `push_at` / `push_batch`), and
  **immutable** for the job's lifetime (a DB trigger rejects any later
  `UPDATE` of the column). `None` = no limit (the pre-feature behavior).
  Keys are opaque, byte-exact (`COLLATE "C"`), case-sensitive, not
  trimmed.
- Limits are **worker configuration**: `WorkerBuilder::concurrency_limits`
  takes a `key → limit` map. A key present on a job but absent from the
  map is unlimited; a `None` key is unlimited.
- For a configured `(queue, concurrency_key)` pair, **at most `limit`
  handler tasks execute concurrently**.

### Claim-time gating (no head-of-line blocking)

The cap is applied **at claim time, not after**. A naive in-handler
semaphore would be wrong: the job is already `running` and leased, so
surplus jobs would park on the semaphore, occupy `Worker` permits,
starve other job kinds (head-of-line blocking), and burn
`attempts` / lease. Instead, the worker simply **does not claim** a job
whose key is saturated — the row stays `queued`, and `Worker` permits
stay free for other keys. Each poll tick the worker computes
`headroom = limit − running_count` per configured key and the keyed
claim CTE claims at most `headroom` rows per key. Cross-key allocation
within a tick stays global `(run_at, id)` FIFO; a saturated key simply
contributes zero rows until a slot frees.

### The guarantee surface: live handler tasks, not `running` rows

The bound is over **live handler tasks in this process**, tracked by an
in-memory counter — *not* over rows in `status = 'running'`. A slot is
consumed when a handler task is spawned and released (via an RAII guard)
when that task exits, regardless of the `mark_*` outcome. A row can be
`running` in the database with no live handler behind it (a crash ghost)
— such a row consumes no real resource, and the worker does not count it.

### Single-instance / single-`Worker`-object assumption

Per-key counting lives in **process memory**, so the limit is only
correct under **exactly one `Worker` object driving the queue**:

- Running **2+ `Worker` processes or objects** against the same queue
  silently multiplies the effective limit — each driver counts only its
  own tasks, so N drivers admit up to `N × limit`.
- `Worker::tick_once` does **not** enforce per-key limits — it claims via
  the plain `claim_batch` path with an empty headroom map. Running
  `tick_once` against a queue that a `start()`ed `Worker` also drives
  defeats the limit.

This is a deliberate v0.1 trade-off: DB-coordinated counting would need a
`running`-row aggregate plus MVCC-race handling. The rest of the crate's
machinery (reaper, fencing tokens, `SKIP LOCKED`) is crash-recovery, not
clustering — it is unaffected and a single instance needs it just as much.

### After a crash: no counter seed

The counter is **never seeded from the database**; it starts at zero for
every configured key. After a previous process crashed, rows it left in
`status = 'running'` are ghosts — their handlers died with that process.
The fresh worker correctly runs up to `limit` *new* tasks. The database
may therefore **transiently show more than `limit` rows in
`status = 'running'`** (ghosts + live tasks) — this is stale metadata,
not real over-execution; the reaper reclaims the ghosts within
`≤ lease_timeout + reaper_interval`.

### Non-cooperative handlers hold their slot

`handler_timeout` is cooperative — a CPU-bound handler with no `.await`
is not cancelled and may run past `lease_timeout` (see [Cancellation
gotcha](#cancellation-gotcha-cpu-bound-work)). Its key slot is held the
whole time it runs — correct, since the work *is* still running.
Meanwhile the reaper may reclaim that job's row to `awaiting_retry` and
the poll loop may re-claim it, acquiring a *second* slot for a second
task of the same job — so the key is transiently double-counted. This is
in the safe direction (a conservative over-count → under-admission) and
self-heals once the stuck task ends.

### Usage

```rust
# use pg_work_queue::{Worker, JobError, JobContext, Pusher};
# use serde::{Deserialize, Serialize};
# #[derive(Serialize, Deserialize)]
# struct ApiCall { endpoint: String }
# async fn run(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
// Worker: cap each key independently. `stripe` -> at most 2 concurrent
// handler tasks; `s3-bulk` -> at most 8. A job whose key is not listed
// here (or whose key is `None`) is unlimited.
let worker = Worker::<ApiCall>::builder()
    .pool(pool.clone())
    .queue("api_calls")
    .concurrency_limits([
        ("stripe".to_string(), 2),
        ("s3-bulk".to_string(), 8),
    ])
    .handler(|task: ApiCall, _ctx: JobContext| async move {
        call(&task).await.map_err(JobError::retry)?;
        Ok(())
    })
    .build()?;

// Pusher: stamp the key at enqueue time (4th argument of `push`).
let mut tx = pool.begin().await?;
Pusher::new("api_calls")
    .push(&mut tx, &ApiCall { endpoint: "/charges".into() }, Some("stripe"))
    .await?;
tx.commit().await?;
# let _ = worker;
# Ok(())
# }
# async fn call(_t: &ApiCall) -> Result<(), String> { Ok(()) }
```

Per-key limits and the worker-wide `concurrency` are **independent
axes** — there is no cross-knob constraint between them. A per-key limit
larger than `concurrency` simply means the worker-wide cap binds first.

## State machine and schema

```
            push                claim_batch
   ∅ ───────────────▶ queued ────────────────▶ running
                       ▲                          │
                       │                          │
                       │     mark_retry           │
                       │   (attempts < max)       │
                       │  + run_at = now+backoff  │
                       │                          │
                  awaiting_retry ◀────────────────┤
                       │                          │
                       │    reaper                │
                       │   (lease expired)        │
                       │                          ▼
                       └─────────── mark_done ──▶ done
                                                   ▼
                                              mark_dead ─▶ dead
                                              (Abort, panic,
                                               attempts ≥ max,
                                               codec error)
```

The five states are encoded as `pgwq.job_status`:

| state             | terminal? | meaning                                                      |
|-------------------|-----------|--------------------------------------------------------------|
| `queued`          | no        | newly pushed; `run_at` may be future-dated                   |
| `running`         | no        | claimed by a worker; `lease_token` + `lease_expires_at` set  |
| `awaiting_retry`  | no        | failed transiently; `run_at` is the next-attempt time        |
| `done`            | yes       | handler returned `Ok(())` and `mark_done` flipped the row    |
| `dead`            | yes       | `JobError::Abort`, `attempts >= max_attempts`, codec error, panic under `PanicPolicy::Dead`, or reaper saw `attempts >= max_attempts` |

The `jobs_status_invariants` CHECK pins the full `(status, attempts,
*_at, lease_token)` shape per state — any code path that produces a
logically impossible row fails loudly.

Table layout:

```sql
CREATE TABLE pgwq.jobs (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    public_id          UUID NOT NULL DEFAULT uuidv7() UNIQUE,
    queue              TEXT COLLATE "C" NOT NULL,
    payload            BYTEA NOT NULL,
    status             pgwq.job_status NOT NULL DEFAULT 'queued',
    attempts           INTEGER NOT NULL DEFAULT 0,
    max_attempts       INTEGER NOT NULL DEFAULT 0,
    lease_token        UUID,
    lease_expires_at   TIMESTAMPTZ,
    last_error         TEXT,
    last_attempted_at  TIMESTAMPTZ,
    first_attempted_at TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    run_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    concurrency_key    TEXT COLLATE "C",   -- NULL = no per-key limit
    /* + CHECK constraints, see below */
);
```

`concurrency_key` is nullable and **immutable after insert** — see
[Per-key concurrency](#per-key-concurrency). It is orthogonal to the
status FSM (not part of `jobs_status_invariants`).

Indexes (all partial):

- `jobs_claim_idx (queue, run_at, id) INCLUDE (concurrency_key) WHERE
  status IN ('queued', 'awaiting_retry')` — claim hot path. The
  `(queue, run_at, id)` KEY prefix is byte-identical to the pre-feature
  index (global FIFO order and the plain `claim_batch` range scan are
  unaffected — `INCLUDE` columns live only in leaf pages); the covering
  `concurrency_key` lets the keyed claim's anti-join filter without a
  heap fetch.
- `jobs_claim_conc_idx (queue, concurrency_key, run_at, id) WHERE status
  IN ('queued', 'awaiting_retry')` — per-key bounded claim hot path. The
  leading `(queue, concurrency_key)` equality makes each per-key range
  scan Sort-free.
- `jobs_reap_idx (queue, lease_expires_at) WHERE status = 'running'` —
  reaper hot path; `queue` first so a per-queue reaper scans only its
  slice.
- `jobs_terminal_idx (finished_at) WHERE status IN ('done', 'dead')` —
  purge hot path.

Each enqueue and each `running → awaiting_retry` transition now
maintains **both** claim indexes (`jobs_claim_idx` and
`jobs_claim_conc_idx` share the partial predicate) — ~2× claim-index
entry churn versus the pre-feature schema. Inherent cost of the feature.

CHECK constraints (defense-in-depth — library validates pre-INSERT too):

- `jobs_queue_nonempty` / `jobs_queue_max_len` (≤ 64).
- `jobs_payload_max_size` (`octet_length(payload) ≤ 1 MiB`).
- `jobs_last_error_max_len` (≤ 8 KiB chars).
- `jobs_concurrency_key_len` — `concurrency_key` is `NULL` or
  `1..=128` characters. Added `NOT VALID` (every pre-existing row is
  `NULL`, so the unscanned rows are provably valid); still enforced on
  every future `INSERT` / `UPDATE`.
- `jobs_attempts_nonneg`, `jobs_max_attempts_nonneg`.
- `jobs_temporal` — monotonic timestamps (`first_attempted_at ≥
  created_at`, `last_attempted_at ≥ first_attempted_at`,
  `finished_at ≥ last_attempted_at`, `updated_at ≥ created_at`,
  `run_at ≥ created_at`).
- `jobs_status_invariants` — full FSM coherence; e.g. `status =
  'running'` ⇔ `lease_token IS NOT NULL AND lease_expires_at IS NOT
  NULL`.

Triggers:

- `set_updated_at` — `BEFORE UPDATE`, touches `updated_at`.
- `assert_concurrency_key_immutable` — `BEFORE UPDATE`, raises an
  exception if `concurrency_key` changes between `OLD` and `NEW`. The
  crate's own claim / mark / reaper UPDATEs never name `concurrency_key`
  in their `SET` lists, so for them the trigger is inert; its real
  purpose is to reject an external hand-rolled `UPDATE pgwq.jobs SET
  concurrency_key = …` that would desync the in-memory per-key counter.
  `BEFORE UPDATE` only, so the `NULL → value` `INSERT` at enqueue is
  allowed.

Table storage parameters:

- `fillfactor = 90` — 10% per-block headroom for non-HOT updates.
- `autovacuum_vacuum_scale_factor = 0.05`,
  `autovacuum_analyze_scale_factor = 0.05` — high-churn table.

The migration starts with a `DO $$ … RAISE EXCEPTION` block that loudly
fails on `server_version_num < 180000` instead of letting `uuidv7()`
surface a cryptic "function does not exist" later.

## API reference

This section walks every public item re-exported from the crate root.
The intent is that you can build a working integration from this section
alone, without reading the source.

### `migrator()`

```rust
pub fn migrator() -> sqlx::migrate::Migrator;
```

Returns the embedded `sqlx::Migrator` for the crate's schema. Call
`.run(&pool).await` once at startup, before `Worker::start`. The library
**does not** migrate automatically (explicit > implicit). Internally
the returned migrator has `set_ignore_missing(true)`, so it co-exists
with other sqlx migrators sharing `_sqlx_migrations` without erroring
on each other's rows. See [Known limitations](#known-limitations) for
the remaining caveats (unique filename `version`, no table namespacing
until sqlx 0.9).

### `Pusher` — enqueue side

```rust
pub struct Pusher<C = JsonCodec> { /* … */ }
```

Cheap-to-clone handle for one queue. Generic over `Codec`; default
`JsonCodec`. Queue-name validity is **fail-late** — `Pusher::new` is
infallible; the check fires on each push call.

| method | signature | notes |
|---|---|---|
| `new` | `Pusher::new(queue: impl Into<String>) -> Self` | Default `JsonCodec`. |
| `with_codec` | `fn with_codec<C2: Codec>(self, codec: C2) -> Pusher<C2>` | Swap the codec, keep the queue name. |
| `push` | `async fn push<T: Serialize + Sync>(&self, tx: &mut PgConnection, payload: &T, concurrency_key: Option<&str>) -> Result<Uuid, PushError>` | Single insert, `run_at = now()`. Returns the row's `public_id` (UUIDv7 generated client-side). |
| `push_at` | `async fn push_at<T: Serialize + Sync>(&self, tx, payload, run_at: DateTime<Utc>, concurrency_key: Option<&str>) -> Result<Uuid, PushError>` | Same as `push`, but explicit `run_at`. |
| `push_batch` | `async fn push_batch<T: Serialize + Sync>(&self, tx, items: &[(T, Option<String>)]) -> Result<Vec<Uuid>, PushError>` | One round-trip insert via `INSERT … SELECT … FROM unnest($2::bytea[], $3::uuid[], $4::text[])`. Each item is a `(payload, concurrency_key)` pair. Returns IDs in input order. |

All four take `tx: &mut PgConnection` so they participate in the
caller's transaction — the job insert commits atomically with your
business writes, or rolls back together. There is no autocommit
convenience overload; if you want one, write a 3-line wrapper:

```rust
# async fn run<T: serde::Serialize + Sync>(
#     pool: sqlx::PgPool, pusher: pg_work_queue::Pusher, payload: T,
# ) -> Result<(), Box<dyn std::error::Error>> {
let mut tx = pool.begin().await?;
let id = pusher.push(&mut tx, &payload, None).await?;
tx.commit().await?;
# let _ = id;
# Ok(())
# }
```

`concurrency_key` is the optional per-key concurrency bucket — `None`
for "no limit" (the common case). See [Per-key
concurrency](#per-key-concurrency). A `Some(key)` must be `1..=128`
**characters** (`MAX_CONCURRENCY_KEY_LEN`) or the push fails with
`PushError::ConcurrencyKeyInvalid`. The key bytes do **not** count
toward `MAX_BATCH_BYTES` (which budgets payloads only).

Validation order on every push (`push` / `push_at` / per-item in
`push_batch`):
1. `validate_queue` — empty or > 64 chars → `PushError::QueueNameInvalid`.
2. `concurrency_key` — `Some` key empty or > 128 chars →
   `PushError::ConcurrencyKeyInvalid` (checked *before* the codec encode
   step — it is cheap and should fail before any encoding work).
3. Codec encode + size — codec failure → `PushError::Codec` (single) /
   `PushError::BatchCodec { index, .. }` (batch); encoded size
   `> MAX_PAYLOAD_BYTES` (1 MiB) → `PushError::PayloadTooLarge`.

`push_batch` adds, around the per-item loop:
1. `payloads.is_empty()` → `PushError::BatchEmpty`.
2. `items.len() > MAX_BATCH_SIZE` (10 000) → `PushError::BatchTooLarge`.
3. Aggregate size `> MAX_BATCH_BYTES` (64 MiB) →
   `PushError::BatchPayloadTooLarge` (short-circuit — we never buffer
   multi-GB transient data only to fail at the end).

### `Worker` / `WorkerBuilder`

```rust
pub struct Worker<T, C = JsonCodec> { /* … */ }
pub struct WorkerBuilder<T, C = JsonCodec, H = ()> { /* … */ }

impl<T: DeserializeOwned + Send + 'static> Worker<T, JsonCodec> {
    pub fn builder() -> WorkerBuilder<T, JsonCodec, ()>;
}
```

The `H` type parameter encodes whether `.handler()` has been called.
Calling `.build()` without `.handler()` is a runtime
`BuildError::HandlerMissing` (not a type error — pragmatic so users can
share a partially-configured builder across helpers).

#### Builder methods — full table

Every knob below is verified by a behavioral test that measures
observable effect at two distinct values. Defaults are picked so a
brand-new `Worker::builder().pool(p).queue("q").handler(h).build()`
produces a sensible runtime.

| method | required | default | floor / ceiling | observable effect | error variant |
|---|---|---|---|---|---|
| `queue(q)` | **yes** | — | 1..=64 chars | per-queue isolation | `QueueNameInvalid` |
| `pool(p)` | **yes** | — | `max_conn ≥ concurrency × 2 + 2` | DB capacity | `PoolMissing`, `PoolTooSmall` |
| `handler(f)` | **yes** | — | `Fn(T, JobContext) -> impl Future<Output = Result<(), JobError>>` | row → side-effect | `HandlerMissing` |
| `max_attempts(n)` | no | 3 | ≥ 1 | retries before dead-letter | `MaxAttemptsZero` |
| `lease_timeout(d)` | no | 30s | 1s floor; cross-knobs (see below) | process-death recovery threshold | `LeaseTimeoutBelowFloor`, `LeaseTimeoutTooShort`, `LeaseTimeoutTooShortForReaper` |
| `batch_size(n)` | no | 32 | 1..=1000 | rows per `claim_batch` | `BatchSizeOutOfRange` |
| `retry_backoff(p)` | no | `Exponential { 1s, 2.0, 5min, 0.2 }` | see `BackoffPolicy::validate` | retry pacing | `BackoffInvalid` |
| `panic_policy(p)` | no | `Retry` | — | terminal status on panic | — |
| `poll_interval(d)` | no | 1s | 10ms floor | pickup latency upper bound | `PollIntervalTooShort` |
| `concurrency(n)` | no | `available_parallelism()` (fallback 4) | ≥ 1 | parallel handler slots | `ConcurrencyZero`, `PoolTooSmall` |
| `concurrency_limits(map)` | no | empty | each key 1..=128 chars; each limit 1..=`i32::MAX` | per-key concurrent-task cap | `ConcurrencyKeyInvalid`, `ConcurrencyLimitInvalid` |
| `handler_timeout(d)` | no | `lease × 0.8`, clamped to ≥ 1s | 1s floor; `handler + 1s ≤ lease` | per-handler wall clock | `HandlerTimeoutBelowFloor`, `HandlerTimeoutTooLong` |
| `mark_timeout(d)` | no | `lease − handler − 1s`, clamped to ≥ 100ms | 100ms floor; `≤ lease − handler` | per-`mark_*` SQL wait | `MarkTimeoutTooShort`, `MarkTimeoutTooLong` |
| `reaper_interval(d)` | no | `lease / 4`, clamped to ≥ 1s | 1s floor; `≤ lease / 2` | reaper tick cadence | `ReaperIntervalTooShort`, `ReaperIntervalTooLong` |
| `shutdown_token(t)` | no | fresh `CancellationToken` in `start` | — | propagate cancel from an external `tokio_util` parent token | — |
| `codec(c)` | no | `JsonCodec` | — | payload (de)serialization | — |

**Cross-knob invariants** enforced at `build()` time:

- `lease_timeout ≥ 5 × poll_interval` — guarantees ≥ 5 poll cycles
  worth of margin to commit `mark_*` before the reaper claws back.
- `lease_timeout ≥ 2s` — the only way a valid `reaper_interval` ∈
  `[1s, lease/2]` can exist.
- `handler_timeout + 1s ≤ lease_timeout` — `mark_retry` must have
  margin to commit before the lease expires.
- `mark_timeout ≤ lease_timeout − handler_timeout` — same rationale.
- `reaper_interval ≤ lease_timeout / 2` — reaper must tick at least
  twice per lease so a stale row never sits beyond `lease × 1.5`.
- `max_connections ≥ concurrency × 2 + 2` — each in-flight handler may
  hold one connection for work + one for `mark_*`; the poll loop and
  reaper each need a slot. Read via
  `pool.options().get_max_connections()` (NOT `pool.size()`, which is
  lazy and returns 0 for fresh pools). **This is a per-worker floor.**
  If several workers share the same `PgPool`, the joint floor is
  `Σᵢ (concurrencyᵢ × 2) + 2N` where `N` is the worker count; the
  builder cannot enforce that because each `Worker::start` only sees
  its own pool handle. Under-provisioning a shared pool manifests as
  mark-timeout cascades and growing `pending_recovery`. Recommendation:
  either size the shared pool to the joint floor, or give each worker
  its own pool.

`concurrency_limits` takes any `IntoIterator<Item = (String, u32)>`
(a `HashMap`, a `Vec`, an array literal). It **accumulates** across
calls; a duplicate key — within one call or across calls — takes the
**last** value and emits a `tracing::warn!` on target `pgwq.builder` at
the point of the overwriting `.concurrency_limits()` call (the warning
may be dropped if no subscriber is installed yet). Keys and limits are
validated at `build()`: each key must be `1..=128` characters
(`ConcurrencyKeyInvalid`), each limit must be `1..=i32::MAX`
(`ConcurrencyLimitInvalid { key, limit }`). There is **no** cross-knob
constraint with `concurrency` — the two are independent axes. See
[Per-key concurrency](#per-key-concurrency) for the runtime semantics
and the single-instance assumption.

`BuildError` is `#[non_exhaustive]` so new constraints can be added
without a breaking change.

#### `Worker::tick_once`

```rust
pub async fn tick_once(&self) -> Result<TickStats, sqlx::Error>;
```

Single-shot — claim a batch, run each handler **sequentially**, flip
each row with the fencing-token guard. Intentionally simple: it does
**not** use `handler_timeout` / `mark_timeout` wrappers, and a handler
panic bubbles up through `.await`. It also does **not** enforce
`concurrency_limits` — it claims via the plain `claim_batch` path; per-key
limiting is a property of the `start()` poll loop only. Use for tests and
ad-hoc scripts; use `start()` for production.

#### `Worker::start`

```rust
pub async fn start(self) -> Result<WorkerHandle, StartError>;
```

Runs a one-shot schema probe (`SELECT 1 FROM pgwq.jobs LIMIT 0`) before
spawning anything. Missing schema → `StartError::SchemaMissing` (loud
fail; the Apalis anti-pattern is a silent infinite warn loop).

On success, spawns:
- the poll loop (`poll_loop`),
- the reaper loop (`reaper_loop`),

and returns a `WorkerHandle` that owns the cancellation token + abort
handles + atomic stats counters.

### `WorkerHandle` — lifecycle

```rust
pub struct WorkerHandle { /* … */ }
```

Three terminal methods:

| method | consumes `self` | returns | use when |
|---|---|---|---|
| `cancel(&self)` | no | `()` | Trigger shutdown signal; pair with `join`. |
| `join(self)` | yes | `Result<(), ShutdownError>` | Wait for natural exit; no timeout, no `Stats`. |
| `shutdown(self, timeout: Duration)` | yes | `Result<Stats, ShutdownError>` | **Preferred** in production. Bounded drain + abort cascade + `Stats` snapshot. |

`shutdown` runs a 7-step sequence:

1. **Soft cancel** — fire the cancellation token; poll / reaper exit at
   their next `.await`.
2. **Soft drain (`timeout / 2`)** — await both `JoinHandle`s.
3. **Hard abort poll + reaper** — `AbortHandle::abort()` defends
   against a hung SQL future under pool starvation.
4. **Handler drain (remaining budget)** — wait for in-flight handlers
   to finish naturally up to the overall deadline.
5. **Hard abort handlers** — `JoinSet::abort_all` + 1s bounded drain;
   each cancelled handler is counted in `stats.aborted`.
6. **`pending_recovery` count** — `SELECT count(*) WHERE status =
   'running'` for our queue, under a 500 ms timeout (best-effort).
7. **Build `Stats`** + classify terminal errors. `Fatal` wins over
   `ReaperPanicEscalation` if both are set.

The `Stats` returned by `shutdown` is monotonic since worker start —
not a per-shutdown delta.

**`Drop` safety net.** If a `WorkerHandle` is dropped *without* calling
`shutdown` / `join` / `cancel` first, its `Drop` impl fires the
cancellation token and `abort()`s both the poll and reaper tasks
best-effort. Without that, a dropped handle would silently leak both
loops (each holding an `Arc<WorkerState>` + a pool connection) — the
classic tokio footgun where a dropped `JoinHandle` *detaches* rather
than aborting. Explicit `shutdown` is still the production-recommended
path because `Drop` is sync (no `await`, no handler drain, no `Stats`).

### `JobContext` — handler argument

```rust
#[non_exhaustive]
pub struct JobContext {
    pub id: i64,
    pub public_id: Uuid,
    pub idempotency_key: Uuid,    // == public_id
    pub queue: String,
    pub attempt: u32,             // 1-indexed
    pub max_attempts: u32,        // per-row, stamped at claim time
    pub first_attempted_at: DateTime<Utc>,
    pub lease_token: Uuid,
}
```

`public_id` and `idempotency_key` carry the *same* UUIDv7 — the dual
names are intentional aliases for readability at the call site
(`public_id` matches DB / `Pusher::push` return; `idempotency_key`
matches handler intent on external API calls).

`max_attempts` is the per-row value stamped by the *claiming* worker
(see [Branching on retry budget](#branching-on-retry-budget)), not the
current `WorkerBuilder`'s `.max_attempts(...)` — rolling-deploy safe.

The struct is `#[non_exhaustive]`: future additive fields are not a
breaking change. Access fields by name (`ctx.attempt`,
`ctx.max_attempts`) rather than destructuring exhaustively.

`Job<T>` is the corresponding *internal* pipeline element produced by
`claim_and_decode`; the only public surface is the `.context()`
view-shape passed to the handler.

### `JobError` — handler outcome

```rust
pub enum JobError {
    Retry { reason: String, retry_in: Option<Duration> },
    Abort { reason: String },
}
```

Constructor helpers:

```rust
JobError::retry("smtp 5xx")                          // backoff via builder policy
JobError::retry_in("rate limited", Duration::from_secs(30))  // explicit delay
JobError::abort("invalid payload schema v1")        // bypass retry budget
```

| handler outcome | terminal action | counter |
|---|---|---|
| `Ok(())` | `mark_done` | `Stats::completed` |
| `Err(JobError::Retry { .. })` with `attempts < max_attempts` | `mark_retry` with backoff-derived `run_at` | `Stats::failed` |
| `Err(JobError::Retry { .. })` with `attempts ≥ max_attempts` | `mark_dead` | `Stats::failed` |
| `Err(JobError::Abort { .. })` | `mark_dead` (bypasses retry budget) | `Stats::failed` |
| `handler_timeout` elapsed | synthesised `Retry { reason: "handler_timeout" }`; same `attempts ≥ max` upgrade | `Stats::timed_out` (plus `Stats::failed` if the synthesised mark commits) |
| handler panic | routed via `PanicPolicy` | — |
| `mark_*` returned `rows_affected = 0` (fenced out) | row already transitioned by reaper / sibling worker | `Stats::fenced_out` |
| `mark_*` SQL exceeded `mark_timeout` | row left `running`; reaper recovers | `Stats::mark_timed_out` |

`retry_in` is clamped to `[max(poll_interval, 100ms), 24h]`; any
out-of-band value emits a `tracing::warn!` on target
`pgwq.retry_in.clamped` and is replaced with the clamped value.

`reason` strings are truncated library-side to `MAX_LAST_ERROR_LEN`
(8192 *characters*, UTF-8-boundary safe via `chars().take(N)`) before
hitting the DB. The DB CHECK is the backstop.

### `BackoffPolicy`

```rust
pub enum BackoffPolicy {
    Linear {
        base: Duration,
        increment: Duration,
        cap: Duration,
    },
    Exponential {
        base: Duration,
        factor: f64,     // > 1.0
        cap: Duration,
        jitter: f64,     // [0.0, 1.0] ratio
    },
}

impl BackoffPolicy {
    pub const fn fixed(d: Duration) -> Self;
    pub fn next(&self, attempt: u32) -> Duration;
}

impl Default for BackoffPolicy { /* Exponential { 1s, 2.0, 5min, 0.2 } */ }
```

`BackoffPolicy::fixed(d)` is a convenience: `Linear { base: d, increment:
0, cap: d }`. There is no separate `Fixed` variant on purpose — fewer
arms to match against, no ambiguity about jitter for "fixed" backoff.

`next(attempt)`:
- **Never panics.** `f64::powi` overflow is filtered through
  `is_finite()` + clamp-to-`cap`.
- Exponential jitter applies as `value × (1 + uniform(-jitter,
  +jitter))`, then a final clamp to `[0, cap]`. Linear ignores jitter
  (matches user expectation for "linear").

`validate()` constraints (raised on `build()` as
`BuildError::BackoffInvalid { reason }`):

- `factor` finite, `> 1.0` (Exponential only).
- `jitter` finite, `∈ [0.0, 1.0]` (Exponential only).
- `cap > 0` and `cap ≤ 24h`.
- `base ≥ 100ms` (below that, tight retry loops hammer the DB).

### `PanicPolicy`

```rust
#[derive(Default)]
pub enum PanicPolicy {
    #[default]
    Retry,   // synthesise JobError::Retry; respect retry budget
    Dead,    // bypass retry budget; mark_dead with reason = "panic: <msg>"
}
```

Default `Retry` matches the at-least-once contract — a bug in user code
shouldn't take a row hostage, and the next attempt may run a fixed
binary. Choose `Dead` only when handlers are pure code-reviewed flows
and any panic indicates an unrecoverable bug.

### `Codec` / `JsonCodec`

```rust
pub trait Codec: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Self::Error>;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error>;
}

#[derive(Default, Clone, Copy, Debug)]
pub struct JsonCodec;
```

Default `JsonCodec` is `serde_json` over `BYTEA`. To swap in CBOR /
bincode / etc., implement `Codec` and pass via
`Pusher::with_codec(...)` and `WorkerBuilder::codec(...)`. Both sides of
a queue must agree on the codec.

Decode errors and decode panics are both fatal for the row:
`mark_dead` with `reason = "codec decode: <err>"` or `"codec panic:
<msg>"`. The library runs `codec.decode` inside `catch_unwind`
explicitly so a buggy decoder cannot kill the worker.

### `Stats` / `TickStats` / `QueueStats`

```rust
pub struct Stats {
    pub completed: u64,         // Ok(()) + mark_done committed
    pub failed: u64,            // JobError::* (or panic-routed) + mark_* committed
    pub timed_out: u64,         // handler_timeout fired
    pub mark_timed_out: u64,    // mark_* SQL exceeded mark_timeout
    pub aborted: u64,           // aborted by WorkerHandle::shutdown cascade
    pub fenced_out: u64,        // mark_* returned 0 rows_affected
    pub pending_recovery: u64,  // rows still 'running' at shutdown return (best-effort)
}

pub struct TickStats {          // returned by tick_once
    pub claimed: u64,
    pub completed: u64,
    pub failed: u64,
    pub fenced_out: u64,
}

pub struct QueueStats {         // returned by queue_stats
    pub queued: u64,
    pub awaiting_retry: u64,
    pub running: u64,
    pub done: u64,
    pub dead: u64,
}
```

Documented edge case (`Stats`): a handler whose `mark_done` UPDATE
committed server-side but whose future was cancelled before returning
to the worker → row is `done` in the DB but `completed` does **not**
count it. Under aggressive shutdown `Stats` may be off by 1–2.

### Retention helpers

```rust
pub async fn purge_done(pool: &PgPool, age: Duration) -> Result<u64, PurgeError>;
pub async fn purge_dead(pool: &PgPool, age: Duration) -> Result<u64, PurgeError>;
pub async fn queue_stats(pool: &PgPool, queue: &str) -> Result<QueueStats, PurgeError>;
```

`purge_*` chunk-DELETE under `FOR UPDATE SKIP LOCKED` in
`PURGE_CHUNK_SIZE = 10_000` iterations and emit **one** aggregate
`tracing::info!` per call with `deleted = <total>` (not per-row). Both
return the total rows deleted across all chunks.

`queue_stats` runs a single grouped `count(*) FILTER (WHERE status = …)`
SELECT. It is operator cookbook material, not a hot-loop dashboard
primitive — each filter is a partial-index scan.

Operators call these from their own scheduler (cron, `tokio::interval`,
manual):

```rust
use std::time::Duration;

let purged_done = pg_work_queue::purge_done(
    &pool, Duration::from_secs(7 * 24 * 3600),
).await?;

let purged_dead = pg_work_queue::purge_dead(
    &pool, Duration::from_secs(90 * 24 * 3600),
).await?;

let stats = pg_work_queue::queue_stats(&pool, "email_send").await?;
println!("running = {}, dead = {}", stats.running, stats.dead);
```

### Error types

All error enums are `#[non_exhaustive]` — new variants can be added
without a breaking change.

| enum | when it surfaces |
|---|---|
| `PushError` | `Pusher::push*`. Includes `is_retriable()` helper — `true` only for `Transient` (network/IO/pool); everything else is a deterministic caller-bug-style error. SQLSTATE class `23` (integrity) is classified as `Constraint`. `ConcurrencyKeyInvalid(String)` carries the offending key when a `concurrency_key` is empty or > 128 characters. |
| `BuildError` | `WorkerBuilder::build`. Covers every per-knob constraint plus cross-knob invariants and `Pool*` mismatches. `ConcurrencyKeyInvalid(String)` — a `concurrency_limits` key is empty or > 128 characters. `ConcurrencyLimitInvalid { key, limit }` — a limit is outside `1..=2147483647` (Display: `concurrency limit for key {key:?} must be in 1..=2147483647, got {limit}`). |
| `StartError` | `Worker::start`. `SchemaMissing` is the loud-fail surface for `42P01` / `3F000` (schema or table missing); other sqlx errors come through `Database`. |
| `ShutdownError` | `WorkerHandle::{join, shutdown}`. `Fatal(Arc<sqlx::Error>)` for fatal classifier hits in poll/reaper **and** for poll loops that exceeded `MAX_CONSECUTIVE_CLAIM_ERRORS` non-fatal errors in a row (the most recent error becomes the payload); `ReaperPanicEscalation { consecutive_panics }` for the reaper's 3-strikes self-shutdown. `Timeout` and `AlreadyShutdown` are reserved for v0.2 (the current API consumes `self` so they're unreachable in practice). |
| `PurgeError` | `purge_done` / `purge_dead` / `queue_stats`. Single `Database(sqlx::Error)` variant; left `#[non_exhaustive]` so future versions may classify retriable-vs-fatal without breaking callers. |
| `JobError` | Returned *by* the handler — see [`JobError`](#joberror--handler-outcome). |

#### Fatal vs transient sqlx classification

The poll loop and the reaper both use the same classifier
(`is_fatal_sqlx`, hidden re-export under `__test_exports`):

- **Fatal** (worker self-shuts via `state.last_fatal.set(...)` +
  `shutdown.cancel()`):
  - sqlx-level: `PoolClosed`, `WorkerCrashed`, `Configuration(_)`,
    `Migrate(_)`, `ColumnDecode { .. }`, `Decode(_)`,
    `TypeNotFound { .. }`, `ColumnNotFound(_)`, `Protocol(_)`.
  - `Database(_)` with SQLSTATE in classes `42` (syntax / undefined
    object — schema drift), `0A` (feature not supported), `23`
    (integrity constraint — CHECK / FK / NOT NULL / unique; our state-
    -machine CHECK is the canonical hit), or `57P01` / `57P02` / `57P03`
    (admin shutdown, crash shutdown, `cannot_connect_now`). These signal
    a server-side state retrying will never escape.
- **Transient** (logged at `warn!`, retry next tick): every other
  `Database(_)`, plus `Io(_)`, `Tls(_)`, `PoolTimedOut`. **Defense-in-
  -depth**: if `MAX_CONSECUTIVE_CLAIM_ERRORS` (30) ticks in a row hit
  the transient branch — e.g., persistent TLS / network / pool-config
  outage that doesn't surface a fatal SQLSTATE — the poll loop escalates
  the most recent error to `last_fatal` and self-shuts. Without this,
  a permanent outage would log `warn!` forever and `WorkerHandle::join`
  would never return `Fatal`. The counter resets on any successful
  `claim_batch` (including empty results).

### Resource limits

Public `pub const` in the `limits` module (single source of truth — each
constant has a matching DB CHECK or builder validation):

| const | value |
|---|---|
| `MAX_PAYLOAD_BYTES` | 1 MiB |
| `MAX_BATCH_SIZE` | 10 000 |
| `MAX_BATCH_BYTES` | 64 MiB |
| `MAX_QUEUE_LEN` | 64 |
| `MAX_CONCURRENCY_KEY_LEN` | 128 (characters — Postgres `length(TEXT)` units) |
| `MAX_LAST_ERROR_LEN` | 8 KiB chars |
| `MIN_POLL_INTERVAL` | 10 ms |
| `MIN_HANDLER_TIMEOUT` | 1 s |
| `MIN_MARK_TIMEOUT` | 100 ms |
| `REAPER_BATCH_SIZE` | 1024 |
| `PURGE_CHUNK_SIZE` | 10 000 |
| `MAX_CONSECUTIVE_CLAIM_ERRORS` | 30 (ticks before transient-error escalation; see fatal-vs-transient classifier above) |

The reaper's `MIN_REAPER_INTERVAL = 1s` lives in the `reaper` module
(it is the only floor that isn't a generic resource bound).

## Tracing / observability

`pg_work_queue` emits structured `tracing` events at well-defined
targets. There is no built-in metrics endpoint — wire `tracing` into
your existing pipeline (`tracing-subscriber`, OpenTelemetry, etc.).

| target | level | when | key fields |
|---|---|---|---|
| `pgwq.state.transition` | `ERROR` / `INFO` / `DEBUG` | every flip of `pgwq.jobs.status` (worker, reaper, or purge — purge emits an aggregate, not per-row) | `worker.id`, `job.id`, `job.public_id`, `queue`, `job.attempts`, `status.from`, `status.to`, `source` (`"worker"` / `"reaper"` / `"purge"`), `lost_race`, `reason` (omitted at ERROR for PII safety — only `reason_length` + `reason_present`) |
| `pgwq.tick_once` | `INFO` span | `Worker::tick_once` | `queue`, `batch_size`, `claimed`, `completed`, `failed`, `fenced_out` |
| `pgwq.poll_tick` | `INFO` span | each poll loop tick | `worker.id`, `queue`, `batch_size`, `claimed` |
| `pgwq.claim` | `DEBUG` | the set of saturated (headroom-0) `concurrency_limits` keys changed **and is non-empty** (de-saturation to an empty set is silent) | `worker.id`, `queue`, `saturated_keys` |
| `pgwq.handle_job` | `INFO` span | each handler invocation | `worker.id`, `queue`, `job.id`, `job.public_id`, `job.attempt`, `timeout_ms` |
| `pgwq.handler.timeout_elapsed` | `WARN` | `handler_timeout` fired | `worker.id`, `job.id`, `job.public_id`, `job.attempt`, `timeout_ms` |
| `pgwq.retry_in.clamped` | `WARN` | per-call `retry_in` was outside `[max(poll_interval, 100ms), 24h]` | `requested_ms`, `applied_ms` |
| `pgwq.reaper.escalation` | `ERROR` | reaper exceeded the panic threshold (3 consecutive panic ticks) and self-shut | `worker.id`, `threshold`, `consecutive_panics` |
| `pgwq.shutdown` | `INFO` | end of `WorkerHandle::shutdown` | all `Stats` fields + `elapsed_ms` |
| `pgwq.shutdown.pending_recovery_failed` / `_timed_out` | `WARN` | best-effort `pending_recovery` query failed | `worker.id`, `queue`, `error` / `timeout_ms` |
| `pgwq.purge` | `INFO` | aggregate, once per `purge_done` / `purge_dead` call | `status`, `age_secs`, `deleted` |

The `pgwq.claim` saturation event is **edge-triggered** — it fires only
when the set of saturated keys *changes* between ticks **and the new set
is non-empty**. Saturation onset and changes between non-empty saturated
sets are logged; the set clearing to empty (de-saturation) is silent.
A steady-state saturated key emits nothing, so a subscriber that attaches
after saturation began sees no event until the set next changes. Use it
to detect saturation transitions, not as a continuously-polled gauge.

`tracing::instrument` is also placed on `Pusher::push*` (`fields(queue =
%self.queue)`) and on every `mark_*` SQL (`fields(job.id = id)`) so
parent spans automatically inherit job context.

## Design decisions

A short rationale for each shape of the public API.

- **One table, one schema.** A second table (e.g. `pgwq.workers`) would
  introduce a reaper-vs-purged-worker race and another migration
  surface. Per-row `lease_token` + `lease_expires_at` cover everything
  the reaper needs.
- **Polling, not `LISTEN/NOTIFY`.** Commit-NOTIFY serializes
  cluster-wide. A 500 ms–1 s polling cadence costs ~2 SELECTs/sec per
  worker — negligible on real workloads, predictable at scale, never
  serializes.
- **Per-row `max_attempts`.** A rolling deploy that changes
  `max_attempts` does not retroactively rewrite in-flight rows. The
  *claiming* worker's value is the one that decides the
  `dead-vs-retry` verdict for that row.
- **Fencing tokens.** A v4 UUID (no time leak, ephemeral) stamped at
  claim time. The DB CHECK pins `status='running'` ⇔ `lease_token IS
  NOT NULL`, so a paused worker that wakes after reclamation finds
  `rows_affected = 0` instead of overwriting the row.
- **Permits-first poll loop.** Acquire concurrency permits *before*
  `claim_batch`, then claim `min(batch_size, free_permits)` rows. No
  spawn backlog, no surplus permits to drop.
- **Codec is a trait, default is JSON.** Pluggable for users who want
  CBOR / bincode. The library never introspects payloads — `BYTEA`
  in/out.
- **`JobContext::idempotency_key == public_id`.** UUIDv7 generated
  client-side in `Pusher` (time-ordered B-tree locality, no
  `RETURNING` needed). Stable across retries. Drop-in for
  `Idempotency-Key` headers.
- **`JobError` is two variants.** `Retry { reason, retry_in }` and
  `Abort { reason }`. No typed retry strategy DSL — operationally,
  every nuance you'd want belongs in `retry_in`, in `BackoffPolicy`,
  or in `max_attempts`.
- **`Pusher::push` takes `&mut PgConnection`.** Atomic enqueue with
  your business writes. No "fire and forget" overload that opens its
  own transaction — the caller is in the best position to decide the
  transaction boundary.
- **`build()` does cross-knob validation.** Eight `BuildError` variants
  cover both per-knob and cross-knob constraints (`handler_timeout +
  1s ≤ lease_timeout`, `reaper_interval ≤ lease / 2`, etc.). Misuse is
  caught at startup, not three weeks into production.
- **Manual retention.** No background sweeper. Operators run
  `purge_done` / `purge_dead` from their own scheduler. The library
  does not own the calendar.
- **No worker dashboard.** Observability is `tracing` events + direct
  queries on `pgwq.jobs`. `queue_stats` is one such query; an `admin`
  module is on the v0.2 roadmap (`list_by_correlation`, `cancel`,
  `requeue`).
- **Loud start-time schema probe.** `Worker::start` runs `SELECT 1
  FROM pgwq.jobs LIMIT 0` and surfaces `StartError::SchemaMissing` if
  the schema / table is gone. The Apalis-style silent infinite warn
  loop is explicitly avoided.
- **Reaper panic isolation + escalation.** Each reaper tick is
  `tokio::spawn`ed; a panic in one tick does not crash the reaper. But
  three consecutive panics fire
  `ShutdownError::ReaperPanicEscalation` — a 100% panic rate is not
  recoverable.
- **`shutdown` consumes `self`.** Double-shutdown is statically
  prevented. `ShutdownError::AlreadyShutdown` is reserved for v0.2 if
  a `Drop`-time guard becomes useful.
- **Per-row `f64`-safe backoff math.** `f64::powi` overflow is
  filtered through `is_finite()` + clamp-to-`cap` so `BackoffPolicy::next`
  cannot panic on extreme attempt counts.

## Known limitations

### Shared `_sqlx_migrations` table

`pg_work_queue::migrator()` uses `sqlx::migrate!()` which (in `sqlx`
`0.8.x`) hard-codes the migration tracking table to `_sqlx_migrations`.
If your application also runs `sqlx::migrate!()` against the same
database, every migrator writes into that one shared table.

The crate's `migrator()` calls `set_ignore_missing(true)`, so it does
**not** error when it encounters migration rows it didn't apply
(yours, or another library's). Net effect: two co-existing migrators
each run only their own SQL and silently ignore each other's rows in
`_sqlx_migrations` — no manual workaround needed for the common case.

Two caveats remain:

- **Unique migration `version` numbers across migrators.** The filename
  prefix is parsed as the integer `version`. If your application
  defines a migration whose prefix collides with one of ours
  (currently only `20260513000000_v01_init.sql`), the insert into
  `_sqlx_migrations` will fail on the primary-key constraint. Use
  fresh timestamps for your own migrations and you're safe.
- **No table-name namespacing yet.** All migrators still touch
  `_sqlx_migrations`. The cleaner fix —
  `sqlx::migrate::Migrator::dangerous_set_table_name` to namespace to
  `_pgwq_migrations` — only landed on the `sqlx` `0.9` line; the
  pinned `0.8.6` here predates it. The crate will switch to a private
  table once `0.9` reaches stable.

Defensive escape hatches if either caveat bites:

- Apply this crate's migrations against a database that is **not**
  shared with your application's own sqlx-managed schema, or
- Run the embedded migration SQL yourself via your own migration
  tooling, skipping `pg_work_queue::migrator()`.

### Painful by design (not bugs, accepted trade-offs)

- **Poll loops scale linearly with queue count.** N queues = N
  workers = N poll cycles. At `poll_interval = 1s` and 50 queues
  that's 50 SELECTs/sec idle — Postgres noise floor. Not a problem
  below ~10 000 queues; revisit `LISTEN/NOTIFY` only if you genuinely
  hit that scale.
- **Orphan queues are stuck, not dead.** If you push to a queue and
  nobody registered a worker, the rows sit in `queued` forever — the
  reaper only acts on `running` rows. v0.2 `admin::active_queues()`
  gives operators a boot-time orphan-detection hook.
- **Stats can be off by 1–2 under aggressive shutdown.** Documented
  in `Stats`'s rustdoc — a `mark_done` UPDATE may commit server-side
  while its future is cancelled before bumping `completed`.
- **Per-key concurrency is single-instance only.** `concurrency_limits`
  is enforced via an in-process counter, so the limit is correct only
  under exactly one `Worker` object driving the queue. 2+ workers (or a
  `tick_once` caller alongside a `start()`ed worker) silently multiply
  the effective limit. See [Per-key
  concurrency](#per-key-concurrency).
- **The concurrency-key migration takes `ACCESS EXCLUSIVE` on
  `pgwq.jobs` for its full duration.** `sqlx` runs each migration file
  in one transaction, and the
  `20260521000000_v01_concurrency_key.sql` migration builds two indexes
  non-`CONCURRENTLY` (`sqlx`'s one-transaction-per-file model precludes
  `CREATE INDEX CONCURRENTLY`). The lock blocks **reads and writes**
  (claims, marks, the reaper, pushes). On a queue table kept small by
  purging — the expected case — the whole migration is sub-second. On a
  large unpurged table it is a full read+write stall proportional to row
  count (order of seconds to minutes at, say, 1M+ rows). Purge before
  migrating a hot, large table.
- **Head-of-queue skew in the keyed claim.** When `concurrency_limits`
  is configured, the keyed claim's unlimited-rows scan walks
  `(run_at, id)`-ordered rows and anti-joins out configured keys. If a
  *saturated* configured key owns a large block of the **oldest** rows,
  that scan skips past all of them — `O(rows-skipped)` index entries per
  tick (cheap per entry, no heap fetch: ~30 MB of index pages at a
  skewed 1M-row backlog while saturation persists). This is largely moot
  when every job carries a configured key (the unlimited scan is then
  near-empty); it only bites a workload that mixes configured-key jobs
  with many unkeyed/unconfigured-key jobs. Not fixed in v0.1.

## Testing

Integration tests run against real Postgres 18 via
[`testcontainers`](https://crates.io/crates/testcontainers). A working
Docker daemon is required.

```bash
cargo test --no-fail-fast
```

Every public knob has a paired behavioral test that measures observable
effect at two distinct values. Beyond per-knob coverage, the suite has
dedicated tests for state-machine invariants (fencing tokens, `SKIP
LOCKED` no-double-claim, `mark_done` loses to reaper), shutdown
semantics (graceful, abort-after-timeout, batch drain, handler abort
leak, reaper-panic escalation, shutdown-token propagation), reaper
behavior (adaptive backlog drain, single-CTE no race, per-queue
isolation, tick-panic recovery, max-attempts → dead, transition
events), schema (loud-fail on missing schema, loud-fail on PG &lt; 18),
the fatal/transient sqlx classifier, and codec swap + decode-error +
panic paths.

## License

MIT.
