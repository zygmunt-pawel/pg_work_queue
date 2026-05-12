# Apalis source analysis — pg_work_queue research

## Sources analyzed

- **`apalis`** repo (`geofmureithi/apalis`), tag **`v1.0.0-rc.9`** — workspace with
  `apalis-core`, `apalis`, `apalis-sql`, `apalis-workflow`. As of rc.7+, the Postgres
  backend was **moved out** of this workspace into a separate crate.
- **`apalis-postgres`** repo (`apalis-dev/apalis-postgres`), tag **`v1.0.0-rc.8`**
  (HEAD; rc.9 not tagged here). This is the actual code PLAN.md targets.
- For historical comparison: `apalis` workspace at tag `v0.7.4` still contains the
  in-tree `PostgresStorage` (pre-split). Symbol names PLAN.md uses (`PgPollFetcher`,
  `LockTaskLayer`, `PgAck`, `initial_heartbeat`, `keep_alive_stream`,
  `reenqueue_orphaned_stream`) **exist only in the split-out `apalis-postgres` crate**,
  not anywhere inside `geofmureithi/apalis`.

Paths below use the working clone roots:
- `APALIS = /Users/pawel/workspace/rust_packages/pg_work_queue/research/apalis-source`
- `PG = /Users/pawel/workspace/rust_packages/pg_work_queue/research/apalis-postgres-source`

---

## Verified claims

### 1. `PgPollFetcher::next_backoff` hardcodes 1s → 5min cap; `Config::with_poll_interval` is dead

**VERIFIED — fully accurate.**

`PG/src/fetcher.rs:84` — initial backoff:
```rust
let initial_backoff = Duration::from_secs(1);
```
`PG/src/fetcher.rs:160-163` — next_backoff:
```rust
fn next_backoff(&self, current: Duration) -> Duration {
    let doubled = current * 2;
    std::cmp::min(doubled, Duration::from_secs(60 * 5))
}
```
Constants are inline literals; no `Config` field is consulted. Reset to 1s after a
successful batch (line 128).

`Config::with_poll_interval` in `apalis-sql/src/config.rs:51-54` does set
`self.poll_strategy = strategy` — but **`PgPollFetcher` never reads
`config.poll_strategy`** (the only fields it pulls in `new` are `pool`, the whole
`Config`, and `WorkerContext`; in `Stream::poll_next` only `config.buffer_size()` and
`config.queue()` are used). The `poll_strategy` field on `Config` is plumbed but
referenced nowhere in `apalis-postgres`. The test `notify_worker` in
`PG/src/lib.rs:501` sets a 6-second strategy and *still works* — only because the
notify listener bypasses the poll fetcher, not because the interval is honored.

### 2. Trigger `pg_notify('apalis::job::insert', …)` emitted per row on INSERT

**VERIFIED for the trigger; lock-semantics claim NUANCED.**

`PG/migrations/20251018165121_notify_run_at.sql:1-22` (latest):
```sql
CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.run_at <= now() THEN
        PERFORM pg_notify('apalis::job::insert',
            json_build_object('job_type', NEW.job_type, 'id', NEW.id, 'run_at', NEW.run_at)::text);
    END IF;
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER notify_workers AFTER INSERT ON apalis.jobs
FOR EACH ROW EXECUTE FUNCTION apalis.notify_new_jobs();
```

Worse than PLAN claims: the *original* migration
(`20220530084123_jobs_workers.sql:82`) used `FOR EACH STATEMENT`. The
`20250722071207_improve_notify.sql` migration regressed this to **FOR EACH ROW**.
Combined with bulk-insert via UNNEST (`queries/task/sink.sql`), a single batch
push fires N pg_notify calls and serializes them through the global notify queue.

**Lock semantics — refuted as stated.** Postgres `NOTIFY` does NOT take
`AccessExclusiveLock on the whole cluster`. It takes `ExclusiveLock` on
`NotifyQueueLock` (a single cluster-wide `LWLock`) at commit time to serialize
appending to the global notify queue. The PLAN claim names the wrong lock; the
practical effect — global serialization of any transaction that calls
`pg_notify` — is real and is a legitimate bottleneck under high write volume,
but it is not table-level AEL and does not block readers.

### 3. `ack = UPDATE` instead of DELETE

**VERIFIED.** `PG/queries/task/ack.sql`:
```sql
UPDATE apalis.jobs
SET status = $4, attempts = $2, last_result = $3, done_at = NOW()
WHERE id = $1 AND lock_by = $5
```
No DELETE anywhere in the ack path. Completed/Failed/Killed rows accumulate
forever. The only cleanup is `vacuum()` (`queries/backend/vacuum.sql`), which
the user must call manually. Side effects:
- `apalis.jobs` grows unbounded
- Every fetch hits an ever-larger heap; SKIP LOCKED has to skip more dead rows
  (heap bloat) until VACUUM
- The hot index `(job_type, status, run_at)` (if it existed — it doesn't, see
  Additional issue #2) would still need to traverse all done rows because there
  is no partial index

### 4. `RetryAfterError`'s duration is not honored anywhere

**VERIFIED — actually stronger than PLAN states.**

`apalis-core/src/error.rs:30-52` (rc.9) defines `RetryAfterError` with a
`duration: Duration` field and a `get_duration()` accessor. A repo-wide grep:

```
$ grep -rn "RetryAfterError\|get_duration" --include="*.rs"
apalis-core/src/error.rs:32:pub struct RetryAfterError {
apalis-core/src/error.rs:38:impl RetryAfterError {
apalis-core/src/error.rs:49:    pub fn get_duration(&self) -> Duration {
apalis-core/src/lib.rs:270:  (doc comment only)
apalis-core/src/lib.rs:319:  (doc comment only)
```
No caller anywhere in `apalis-core`, `apalis`, `apalis-sql`, `apalis-postgres`
ever downcasts to `RetryAfterError` or reads `get_duration()`. The
`AcknowledgeLayer` in `apalis-core/src/worker/ext/ack/mod.rs:173-184` passes the
`Result<Res, BoxDynError>` straight to the ack handler; `PgAck` in
`PG/src/ack.rs:25-61` only does `serde_json::to_value(res.as_ref().map_err(|e|
e.to_string()))` — the typed error is discarded into a string. The duration is
**never threaded through to `run_at`** and the retry layer
(`apalis/src/layers/retry/mod.rs:124-166`) only uses its own backoff
configuration, ignoring `RetryAfterError` entirely. PLAN claim verified.

### 5. Visibility of `LockTaskLayer`, `AcknowledgeLayer`, `PgAck`, `initial_heartbeat`, `keep_alive_stream`, `reenqueue_orphaned_stream`

**REFUTED.** Every one of these is `pub` and re-exported at the crate root in
`PG/src/lib.rs:25-33`:
```rust
pub use crate::{
    ack::{LockTaskLayer, PgAck},
    fetcher::{PgFetcher, PgPollFetcher},
    queries::{
        keep_alive::{initial_heartbeat, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
    sink::PgSink,
};
```
A `grep -rn "pub(crate)"` on `PG/src/` returns **zero matches**.
`AcknowledgeLayer` is `pub` in `apalis-core/src/worker/ext/ack/mod.rs:118`.
PLAN claim is wrong. (They are awkward to *use* without rebuilding most of the
glue — that's a different criticism.)

### 6. Double retry budget — apalis `RetryPolicy::retries(N)` + DB-side `max_attempts`

**VERIFIED — and there is a third counter.**

Three independent counters interact:
- DB `max_attempts` (column default 25) is enforced inside the SQL `get_jobs`
  function (`AND attempts < max_attempts`) — DB side.
- DB `attempts` is incremented by `apalis.jobs SET attempts = $2` in `ack.sql`
  on each ack and also `attempts + 1` in `reenqueue_orphaned.sql` on
  orphan-recovery.
- Tower `RetryPolicy { retries: usize }` in
  `apalis/src/layers/retry/mod.rs:170-245` lives entirely in-memory inside the
  worker process, gated on `req.parts.attempt.current()`. There is **no
  read-back** from the DB `attempts` column into this counter.

Disagreement scenarios:
- A worker crashes after `lock_task` but before ack — `attempts` in DB is what
  was set on insert (0). When the orphan reaper picks it up, it does
  `attempts + 1`, but the new worker's in-process `Attempt` starts from 0
  again. So the in-memory `RetryPolicy` budget is fresh each time a worker dies.
- `RetryPolicy::retries(5)` will burn through 5 in-process retries *per worker
  lease*; the DB `max_attempts` (default 25) is what actually stops the loop.
- An abort via `AbortError` short-circuits in-memory retries but the DB row
  still has `status = 'Failed'` rather than `Killed` (see calculate_status:
  `Error::Abort` branch is **commented out** in `PG/src/ack.rs:70`).

---

## Refuted or nuanced claims

### "1s → 5min" is the published cap — half right

The *hardcoded* cap in `PgPollFetcher` is indeed 5 minutes, matching PLAN. But
the generic `BackoffConfig` in `apalis-core/src/backend/poll_strategy/strategies/backoff.rs:66`
defaults to **60s max**, not 5min, with multiplier 2.0 and jitter 0.1, starting
from `IntervalStrategy.poll_interval` (100ms by default).
`PgPollFetcher` ignores all of that. So the "1s → 5min" cap is real but is
specific to `PgPollFetcher`'s hand-rolled backoff and is **inconsistent with**
the configurable backoff the rest of the system advertises.

### `pg_notify` lock — see claim #2 above. PLAN's "AccessExclusiveLock on the whole cluster" is wrong terminology. The real lock is `LWLock NotifyQueueLock` (Postgres source: `src/backend/commands/async.c`).

### "apalis-postgres in rc.7 paths"

PLAN.md references `packages/apalis-postgres/...`. There is no such path in
either repo. The real layout is `apalis-postgres-source/src/{lib.rs,fetcher.rs,
ack.rs,sink.rs,shared.rs,queries/}` with co-located `queries/` and `migrations/`
directories at the repo root. PLAN's file paths need rewriting.

### "pub(crate)" symbols

Refuted, see claim #5.

---

## Additional issues found (not in PLAN.md)

### A. Worker registration uses session-scoped advisory lock that is never released

`PG/queries/worker/register.sql`:
```sql
INSERT INTO apalis.workers (...) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE SET ... last_seen = NOW()
WHERE pg_try_advisory_lock(hashtext(workers.id));
```
`pg_try_advisory_lock` (session-level) is acquired and never released. The
connection returns to the sqlx pool with the lock still held. When that same
connection is reused for the *same* worker name (which `register` will run
again on next initial_heartbeat), the lock is already held — the call returns
true (locks are reentrant for the same session) so it appears to work, but if
two distinct pool connections each try to (re)register the same worker name
they will silently fail to update `last_seen`. The lock is also a permanent
state leak on long-lived pool connections. This should be `pg_advisory_xact_lock`
inside an explicit transaction, or simpler — no advisory lock at all.

### B. Schema problems

`PG/migrations/20220530084123_jobs_workers.sql`:
- `status TEXT` — no `CHECK` constraint, no ENUM. Typos in app code → invalid
  rows that the SP filter silently skips.
- `attempts INTEGER` — no `CHECK (attempts >= 0)`, no `CHECK (max_attempts > 0)`.
- Duplicate indexes: `TIdx ON jobs(id)` + `unique_job_id ON jobs(id)` (the
  unique index alone suffices). Same on `workers(id)`. Wasted disk + writes.
- **No composite index on the fetch hot path.** The SP scans
  `WHERE (status='Pending' OR (status='Failed' AND attempts < max_attempts))
  AND run_at < now() AND job_type = $`. Useful index:
  `(job_type, status, run_at) WHERE status IN ('Pending','Failed')` —
  none exists. SIdx on `status` alone is low-cardinality and unhelpful.
- `CONSTRAINT fk_worker_lock_by FOREIGN KEY(lock_by) REFERENCES apalis.workers(id)`
  has **no `ON DELETE` action**. Deleting a worker row is a runtime error if any
  job still references it. Also forces the FK lookup on every lock/update —
  cost on every ack.
- Primary keys were added **5 years late** in
  `migrations/20251225090252_include_primary_keys.sql`. Up to that point
  `apalis.jobs.id` was only a unique index — replication-unfriendly,
  pg_dump-unfriendly.
- Migration `20251018165007_move_to_bytes.sql` converts `job` from JSONB to
  BYTEA — this is a full table rewrite with `ALTER COLUMN ... TYPE bytea USING
  convert_to(job::text, 'UTF8')`. Locks the table in `AccessExclusiveLock` for
  the duration. Will OOM on big tables.

### C. `claim_batch` semantics (`apalis.get_jobs`)

- Does use `FOR UPDATE SKIP LOCKED` — good.
- **Does NOT increment `attempts`** at claim time. `attempts` is only
  incremented on ack (in `ack.sql`) or on orphan recovery. So if a worker
  crashes between `get_jobs` and the first ack, attempts stays at 0. Combined
  with the in-memory RetryPolicy (issue #6 above), poison pills can loop more
  times than `max_attempts` would suggest.
- The function changes status to `'Queued'` (since
  `20251018165056_queue_jobs.sql`). Then the per-task `LockTaskLayer.call()` in
  `PG/src/ack.rs:132-159` runs `lock_by_id.sql` to transition to `'Running'`.
  This is **two round-trips per task** — `get_jobs` batched, then an
  individual `lock_by_id` call per task before the user's service runs. With
  buffer_size=10 that's 10 extra UPDATE statements per fetch cycle. The
  `'Queued'` state seems to exist only to support the notify-fetcher flow
  (`queue_by_id.sql`); for the polling fetcher it's pure overhead.

### D. Reaper logic and races

`PG/src/queries/reenqueue_orphaned.rs` + `PG/queries/backend/reenqueue_orphaned.sql`:
- Detects "orphan" by `INNER JOIN apalis.workers ON lock_by = workers.id` and
  `workers.last_seen < now() - $interval`.
- **Race**: if a worker row is deleted (manually, by some retention job, etc.)
  any task it locked becomes permanently orphaned — the join fails and the
  reaper cannot match it. Tasks are stuck in `Running` forever.
- **Race**: worker A times out and reaper sets task to Pending. Worker A wakes
  up, finishes the task, calls ack — `WHERE id = $1 AND lock_by = $5` — ack
  succeeds and overwrites with `Done`. Now the task ran twice and the second
  re-claim by worker B also runs it. No fencing token.
- No advisory locking around the reaper itself — multiple workers all running
  `reenqueue_orphaned` simultaneously will race on the same orphan set; SKIP
  LOCKED is **not used here**, so under high concurrency the reaper takes
  row-level locks that contend.
- Reaper bumps `attempts + 1` on re-enqueue but the in-memory `Attempt` counter
  on the new worker reads from `parts.attempt.current()` which derives from
  the row — so the budget is honored *across workers*. But the in-memory
  RetryPolicy resets per-process-lease (see claim #6).

### E. Shutdown path

- `apalis-core/src/worker/mod.rs` has graceful shutdown via `WorkerContext::stop()`.
- However, `SharedPostgresStorage::new` in `PG/src/shared.rs:53-54` does
  `PgListener::connect_with(&p).await.unwrap()` and
  `listener.listen("apalis::job::insert").await.unwrap()` — any connect or
  LISTEN failure **panics the listener task**.
- `SharedPostgresStorage` Drive loop (lines 70-77): `sender.send(ev.id).await.unwrap()`
  panics if the receiver was dropped — i.e. if any worker shuts down while
  notifications are in flight, the entire shared driver crashes and all other
  workers stop receiving notifications.
- The polling fetcher in `PG/src/fetcher.rs` has no shutdown integration; it
  blocks on `futures_timer::Delay` and the next poll. A `WorkerContext::stop()`
  during a 5-minute backoff means the worker waits up to 5 minutes before
  exiting.

### F. PgBouncer transaction-mode incompatibility

The whole notify-based pipeline (`PostgresStorage::poll_with_notify`,
`SharedPostgresStorage`) uses `LISTEN apalis::job::insert` on a long-lived
connection. This is **fundamentally incompatible with PgBouncer's
transaction-mode pooling** — LISTEN persists for the session, and txn-mode
returns the connection to the pool after each transaction. The crate gives no
warning, no feature flag, no fallback. Users on managed PG (Supabase, RDS+
PgBouncer, Heroku) silently get a broken notify path and fall back to
polling — and the 1s/5min polling, see claim #1, is intentionally agnostic
of `Config::poll_strategy`.

### G. Idempotency UX

`migrations/20260508093314_idempotency_key.sql` adds
`UNIQUE (job_type, idempotency_key)`. `queries/task/sink.sql` is a batched
UNNEST INSERT with **no `ON CONFLICT`**. A duplicate key fails the whole batch
with a unique-violation error. There is no helper to "push or get existing".

### H. `metrics::global` is 23+ table scans per call

`PG/queries/backend/overview.sql` is a 24-way UNION ALL where every branch
scans `apalis.jobs`. Each call is ~24 seq-scans plus a few `pg_total_relation_size`
calls. No materialized view, no caching. Calling this from a dashboard at
1Hz on a million-row table will saturate a small Postgres instance.

### I. `wait_for` is a polling sleep loop

`PG/src/queries/wait_for.rs:35-76` — for each tick: query, if empty sleep 500ms,
repeat. Multiple `.unwrap()` calls in the result-decoding path (lines 43, 60,
65) will panic on malformed rows. No use of LISTEN/NOTIFY for completion
events (which would be the natural fit since the notify-channel already exists).

### J. `stats.sql` references `Jobs` (capitalized) with `?1` placeholder

`PG/queries/backend/stats.sql` is **SQLite syntax** (`?1` parameter style,
unquoted `Jobs` which Postgres would lower-case but the schema uses `apalis.jobs`
qualified). The file is shipped in the postgres crate but I can find no `Rust`
caller for it via `query_file!("queries/backend/stats.sql")`. Probable dead
code copy-paste from a sister crate.

### K. Aborts collapsed into Failed

`PG/src/ack.rs:63-75`:
```rust
pub fn calculate_status<Res>(...) -> Status {
    match &res {
        Ok(_) => Status::Done,
        Err(e) => match &e {
            // Error::Abort(_) => State::Killed,   <-- commented out
            _ if parts.ctx.max_attempts() as usize <= parts.attempt.current() => Status::Killed,
            _ => Status::Failed,
        },
    }
}
```
The Abort handling branch is literally commented out. `AbortError` from user
code is silently converted to `Status::Failed`, which means it gets retried
until `max_attempts` exhausts. This is the opposite of the documented
semantics in `apalis-core/src/error.rs:13-26`.

### L. Tower middleware integration: minor overhead, major coupling

The fetcher returns `Result<Option<PgTask>, sqlx::Error>` and is wrapped in
`Stack<LockTaskLayer, AcknowledgeLayer<PgAck>>` (`PG/src/lib.rs:205`). Every
task goes through 2 extra Tower layers (one DB call each in LockTaskLayer,
one DB call in AcknowledgeLayer). The whole `apalis-core` `Backend`/`Stream`/
`Layer`/`Service` trait surface (~11k LOC) is required just to plug in a
Postgres queue.

### M. Generic backend abstraction overhead

To use `apalis-postgres` you transitively pull:
- `apalis-core` (~11k LOC, ~38 files; pulls `tower`, `futures-*`, `pin-project`,
  `tracing`)
- `apalis-sql` (~650 LOC of shared SQL utilities, mostly `Config` + `DateTime` +
  `TaskRow`; you don't use SQLite/MySQL but the crate is feature-gated, not
  split)
- `apalis-codec` (separate small crate for JSON codec wrapper)
- sqlx (with postgres + macros features — compile time hit)
- ulid

The `Backend` trait alone requires defining `Args`, `IdType`, `Context`,
`Error`, `Stream`, `Beat`, `Layer` associated types (`PG/src/lib.rs:186-206`).
For a single-target Postgres queue this is enormous ceremony.

### N. Migration versioning and idempotency

- 19 migrations in `PG/migrations/`. No `down`/rollback scripts. `sqlx::migrate!`
  is forward-only.
- Several non-idempotent statements (e.g. `migrations/20251018165007_move_to_bytes.sql`
  does `ALTER COLUMN ... TYPE bytea USING convert_to(job::text, 'UTF8')` —
  re-running on already-bytea data errors).
- Run lock: sqlx maintains its own `_sqlx_migrations` table; not a problem per
  se, but no docs warn that running two app instances during a deploy can
  cause one to error on missing FN.

### O. Observability — almost none

- The only `log::` calls in `PG/src/queries/reenqueue_orphaned.rs` are
  **commented out** (lines 26-29, 34).
- No `tracing::instrument` on any DB-facing function in `apalis-postgres`.
- No metrics counters in the backend itself; you have to layer Prometheus/OTEL
  via the optional `apalis` features on top.

### P. Multi-queue support is single-config-per-storage

`PostgresStorage::new` (and `new_with_config`) binds a single `Queue` (=
`config.queue`) to one storage instance. To handle 3 queues you build 3
`PostgresStorage` instances (with 3 connection pools or 1 shared pool +
3 `SharedPostgresStorage` calls). The notify-payload includes `job_type` but
the *channel name* (`apalis::job::insert`) is fixed and global — so the listener
wakes for every queue's inserts, even if you only care about one, and filters
in user space (`PG/src/lib.rs:388-400`). Fan-out overhead grows with the
number of registered queues.

### Q. Scheduled jobs (`run_at`) — partial support

- Inserts can set `run_at` in the future via the `run_at` column.
- The trigger only fires `pg_notify` for `IF NEW.run_at <= now()` — so
  *scheduled* inserts produce no notification. The poller will pick them up,
  but ONLY after the next backoff tick (potentially 5 minutes late).
- No "wake at exactly run_at" mechanism — there is no per-row scheduled
  notification. A `run_at` set to 1.1 seconds in the future is rounded up to
  the next poll interval (1 → 2 → 4 → ... → 300 seconds).
- No cron support whatsoever in `apalis-postgres`; there is a separate
  `apalis-cron` crate but in the `apalis` workspace at v0.7.4 only.

---

## Surprisingly good parts (patterns worth keeping)

### 1. `FOR UPDATE SKIP LOCKED` based claim function

The classic `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT N)
RETURNING *` pattern in `apalis.get_jobs` is the right thing. Single round-trip,
contention-free, batch-aware. Keep it. Drop the `'Queued'` intermediate state
and go straight to `'Running'` to avoid the LockTaskLayer second roundtrip.

### 2. Bulk insert via UNNEST

`PG/queries/task/sink.sql` uses `unnest($1::text[])` etc. to batch-insert
multiple rows in a single INSERT. This is the right pattern for high write
throughput. Keep it; just add `ON CONFLICT ... DO NOTHING` for idempotency
keys.

### 3. ULID for task IDs

ULIDs are k-sortable, fit in 26 chars, and the ID encodes a timestamp so
`ORDER BY id ASC` is a free approximate FIFO. Cheaper than UUIDv4 for queue
indexes. The PostgreSQL `generate_ulid()` plpgsql function in
`migrations/20240225141841_replace_add_job_fn_remove_jid.sql` is a useful
copy-paste — though the Rust side now generates ULIDs client-side, so the
SQL function is dead in current code.

### 4. Notify payload carries enough info to skip the SELECT

The payload contains `(job_type, id, run_at)`. A listener can route IDs
directly into a batched `queue_by_id.sql` (`PG/queries/task/queue_by_id.sql`)
without re-scanning the queue. This is faster than "notify → SELECT
FOR UPDATE" used by many naive designs. Worth keeping if we use LISTEN/NOTIFY.

### 5. Separate "fetch then lock" two-phase pattern (for notify path only)

For the notify path, `queue_by_id.sql` (sets to `'Queued'`) followed by
`lock_by_id.sql` (sets to `'Running'`) is actually defensible: it prevents a
storm of notify-driven workers all racing to claim the same row. Keep this
*pattern* if doing LISTEN-based dispatch with many workers; **don't** use
it on the polling path where SKIP LOCKED already does the job.

### 6. `reenqueue_orphaned_after` is configurable per-queue

The reaper interval is per-`Config` (default 5 min). Good idea. Just fix
the join-with-workers race (issue D) — use `lock_at < now() - lease` directly
on the jobs table and forget the workers table entirely.

### 7. Heartbeat via separate `workers` table is conceptually sound — for visibility only

Having a `workers` row makes "list workers in dashboard" easy. The mistake
is hanging task orphan detection off it (issue D). Heartbeat purely for
observability + put a `lease_until` column on `apalis.jobs` instead.

---

## Code-size breakdown (what we're replacing)

Measured at `apalis v1.0.0-rc.9` + `apalis-postgres v1.0.0-rc.8`:

| Component                     | Rust LOC | SQL/migration LOC | Files |
|-------------------------------|---------:|------------------:|------:|
| `apalis-core`                 |  ~10,974 |                 - |    38 |
| `apalis` (main crate)         |   ~2,959 |                 - |   ~25 |
| `apalis-workflow`             |   ~3,987 |                 - |   ~15 |
| `apalis-sql` (shared)         |     ~652 |                 - |     6 |
| **`apalis-postgres` (split)** |  **2,159** | **934 (queries) + 610 (migrations) = 1,544** | **17 + 22 SQL** |
| **Postgres-only subset**      |  **~13,800 LOC** Rust (core+sql+postgres) + ~1.5k SQL | | |

The Postgres-relevant code that a user transitively depends on is roughly
**15 kLOC of Rust + 1.5 kLOC of SQL**, plus the entire `tower` / `futures` /
`pin-project` / `sqlx` dependency tree.

Plus runtime dependencies introduced by `apalis-core` even when only Postgres
is used: tracing (always), opentelemetry (optional), sentry-core (optional),
metrics + metrics-exporter-prometheus (optional). The "minimal" footprint
without features is still ~15 k LOC of generic abstraction surface to
support one SQL-backed queue.

A `pg_work_queue` replacement targeting Postgres-only with no Tower/Backend
trait abstraction should realistically come in at **~1.5–3 kLOC of Rust +
~100 LOC of SQL** for parity on the working subset (push, fetch with SKIP
LOCKED, ack=DELETE, scheduled run_at, retry+backoff, orphan reaper,
LISTEN/NOTIFY wake, bulk insert), saving ~12 kLOC of dependency surface.
