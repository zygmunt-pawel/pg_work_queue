# Postgres job-queue patterns — research notes

> Source survey: River (Go), gue (Go), neoq (Go), pg-boss (Node),
> Solid Queue (Rails), Que (Ruby). Cross-checked against Postgres
> source (`src/backend/commands/async.c`) and the Recall.ai
> "Postgres LISTEN/NOTIFY does not scale" post-mortem.
> Date: 2026-05-12.

---

## Schema patterns observed

| Library      | PK type            | Payload    | Status model                                                                              | Lease columns                              | Notes                                                                                          |
|--------------|--------------------|------------|-------------------------------------------------------------------------------------------|--------------------------------------------|------------------------------------------------------------------------------------------------|
| River        | `bigserial`        | `jsonb`    | ENUM `river_job_state` = available, scheduled, retryable, running, completed, cancelled, discarded, pending | `attempted_at`, `attempted_by text[]`      | `errors jsonb[]` (history per attempt). Partial unique indexes for unique-jobs. Partitionable. |
| gue          | `text` (job_id)    | `bytea`    | implicit (row deletion on success; `finished_jobs_log` table)                             | `run_at` only (no explicit lease)          | Composite idx `(queue, run_at, priority)`. Uses advisory locks (que-style heritage).           |
| neoq         | `int`              | `bytea`    | `status` text + separate `neoq_dead_jobs` table for terminal failures                     | `deadline` (absolute), `ran_at`            | No heartbeat. Relies on `IdleTransactionTimeout`.                                              |
| pg-boss      | `uuid`             | `jsonb`    | ENUM job_state = created, retry, active, completed, cancelled, failed                     | `heartbeat_on`, `heartbeat_seconds`, `expire_seconds` | **Partitioned `BY LIST (name)`**. `keep_until` for retention. Separate `archive` table.        |
| Solid Queue  | `bigint`           | text-JSON  | **Split across tables** (`jobs`, `ready_executions`, `scheduled_executions`, `failed_executions`) | Process heartbeat table (`solid_queue_processes`) | "Outbox-ish": claim moves rows between tables → small hot ready-set.                           |
| Que          | `bigserial`        | `jsonb`    | derived from `finished_at IS NULL AND expired_at IS NULL` (no explicit status enum)       | `pg_try_advisory_lock(id)` (in-memory)     | Recursive CTE walks `(priority, run_at, id)` btree. Separate unlogged `que_lockers` heartbeat table. |

**Convergent design choices across libraries:**

- A `run_at` / `scheduled_at` / `start_after` TIMESTAMPTZ column for scheduled jobs is universal.
- Some integer attempts counter (`attempt`, `retry_count`, `retries`, `error_count`) is universal.
- A `last_error` / `errors[]` field is universal; most store a single string, River stores the full history as `jsonb[]`.
- Composite btree on **(queue, run_at-ish, ordering_tiebreaker)** is universal. The tiebreaker is `priority` (gue/Que) or `id` (River with `priority` also).
- Partial indexes (`WHERE status IN (...)`) appear in River, pg-boss, Solid Queue.

**Divergent design choices:**

- Payload type: `jsonb` (River, pg-boss, Que) vs `bytea` (gue, neoq). Plan's `bytea` matches the "library is codec-agnostic" school.
- Single table + queue column (gue, neoq, Que, River) vs split tables per state (Solid Queue) vs partitioned per queue name (pg-boss).
- Status as ENUM (River, pg-boss) vs derived from null timestamps (Que) vs free-text (neoq).

---

## Claim patterns observed

### 1. `SELECT … FOR UPDATE SKIP LOCKED` + UPDATE in a CTE (the modern default)

Used by **River, gue, pg-boss, Solid Queue, neoq**, and what the plan chose. Pattern:

```sql
WITH claimed AS (
  SELECT id FROM jobs
  WHERE queue = $1 AND status IN (...) AND run_at <= now()
  ORDER BY ..., id
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE jobs SET status='running', attempts=attempts+1, ... FROM claimed WHERE jobs.id=claimed.id
RETURNING ...;
```

- **Pros**: single round-trip; batch size N; no application-side retry loop;
  uses one heavy-weight row lock per claim, released at commit;
  predictable, well-understood, debuggable with `pg_locks`.
- **Cons**: row lock = WAL write (heap tuple update marks xmax even if you abort);
  on extreme contention, walker through bloat must skip many invisible
  tuples (Brandur's "wild goose chase" scenario).
- pg-boss claim line (from `plans.ts`): `FOR UPDATE OF j SKIP LOCKED`.
- Solid Queue: `FOR UPDATE SKIP LOCKED` against a small `ready_executions`
  table — the trick is to keep the hot scan set tiny.

### 2. Advisory locks + recursive CTE walk (Que)

Que's poller uses `pg_try_advisory_lock(id)` while walking the
`(priority, run_at, id)` btree recursively. README claim: "Locks are
held in memory, so locking a job doesn't incur a disk write … Workers
don't block each other when trying to lock jobs."

- **Pros**: lock state in shared memory only — no WAL, no MVCC bloat
  from row-lock xmax churn.
- **Cons**: advisory locks are **session-scoped** (not transaction-scoped
  unless using `_xact_` variants). Holding through processing requires
  holding the same connection — incompatible with PgBouncer transaction
  pooling. Que docs explicitly mention this caveat. Also: lossy with
  partial indexes; the recursive CTE is hard to reason about.

### 3. `UPDATE … RETURNING` against an exists-subquery (older pattern)

No surveyed library uses this anymore; superseded everywhere by
SKIP LOCKED.

**Verdict**: Plan's `SELECT … FOR UPDATE SKIP LOCKED` is the
mainstream choice in 2026. Que's advisory-lock approach has a real
performance argument but trades PgBouncer compatibility — pg_work_queue
explicitly wants PgBouncer compatibility (PLAN.md open question 9), so
SKIP LOCKED is the right call.

---

## Lease patterns observed

| Library     | Lease mechanism                                                                                  |
|-------------|--------------------------------------------------------------------------------------------------|
| River       | `attempted_at` + scheduled `JobRescuer` job that flips `running` → `retryable` after deadline.   |
| gue         | None explicit (no `running` state — row stays until DELETE or move to `finished_jobs_log`).      |
| neoq        | None — relies on Postgres `idle_in_transaction_session_timeout`.                                 |
| pg-boss     | Explicit `heartbeat_on`, `heartbeat_seconds`, `expire_seconds`. Workers update heartbeat.        |
| Solid Queue | **Process heartbeat table** (`solid_queue_processes`) — worker registers itself, claims attribute to a process_id, reaper scans for dead processes. |
| Que         | `que_lockers` (UNLOGGED) — worker advertises itself, no per-job timeout.                          |
| Plan        | `last_attempted_at` + worker-side `lease_timeout` → reaper flips stale `running` → `awaiting_retry`. |

**Two genuinely different philosophies:**

- **Implicit timestamp + reaper** (River, plan): cheap, no extra table,
  works for "lease expired" detection. Cannot distinguish "worker
  dead" from "worker still alive but slow handler". Reaping a still-
  running handler causes double-execution risk if handler later
  succeeds and tries to mark_done — the plan's `WHERE status='running'`
  guard handles this (lesson noted in PLAN.md).
- **Process heartbeat table** (Solid Queue, Que, pg-boss): worker
  process advertises liveness; jobs attributed to processes; reaper
  detects dead processes and reclaims their jobs. More accurate
  detection, supports `worker list` / dashboards, but adds an extra
  table and an extra heartbeat write path. Apalis attempted this and
  the cost showed.

Plan's choice is defensible. Worth recording the tradeoff as an
explicit "we don't need worker liveness tracking" decision (PLAN open
question 5 — keep it documented).

---

## LISTEN/NOTIFY truth — definitive

**Claim under test**: "trigger `pg_notify('apalis::job::insert', …)`
per INSERT takes `AccessExclusiveLock` on the entire cluster at commit"
(PLAN.md §Motywacja, point 2).

### Evidence

1. **Postgres source (`src/backend/commands/async.c`)**: NOTIFY
   delivery at commit time uses the lwlock `NotifyQueueLock`
   (formerly named `AsyncQueueLock`) plus a heavyweight lock acquired
   in the `pg_database` namespace. Comment: *"all writers serialize on
   a cluster-wide heavyweight lock"*. The lock protects the shared
   notification queue (a circular buffer in shared memory), not user
   tables.

2. **Recall.ai post-mortem** ("Postgres LISTEN/NOTIFY does not scale",
   2025): they observed hundreds of backends waiting on
   `AccessExclusiveLock` on **`database 0`** (i.e., the database-level
   lock — locktype `database`, not `relation`). Quote:
   > "This lock is acquired during `COMMIT` queries when a transaction
   > has previously issued a `NOTIFY` … This is a global lock on the
   > entire database."

   Their hosts went CPU-idle while backends waited for the database-
   level lock — symptom of pure serialization, not contention on a
   busy table.

3. **Postgres docs (`sql-notify.html`)** confirm:
   > "if a `NOTIFY` is executed inside a transaction, the notify
   > events are not delivered until and unless the transaction is
   > committed."
   And: "If this queue becomes full, transactions calling `NOTIFY`
   will fail at commit." (Queue is cluster-wide, 8 GB default cap.)

### What the lock actually does and does NOT do

| Question                                                                | Answer                                                                                                          |
|-------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| Does NOTIFY take `AccessExclusiveLock` on user tables?                  | **No.** The user table sees only its own row-level locks for the INSERT.                                        |
| Does NOTIFY take a heavyweight lock at COMMIT?                          | **Yes**, on `database` locktype (database 0 in Recall.ai's logs).                                                |
| Does that lock block other transactions' COMMITs?                       | **Only other transactions that themselves issued NOTIFY.** Plain INSERT/UPDATE/DELETE commits are not blocked.   |
| Does it block other NOTIFY transactions cluster-wide?                   | **Yes.** All NOTIFY-issuing transactions in the cluster serialize at COMMIT through that one lock.              |
| Is the contention "cluster-wide" or "database-wide"?                    | The lock is per-database in `pg_locks`, but the underlying notification queue is **cluster-wide** (shared mem). |
| Would purely write-heavy outbox INSERT-with-NOTIFY hit this?            | **Yes — this is exactly the pattern that fails at scale.** Apalis emits one NOTIFY per job INSERT.              |

### So is the plan's motivation correct?

**Yes, but with a sharpening of language.** The original PLAN.md
wording — "takes `AccessExclusiveLock` on the whole cluster at commit"
— is correct in spirit (the lock IS `AccessExclusiveLock`, the effect
IS serialization across many backends) but is technically a
**database-level lock that serializes only NOTIFY-issuing commits**.
For a write-heavy outbox where *every* INSERT is paired with NOTIFY,
the practical effect is "all commits serialize", which matches Recall.ai's
outage profile. The plan's refusal to use NOTIFY is justified.

**Recommended PLAN.md edit (optional, for technical precision):**

> "Trigger `pg_notify(...)` emitowany per INSERT bierze
> `AccessExclusiveLock` na `pg_database` (locktype=database, dbid=0)
> przy COMMIT, serializując **wszystkie transakcje które wywołały
> NOTIFY** w klastrze. Dla write-heavy outbox gdzie każdy INSERT
> emituje NOTIFY, to de facto oznacza serializację wszystkich
> commitów. Recall.ai miał 3 outage'e z tego powodu (mar 2025)."

---

## What pg_work_queue PLAN gets right

1. **`SELECT … FOR UPDATE SKIP LOCKED` for claim.** This is the modal
   choice across River, gue, pg-boss, Solid Queue, neoq. Plan's claim
   query is essentially identical in shape to pg-boss's `fetchNextJob`.
2. **Single `jobs` table + `queue` column.** River, gue, neoq, Que all
   do this. pg-boss partitions by queue name — but only because they
   support thousands of queues. For pg_work_queue's target (≤ dozens
   of queues), single table is correct.
3. **Partial indexes on hot paths.** `WHERE status IN ('queued',
   'awaiting_retry')` matches River's prioritized fetch index and
   pg-boss's active-job index.
4. **Status ENUM with CHECK invariants.** Tighter than Que's
   "derive from nulls" approach; matches River.
5. **`BIGINT IDENTITY` internal PK + `UUID public_id`.** River uses
   `bigserial`. Compact FKs and index keys matter at scale.
6. **`payload BYTEA` (codec-agnostic).** Matches gue and neoq. The
   tradeoff: lose SQL-level filtering (River and pg-boss use `jsonb`
   for queryability). Defensible for a minimal library.
7. **Advisory-lock guard on reaper.** Que and Solid Queue use similar
   patterns for singleton background loops.
8. **No `LISTEN/NOTIFY`.** Validated above. River, even though it
   uses NOTIFY for low-latency, still polls (default ~1s) as a
   fallback — the issue at riverqueue/river#960 shows users do hit
   the polling latency floor when NOTIFY isn't usable. A pure-polling
   design is honest about its latency floor and avoids the commit-
   serialization cliff.
9. **Determined retry knob via `Outcome::Retry { in_ }` + `run_at`.**
   Matches gue's `run_at` + Que's recursive scheduling.
10. **No background worker registration table.** Plan correctly
    identifies this as future-work, not v0.1 necessity. Reaper using
    `last_attempted_at` as proxy is the same pattern River uses with
    its `JobRescuer`.

---

## What pg_work_queue PLAN might be missing or oversimplifying

### A. Bloat / vacuum / autovacuum tuning is not addressed

Brandur's "postgres-queues" essay is the canonical warning: hot
queue tables churn dead tuples FAST. Symptoms when this goes bad:
worker claim latency goes from <10ms → >100ms; the partial index
scan must traverse thousands of dead-but-not-yet-vacuumed tuples
(indexes don't store visibility info).

**Specific recommendations to add to the plan or migration:**

- Per-table autovacuum settings on `pg_work_queue.jobs`:
  ```sql
  ALTER TABLE pg_work_queue.jobs SET (
      autovacuum_vacuum_scale_factor = 0.05,   -- default 0.2 is way too lazy
      autovacuum_analyze_scale_factor = 0.05,
      autovacuum_vacuum_cost_delay = 0,        -- run hot
      fillfactor = 80                          -- room for HOT updates
  );
  ```
- `fillfactor = 80` is critical: it leaves room on each page for
  HOT (heap-only-tuple) updates, which avoid index bloat entirely
  when the updated columns are NOT indexed. Status/attempts/timestamps
  updates qualify as HOT if no index column is touched.
- HOT-blocker: any update that changes an indexed column (here:
  `status`, `run_at`, `last_attempted_at`, `finished_at` are all in
  partial indexes) defeats HOT. Tradeoff: more indexes = more index
  maintenance + less HOT eligibility.

### B. `mark_done` keeps the row — long-term bloat strategy unstated

Plan keeps `done` rows in-table behind a partial index. Two patterns
seen elsewhere:

- **DELETE on success** (gue): row vanishes; tiny working set; loses
  audit history.
- **Move to archive table** (pg-boss): partitioned `archive` table;
  main table stays small.
- **Keep with retention** (River, plan): `purge_terminal(ttl)` runs
  periodically. Works if the purge runs reliably. The Recall outage
  cited in PLAN.md was partly amplified by orphan `Done` rows from
  apalis. The plan acknowledges `purge_terminal` (Faza 8) but doesn't
  recommend a default interval/TTL.

**Recommendation**: ship a default `purge_terminal(retention:
Duration::from_secs(86400 * 7))` background loop (or a documented
cron hook) and make the TTL a builder knob. Document the index-
bloat-vs-audit tradeoff in the README.

### C. Dead-letter handling: in-table vs separate

Plan: `status='dead'` in same table. neoq, river all keep dead
in-table. pg-boss has `dead_letter` field pointing at a different
queue name. gue moves to `finished_jobs_log`.

In-table is simpler. The risk is "dead rows are forever" unless
purged. Recommendation: `purge_terminal` should be opinionated about
NOT purging dead rows by default — they're the diagnostic value.
Two separate retention knobs: `done_retention` + `dead_retention`
(or "never" for dead).

### D. `attempts SMALLINT` will overflow with weird user code

SMALLINT max is 32767. Realistic? Yes, in a flapping retry loop a
job could hit it. River uses `smallint` too; pg-boss uses `integer`.
Either add `CHECK (attempts <= max_attempts)` (already implied by
the dead-letter logic) or use `INT`. Cost difference is 2 bytes per
row — negligible relative to UUID + payload. **Minor: consider INT.**

### E. No priority

Every surveyed library has priority (River `priority smallint
1..4`, gue `priority smallint`, Que `priority`, pg-boss `priority`).
Plan deliberately omits. This is fine for v0.1 but a common feature
ask. Adding later requires: a new column, a new index column order
`(queue, priority, run_at, id)`, and updates to the claim ORDER BY.
Non-trivial but additive. **Doc as explicit non-feature.**

### F. No batch insert API

Plan's `Pusher::push` is single-row. River's main perf claim is "batch
job insertion using Postgres `COPY FROM`." For pg_work_queue's
target consumer (`rust_event_outbox` dispatching events), batch
INSERT in one round-trip can be 10-100× faster than per-row INSERT.
**Recommendation**: add `Pusher::push_batch(&mut tx, items: &[T])`
in v0.1 or v0.2. Use `INSERT … SELECT * FROM UNNEST($1::bytea[], …)`
or sqlx's `COPY` support.

### G. Reaper concurrency: SKIP LOCKED as self-throttle (alternative to advisory lock)

Plan uses `pg_try_advisory_lock` to ensure a single reaper across
replicas. Solid Queue uses leader-elected dispatcher. Alternative
that's actually quite elegant: reaper SQL itself uses
`FOR UPDATE SKIP LOCKED` on stale `running` rows, so N replicas
running it concurrently just partition the work:

```sql
WITH stale AS (
  SELECT id FROM pg_work_queue.jobs
  WHERE status='running' AND last_attempted_at < now() - $1::interval
  ORDER BY last_attempted_at
  LIMIT 100
  FOR UPDATE SKIP LOCKED
)
UPDATE pg_work_queue.jobs SET status='awaiting_retry', ... FROM stale WHERE jobs.id=stale.id;
```

No advisory lock needed; replicas naturally split work; no leader-
election complexity. **Recommendation**: consider replacing advisory
lock with SKIP LOCKED in reaper. Same correctness (idempotent), no
extra primitive.

### H. Periodic / scheduled / cron jobs

Not in plan. Every other library has them. For pg_work_queue v0.1
this is fine — outbox doesn't need cron. But it's the #1 likely
feature request after open-sourcing. Architecture sketch (for
future): a separate `pg_work_queue.schedules` table, a leader-elected
"scheduler" loop that inserts jobs when due, the user's own cron
syntax (or just `Duration` for "every N").

### I. Polling cadence under load — fixed interval can waste DB CPU

Plan explicitly chooses fixed `poll_interval` and rejects backoff.
The tradeoff being missed: at `poll_interval=100ms` × 16 workers ×
4 services × 4 queues each = 2,560 empty `SELECT FOR UPDATE` per
second just for "is there work". Each one is cheap (~0.2ms) but the
sum is real and consumes a connection from the pool for 0.2ms each.

**Counter-argument the plan is making, and it's defensible**: that's
the user's choice and the user gets predictable latency. Adaptive
backoff hides DB load behind unpredictable latency cliffs (apalis
1s → 5min cap = exactly the kind of "why is my job stuck for 5
minutes?" UX bug).

**Recommendation**: document the math. Example: "With
poll_interval=100ms and N concurrent worker handles, expect ~10N
empty SELECTs/sec on idle queue. Tune accordingly."

### J. `Outcome::Retry { in_: Option<Duration> }` — what's "None"?

Plan says "domyślnie 0 (czyli już-następny poll cycle)" — i.e.,
`in_=None` means "retry immediately on next poll". But: if the same
handler keeps failing and returning `in_=None`, the row burns
through `max_attempts` very fast (`poll_interval` apart). This is
correct behavior but easy footgun.

**Recommendation**: rename / re-default. Options:

- `in_: Option<Duration>` with `None` meaning "library decides"
  (default: exponential backoff `2^attempts * base_delay`, capped).
- Or: keep `None` = "immediate" but add a separate builder knob
  `default_retry_backoff: Option<Box<dyn Fn(u32) -> Duration>>`
  that the library applies when handler returns `in_=None`.

Sane default backoff is in every other library (gue, pg-boss, River,
Que). Plan's "0 by default" is unusual and probably user-hostile.

### K. `BYTEA` vs `JSONB` — pragmatic recommendation

Plan chose BYTEA. Pros: codec-agnostic, sqlx round-trips it cleanly,
no jsonb overhead. Cons:

- Can't inspect payload from `psql` ("what's stuck in the queue?"
  becomes "let me write a script to decode the bytea").
- Can't filter (`WHERE payload->>'user_id' = 'X'`) — useful for
  manual ops.
- Can't GIN-index sub-fields for unique-job-by-args (River does
  this).

**Alternative**: ship BYTEA in v0.1 as planned, but document that v0.2
might add a `JSONB` column variant. Or: accept JSONB always but
require users to provide a `serde_json::Value` payload, which works
for everyone serializing with serde.

The plan's note "BYTEA na start, JSONB później jeśli potrzebne" is
fine. Just keep migration headroom (don't make the column type
hard-coded in the binary protocol).

### L. PgBouncer compat — `LISTEN` would have killed it, SKIP LOCKED is fine

Plan open question 9 — "PgBouncer transaction-pooling compatibility".
SKIP LOCKED, advisory `_xact_` locks, and short-lived transactions are
ALL fine under PgBouncer. The plan is already PgBouncer-compatible by
construction. Worth a one-line test under PgBouncer container and a
README note.

### M. Replication / read-replicas

Plan doesn't address replicas. Some libraries (river issue #1101)
explicitly fail on read-only connections. pg_work_queue inherits this
constraint: the worker pool MUST be on a primary, not a hot standby
(SKIP LOCKED requires WAL writes). Document.

---

## Open recommendations (prioritized)

**Must do for v0.1:**

1. **Sharpen the NOTIFY motivation in PLAN.md** — the lock IS
   `AccessExclusiveLock`, the locktype IS `database` (per Recall.ai
   `pg_locks` evidence), it serializes NOTIFY-issuing commits cluster-
   wide. The current PLAN wording is correct in spirit but imprecise.
2. **Add per-table autovacuum + fillfactor settings to the migration**:
   `autovacuum_vacuum_scale_factor=0.05`, `fillfactor=80`. Without
   this the table will bloat under load. Cheap, additive, defensible.
3. **Document the polling-load math** in README. "10N empty SELECTs/sec
   per worker handle per queue at poll_interval=100ms". Lets users
   size confidently.
4. **Default retry backoff**. `Outcome::Retry { in_: None }` should not
   mean "0ms" by default — that's a footgun. Either compute
   `base * 2^attempts` as library default, or require users to specify.
5. **Document the dead-row retention policy.** `purge_terminal` should
   default to "purge done after 7 days, keep dead forever". Two
   retention knobs.

**Should do for v0.1 or v0.2:**

6. **Batch INSERT API** — `Pusher::push_batch`. Outbox dispatch is the
   first consumer and it'll insert N rows per outbox event; per-row
   round-trips will dominate latency.
7. **Reaper without advisory lock** — use SKIP LOCKED on stale rows so
   N replicas naturally partition. Simpler than the advisory-lock
   guard.
8. **`attempts` as INT not SMALLINT** — 2 bytes saved per row vs.
   robustness against runaway retry counters. Tradeoff is trivial.

**Nice to have (post-v0.1):**

9. **Priority column** — every other queue has it; add it as `priority
   SMALLINT DEFAULT 0` with claim ORDER BY `(priority DESC, run_at, id)`.
10. **Periodic/cron jobs** — separate table + leader-elected scheduler.
11. **JSONB payload variant** — allows GIN indexes for unique-by-args
    semantics later.
12. **README: PgBouncer compat note + replica restriction note.**

**Explicit non-features to document (so users don't ask):**

- No worker dashboard / liveness table (Que / Solid Queue have it; we
  don't need it).
- No multi-queue per worker (one queue per `Worker::builder`).
- No retry middleware (tower-style); wrap handler manually.
- No LISTEN/NOTIFY (with reference to lock-truth section above).

---

## Concrete files cited

- `/Users/pawel/workspace/rust_packages/pg_work_queue/PLAN.md`
- Postgres source: `src/backend/commands/async.c` — `NotifyQueueLock` definition
- River migrations: `riverdriver/riverpgxv5/migration/main/002_initial_schema.up.sql`
- Que poller: `lib/que/poller.rb` (recursive CTE + `pg_try_advisory_lock`)
- pg-boss schema: `src/plans.ts` (CREATE TABLE job, partitioned by name)
- Recall.ai post-mortem: "Postgres LISTEN/NOTIFY does not scale"
- Brandur Leach: "Postgres job queues & failure modes" (MVCC bloat)
