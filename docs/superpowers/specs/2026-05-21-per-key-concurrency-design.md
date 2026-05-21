# Design: per-key concurrency limiting (single-instance)

**Status:** design — revised after review round 3
**Date:** 2026-05-21 (rev 3: 2026-05-22)
**Crate:** `pg_work_queue` (v0.1, pre-1.0; breaking changes allowed)

## Problem

`pg_work_queue` today has one concurrency knob: `WorkerBuilder::concurrency`,
a process-wide semaphore over the single handler. The consumer `rust_events`
(a transactional outbox) routes every handler delivery onto one `pgwq` queue
and needs a second, narrower control: cap how many jobs of a given *kind* run
at once — e.g. one handler hits a rate-limited external API, another is heavy
and must not be flooded.

The cap must be applied **at claim time, not after**. The naive alternative —
a semaphore inside the handler, after the job is claimed — is wrong: the job is
already `running` and leased, so surplus jobs park on the semaphore, occupying
Worker permits, starving other job kinds (head-of-line blocking), and burning
`attempts`/lease. The correct behavior is to **not claim** a job whose key is
saturated — it stays `queued`, and Worker permits stay free for other keys.

## What the feature guarantees

For a given `(queue, concurrency_key)`, **at most N handler tasks execute
concurrently** — where N is the configured limit. The guarantee surface is
*live handler tasks*, not rows in `status='running'`: a job row can be
`running` in the database with no live handler (a crash ghost — see §4), and
such a row consumes no real resource. The Worker enforces the bound on what it
controls — the tasks it spawns.

## Hard assumption: single instance / single Worker object

The deployment runs **exactly one Worker process, and exactly one `Worker`
object drives the queue** — no clustering, and no second `Worker` (or
`tick_once` caller) targeting the same queue. This is a hard, confirmed
constraint. It lets the per-key count live **in process memory** instead of
being coordinated through the database.

Consequences (documented in the README):
- 2+ Worker processes/objects → the per-key limit silently becomes `N ×`
  drivers (each counts only its own tasks). Lifting this would require
  DB-coordinated counting (a `running_counts` aggregate + MVCC-race handling).
- `tick_once` does not enforce per-key limits (§7). Running `tick_once` against
  the same queue as a `start()`ed Worker defeats the limit.

The rest of the crate's machinery (reaper, `lease_token` fencing,
`FOR UPDATE SKIP LOCKED`, per-row `max_attempts`/`lease_expires_at`) is **not**
clustering machinery — it is crash-recovery, which a single instance needs just
as much. It stays untouched.

## Model

- A job carries an optional `concurrency_key: Option<String>`, stamped once at
  enqueue, immutable for the job's lifetime. `NULL` = no limit (today's
  behavior). Keys are opaque, `COLLATE "C"` byte-exact, case-sensitive, not
  trimmed.
- Limits are Worker configuration: a `key → limit` map on `WorkerBuilder`.
  A key present on a job but absent from the map = unlimited.
- The Worker keeps an **in-memory per-key counter of live handler tasks**,
  starting at zero. Each poll tick it computes `headroom = limit − count` per
  configured key and passes the headroom map into the claim query. The claim
  SQL claims at most `headroom` rows per key.

---

## 1. Schema — migration `20260521000000_v01_concurrency_key.sql`

```sql
-- New column. ADD COLUMN with no default is metadata-only on PG18 (no table
-- rewrite). Rows existing at migration time get NULL -> unlimited.
ALTER TABLE pgwq.jobs
    ADD COLUMN concurrency_key TEXT COLLATE "C";   -- NULL = no limit

-- NOT VALID: skips the validating full-table scan. This is correct, not a
-- shortcut: every pre-existing row has concurrency_key = NULL, which trivially
-- satisfies the CHECK, so the unscanned rows are provably valid. NOT VALID
-- still enforces the CHECK on every INSERT/UPDATE from now on. No VALIDATE
-- step is needed.
ALTER TABLE pgwq.jobs
    ADD CONSTRAINT jobs_concurrency_key_len
    CHECK (concurrency_key IS NULL
           OR (length(concurrency_key) >= 1 AND length(concurrency_key) <= 128))
    NOT VALID;

-- Rebuild jobs_claim_idx with concurrency_key as an INCLUDE (covering) column.
-- The (queue, run_at, id) KEY prefix is byte-identical to the old index, so
-- the empty-limits `claim_batch` range scan and global FIFO order are
-- unaffected (INCLUDE columns live only in leaf pages, not the B-tree key).
-- The INCLUDE serves ONLY the keyed claim path: it lets `eligible_unlimited`'s
-- anti-join filter on concurrency_key without a heap fetch. It does nothing
-- for the empty-limits path or for RETURNING.
DROP INDEX pgwq.jobs_claim_idx;
CREATE INDEX jobs_claim_idx
    ON pgwq.jobs (queue, run_at, id) INCLUDE (concurrency_key)
    WHERE status IN ('queued', 'awaiting_retry');

-- Per-key bounded claim. Leading (queue, concurrency_key) equality makes each
-- per-key LATERAL a tight (run_at, id) range scan, no Sort node.
CREATE INDEX jobs_claim_conc_idx
    ON pgwq.jobs (queue, concurrency_key, run_at, id)
    WHERE status IN ('queued', 'awaiting_retry');

-- Immutability guard for concurrency_key — a SEPARATE single-purpose trigger
-- (NOT folded into the unrelated pgwq.set_updated_at touch function). The
-- crate's own claim/mark/reaper UPDATEs never name concurrency_key in their
-- SET lists, so for them OLD IS NOT DISTINCT FROM NEW holds and this trigger
-- is provably inert. Its real blast radius is an *external* hand-rolled
-- `UPDATE pgwq.jobs SET concurrency_key = …` — it guards against tampering
-- the in-memory counter and the claim CTE rely on. BEFORE UPDATE only, so
-- INSERT (NULL -> value at enqueue) is allowed.
CREATE FUNCTION pgwq.assert_concurrency_key_immutable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.concurrency_key IS DISTINCT FROM NEW.concurrency_key THEN
        RAISE EXCEPTION
            'pgwq.jobs.concurrency_key is immutable (job id %)', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER assert_concurrency_key_immutable
    BEFORE UPDATE ON pgwq.jobs
    FOR EACH ROW EXECUTE FUNCTION pgwq.assert_concurrency_key_immutable();
```

Notes:

- `concurrency_key` is **not** part of `jobs_status_invariants` — it is
  orthogonal to the status FSM. PG fires `BEFORE UPDATE` row triggers
  alphabetically; `assert_concurrency_key_immutable` runs before `touch_jobs`
  — order is irrelevant (the immutability trigger only reads `OLD`/`NEW`).
- **Migration lock cost.** `ADD COLUMN` and `ADD CONSTRAINT … NOT VALID` are
  metadata-only and effectively instant — but they take `ACCESS EXCLUSIVE` on
  `pgwq.jobs`, and Postgres holds locks until the transaction commits. Since
  `sqlx` runs the whole migration file in one transaction, the migration holds
  `ACCESS EXCLUSIVE` on `pgwq.jobs` for its **entire** duration — it blocks
  **reads and writes** (claims, marks, the reaper, pushes), not just writes.
  The duration is dominated by the two `CREATE INDEX` builds (non-`CONCURRENTLY`
  — `sqlx`'s one-transaction-per-file model precludes `CREATE INDEX
  CONCURRENTLY`). `DROP INDEX jobs_claim_idx` + `CREATE INDEX` of the same name
  in one transaction is sound, and outside observers never see a missing index
  (they see the old index, or — post-commit — the new one). For a queue table
  kept small by purging (the expected case) the whole migration is sub-second.
  For a large unpurged table it is a full read+write stall proportional to row
  count — README "Known limitations" must state this with a concrete
  order-of-magnitude.
- **Write amplification.** `jobs_claim_conc_idx` shares the partial predicate
  (`status IN ('queued','awaiting_retry')`) with `jobs_claim_idx`. Per the
  existing schema comment (HOT updates already impossible — every transition
  crosses a partial-index predicate), each enqueue and each
  `running→awaiting_retry` retry now maintains **two** claim indexes instead of
  one — ~2× claim-index entry churn and matching dead-tuple autovacuum load.
  Inherent cost of the feature; documented, not a regression.

New public constant in `limits.rs`:

```rust
/// Maximum length of a job's concurrency key, in **characters**
/// (Postgres `length(TEXT)` units — matches the DB CHECK).
pub const MAX_CONCURRENCY_KEY_LEN: usize = 128;
```

---

## 2. Pusher API (breaking change — allowed pre-1.0)

```rust
push(tx, payload, concurrency_key: Option<&str>)            -> Result<Uuid, PushError>
push_at(tx, payload, run_at, concurrency_key: Option<&str>) -> Result<Uuid, PushError>
push_batch(tx, &[(T, Option<String>)])                      -> Result<Vec<Uuid>, PushError>
```

- **Validation, fail-fast.** `concurrency_key` validity is checked **before**
  the codec encode step (it is cheap and should fail before any encoding work).
  Per-push order: queue-name → concurrency-key → encode/size. `Some(k)` must be
  `1..=MAX_CONCURRENCY_KEY_LEN` **characters** (`k.chars().count()`, *not*
  `.len()` — a multi-byte CJK/Polish key of 128 chars is up to 384 bytes; the
  DB `length()` counts characters, so Rust must too). Empty `Some("")` is
  rejected. New error `PushError::ConcurrencyKeyInvalid(String)` — tuple
  variant carrying the offending key, matching `PushError::QueueNameInvalid`.
- **Adjacent fix (in scope):** `pusher.rs::validate_queue` and `worker.rs::
  build()` currently length-check the queue name with `.len()` (bytes) against
  `MAX_QUEUE_LEN` while the DB CHECK uses `length()` (characters) — a latent
  inconsistency in the exact validation code this change touches. Both switch
  to `.chars().count()`.
- `push_batch` element type changes `&[T]` → `&[(T, Option<String>)]`. The
  encode loop encodes `&tuple.0`; `BatchCodec { index }` / `PayloadTooLarge
  { index }` indices remain the tuple's slice position. `BatchEmpty` still
  fires before the INSERT, so the INSERT always sees `N ≥ 1` rows. The INSERT
  becomes `unnest($2::bytea[], $3::uuid[], $4::text[])`; keys bind as a
  full-length `Vec<Option<String>>` (`None` → SQL `NULL`); all three arrays
  share `payloads.len()`, satisfying `unnest`'s equal-length requirement.
  Order is positional and preserved.
- `concurrency_key` bytes do **not** count toward `MAX_BATCH_BYTES` (which
  budgets payloads only). Documented.
- `PushError` gains `#[non_exhaustive]` in this change (the only error enum
  currently lacking it).
- **Cost acknowledged:** the `push_batch` element-type change breaks every
  existing call site — exactly **17 files** in `tests/` (verified). Mechanical
  edits (`&[job]` → `&[(job, None)]`) on the crate's own test suite, updated in
  lockstep.

---

## 3. WorkerBuilder

```rust
.concurrency_limits(impl IntoIterator<Item = (String, u32)>)
```

- Accumulates into a `HashMap<String, u32>`. Multiple calls accumulate; a
  duplicate key (within one call or across calls) takes the **last** value and
  emits a `tracing::warn!` at `build()` time naming the overwritten key. (The
  `warn!` may be dropped if no subscriber is installed yet — acceptable, and
  consistent with the crate's existing `build()`-time `warn!` usage.)
- `build()` validates: each key `1..=MAX_CONCURRENCY_KEY_LEN` characters; each
  limit `1..=i32::MAX`. New errors:
  - `BuildError::ConcurrencyKeyInvalid(String)` — tuple variant, matching
    `BuildError::QueueNameInvalid`.
  - `BuildError::ConcurrencyLimitInvalid { key: String, limit: u32 }` — covers
    `limit == 0` and `limit > i32::MAX`. Display string:
    `"concurrency limit for key {key:?} must be in 1..=2147483647, got {limit}"`.
- **No** cross-knob constraint with `.concurrency()` — the per-key limit and
  the worker-wide `concurrency` are independent axes. A per-key limit larger
  than `concurrency` simply means the worker-wide cap binds first.
- **Plumbing.** `concurrency_limits` is a new field on `WorkerBuilder`; it must
  be added to `WorkerBuilder::new()` and threaded through the two type-state
  transition methods `.codec()` and `.handler()`, which rebuild `WorkerBuilder`
  field-by-field (they will not compile until the field is carried through
  both). It then flows `WorkerBuilder` → `Worker` → `WorkerState` (three
  structs); `start()`'s `WorkerState { … }` literal gains the field. The
  accumulating method does not interact with the `H` handler type-state.

---

## 4. In-memory per-key counter — RAII drop-guard

The counter tracks **live handler tasks per key** in this process. A slot is
consumed by a running handler task and released when that task exits —
*regardless of the `mark_*` outcome*. A handler whose `mark_done` fails
transiently is still *done*; its task exits, its slot frees, and the reaper
later reclaims the stale `running` row. Tracking tasks (not DB rows) is what
makes the headroom guarantee a property of process state the Worker fully
controls.

`WorkerState` holds:

```rust
concurrency_running: Arc<std::sync::Mutex<HashMap<String, u32>>>
```

Fully-qualified `std::sync::Mutex` — `worker.rs` shadows the bare name with
`tokio::sync::Mutex`. A `std::sync::Mutex` is required because the decrement
runs inside a synchronous `Drop` (a `tokio::sync::Mutex` would need `.await`
to lock).

**No DB seed.** The counter starts with every configured key initialized to
`0` (`{k: 0 for k in concurrency_limits}` — so `acquire`/decrement/headroom
never hit a missing key) and is never seeded from the database. The counter is
a pure process-local gauge of *this process's* live handler tasks; at startup
there are none. `running` rows left in the database by a crashed previous
process are **ghosts** — their handlers died with that process and consume no
real resource — so the new process correctly running up to `limit` tasks is
the true concurrency. The database transiently showing more than `limit` rows
in `status='running'` (ghosts + new tasks) is stale metadata, reclaimed by the
reaper within `≤ lease_timeout + reaper_interval`; it is not real
over-execution. `Worker::start` does no extra query for this.

### Decrement is structural, not a checklist — `KeySlotGuard`

```rust
/// Owns one per-key slot: increments on construction, decrements on Drop.
/// A no-op for jobs without a configured-limit key.
struct KeySlotGuard {
    slot: Option<(Arc<std::sync::Mutex<HashMap<String, u32>>>, String)>,
}
impl KeySlotGuard {
    /// Increments the counter for `key` and returns a guard owning that slot.
    /// On a poisoned mutex (unreachable — see below) returns `none()` so the
    /// guard owns NO slot: "increment succeeded ⟺ guard owns a slot".
    fn acquire(map: Arc<…>, key: String) -> Self {
        match map.lock() {
            Ok(mut m) => {
                let n = m.entry(key.clone()).or_insert(0);
                *n = n.saturating_add(1);          // symmetric with Drop
                Self { slot: Some((map, key)) }
            }
            Err(_) => Self { slot: None },         // poisoned -> own nothing
        }
    }
    fn none() -> Self { Self { slot: None } }
}
impl Drop for KeySlotGuard {
    fn drop(&mut self) {
        if let Some((map, key)) = &self.slot {
            if let Ok(mut m) = map.lock() {
                if let Some(n) = m.get_mut(key) { *n = n.saturating_sub(1); }
            }
        }
    }
}
```

- `saturating_add`/`saturating_sub` (never `+= 1`/`-= 1`) — an overflow panic
  would violate the crate's `panic = deny` lint. Overflow is physically
  unreachable (count is bounded by `concurrency`), but the symmetry keeps the
  lint posture uniform.
- **Poisoning is unreachable**, not "tolerated": the `std::Mutex` critical
  sections do only O(1), panic-free `HashMap` work (no user code, no
  fallible allocation relied upon). A `std::sync::Mutex` poisons only if a
  thread panics *while holding* it; nothing here can. The `Err(_)`/`if let Ok`
  arms are purely defensive — they exist so a (theoretically impossible)
  poison cannot itself panic. The asymmetry is deliberate: on a poisoned lock
  `acquire` owns no slot (so its `Drop` is a no-op) — never a `Some`-slot guard
  whose increment was skipped, which would `Drop`-decrement into an under-count
  → over-admission.

`handle_job` takes a `KeySlotGuard` parameter, exactly like the existing
`_permit: OwnedSemaphorePermit`. Because `Drop` runs on **every** task exit —
normal return through any `match` arm, handler panic, the
`Some(Err(_cancelled))` / `None` arms, and tokio task cancellation from
`JoinSet::abort_all` during shutdown — the decrement is exhaustive by
construction. There is no per-outcome decrement code.

### Lock discipline

The counter `std::Mutex` is held only for O(1), `.await`-free `HashMap` work.
That bounds the only real hazard — a `std::sync::Mutex::lock()` inside an async
task briefly *blocks the tokio worker thread* — to a negligible window; it is
**not** a deadlock concern. For completeness: no code path locks the counter
mutex then acquires the `tasks` (`tokio::sync::Mutex`) — the nesting is always
`tasks`-outer / counter-inner (the spawn loop, §below) or counter-alone
(headroom snapshot, guard `Drop`), never reversed.

### Increment — atomic with spawn, over the spawned subset

In the poll loop's `Ok(rows) =>` arm, inside the `tasks.lock().await` critical
section that spawns handlers, in the same non-`await` loop iteration as
`tasks.spawn` — and over the post-`zip` subset, so the increment covers exactly
the rows that get a `handle_job` task (never a surplus row `zip` would drop):

```rust
for (row, permit) in rows.into_iter().zip(permits) {
    let guard = match &row.concurrency_key {
        Some(k) if state.concurrency_limits.contains_key(k) =>
            KeySlotGuard::acquire(state.concurrency_running.clone(), k.clone()),
        _ => KeySlotGuard::none(),
    };
    tasks.spawn(handle_job(row, state.clone(), permit, guard));
}
```

The guard is constructed (incrementing) and the task spawned (taking
ownership) in the same iteration, with no `.await`/`?`/`return`/panic between
them — there is never an increment not already owned by a spawned task. The
poll loop is single-threaded and the spawn loop runs synchronously before the
next `ticker.tick()`, so this is race-free against the next tick's headroom
snapshot. The increment runs inside the `tasks` mutex critical section — it
must stay there.

`concurrency_key: Option<String>` is added to the claim `RETURNING` list, to
`RawClaimedRow`, and to the internal `Job<T>` struct (extracted via
`try_get::<Option<String>, _>("concurrency_key")` — sqlx maps SQL `NULL` →
`None`; the field **must** be `Option<String>` or every unkeyed row fails
decode). `handle_job` captures it into a local to build the guard. It is
**not** added to the public `JobContext` (YAGNI; addable later non-breaking).

### Headroom

At each tick start the poll loop snapshots the counter: for every configured
key, `headroom = limit.saturating_sub(count)`. The headroom map passed to the
claim contains **every** configured-limit key (including headroom `0`) — the
invariant `headroom.len() == concurrency_limits.len()` holds and is asserted
before binding. A saturated key must still appear, or the claim's
`eligible_unlimited` anti-join would treat its rows as unlimited.

### Lifecycle notes

- **Shutdown does not drain the counter.** It is process-lifetime state; the
  process exits after shutdown. `Worker::start` consumes `self` and
  `WorkerHandle::shutdown` consumes the handle — there is no restart-in-place
  path.
- **Non-cooperative handler.** `handler_timeout` is cooperative — a CPU-bound
  handler with no `.await` is not cancelled and can run past `lease_timeout`.
  Its `KeySlotGuard` is not dropped until it finishes, so it legitimately holds
  its key slot the whole time — correct: the work *is* still running. The
  reaper may meanwhile reclaim that row to `awaiting_retry`, and the poll loop
  may re-claim it and `acquire` a *second* guard for a second task of the same
  job → the key is transiently double-counted. This is in the safe direction
  (conservative over-count → under-admit) and self-heals when the stuck task
  ends. README documents both the slot-held-past-lease and the transient
  double-count.
- **`tick_once`** is a standalone single-tick entry point with no persistent
  counter; it calls `claim_and_decode` with an empty headroom map
  (`&HashMap::new()`) → the unchanged `claim_batch` SQL. It does **not** enforce
  per-key limits — documented as a limitation; per-key limiting is a property
  of the `start()` poll loop only.

---

## 5. Claim query

`claim_and_decode` takes a new parameter `headroom: &HashMap<String, u32>`
(the per-tick headroom map — *not* the raw limits; the Worker owns the counter
and computes headroom, so `claim.rs` never sees `WorkerState`). `claim.rs`
branches:

- **`headroom` empty** (Worker has no `concurrency_limits`, or `tick_once`) →
  the existing `claim_batch` SQL. Its `RETURNING` list **gains
  `concurrency_key`** — `RawClaimedRow` is shared between both paths, so
  "unchanged" applies to the claim *logic*, not the `RETURNING` columns.
- **`headroom` non-empty** → the query below, headroom map bound as `jsonb`
  (`$5`).

`$2` is bound to **`want`** — the per-tick free-permit count — exactly as
`claim_batch_raw` binds `batch_size` today. `want = permits.len()`. The claim
must never return more rows than the Worker holds permits for, or
`zip(rows, permits)` silently drops surplus claimed rows (stalled until the
reaper reclaims them, plus a counter mismatch).

```sql
WITH
hr AS (
    -- headroom map from the Worker: every configured key, headroom >= 0
    SELECT key AS concurrency_key, value::int AS h
    FROM jsonb_each_text($5)
),
eligible_keyed AS (
    SELECT e.id
    FROM hr
    CROSS JOIN LATERAL (
        SELECT j.id
        FROM pgwq.jobs j
        WHERE j.queue = $1
          AND j.status IN ('queued', 'awaiting_retry')
          AND j.concurrency_key = hr.concurrency_key
          AND j.run_at <= now()
        ORDER BY j.run_at, j.id
        LIMIT LEAST(GREATEST(hr.h, 0), $2)   -- per-key headroom, capped at want
    ) e
),
eligible_unlimited AS (
    -- NULL key OR a key with no configured limit -> unlimited
    SELECT j.id
    FROM pgwq.jobs j
    LEFT JOIN hr ON hr.concurrency_key = j.concurrency_key
    WHERE j.queue = $1
      AND j.status IN ('queued', 'awaiting_retry')
      AND j.run_at <= now()
      AND hr.concurrency_key IS NULL       -- anti-join: unmatched = unlimited
    ORDER BY j.run_at, j.id
    LIMIT $2
),
locked AS (
    SELECT j.id
    FROM pgwq.jobs j
    WHERE j.id IN (SELECT id FROM eligible_keyed
                   UNION ALL
                   SELECT id FROM eligible_unlimited)
      AND j.status IN ('queued', 'awaiting_retry')   -- re-assert full claim
      AND j.run_at <= now()                          -- predicate for EvalPlanQual
    ORDER BY j.run_at, j.id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pgwq.jobs j
SET status = 'running',
    attempts = j.attempts + 1,
    max_attempts = $4,
    last_attempted_at = now(),
    first_attempted_at = COALESCE(j.first_attempted_at, now()),
    lease_token = gen_random_uuid(),
    lease_expires_at = now() + $3::interval,
    last_error = NULL
FROM locked
WHERE j.id = locked.id
RETURNING j.id, j.public_id, j.queue, j.concurrency_key, j.payload,
          j.attempts, j.max_attempts, j.first_attempted_at,
          j.lease_token, j.lease_expires_at;
```

Properties:

- **Row count bound.** `eligible_keyed` ≤ Σ `LEAST(h_k, want)`;
  `eligible_unlimited` ≤ `want`; `locked`'s `LIMIT $2` over the `UNION ALL`
  caps the final set at `want`. So `rows.len() ≤ want = permits.len()` always —
  `zip` never drops a row.
- **Per-key headroom is a ceiling, not a reservation.** `locked` orders the
  union by `(run_at, id)` and takes `LIMIT want` — cross-key allocation within
  a tick is global FIFO. A key with headroom 3 may get 0 rows this tick if
  `want` older rows of other keys fill the batch; it gets its turn on
  subsequent ticks. No long-term starvation: a key claiming aggressively raises
  its own count, shrinks its own headroom to 0, and stops contributing — the
  per-key cap *is* the fairness mechanism.
- **Split-CTE safety.** Unlike `claim_batch` (which locks in the same CTE as
  the predicate), `eligible_keyed`/`eligible_unlimited` select ids unlocked,
  then `locked` locks. Safe here: the whole statement is one MVCC snapshot;
  the single Worker object is the only claimer (no claim-vs-claim race); the
  reaper and `mark_*` only move rows *out of* `running` (disjoint from the
  `queued`/`awaiting_retry` rows the eligible CTEs select); and `locked`'s
  `FOR UPDATE SKIP LOCKED` + re-asserted `status`/`run_at` predicates are the
  READ COMMITTED EvalPlanQual backstop. `concurrency_key` is deliberately *not*
  re-asserted in `locked` — it is immutable (trigger-enforced), so the inner
  CTE's key selection cannot be invalidated.
- **Anti-join.** `eligible_unlimited` selects rows whose key is `NULL` or
  absent from `hr` (unlimited) and excludes configured keys (handled by
  `eligible_keyed`) — the three cases (key NULL / in `hr` / not in `hr`)
  partition the id space, so the `UNION ALL` cannot double-count.
- **`value::int` is safe** — headroom is a small non-negative integer; limits
  are validated `1..=i32::MAX` at `build()`. A saturated key's `LIMIT 0`
  short-circuits (zero rows scanned for that LATERAL).
- **Plans.** `eligible_keyed`'s LATERAL is an index range scan on
  `jobs_claim_conc_idx` with no Sort node (`(queue, concurrency_key)` equality
  + `(run_at, id)` ordered suffix); index-only when the visibility map is
  current (a high-churn table — autovacuum cadence, already tuned to
  `scale_factor 0.05`, is what keeps it index-only in practice). The
  `jsonb_each_text` SRF has no statistics — the planner estimates `hr` at a
  fixed 100 rows; harmless because the configured-key count is small and
  config-bounded.
- **Head-of-queue skew.** `eligible_unlimited` scans `(run_at, id)`-ordered
  rows and anti-joins out configured keys; the `INCLUDE (concurrency_key)` on
  `jobs_claim_idx` keeps that filter index-only (VM-permitting). If a saturated
  configured key owns a large block of the *oldest* rows, `eligible_unlimited`
  still scans past all of them before reaching `$2` unlimited rows:
  `O(skipped)` index entries (cheap per entry, no heap fetch) — at a 1M-row
  skewed backlog this is ~30 MB of index pages scanned per tick while
  saturation persists. This is **largely moot for the `rust_events` workload**
  (every job carries a configured `concurrency_key`, so `eligible_unlimited` is
  near-empty). README "Known limitations" documents the skew with the concrete
  1M-row figure; not fixed (no `O(want)` rewrite without a third index).

---

## 6. Observability

The Worker knows its saturated keys (headroom `0`) in memory before each claim.
The poll loop keeps the previous saturated-key set as a **poll-loop local**
(like `consecutive_claim_errors`); when the set changes it emits a
`tracing::debug!` on target `pgwq.claim` naming the saturated keys and counts.
Edge-triggered, so a steady-state saturated key is invisible to a subscriber
that attaches late — README notes this.

---

## 7. Non-goals

- No new `Stats` / `TickStats` fields. Saturation is observable via the
  tracing event above; per-key stats deferred (YAGNI for v0.1).
- Reaper, `mark_*`, and `transition` SQL unchanged (the empty-limits
  `claim_batch` gains only `concurrency_key` in `RETURNING`). The reaper does
  not touch the counter.
- `concurrency_key` is not exposed on `JobContext`.
- `tick_once` does not enforce per-key limits (§4). Running `tick_once` against
  a queue a `start()`ed Worker also drives defeats the limit — forbidden by
  the single-`Worker`-object assumption.
- No DB-coordinated counting — out of scope under the single-instance
  assumption.
- No counter seeding from the database (§4).

---

## 8. README / rustdoc updates (lockstep — required by CLAUDE.md)

- `## Quick start` and the crate-root rustdoc example in `lib.rs` — `push` /
  `push_batch` call sites change signature; the doctests must compile.
- `### Pusher — enqueue side`: all three `push*` signatures + `concurrency_key`;
  the `push_batch` method-table row signature (`&[T]` → `&[(T,
  Option<String>)]`); the `unnest(…)` SQL string gains `$4::text[]`; the
  per-push "Validation order" list gains the concurrency-key step (before
  encode).
- `#### Builder methods — full table`: new `concurrency_limits` row.
- `### State machine and schema`: `concurrency_key` column, the rebuilt
  `jobs_claim_idx` (now `INCLUDE`), `jobs_claim_conc_idx`, the immutability
  trigger.
- `### Error types`: `PushError::ConcurrencyKeyInvalid`,
  `BuildError::ConcurrencyKeyInvalid`, `BuildError::ConcurrencyLimitInvalid`
  (with Display text); `PushError` now `#[non_exhaustive]`.
- `### Resource limits`: `MAX_CONCURRENCY_KEY_LEN` (character units).
- `### Tracing / observability`: edge-triggered `pgwq.claim` saturation event.
- New section `### Per-key concurrency`: model; claim-time gating; the
  guarantee surface (live tasks, not `running` rows); the single-instance /
  single-`Worker`-object assumption and its consequences; no-seed restart
  behavior (transient stale-high `running` count); the non-cooperative-handler
  slot-hold + transient double-count; the `tick_once` limitation.
- `## Architecture`: claim-path branch narrative.
- `## Known limitations`: single-instance assumption; large-table migration
  read+write stall (with concrete magnitude); head-of-queue skew (with the
  ~1M-row figure).

---

## 9. Tests (`tests/`, one file per behavior)

New behavioral tests:

- Per-key limit respected — paired test at two limit values (e.g. 1 and 3).
- Per-key limit vs `concurrency`: `concurrency=2`, one key `limit=10` → no more
  than 2 run concurrently (worker-wide cap binds).
- `NULL` key unlimited; on-job-but-unconfigured key unlimited.
- Saturated key stays `queued` while other keys keep claiming (no head-of-line
  blocking).
- Counter leak test: run N jobs to completion, assert counter returns to `0`;
  **and** abort handlers mid-run (shutdown path) and assert the `KeySlotGuard`
  still decremented.
- Counter decrement on every exit kind: `done`, `retry`, `dead`, handler
  timeout, handler panic.
- No-seed restart: leave `running` ghost rows, start a fresh Worker, assert it
  claims up to `limit` immediately (counter starts at 0).
- `headroom`-map completeness (`len == concurrency_limits.len()`, including
  when all keys are saturated → non-empty `$5`).
- Claim ordering: two same-key jobs claimed in `run_at` order.
- The `pgwq.claim` saturation tracing event fires (extend the existing
  tracing-events test).
- Multi-byte (CJK/Polish) key at exactly 128 chars accepted, 129 rejected, at
  all three layers (pusher, builder, DB CHECK).
- `push_batch` with mixed/`None` keys — order preserved, NULLs round-trip.
- `concurrency_key` immutability — `mark_*` and reaper preserve it; a direct
  `UPDATE` of the column is rejected by the trigger.
- Empty-`concurrency_limits` worker uses the `claim_batch` SQL path.

Lockstep updates to existing tests:

- 17 `tests/*.rs` files call `push_batch` — element type change to
  `&[(T, Option<String>)]`.
- `claim_and_decode` (re-exported via `__test_exports`) gains the `headroom`
  parameter — callers `skip_locked_no_double_claim.rs`,
  `fencing_token_no_double_run.rs`, `batch_size_behavior.rs`,
  `codec_panic_marks_dead.rs`, `codec_decode_error_marks_dead.rs`.
- `migrator_schema.rs` — add `concurrency_key` to the required-columns list;
  add `jobs_claim_conc_idx` to the index assertions (the existing
  `three_partial_indexes_present` test asserts a closed set of 3 indexes by a
  name that becomes a lie — rename/relax it to 4); add new assertions for the
  `jobs_concurrency_key_len` constraint and the
  `assert_concurrency_key_immutable` trigger (no existing test covers those).

---

## Open risks for review

1. **Counter leak** — closed structurally by `KeySlotGuard` (§4); no DB seed,
   so no ghost-leak. Residual risk: a future refactor adding a second
   decrement; mitigated by `saturating_sub` + the leak test.
2. **Head-of-queue skew** — `eligible_unlimited` is `O(skipped)` index entries
   when a saturated key owns the oldest rows (§5). Moot for the all-keys-
   configured `rust_events` workload; documented, not fixed.
3. **Migration read+write stall** on a large unpurged table (§1) — bounded by
   `sqlx`'s no-`CONCURRENTLY` constraint; documented with magnitude.
4. **Transient over-count** from a reaper re-claim of a non-cooperative
   handler's row (§4) — safe direction (under-admit), self-heals; documented.
