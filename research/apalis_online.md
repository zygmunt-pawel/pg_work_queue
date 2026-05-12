# apalis online research — what people actually report

Date: 2026-05-12. Scope: `apalis` (org `apalis-dev`, prior `geofmureithi/apalis`),
`apalis-postgres` (separate repo `apalis-dev/apalis-postgres`, first published
2025-10-25 as alpha.1, current `1.0.0-rc.8` 2026-05-08).

This file is **evidence-first**. Every claim links to either a GitHub issue/PR,
a verbatim source-code line, or a blog post. Where PLAN.md asserts something we
couldn't independently verify, we say so.

---

## Repository topology (relevant for cross-references)

- **`apalis-dev/apalis`** — formerly `geofmureithi/apalis`. The repo redirects:
  `gh api /repos/geofmureithi/apalis` returns `full_name: apalis-dev/apalis`.
  Contains: `apalis-core`, `apalis-sql` (the *old* monolithic backend code),
  `apalis-workflow`, `apalis` umbrella.
- **`apalis-dev/apalis-postgres`** — new dedicated repo, created
  `2025-08-19T12:39:37Z`. Issues filed there are the freshest signal for
  Postgres-specific complaints. Has its own migrations, queries/, src/.
- **Old `apalis-sql` (in main apalis repo)** still hosts pre-split Postgres
  code (no longer the source of truth for new releases). The 0.7.x line for
  `apalis-postgres` still ships from the old repo; rc.1+ ships from the new
  one.

Architectural split is itself a red flag for design churn: there is no
`apalis-postgres` crate before 0.7.x — Postgres lived inside `apalis-sql`.
That migration happened in alpha.1 (`2025-10-25`). All 1.0.0-rc.* versions
target the new layout.

---

## Confirmed issues (with sources)

### C1. `pg_notify` trigger fires per-INSERT and is the channel referenced in the docs

- Migration `migrations/20251018165121_notify_run_at.sql`
  (https://github.com/apalis-dev/apalis-postgres/blob/main/migrations/20251018165121_notify_run_at.sql)
  — verbatim summary from raw file:
  > "Function: `apalis.notify_new_jobs()`. This trigger function executes
  > after new rows insert into `apalis.jobs`. … If [`run_at` ≤ now()], it
  > sends a PostgreSQL notification via `pg_notify()` on the channel
  > `'apalis::job::insert'`. … Trigger: `notify_workers` — fires after each
  > insert on the `apalis.jobs` table, invoking the notification function for
  > every new row."
- The earlier `migrations/20250722071207_improve_notify.sql` already
  established this pattern in v0.7.x — the 1018 migration is an iteration.
- Library side (main branch) uses `PgListener::connect_with(&pool); fetcher
  .listen("apalis::job::insert").await.unwrap();` in `poll_with_notify`
  — sourced from `src/lib.rs`. `unwrap()` on connect failure is its own
  reliability concern.
- Postgres' own docs are explicit (and Recall.ai documents this in detail):
  `NOTIFY` issued inside a transaction takes
  `AccessExclusiveLock on object 0 of class 1262 of database 0` at commit
  time, serializing **all commits in the database**, not just the table.
  Sources:
  - Recall.ai post-mortem: https://www.recall.ai/blog/postgres-listen-notify-does-not-scale
    (verbatim quote: "When a `NOTIFY` query is issued during a transaction, it
    acquires a global lock on the _entire database_ during the commit phase
    of the transaction, effectively serializing all commits.")
  - HN discussion: https://news.ycombinator.com/item?id=44490510 (none of the
    HN commenters identified apalis as the source — Recall.ai's blog post is
    library-agnostic).

**Status**: the SQL trigger exists in `apalis-postgres` exactly as PLAN.md
describes. The blast-radius claim ("global commit lock") is independently
documented by Postgres core and re-confirmed by Recall.ai. **The attribution
to apalis specifically is PLAN.md's own — Recall's blog never names apalis or
Rust**, see C2 below.

### C2. Recall.ai outage attribution — "March 2025, 3 outages, 1-day migration"

Verbatim from Recall.ai blog
(https://www.recall.ai/blog/postgres-listen-notify-does-not-scale):

> "Between the dates 2025-03-19 to 2025-03-22 our core Postgres database
> experienced three periods of downtime."

> "Given we had only a single (but critical) codepath relying on it, the
> migration took under a day to ship."

So the *factual* parts of PLAN claim 2 (three outages, sub-1-day migration)
are accurate to the source. **However: Recall.ai never names apalis. They
don't say what language/framework they use; nothing in the post is
Rust-specific.** They describe a single codepath that called `NOTIFY` for
"a running bot config update," not a job queue. The HN thread (top discussion
of this blog) does not mention apalis either. So:

- The Postgres `AccessExclusiveLock` mechanism is **objectively documented**.
- The behavioral risk for write-heavy workloads is **real and confirmed**.
- The "Recall.ai migrated off apalis" framing in PLAN.md is **inference, not
  documented fact**. Avoid presenting it as something Recall claimed.

### C3. `PgPollFetcher` hardcoded backoff that ignores `Config.poll_interval`

- File: `apalis-postgres/src/fetcher.rs` at tags `v1.0.0-rc.7` and `v1.0.0-rc.8`.
- Direct quote from `next_backoff` (verbatim, lines ~147-150 of rc.7):

```
fn next_backoff(&self, current: Duration) -> Duration {
    let doubled = current * 2;
    std::cmp::min(doubled, Duration::from_secs(60 * 5))
}
```

- Initial backoff is hardcoded `Duration::from_secs(1)`. Max is hardcoded
  `Duration::from_secs(60 * 5)` (5 minutes).
- `Config::with_poll_interval(StrategyBuilder::…)` exists, stores the
  strategy on `self.config`, and `self.config` is passed by reference into
  `PgPollFetcher::new(&self.pool, &self.config, worker)` — but the strategy
  is **not consulted by `next_backoff`**. The fetcher only reads
  `config.queue()` and `config.buffer_size()` from the config.
- The presence of `apalis-core::backend::poll_strategy::{MultiStrategy,
  IntervalStrategy, BackoffStrategy, …}` in the parent crate suggests this
  was intended to be pluggable; the wiring is just incomplete.
- PLAN.md's "confirmed dead-code in rc.7 and rc.8" claim is **confirmed**.

### C4. `ack` is UPDATE, not DELETE — done rows remain in the queue table

- `apalis-postgres/queries/task/ack.sql` (verbatim from main):

```sql
UPDATE
    apalis.jobs
SET
    status = $4,
    attempts = $2,
    last_result = $3,
    done_at = NOW()
WHERE
    id = $1
    AND lock_by = $5
```

- The status set on success is `Done` (parameter `$4`). The row stays in
  `apalis.jobs`. Without explicit retention/purge tooling, the table grows
  unboundedly. This is the source of the `purge_apalis_done_jobs` cron that
  PLAN.md mentions outbox users had to add.

### C5. Schema migration coexistence is fragile — `_sqlx_migrations` table conflict

- `apalis-postgres` issue #64
  (https://github.com/apalis-dev/apalis-postgres/issues/64): "When using
  `PostgresStorage::setup()` alongside application-level sqlx migrations on
  the same database, the two migrators share the same `_sqlx_migrations`
  table. Apalis's internal migrator only knows its own 2 migrations, so when
  it encounters user-defined migration entries in the table, it panics
  with: `migration XXXXXXXXXXXXXXX was previously applied but is missing in
  the resolved migrations`."
- The fix the user discovered themselves: call
  `PostgresStorage::migrations().set_ignore_missing(true).run(con)` manually
  instead of `setup()`. Library does not expose a knob for this in `setup()`.
- Apalis `geofmureithi/apalis#439` is the same problem from before the split.
  See https://github.com/geofmureithi/apalis/issues/439

### C6. Schema migration re-execution failure (`CREATE SCHEMA` without `IF NOT EXISTS`)

- Migration `20220530084123_jobs_workers.sql` creates the `apalis` schema
  without `IF NOT EXISTS`. Apalis issue #588
  (https://github.com/geofmureithi/apalis/pull/588) was a PR to fix this and
  was **rejected** ("Unfortunately this PR will not make it because it would
  break other peoples migrations. You cannot modify migrations, you need to
  add a new one.") — Geoffrey closed it. The PR author replied: "I'm not sure
  how this could be fixed via a new migration since it's a create statement
  in the old migration."
- `apalis-postgres#45` is the same complaint
  (https://github.com/apalis-dev/apalis-postgres/issues/45); the maintainer's
  response was hostile ("Please do not AI generate issues, This is not a
  valid issue if you are actually using this library"), and closed without
  fixing.

### C7. Postgres `done_at` column type mismatch

- `geofmureithi/apalis#539`: `column "done_at" is of type timestamp with
  time zone but expression is of type bigint`. Triggered by
  `task.parts.context.set_status(State::Killed); job_storage.update(task)`.
- Fixed in PR #561 — but only after the user dug into the codebase. The
  underlying issue: SQL backend functions sometimes return `i64` for
  timestamps, the column is `timestamptz`. The fix used `to_timestamp`
  conversions, leaving a comment that the Rust-side types should themselves
  be `DateTime` not `i64`.

### C8. `list_jobs()` regression — `OFFSET` bound as text

- `geofmureithi/apalis#528`: `argument of OFFSET must be type bigint, not
  type text`. Code: `.bind(((page - 1) * 10).to_string())`. Was actively
  affecting users who migrated from Redis to Postgres. Fixed in PR #524.

### C9. Postgres scheduled jobs run immediately due to missing parentheses

- `geofmureithi/apalis#523`: SQL function had
  `WHERE status='Pending' OR status='Failed' AND attempts < max_attempts
  AND run_at < now()`. Operator precedence makes this evaluate as
  `Pending OR (Failed AND … AND run_at)`, so any Pending job is picked up
  regardless of `run_at`. Scheduled jobs fire immediately.
- Fixed in migration `20250223193249_fix_get_jobs_conditional.sql` (now lives
  in `apalis-postgres/migrations/`). This was in production for at least one
  release — `0.7.0` cycle.

### C10. Worker dispatch is by `job_type`, not `worker_id` — tests / multi-worker hazard

- `apalis-postgres#60`: "When running integration tests in parallel, each
  test creates unique workers (via `Uuid::new_v4()`) and waits for its jobs
  to finish by polling `lock_by IN (worker_a, worker_b)`. However, apalis
  dispatches jobs based on `job_type`, not `worker_id`. … 2 out of 6
  integration tests always fail with 'worker did not finish in time' —
  jobs were already processed by a different worker."
- Maintainer recommended: use `--test-threads=1` or unique `job_type` per
  test. No library fix planned. This **is** the correct behavior for a
  shared queue but it surprised the user and there is no escape hatch
  ("lock_job_by_worker_id(true)").

### C11. `catch_panic` does not compile against `PostgresStorage`

- `geofmureithi/apalis#642` (against 1.0.0-beta.1): `catch_panic()`
  works with `MemoryStorage` but fails to compile with `PostgresStorage`.
  Maintainer confirmed bug in `CatchPanicService` — missing generic
  parameter `TaskId`. Quote: "yeah it is a bug: `impl<S, Req, Res, Ctx, F,
  PanicErr> Service<Task<Req, Ctx>> for CatchPanicService<S, F>` should be
  `impl<S, Req, Res, Ctx, F, PanicErr, TaskId> Service<Task<Req, Ctx,
  TaskId>> for …`". The bug shipped in beta.1. Not clear if fixed in rc.8.

### C12. Crashed-worker recovery / orphan jobs (Redis side, but pattern same)

- `geofmureithi/apalis#504` (Redis): "Long running task in the middle of
  running. Process goes down and restarts. … When the task processor comes
  back up, it will never run that task again, despite it never finishing."
- Maintainer comment: "I can confirm this is a bug caused by the
  `reenqueue_scheduled` approach. … The problem is with `reenqueue_orphaned`
  code, it checks for expired consumers (workers), but if you use the same
  worker id, then it will never expire. The workaround is to use unique
  worker id each time you launch your consumer."
- For Postgres, recovery is via `reenqueue_orphaned_after` (default 5 min)
  via `reenqueue_orphaned_stream`. The Redis-flavored bug above explains why
  PLAN.md's "fresh ULID per push" workaround was the right call: identity
  drift across restarts is a known cross-backend hazard.

### C13. `reenqueue_orphaned_after` makes long-running jobs duplicate

- `geofmureithi/apalis#530`: User running long polling loops on MQTT — apalis
  considered the workers orphaned after 5 min and re-enqueued duplicates.
  User had to set the value to >100 years to disable. No `Option<Duration>`
  knob. Maintainer's reply blamed the user's worker pattern.

### C14. Retry semantics broken historically

- `geofmureithi/apalis#372`: "v0.6.0 job failures go into a continuous loop
  regardless of # of retries. Using sqlite and for the given job, the
  attempts column is zero."
- `geofmureithi/apalis#494`: "Attempts are never increased [sqlite]".
- `geofmureithi/apalis#510` (0.6.4): "endless retries of failures even
  without retries configuration" — explicitly without a retry layer. User
  asked "How do retries work exactly? I feel lack of documentation (even if
  it's a bug) — especially storage vs layer retries."
- The 0.7.x cycle saw PR #498 (generic retry persist check), PR #507
  (`reenqueue oprphaned before starting streaming`), PR #512 (retry layer
  integration with task handling) — a multi-PR push that suggests retry was
  not solved by a single fix.

### C15. `apalis-sql` compilation requires live database (sqlx macros)

- `apalis-postgres#24`: `cargo build` fails without `DATABASE_URL` set,
  because `query_file!` macro tries to validate against a live DB.
  `SQLX_OFFLINE=true` also fails because cached query data is missing.
- Was fixed on `main` per maintainer, but indicates the published crate's
  build was broken at beta.2. Build-time DB dependency is non-trivial
  friction for downstream users.

### C16. `0.7.x` ergonomics: `setup()` does nothing for `SharedPostgresStorage`

- `apalis-postgres#34`: "When following the example using
  `SharedPostgresStorage`, the database is not initialized with migrations,
  as is performed when using `PostgresStorage::setup(&pool).await.unwrap();`."
- "Nice catch. Will fix this ASAP" — but the bug shipped to beta.3.

### C17. `apalis-postgres` 1.0 changed payload column type silently (`JSONB` → `BYTEA`)

- `apalis-postgres#19`: "I've noticed that in `1.0.0-beta1` column
  `apalis.jobs.job` has `BYTEA` type (aka binary), while in version `0.7.4`
  the messages were encoded in plain JSON. I am curious what was a driver
  for this change? In my opinion generally storing payload as plain JSON
  may significantly simplify debugging."
- Maintainer accepted the rationale (bincode/msgpack support) but the user
  was clearly surprised. Anyone with admin tooling that queried `job` as
  JSON broke silently.

### C18. Missing primary keys on `apalis.jobs` and `apalis.workers`

- `apalis-postgres#36`: tables had indexes but no PRIMARY KEY. Diesel
  tooling broke. Maintainer's reply: "This should be considered a bug. Not
  sure how I missed this." Fixed in rc.1, see
  `20251225090252_include_primary_keys.sql`.

### C19. `RetryAfter` duration handling

- Inspected `apalis-postgres/src/ack.rs` at rc.7. The `calculate_status`
  function returns only `Status::Done` / `Status::Killed` / `Status::Failed`.
  No branch references `ChannelError::RetryAfter` or honors a backoff
  duration passed by the handler.
- PLAN.md's claim 4 ("ack layer treats `RetryAfter` as plain `Transient` —
  duration not honored") is **consistent with the source we read**, though
  the variant might be in apalis-core / handler-side code. We couldn't find
  a path where the duration is read back into the SQL update.

---

## Anecdotal / unverified claims

- **"Apalis migrated three times: from `geofmureithi-zz` to `geofmureithi` to
  `apalis-dev`."** The redirect from `geofmureithi/apalis` to
  `apalis-dev/apalis` is real (`gh api /repos/geofmureithi/apalis` returns
  `full_name: apalis-dev/apalis`). A `geofmureithi-zz/apalis` repo exists
  (Actix+Redis original) — see search result. So the lineage is real but
  the "three migrations" framing is anecdote.
- **"Multi-worker parallelism is weak."** `geofmureithi/apalis#589` —
  closed without a definitive fix conversation in the public summary. Need
  to read full thread to characterize. Not a directly actionable bug for
  pg_work_queue.
- **PLAN.md claim 6: `LockTaskLayer`, `AcknowledgeLayer`, `PgAck`,
  `initial_heartbeat`, `keep_alive_stream`, `reenqueue_orphaned_stream` are
  `pub(crate)`.** This is **factually incorrect for rc.7 and main**. Direct
  source reads:
  - `src/lib.rs` (rc.7): `pub use crate::{ack::{LockTaskLayer, PgAck},
    fetcher::{PgFetcher, PgPollFetcher}, queries::{keep_alive::
    {initial_heartbeat, keep_alive_stream}, reenqueue_orphaned::
    reenqueue_orphaned_stream}, sink::PgSink};` — all `pub`.
  - `src/queries/keep_alive.rs` (rc.7): all three functions are `pub`.
  - `src/queries/reenqueue_orphaned.rs` (rc.7): `pub fn`.

  PLAN.md may be looking at an older alpha or has misread the source.
  `AcknowledgeLayer` (note the spelling vs `LockTaskLayer`) is the one
  type that is **not** re-exported in the public surface — but it's not
  in the same path either (lives in `apalis-core` if it exists). The other
  five names are publicly exposed. **Recommend removing claim 6 from
  PLAN.md or rewriting it with the correct fact**: "the API surface is
  exposed but not stable; re-implementing them as part of a custom
  Backend is still painful because the abstractions don't line up with a
  poll-only worker model".

- **PLAN.md claim about Recall.ai outage being caused by apalis.** As noted
  in C2: not documented anywhere. Recall.ai's article is library-agnostic
  and never mentions Rust, apalis, or any specific job-queue library.

- **"Double retry budget" claim 5.** Plausible (apalis has both
  `RetryPolicy::retries(N)` tower layer and `max_attempts` SQL column), but
  we didn't find an issue thread documenting users hitting this. It's a
  legitimate design concern; framing it as a known apalis bug would
  overreach without an issue link.

---

## What users want instead

Search results surfaced these as Postgres-on-Rust alternatives users
reach for. None of these are emerging as a clear successor, but they show
the design space:

- **`graphile_worker_rs`** — Rust port of the JS graphile-worker. PostgreSQL
  job queue with type-safe SQL. https://github.com/leo91000/graphile_worker_rs
- **`sqlxmq`** — sqlx-native job queue, PostgreSQL only. Smaller scope.
  https://docs.rs/sqlxmq
- **`underway`** — durable step functions on Postgres. Different abstraction
  level (workflow > job). https://github.com/maxcountryman/underway
- **`fang`** — Postgres/SQLite/MySQL, async + threaded workers, cron.
  https://github.com/ayrat555/fang
- **`effectum`** — SQLite-based job queue, currently library-embeddable.
  https://github.com/dimfeld/effectum
- **`pgmq`** — Postgres extension (not pure-Rust queue), AWS SQS-like.
  https://github.com/pgmq/pgmq. Apalis even has an `apalis-pgmq` adapter
  living at `apalis-dev/apalis-pgmq` — interesting that apalis itself
  acknowledges pgmq as an alternative storage layer.
- **Custom implementation** — Multiple blog posts ("Implementing a Postgres
  job queue in less than an hour", "How to build a job queue with Rust and
  PostgreSQL" — Kerkour) advocate writing the ~100-line `FOR UPDATE SKIP
  LOCKED` loop yourself instead of taking a dependency.

Recurring criticism across these projects' README/issue threads:
- Apalis is "trying to be too many things" (Redis + Postgres + SQLite +
  MySQL + cron + workflow + dashboard). Each backend gets less attention.
- Multi-backend abstraction leaks: Postgres-specific concerns (LISTEN/NOTIFY,
  `FOR UPDATE SKIP LOCKED` semantics, lease timeouts) are wrapped in
  trait abstractions that aren't a good fit.
- Retry semantics are split between a tower layer and SQL columns; users
  expect one source of truth.

No public blog post or HN thread we found explicitly says "we migrated
off apalis." The criticism is mostly issue threads on the apalis repos
themselves.

---

## Notable PRs / issues to read for context

Issues (closed, with substantive technical detail):

- `geofmureithi/apalis#523` — scheduled jobs ignored due to SQL precedence
  bug (the operator-precedence trap PLAN.md should test for).
- `geofmureithi/apalis#528` — `OFFSET` bind regression. Quick read.
- `geofmureithi/apalis#530` — `reenqueue_orphaned_after` interaction with
  long-running jobs.
- `geofmureithi/apalis#539` — `done_at` timestamptz vs i64 type mismatch
  (cross-backend `i64` time representations).
- `geofmureithi/apalis#588` (PR, rejected) — `CREATE SCHEMA IF NOT EXISTS`
  migration. Maintainer's "you cannot modify migrations" reply is a
  cautionary tale for our own migration discipline.
- `geofmureithi/apalis#504` — orphan jobs and `reenqueue_orphaned` for
  Redis. Same general failure mode pattern as Postgres.
- `geofmureithi/apalis#372`, `#494`, `#510` — chain of retry-counter bugs
  across 0.6.x.
- `geofmureithi/apalis#642` — `catch_panic` doesn't compile against
  `PostgresStorage` in 1.0.0-beta.1.
- `apalis-postgres#17` — worker crashes after `lock_by_id` SQL files
  conflict (duplicate file in different paths confused sqlx). Real
  packaging hazard.
- `apalis-postgres#19` — silent breaking change (JSONB → BYTEA).
- `apalis-postgres#24` — build requires live DB. Affects all downstream.
- `apalis-postgres#36` — missing PRIMARY KEY in tables.
- `apalis-postgres#60` — multi-worker dispatch surprise.
- `apalis-postgres#64` — `_sqlx_migrations` table conflict (the user's
  experience PLAN.md describes about co-located migrations).

PRs:
- `geofmureithi/apalis#543` — drop conflicting `apalis.push_job` function.
  Illustrates "API broken between minor versions if you bypass the Rust
  surface and call SQL directly."
- `geofmureithi/apalis#524` — fix for #528 and `get_jobs` status
  conditional, but also restructures internals in a way that broke other
  users (see #522).

Relevant source files to read once (in `apalis-dev/apalis-postgres`):
- `src/fetcher.rs` — `PgPollFetcher::next_backoff` hardcoded; state
  machine: `Ready → Fetch → Delay → Buffered`. Demonstrates *why*
  PLAN.md's "deterministic ticker" decision is simpler and saner.
- `src/ack.rs` (rc.7) — `calculate_status` only emits `Done/Killed/Failed`,
  no RetryAfter duration honoring.
- `queries/task/ack.sql` — UPDATE not DELETE.
- `migrations/20251018165121_notify_run_at.sql` — the `pg_notify` trigger.
- `migrations/20250223193249_fix_get_jobs_conditional.sql` — the
  `(Pending OR Failed) AND run_at` precedence fix.

---

## Verdict for PLAN.md

| PLAN claim | Verdict | Action |
|---|---|---|
| 1. PgPollFetcher hardcodes 1s→5min, ignores Config strategy | **Confirmed** in rc.7 & rc.8 source | Keep as motivation. |
| 2. `pg_notify` trigger emits per-INSERT; AccessExclusiveLock at commit | **Confirmed** (migration + Postgres core docs + Recall) | Keep. |
| 2b. Recall.ai had 3 outages and migrated in 1 day **because of apalis** | **Partially**: 3 outages and <1-day migration are documented at Recall. **Attribution to apalis is not** — Recall never names apalis or Rust. | Soften phrasing: "Recall.ai's documented outage demonstrates the failure mode that apalis' notify trigger exposes us to." |
| 3. ack=UPDATE not DELETE; orphan Done rows; PK collisions | **Confirmed** (queries/task/ack.sql) | Keep. |
| 4. `RetryAfter` duration not honored | **Consistent** with source but not 100% provable from ack.rs alone | Keep as design rationale; flag as "we observed this in ack.rs; full proof requires tracing through apalis-core". |
| 5. Double retry budget (`RetryPolicy::retries(N)` + `max_attempts`) | **Plausible / no public bug report** | Keep but soften: "we are choosing a single source of truth to avoid the kind of split that has produced bugs like #372 #494 #510". |
| 6. Layer/internals are `pub(crate)` | **FALSE** — all the named items are `pub` in rc.7 & main | **Remove or rewrite**. The honest version is: "the layered Backend trait surface is complex enough that writing a custom impl duplicates ~300 lines even though items are public." |

---

## One-liner takeaway

apalis-postgres is a moving target — separated from `apalis-sql` only in
Oct 2025, currently mid-`1.0.0-rc.*` cycle (rc.1 in Dec 2025, rc.8 in May
2026). The features that motivate pg_work_queue's existence (deterministic
polling, no NOTIFY, DELETE-on-ack, single source of retry truth) are *real
weak spots in apalis-postgres backed by source-level evidence and user
issues* — but a couple of PLAN.md's framings (Recall causal claim,
pub(crate) claim) overreach the public evidence and should be tightened
before they become quotable marketing material.
