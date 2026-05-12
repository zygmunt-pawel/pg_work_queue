# pg_work_queue — plan i przemyślenia

> Status: design draft, pre-implementation. Konwencja po polsku
> (kod/identyfikatory po angielsku) — zgodnie z `rust_event_outbox`.

## Co to jest

Minimalna, generyczna biblioteka Rust do **polling-based Postgres job
queue**. Jeden user-controlled knob: `poll_interval(Duration)`. Worker
loop deterministycznie polluje tabelę co N ms, niezależnie od
hot/idle state. Brak hidden exponential backoff, brak `LISTEN/NOTIFY`
(unika global commit-lock contention), brak rc-release bugów typu
"config field stored but never read".

Pomyślana jako **lighter alternatywa** dla `apalis-postgres` w
przypadkach gdzie user chce pełną kontrolę nad cadence i nie potrzebuje
worker dashboard / multi-backend abstraction.

Pierwszy konsument: `rust_event_outbox` (v0.6+), który dropuje apalis
całkowicie i używa `pg_work_queue` jako warstwy worker pool.

## Motywacja — dlaczego nie apalis

`apalis-postgres` (1.0.0-rc.7 i rc.8 stan na 2026-05) ma serię realnych
ograniczeń, które wynikiem audytu w `rust_event_outbox`:

1. **`PgPollFetcher::next_backoff` hardcoduje `1s → 5min` exponential
   cap.** `Config::with_poll_interval(MultiStrategy)` jest zapisywany do
   `self.config` (`apalis-postgres-1.0.0-rc.7/src/lib.rs:87`), ale
   fetcher go **nigdy nie czyta**
   (`apalis-postgres-1.0.0-rc.7/src/fetcher.rs:84,160-163`). To
   confirmed dead-code w rc.7 i rc.8.
2. **Trigger `pg_notify('apalis::job::insert', ...)` emitowany per
   INSERT** (apalis migracja `20251018165121_notify_run_at.sql`)
   bierze `AccessExclusiveLock` na cały klaster przy commit. Recall.ai
   miał 3 outage'e w marcu 2025 z tego powodu i wymigrowali w 1 dzień.
   Outbox jest write-heavy z natury → ryzyko serializacji.
3. **`ack=UPDATE` zamiast DELETE** w rc.7 (`queries/task/ack.sql`).
   Konsekwencja w `rust_event_outbox`: reaper musi pushować **fresh
   ULID per push** żeby uniknąć PK collision z orphan `Done` rows.
   Plus userzy muszą cron'ować `purge_apalis_done_jobs`.
4. **`ChannelError::RetryAfter(_, duration)` — `duration` nie jest
   honored przez ack layer.** Treat as plain `Transient`.
5. **Double retry budget** — apalis `RetryPolicy::retries(N)` + nasz
   DB-side `max_attempts` counter. Library musi wymusić DB-side jako
   authoritative, apalis-side jest noise.
6. **`LockTaskLayer`, `AcknowledgeLayer`, `PgAck`, `initial_heartbeat`,
   `keep_alive_stream`, `reenqueue_orphaned_stream` są
   `pub(crate)`** — niemożliwe napisać custom `Backend` impl bez
   duplikacji ~300 linii kodu.

Wynik audytu: każdy z tych warts wymagałby workaroundu / hacka w
`rust_event_outbox`. `pg_work_queue` eliminuje wszystkie naraz przez
nie używanie apalis w ogóle.

## Co `pg_work_queue` świadomie NIE robi (anti-features)

- **Brak `LISTEN/NOTIFY`.** Globalny `AccessExclusiveLock` przy commit
  to nieakceptowalne ryzyko dla write-heavy workload. Zawsze poll.
- **Brak adaptive / exponential backoff.** Cadence jest deterministyczna
  (`poll_interval`). User chce 500ms → poll co 500ms. Trade-off
  load-vs-latency robi user explicit, nie biblioteka.
- **Brak multi-backend abstraction.** Postgres-only by design. Jeśli
  ktoś chce Redis — niech używa innej biblioteki.
- **Brak worker dashboard / GUI / metrics endpoint.** Observability
  przez `tracing` spans + DB queries po queue table. Userzy budują
  własne dashboards jeśli chcą.
- **Brak typed retry strategies.** Handler zwraca `Outcome::Retry { in:
  Option<Duration> }` lub `Outcome::Dead(reason)`. Library decyduje
  retry-vs-dead po `attempts < max_attempts`. Backoff dla retry
  to czas oczekiwania w `awaiting_retry` zanim wiersz może być re-claim
  przez worker — domyślnie 0 (czyli już-następny poll cycle).
- **Brak cross-worker priorities, fairness, multi-tenant isolation
  beyond `queue` column.** Trzymamy się prostoty. Jeden queue name =
  jeden FIFO stream tasks.

## Public API — sketch

```rust
use pg_work_queue::{Worker, Outcome, JobContext, Pusher};
use serde::{Serialize, Deserialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct EmailTask {
    to: String,
    body: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;

    // Migracja schemy (idempotent).
    pg_work_queue::migrator().run(&pool).await?;

    let worker = Worker::builder(pool.clone())
        .queue("email_send")
        .poll_interval(Duration::from_millis(500))
        .concurrency(16)
        .max_attempts(5)
        .lease_timeout(Duration::from_secs(300))
        .handler(|task: EmailTask, ctx: JobContext| async move {
            tracing::info!(to = %task.to, attempt = ctx.attempt, "sending");
            send_smtp(&task).await
                .map(|_| Outcome::Done)
                .unwrap_or_else(|e| Outcome::Retry { reason: e.to_string(), in_: None })
        })
        .build()?;

    let handle = worker.start();  // spawns tokio tasks for poll loop + reaper

    // Push side (in your own transaction):
    let mut tx = pool.begin().await?;
    Pusher::new("email_send")
        .push(&mut tx, &EmailTask { to: "x@y".into(), body: "hi".into() })
        .await?;
    tx.commit().await?;

    // Graceful shutdown:
    tokio::signal::ctrl_c().await?;
    handle.shutdown(Duration::from_secs(10)).await?;  // returns when drained or timed out
    Ok(())
}
```

### Builder knobs (wszystkie z observable behavior, każdy z testem)

| Knob | Default | Effect |
|---|---|---|
| `queue(&str)` | required | nazwa queue (PG column lookup) |
| `poll_interval(Duration)` | 1s | **deterministyczny** cycle (nie backoff) |
| `concurrency(usize)` | num_cpus | max parallel handlers per worker |
| `max_attempts(u32)` | 3 | przed dead-letter |
| `lease_timeout(Duration)` | 5min | po tym czasie stale-running wiersz jest reap'owany do awaiting_retry |
| `reaper_interval(Duration)` | 60s | jak często sprawdzać stale-running |
| `batch_size(usize)` | 10 | ile wierszy claim'ować per poll |

Każdy z tych ma **integracyjny test który sprawdza observable behavior
przy dwóch różnych wartościach** (np. `poll_interval(100ms)` vs
`poll_interval(500ms)` → różnica latency mierzalna).

## Schema (DB layout)

Schema `pg_work_queue` (namespace separation):

```sql
CREATE SCHEMA pg_work_queue;

CREATE TYPE pg_work_queue.job_status AS ENUM (
    'queued', 'running', 'awaiting_retry', 'done', 'dead'
);

CREATE TABLE pg_work_queue.jobs (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    public_id          UUID NOT NULL UNIQUE,
    queue              TEXT COLLATE "C" NOT NULL,
    payload            BYTEA NOT NULL,
    status             pg_work_queue.job_status NOT NULL DEFAULT 'queued',
    attempts           SMALLINT NOT NULL DEFAULT 0,
    last_error         TEXT,
    last_attempted_at  TIMESTAMPTZ,
    first_attempted_at TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    run_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT jobs_queue_nonempty CHECK (length(queue) > 0),
    CONSTRAINT jobs_attempts_nonneg CHECK (attempts >= 0),
    CONSTRAINT jobs_temporal CHECK (
        (first_attempted_at IS NULL OR first_attempted_at >= created_at)
        AND (last_attempted_at IS NULL OR last_attempted_at >= COALESCE(first_attempted_at, created_at))
        AND (finished_at IS NULL OR finished_at >= COALESCE(last_attempted_at, created_at))
        AND updated_at >= created_at
        AND run_at >= created_at
    ),
    CONSTRAINT jobs_status_invariants CHECK (
        (status = 'queued'
            AND attempts = 0
            AND last_attempted_at IS NULL
            AND finished_at IS NULL)
        OR (status IN ('running', 'awaiting_retry')
            AND attempts > 0
            AND last_attempted_at IS NOT NULL
            AND finished_at IS NULL)
        OR (status IN ('done', 'dead')
            AND finished_at IS NOT NULL)
    )
);

-- Hot path: poll claim
CREATE INDEX jobs_claim_idx
    ON pg_work_queue.jobs (queue, run_at, id)
    WHERE status = 'queued' OR status = 'awaiting_retry';

-- Reaper hot path
CREATE INDEX jobs_reap_idx
    ON pg_work_queue.jobs (last_attempted_at)
    WHERE status = 'running';

-- Terminal cleanup
CREATE INDEX jobs_terminal_idx
    ON pg_work_queue.jobs (finished_at)
    WHERE status IN ('done', 'dead');
```

Decyzje względem `rust_event_outbox` lessons:
- `BIGINT IDENTITY` internal PK + `public_id UUID` external (te same powody:
  compact FK/index, sortable wire format).
- Named CHECK constraints — defense-in-depth przeciw buggy code.
- Partial indexes na hot paths (claim, reap, terminal).
- `COLLATE "C"` na `queue` (byte-exact `=` lookup).
- ENUM zamiast CHECK constraint na status (rygorystyczniej).
- Schema namespacing (`pg_work_queue.*`) — kolizja-safe wzgl. user
  tables i innych libów.
- `run_at` — pozwala scheduled jobs (push z `run_at = now() + 5min`),
  worker claim wymaga `run_at <= now()`.
- `payload BYTEA` (nie `JSONB`) — biblioteka nie wnika w format, user
  decyduje (typowo serde_json::to_vec, można też cbor / msgpack).

## Internal architecture

```
                            ┌──────────────────┐
                            │   Pusher::push   │  (in user's tx)
                            │  INSERT pending  │
                            └──────────────────┘
                                     │
                            ┌────────▼────────┐
                            │ pg_work_queue.  │
                            │      jobs       │
                            └────────┬────────┘
                                     │
                ┌────────────────────┼────────────────────┐
                │                    │                    │
        ┌───────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
        │  Poll Loop     │  │  Reaper Loop    │  │  (your worker   │
        │  every N ms    │  │  every M sec    │  │   pool, JoinSet)│
        │  SELECT FOR    │  │  flip stale     │  │                 │
        │  UPDATE SKIP   │  │  running →      │  │                 │
        │  LOCKED        │  │  awaiting_retry │  │                 │
        └───────┬────────┘  └─────────────────┘  └─────────────────┘
                │
                │ Vec<Job<T>>
                ▼
        ┌──────────────────┐
        │ tokio::JoinSet   │
        │ spawn handler    │
        │ per claimed row  │
        │ (max concurrency)│
        └─────────┬────────┘
                  │
                  ▼
         handler return → mark_done / mark_retry / mark_dead
```

### Poll loop (heart)

```rust
async fn poll_loop<T>(state: &WorkerState<T>) {
    let mut ticker = tokio::time::interval(state.poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            _ = state.shutdown.cancelled() => break,
        }

        // Wait until concurrency slot free.
        let permit = state.semaphore.clone().acquire_owned().await;
        if permit.is_err() { break; }  // semaphore closed = shutdown

        // Claim batch.
        let batch = claim_batch(&state.pool, &state.queue, state.batch_size).await;
        match batch {
            Ok(rows) => {
                for row in rows {
                    let permit = state.semaphore.clone().acquire_owned().await.unwrap();
                    state.tasks.spawn(handle_job(row, state.clone(), permit));
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "claim batch failed; will retry next tick");
            }
        }
    }
}
```

### `claim_batch` SQL

```sql
WITH claimed AS (
    SELECT id FROM pg_work_queue.jobs
    WHERE queue = $1
      AND status IN ('queued', 'awaiting_retry')
      AND run_at <= now()
    ORDER BY run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pg_work_queue.jobs j
SET status = 'running',
    attempts = j.attempts + 1,
    last_attempted_at = now(),
    first_attempted_at = COALESCE(j.first_attempted_at, now())
FROM claimed
WHERE j.id = claimed.id
RETURNING j.id, j.public_id, j.queue, j.payload, j.attempts, j.first_attempted_at;
```

To załatwia:
- Atomic claim (`FOR UPDATE SKIP LOCKED` — multi-worker safe).
- Inkrementacja attempts w tej samej query (jak `try_mark_running`
  w `rust_event_outbox`).
- Scheduled jobs (`run_at <= now()` — skip jobs z przyszłością).

### Reaper

```sql
WITH reaped AS (
    UPDATE pg_work_queue.jobs
    SET status = 'awaiting_retry',
        last_error = COALESCE(last_error, 'lease_expired')
    WHERE status = 'running'
      AND last_attempted_at < now() - $1::interval
    RETURNING id, attempts
)
SELECT count(*) FROM reaped;
```

Plus advisory-lock guard żeby multi-replica nie reapował równolegle
(jak w `rust_event_outbox::run_reap_tick`).

### Mark queries

```sql
-- mark_done
UPDATE pg_work_queue.jobs
SET status = 'done', finished_at = now(), last_error = NULL
WHERE id = $1 AND status = 'running'

-- mark_retry (po Err)
UPDATE pg_work_queue.jobs
SET status = 'awaiting_retry',
    last_error = $2,
    run_at = $3  -- now() + retry_delay if user supplied
WHERE id = $1 AND status = 'running'

-- mark_dead (Permanent error lub max_attempts exhausted)
UPDATE pg_work_queue.jobs
SET status = 'dead', finished_at = now(), last_error = $2
WHERE id = $1 AND status IN ('running', 'awaiting_retry')
```

Wszystkie zachowują `WHERE status = ...` guard żeby chronić przed race
między worker'em a reaper'em (lesson z `rust_event_outbox` v0.2).

## Shutdown semantics

`WorkerHandle::shutdown(timeout: Duration) -> Result<Stats, ShutdownError>`:

1. Cancel `state.shutdown` token → poll loop wychodzi natychmiast z
   tokio::select.
2. Drop semaphore permits acquire — nowe handler'y się nie spawn'ują.
3. Czekaj na `JoinSet::join_next` w pętli z `timeout`.
4. Po timeout: abort wszystkich pozostałych tasks, return Stats z
   `aborted_count`.

Reaper loop reaguje na ten sam `shutdown` token. Po cancel exit.

Bez duplikacji apalis Monitor — jedna metoda, jeden token, pełna
kontrola.

## Test strategy (TDD-first od początku)

### Behavioral tests (critical — żaden knob nie kompiluje bez)

Każdy public knob ma test który mierzy **observable behavior** przy
dwóch różnych wartościach, asercja na różnicę:

1. **`tests/poll_interval_behavior.rs`** — dispatch + measure pickup
   latency z `poll_interval(100ms)` vs `poll_interval(500ms)`. Average
   latency dla pierwszego < 200ms, dla drugiego ≥ 250ms i ≤ 700ms.
2. **`tests/concurrency_behavior.rs`** — `concurrency(1)` powoduje że
   N taski idą sekwencyjnie (każda > X ms), `concurrency(N)` że
   wykonują się parallel (całość ≤ X ms + epsilon).
3. **`tests/max_attempts_behavior.rs`** — handler zawsze fail
   `Transient`. Po `max_attempts(3)` row jest `dead`, po `max_attempts(5)`
   row jest `dead` po 5 próbach. Zliczamy attempts w DB.
4. **`tests/lease_timeout_behavior.rs`** — symulacja crashed worker
   (manual UPDATE status='running' z stale last_attempted_at), reaper
   z `lease_timeout(1s)` flipuje wcześniej niż z `lease_timeout(10s)`.
5. **`tests/batch_size_behavior.rs`** — push 100 jobs, `batch_size(10)`
   vs `batch_size(50)` daje różny shape claim batches (zliczamy via
   tracing instrumented).
6. **`tests/scheduled_run_at.rs`** — push z `run_at = now() + 2s`,
   worker nie pickupuje przed t+2s.

### Crash safety / correctness tests

7. **`tests/skip_locked_no_double_claim.rs`** — 2 workery równolegle
   pollują, push 100 jobs, suma claimed == 100 (no double-claim, no
   missed).
8. **`tests/stale_running_reaped.rs`** — analog do
   `rust_event_outbox::stale_running_reaper.rs`.
9. **`tests/shutdown_graceful.rs`** — `shutdown(5s)` czeka na drain;
   jeśli handler trwa > 5s, abort'owany; stats poprawne.
10. **`tests/shutdown_cancels_poll_loop.rs`** — mid-poll-sleep
    shutdown wychodzi natychmiast (nie czeka end of `poll_interval`).
11. **`tests/migrator_schema.rs`** — schema CREATE'd correctly,
    indexes, CHECK constraints, deny_update na finished rows (TBD?).

### No-DB / unit tests

12. **`tests/builder_validation.rs`** — config validation
    (`poll_interval == 0` rejected, `concurrency == 0` rejected, etc.)
13. **`tests/payload_codec.rs`** — round-trip serde + bytea.

### Anti-pattern guard

Nie dodawać testów które testują **identyczność struktury**
(np. `assert_eq!(builder.build().poll_interval, Duration::from_millis(500))`).
To było source bug'a w `rust_event_outbox` v0.4 (`handler_max_poll_backoff`
test sprawdzał `format!("{:?}", config.poll_strategy())` zamiast
real latency). Każdy test musi mierzyć **behavior** widoczny w DB lub
przez observable side-effect.

## Implementation phases

### Faza 0 — repo init

- `cargo init --lib` w `pg_work_queue/`.
- `Cargo.toml`: `sqlx 0.8` (postgres, runtime-tokio-rustls, uuid,
  chrono, json, macros, migrate), `tokio` (full), `tracing`, `serde`,
  `thiserror`, `async-trait`, `chrono`, `uuid` (v4 + v7 + serde),
  `anyhow`.
- `tokio-util` (sync feature dla `CancellationToken`).
- Dev: `testcontainers`, `testcontainers-modules` (postgres),
  `tracing-subscriber`.
- `migrations/20260513000000_v01_init.sql` z schemą wyżej.
- Skeleton `lib.rs`: `pub mod migrator; pub mod worker; pub mod
  pusher; pub mod codec;`.

### Faza 1 — push + migracja + manual claim (no worker yet)

- `Pusher::push<T: Serialize>(tx, payload, run_at)` → INSERT.
- `pg_work_queue::migrator()` re-export sqlx::Migrator.
- Manual test: push, SELECT z DB, verify status='queued'.
- Test: `migrator_schema.rs`.

### Faza 2 — claim_batch SQL + Job/JobContext types

- `claim_batch(pool, queue, batch_size, now)` → `Vec<JobRow>`.
- `pub struct Job<T>` + `pub struct JobContext` types.
- Codec generic: handler dostaje deserializowany `T`.
- Test: `skip_locked_no_double_claim.rs` (multi-worker).

### Faza 3 — single-shot worker (no loop, no concurrency)

- `Worker::tick_once(...)` — fetches batch, runs handlers sekwencyjnie,
  marks done/retry/dead.
- Test: end-to-end smoke (push, tick, assert delivered).

### Faza 4 — poll loop + concurrency

- `Worker::start()` → spawn poll loop + JoinSet.
- `CancellationToken` shutdown plumbing.
- Test: `poll_interval_behavior.rs`, `concurrency_behavior.rs`.

### Faza 5 — reaper

- Spawned alongside poll loop, advisory-lock guarded.
- Test: `stale_running_reaped.rs`, `lease_timeout_behavior.rs`.

### Faza 6 — retry semantics, mark_retry z `run_at`

- Handler zwraca `Outcome::Retry { in_: Option<Duration> }`.
- `mark_retry` ustawia `run_at = now() + in_` (lub natychmiast).
- Test: `max_attempts_behavior.rs`, `scheduled_run_at.rs`.

### Faza 7 — shutdown semantics

- `WorkerHandle::shutdown(timeout)`.
- Test: `shutdown_graceful.rs`, `shutdown_cancels_poll_loop.rs`.

### Faza 8 — purge / retention

- `purge_terminal(pool, ttl) -> u64` (analog `rust_event_outbox`).
- Test: jeden DB-level.

### Faza 9 — docs + README

- README z quick start (skopiowane z sketch wyżej).
- Doc comments na każdym public knob z "obserwable effect" wzmianką.
- Cargo.toml `description`, `repository`, `documentation`, `categories`,
  `keywords`.
- Możliwe: tag `v0.1.0`. Crates.io publish: TBD.

### Faza 10 — integracja z `rust_event_outbox`

- W `rust_event_outbox` v0.6:
  - Drop `apalis`, `apalis-postgres` z deps.
  - Dodaj `pg_work_queue = "0.1"` (lub path dep w workspace na start).
  - Drop `outbox.events.id` apalis_job_id columns? (TBD — wciąż
    użyteczne dla forensics).
  - W `dispatch_in_tx`: zamiast `apalis_postgres::sink::push_tasks`
    użyj `pg_work_queue::Pusher::push` z payload =
    `DeliveryJob { delivery_id: i64, mode: Handler|Channel }`.
  - W `outbox.rs::start_workers`: zamiast `WorkerBuilder` z apalis,
    użyj `pg_work_queue::Worker::builder` z naszym handler closure
    który calluje `handler_runner::run` / `channel_runner::run`.
  - Drop reaper (`spawn_stale_reaper`) — `pg_work_queue` ma własny.
    Nasza `outbox.*_deliveries` table nie potrzebuje już reaper'a, bo
    queue table to teraz `pg_work_queue.jobs`. Ale: deliveries z
    `status='running'` może wciąż się zacinać jeśli handler zawisł
    przed mark_sent po pg_work_queue ack. Wymaga przemyślenia —
    możliwie zachować outbox-side reaper jako defense-in-depth.
  - Drop `purge_apalis_done_jobs` (nieaktualne).
  - Drop `apalis_done_retention` z config.
  - Update wszystkie testy DB — `apalis.jobs` znika, zastąpione
    `pg_work_queue.jobs`.
  - Update `pg::run_all_migrations` — usuń apalis migrator, dodaj
    `pg_work_queue::migrator()`.
  - Bump 0.5.0 → 0.6.0, breaking change, fresh schema, dokumentacja.

## Open questions / decisions TBD

1. **MSRV** — Rust 2024 edition? (`rust_event_outbox` ma 1.88+). Pewnie
   też 1.88+ dla konsystencji.
2. **`payload BYTEA` vs `JSONB`** — BYTEA bardziej generic (user wybiera
   codec), JSONB pozwala filtering / inspection w SQL. Decyzja: BYTEA na
   start, JSONB później jeśli potrzebne (można dodać index na payload
   po stronie usera w jego migracji).
3. **Multi-tenant via `queue` column czy multi-table?** Decyzja: single
   table, `queue` column. Skala mniejszych liczby queue'ów (kilka,
   nie tysiące).
4. **Plugin layer architecture (tower middleware)?** Decyzja: NIE na
   start. Handler to prosta `async fn(T, JobContext) -> Outcome`.
   Jeśli ktoś chce retry/timeout/circuit-breaker layer, niech wrappuje
   handler ręcznie.
5. **Worker registration table (`pg_work_queue.workers`)?** Apalis ma —
   pozwala enumerate live workers, ich heartbeat. Decyzja: NIE na
   start. Reaper używa `last_attempted_at` jako proxy (nie worker
   liveness check). Można dodać później jako optional feature.
6. **`Outcome::Pause` / `Outcome::Abort`?** Niepotrzebne na start.
7. **Tracing span name conventions?** `pg_work_queue.poll_tick`,
   `pg_work_queue.claim_batch`, `pg_work_queue.job` (per handler
   invocation). Analog `outbox.dispatch`, `outbox.delivery` z
   `rust_event_outbox`.
8. **Apalis-style `Pusher` builder or simpler fn?** Decyzja: simple fn
   `pg_work_queue::push(&mut tx, "queue", &payload, run_at)`. Builder
   overkill dla jednorazowego call.
9. **PgBouncer transaction-pooling compatibility** — bo nie używamy
   `LISTEN/NOTIFY`, powinno być compatible. Sprawdzić w testach pod
   PgBouncer.
10. **Generic vs concrete payload type at Worker level** — `Worker<T>`
    gdzie `T: DeserializeOwned + Serialize` per queue. Handler closure
    dostaje typed `T`. Decyzja: tak, `Worker<T>` generic.
11. **Multi-queue worker** — jeden worker handle pollujący N kolejek? Albo
    user spawn'uje N worker handles, każdy per queue. Decyzja:
    one-queue-per-worker, prościej i czystsze. User spawn'uje multiple
    handles jeśli chce multi-queue.
12. **License** — MIT? (`rust_event_outbox` ma MIT). OK.

## Wpływ na `rust_event_outbox`

### Public API zmiany (v0.5 → v0.6)

- **`Outbox::start_workers(monitor)` → `Outbox::start_workers()`** —
  zwraca `OutboxHandle { shutdown(timeout) }` zamiast `apalis Monitor`.
  Breaking dla wszystkich konsumentów (1 — `apps/api` u nas).
- **`OutboxConfigBuilder` knoby zmiana**: dochodzi
  `handler_poll_interval(Duration)` + `channel_poll_interval(Duration)`
  (oba domyślnie 1s). Reszta (`max_attempts`, `concurrency`, retry
  backoff) **bez zmian semantycznie** — pg_work_queue obsługuje to
  samo.
- **Drop `apalis_done_retention` + `purge_apalis_done_jobs`** — orphan
  Done rows znikają wraz z apalis.
- **Drop `ApalisConfig` reexport** (już zrobiony w v0.5).
- **Wewnętrznie**: dispatch.rs używa `pg_work_queue::push` zamiast
  `apalis_postgres::sink::push_tasks`. Reaper w outbox.rs prawdopodobnie
  całkowicie usuwalny (pg_work_queue ma własny reaper dla queue jobs;
  outbox-side `*_deliveries.status='running'` lease może wymagać
  osobnego mechanizmu — TBD).

### Co zostaje bez zmian

- Schema `outbox.*` (events, dispatch_keys, handler_deliveries,
  channel_subscriptions, channel_deliveries) — wszystkie pola, CHECK
  constraints, indexes, FK relationships.
- Public API dispatch (`Outbox::dispatch<E: DomainEvent>(...)`),
  history (`HistoryApi`), subscriptions (`ChannelSubscriptionsApi`),
  channel impls (`Channel<C>`), handler trait (`EventHandler<E>`).
- Handler context (`HandlerContext.delivery_id` — wciąż public_id UUID).
- Retry semantics z perspektywy user'a (Transient/Permanent return
  values).
- Reaper threshold + interval defaults.

### Risk + mitigation

- **Wszystkie obecne 44 testy DB to safety net.** Po migracji każdy
  musi nadal przechodzić. Jeśli nie — regression, fixujemy przed
  release.
- **Behavioral test pickup-latency w pg_work_queue jest red-first**
  (TDD od początku). Jeśli pg_work_queue Wymyślnie nie działa, test
  to wyłapie zanim integrujemy z rust_event_outbox.

## Roadmap stages (dla nas)

1. **pg_work_queue v0.1.0** — Fazy 0-9 wyżej. ~2-3 dni roboty.
2. **rust_event_outbox v0.6.0** — Faza 10. ~1-2 dni roboty (mniej, bo
   pg_work_queue ma już test coverage, tylko integracja).
3. **(later) OSS publish** — crates.io publish dla pg_work_queue jeśli
   wartościowe. Może też GH README z demo + benchmark vs apalis-postgres.

## Anti-patterns z których wyciągnęliśmy lekcje (rust_event_outbox v0.4)

Te zasady są **hard rules** w pg_work_queue:

1. **Każdy public knob musi mieć integracyjny test który sprawdza
   observable effect przy dwóch różnych wartościach.** Nie test
   "config builds correctly" — test "behavior X przy A, behavior Y
   przy B, X ≠ Y".
2. **Nie wystawiaj knoba dopóki nie zweryfikowałeś że jest READ w
   hot-path.** `apalis Config::with_poll_interval` był zapisywany ale
   nigdy nie czytany. To było source bug'a `handler_max_poll_backoff`.
3. **Nie ufaj nazwie struct'a, dopóki nie przeczytałeś `Stream::poll_next`
   / `Future::poll` implementation.** apalis `PgPollFetcher` ma pole
   `config`, ale w poll_next używa hardcoded literal `Duration::from_secs(1)`.
4. **Test który passował od pierwszego compile'a to red flag.** TDD
   wymaga RED przed GREEN. Test który już-zielony testuje
   *istniejące zachowanie*, nie *required behavior*.
5. **Verify-before-completion przed każdym claim "fix done".** Cargo
   test passes ≠ fix works. Behavioral end-to-end z observable side
   effect to minimum.
