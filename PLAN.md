# pg_work_queue — plan i przemyślenia

> Status: design draft, pre-implementation. Konwencja po polsku
> (kod/identyfikatory po angielsku) — zgodnie z `rust_event_outbox`.
> Plan przeszedł 5 rund review (multi-agent + Opus + iteracyjne
> correctness reviews). Pełna ewolucja w git history; ten plik to
> single source of truth dla implementacji.

## Co to jest

Minimalna, generyczna biblioteka Rust do **polling-based Postgres job
queue**. Jeden user-controlled knob: `poll_interval(Duration)`. Worker
loop deterministycznie polluje tabelę co N ms, niezależnie od
hot/idle state. Brak hidden exponential backoff w pollerze, brak
`LISTEN/NOTIFY` (unika serializacji commit-NOTIFY), brak rc-release
bugów typu "config field stored but never read".

Pomyślana jako **lighter alternatywa** dla `apalis-postgres` w
przypadkach gdzie user chce pełną kontrolę nad cadence i nie potrzebuje
worker dashboard / multi-backend abstraction.

Pierwszy konsument: `rust_event_outbox` (v0.6+), który dropuje apalis
całkowicie i używa `pg_work_queue` jako warstwy worker pool.

## Motywacja — co apalis-postgres robi źle

`apalis-postgres` jest w aktywnej re-architekturze: PR #586 (rc.1) wyciął
backendy z monorepo do osobnych repo (`apalis-dev/apalis-postgres` —
repo utworzone 2025-08-19, pierwsza alpha 2025-10-25, cykl rc.1 →
rc.8 w pół roku, silently-breaking changes — np. payload JSONB → BYTEA).

Krzywizujący użytkownik ma trzy opcje: (a) zostać na rc.7 z znanymi
bugami, (b) skoczyć na ruchomy cel rc.x bez gwarancji że bugi są
fixnięte, (c) napisać własne. Wybieramy (c).

### Konkretne bugi i ograniczenia (zweryfikowane na źródle rc.8):

1. **Dead-code `Config::with_poll_interval`** — `PgPollFetcher::next_backoff`
   hardcoduje `1s → 5min` cap (`fetcher.rs:84,160-163`); config nigdy
   nie czytany. Knob istnieje w API, nic nie robi.
2. **`pg_notify` trigger per INSERT** (`migrations/20251018165121_notify_run_at.sql`).
   Commit-NOTIFY bierze `LWLock NotifyQueueLock` (`AccessExclusiveLock`
   na locktype=`database`) → serializuje wszystkie NOTIFY-issuing
   commits cluster-wide. Recall.ai publicznie udokumentował tę patologię
   (`postgres-listen-notify-does-not-scale`, marzec 2025).
3. **`ack=UPDATE` not DELETE** (`queries/task/ack.sql`) → row accumulation;
   user musi cron'ować `vacuum()`.
4. **`RetryAfterError(_, duration)` dead-field** — `get_duration()` ma
   zero callers w całym crate; nigdy nie wpływa na `run_at`.
5. **Triple retry budget** — apalis `RetryPolicy::retries(N)` (in-memory)
   + DB `attempts` + DB `max_attempts`. In-memory counter resetuje się
   per worker lease (crashed worker = retry budget reset).
6. **Live bugs w rc.8:** `AbortError` branch zakomentowany w
   `calculate_status:70` (aborty silently → Failed); worker registration
   advisory lock leak; reaper join-to-workers race (purged worker row
   = jego jobs stuck); `metrics::global` = 24 full-table scans per call;
   `Shared` driver `.unwrap()` na listener-connect/listen/send;
   `wait_for` = 500ms sleep-poll z `.unwrap()` panics; broken SQLite-
   syntax stats query w postgres queries dir.
7. **Schema warts:** brak `CHECK`/`ENUM` na status; brak composite indeksu
   `(job_type, status, run_at)`; redundant indexes; PRIMARY KEY-e
   dodane dopiero w rc.1 (po 5 latach lifetime).

### API ergonomics warts:

8. **`Backend` trait + Tower middleware stack** — custom Backend impl
   wymaga reimplementacji ~2.2 k LOC; `apalis-core` ~11 k LOC trzeba
   zrozumieć żeby cokolwiek customować.
9. **`Monitor` lifecycle complexity** — restart policies, factory
   parametryzacja, multi-stage shutdown.
10. **`service_fn` / `taskfn` macro** — typing/lifetime issues w handlerach.
11. **Multi-backend abstraction leakage** — `apalis::prelude::*` eksportuje
    rzeczy które nie zawsze apply per-backend, surprising failures.
12. **Status as plain `TEXT`** — DB nie chroni przed state machine bugami;
    apalis łapie je w app-level CHECK logice, my łapiemy w DB CHECK.
13. **Brak fencing token** w `mark_*` queries — race window między
    reaperem a starym workerem realny.

## Co rozwiązujemy z apalis-postgres — explicit mapping

| Apalis pain point | Rozwiązanie w pgwq |
|---|---|
| Config knobs nieczytane w hot-path (#1) | **Hard rule:** każdy knob ma behavioral test 2-wartościowy. PR review odrzuca knob bez testu. |
| `pg_notify` per INSERT cluster lock (#2) | **No LISTEN/NOTIFY w ogóle.** Polling-only. Deterministyczny `poll_interval`. |
| `ack=UPDATE` accumulation (#3) | `mark_done` → status='done' + finished_at; user wywołuje `pgwq::purge_done(pool, ttl)` ręcznie kiedy chce. Manual control, no surprise. |
| RetryAfter duration ignored (#4) | `Err(JobError::Retry { retry_in: Some(d), .. })` przekłada się 1:1 na `run_at = now() + d` w `mark_retry` SQL. Test: `tests/retry_in_override.rs`. |
| Triple retry counter (#5) | **DB-side `attempts` jest single source of truth.** Brak in-memory retry budget. Reaper i mark_* używają tej samej kolumny. |
| AbortError commented out (#6a) | Code review hard-rule: zero commented-out kodu w state-machine logic (PR-blocker). |
| Advisory lock leak (#6b) | **Nie używamy advisory locków w ogóle.** Reaper przez SKIP LOCKED. |
| Reaper join race (#6c) | **Brak `pgwq.workers` table.** Reaper widzi tylko `jobs.last_attempted_at` + `lease_token`. |
| metrics 24 full scans (#6d) | **Brak `metrics::global` API.** Observability przez `tracing` events + DB queries po queue table. User buduje swój dashboard. |
| `.unwrap()` w hot path (#6e,f) | `Cargo.toml`: `unwrap_used = "deny"`, `expect_used = "deny"`, `panic = "deny"`. Nie skompiluje się. |
| Broken stats.sql (#6g) | TDD + verify-before-completion: każda query exercised w teście. |
| No CHECK on status (#7) | `pgwq.job_status` ENUM + `jobs_status_invariants CHECK` z explicit `(status, attempts, *_at, lease_token)` invariantami. |
| `Backend` trait abstraction (#8) | **Brak Backend trait.** Library jest postgres-only, no abstraction layer. ~1.5–3 k LOC vs 13.8 k LOC apalis. |
| `service_fn` macro (#10) | Handler jest po prostu `async fn(T, JobContext) -> Result<(), JobError>`. Zero macro, zero lifetime puzzles, full `?` operator interop. |
| `Monitor` complexity (#9) | `Worker::start() -> WorkerHandle`; `WorkerHandle::shutdown(timeout) -> Result<Stats>`. Jedna metoda, jeden cancellation token. |
| Multi-backend leakage (#11) | Single-backend; `pgwq::*` re-eksportuje tylko to co aktualnie używa. |
| Status as plain TEXT (#12) | ENUM + CHECK invariants enforced w DB. |
| No fencing token (#13) | `lease_token UUID` column. Każdy `mark_*` ma `WHERE id=$1 AND status=$2 AND lease_token=$3`. |

## Co `pg_work_queue` świadomie NIE robi (anti-features)

- **Brak `LISTEN/NOTIFY`.** Commit-NOTIFY serializuje cluster-wide.
- **Brak adaptive backoff na pollerze.** Cadence deterministyczna.
- **Brak multi-backend abstraction.** Postgres-only.
- **Brak worker dashboard / GUI / metrics endpoint.**
- **Brak Tower middleware stack.**
- **Brak typed retry strategies w handler API.** Tylko `Err(JobError::Retry { reason, retry_in })` lub `Err(JobError::Abort { reason })`. `Ok(())` = done.
- **Brak cross-worker priorities / fairness.**
- **Brak automatycznego retention sweepera.** User wywołuje `pgwq::purge_done` / `pgwq::purge_dead` ręcznie kiedy chce (cron, tokio interval, manual). Library nie spawn'uje background cleanup task — user ma pełną kontrolę kiedy DELETE leci.
- **Brak push-side dedup column (`unique_key TEXT UNIQUE`)** w v0.1. User może zrobić własny `INSERT...ON CONFLICT` przed `Pusher::push` jeśli potrzeba.
- **Brak Worker registration table** (`pgwq.workers`). Lessons z apalisa.

## Delivery semantics & idempotency

**`pg_work_queue` daje at-least-once delivery.** Handler **MOŻE** być
zawołany ≥1 raz dla tego samego logicznego jobu.

**Cancellation model — dwa poziomy:**

1. **`handler_timeout`** (per-handler wall clock; default `lease_timeout × 80%`):
   library wraps każdą inwokację handler'a w `tokio::time::timeout`. Po
   elapsed handler future jest dropped (Drop runs, transactions roll back),
   library wywołuje `mark_retry` z `reason = "handler_timeout"`; backoff
   policy applies normalnie; jeśli `attempts ≥ max_attempts` → upgrade do
   `mark_dead`. To jest **primary path** dla zawieszeń handler'a (slow API,
   deadlock w user code, niekończący się retry-loop). Bez tego library nie
   ma sposobu odzyskać slot concurrency'i (handler future stojący na await
   blokuje permit w nieskończoność).
2. **`lease_timeout`** (per-row deadline w DB; default 5min):
   reaper threshold dla **process-death recovery** — worker process zginął
   (crash, OOM, kernel kill, partycja sieciowa worker↔DB), nigdy nie
   wywoła mark_*. Reaper flipuje status → kolejny worker re-claims
   immediately (bez backoff'u, bo to infrastruktura, nie content jobu). To
   jest **rzadka ścieżka**, nie hot-path; reaper SQL świadomie nie aktualizuje
   `run_at` na ścieżce `awaiting_retry`, bo immediate retry jest pożądane.

W obu przypadkach `attempts` jest już inkrementowane (przez `claim_batch`
zanim handler ruszy).

**Konkretne at-least-once scenarios:**

- Handler timeout: future dropped na `.await` point. Side-effects
  already-issued (HTTP POST wysłany przed cancellation, external API
  charge committed serverside przed naszym network-cut) **survive**.
  Mitigacja: `ctx.idempotency_key` jako Idempotency-Key dla external APIs.
- Handler success, `mark_done` connection lost przed commit → row stays
  `running` z lease_token → reaper after `lease_timeout` flips do
  `awaiting_retry` → kolejny worker re-execute. Side-effect powtarza się.
- Worker process crashuje po handler success ale przed `mark_done`
  commit → reaper recovery jak wyżej.
- **Pułapka:** udany handler którego `mark_done` zostało fenced-out
  (rare: process pauza > `lease_timeout − handler_timeout`, default
  margin ~1min przy default'ach) → row flipnięty do `awaiting_retry` z
  `attempts` już inkrementowanym → kolejny worker re-execute → jeśli
  `attempts >= max_attempts` skończy `dead` mimo że **pierwszy run się
  powiódł externally**. Dead-letter `tracing::error!` jest wtedy mylący.
  `Stats::fenced_out` policzy te zdarzenia.

**Cancellation gotcha (CPU-bound work):** `tokio::time::timeout` cancel'i
handler **tylko przy `.await` point**. Handler CPU-bound bez yield'ów
(gorące computation, blokujące I/O bez `spawn_blocking`) **nie zostanie
cancelled** — blokuje worker thread aż do completion; w międzyczasie lease
może expire'ować i reaper przejmie row. Użyj `tokio::task::spawn_blocking`
albo periodic `tokio::task::yield_now().await` dla CPU-bound work. Library
nie ma sposobu enforce'ować tego z zewnątrz.

**Semantyka `attempts`:** counter inkrementowany w `claim_batch`, czyli
liczy **rozpoczęte attempts**, nie completed. Timeout, panic, fence-out,
reaper recovery — wszystkie zliczają się jako attempt. Jeśli chcesz że
`max_attempts=N` znaczy "N real retry chances after first try", ustaw
`max_attempts ≥ ceil(N × (1 + expected_anomaly_rate))`. Default
`max_attempts=3` + prawidłowo skalibrowane `handler_timeout`/`lease_timeout`
(anomaly rate ≈ 0) daje 3 real chances.

**Semantyka `last_error`:** ustawiane na NULL przez `claim_batch` (start
attempta, brak stale data z poprzedniej próby), overwritten przez
`mark_retry`/`mark_dead` (handler-side reason) lub `reaper` (lease_expired/
lease_expired_max_attempts). **Bez COALESCE w reaperze** — `last_error`
zawsze odzwierciedla **most recent transition reason**, nigdy stary error.
To jest **canonical signal dla dead-letter**: gdy `tracing::error!` w
emit_transition wyraża dead-letter event, `last_error` w DB matches reason
attribute eventu. Operator widzi spójny obraz "why did this die".

### Handler-side idempotency contract

`JobContext` zawiera **`public_id: Uuid`** które:

1. Jest **stabilne across retries** dla tego samego jobu (jednorazowo
   ustawione na push, niezmienne podczas retries).
2. Jest **unikalne per logical operation** (UUIDv7, near-zero collision).
3. Jest **przekazywane do handlera w każdym attempt**.

Handler który **musi** wykonać external side-effect dokładnie raz
(np. SMTP send, payment charge, webhook POST) **musi** używać
`ctx.idempotency_key` jako idempotency key dla dedup'u:

```rust
.handler(|task: ChargeTask, ctx: JobContext| async move {
    // Dedupe via public_id (stable across retries) — repeated invocations no-op.
    if redis.get(format!("charge:{}", ctx.idempotency_key)).await?.is_some() {
        return Ok(());
    }
    stripe.charge(task.amount, &ctx.idempotency_key.to_string()).await?;
    redis.set(format!("charge:{}", ctx.idempotency_key), "1").await?;
    Ok(())
})
```

Library **nie ma sposobu** dać exactly-once external side-effects.
To jest fundamentalny limit każdej polling queue. **Handler dostaje
narzędzie (`ctx.idempotency_key`); użycie jest jego odpowiedzialnością.**

External APIs (Stripe, AWS, etc.) standardowo wspierają `Idempotency-Key`
header — nasz UUID v7 jest perfect fit.

## Public API — sketch

```rust
use pg_work_queue::{Worker, JobError, JobContext, Pusher, BackoffPolicy, JsonCodec};
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

    pg_work_queue::migrator().run(&pool).await?;

    let worker = Worker::builder(pool.clone())
        .queue("email_send")
        .poll_interval(Duration::from_millis(500))
        .concurrency(16)
        .max_attempts(5)
        .lease_timeout(Duration::from_secs(300))
        .handler_timeout(Duration::from_secs(240))  // 80% of lease; mark_retry has 60s margin
        .mark_timeout(Duration::from_secs(59))       // < 60s margin; library aborts mark_* before lease expiry
        .reaper_interval(Duration::from_secs(60))
        .retry_backoff(BackoffPolicy::exponential(
            Duration::from_secs(1),     // base
            2.0,                         // factor
            Duration::from_secs(300),   // cap
            0.2,                         // jitter ratio
        ))
        .handler(|task: EmailTask, ctx: JobContext| async move {
            tracing::info!(
                job.id = ctx.id,
                job.attempt = ctx.attempt,
                idempotency_key = %ctx.idempotency_key,
                "handling email"
            );
            // SmtpError impls From<...> for JobError (defined elsewhere).
            send_smtp(&task, &ctx.idempotency_key).await?;
            Ok(())
        })
        .build()?;

    let handle = worker.start(); // spawns poll loop + reaper

    // Push side (in your own transaction):
    let mut tx = pool.begin().await?;
    let pusher = Pusher::new("email_send"); // JsonCodec default
    let public_id: uuid::Uuid = pusher
        .push(&mut tx, &EmailTask { to: "x@y".into(), body: "hi".into() })
        .await?;
    tx.commit().await?;

    // Batch push:
    let mut tx = pool.begin().await?;
    let ids: Vec<uuid::Uuid> = pusher
        .push_batch(&mut tx, &[task1, task2, task3])
        .await?;
    tx.commit().await?;

    // Manual cleanup (user-controlled — call from your scheduler):
    let purged_done = pg_work_queue::purge_done(&pool, Duration::from_secs(7 * 24 * 3600)).await?;
    let purged_dead = pg_work_queue::purge_dead(&pool, Duration::from_secs(90 * 24 * 3600)).await?;

    // Graceful shutdown:
    tokio::signal::ctrl_c().await?;
    let stats = handle.shutdown(Duration::from_secs(10)).await?;
    tracing::info!(
        completed = stats.completed,
        failed = stats.failed,
        aborted = stats.aborted,
        "worker shut down"
    );
    Ok(())
}
```

### `JobContext` (handler argument)

```rust
pub struct JobContext {
    pub id: i64,                       // internal BIGINT PK
    pub public_id: Uuid,               // external job handle (= idempotency_key, same value)
    pub idempotency_key: Uuid,         // stable across retries; use as Idempotency-Key for external APIs
    pub queue: String,
    pub attempt: u32,                  // 1-indexed, current attempt
    pub first_attempted_at: DateTime<Utc>,
    pub lease_token: Uuid,             // current claim's fencing token
}
```

`public_id` i `idempotency_key` to ta sama wartość (alias dla
readability). `public_id` matches DB column name, `idempotency_key`
matches handler intent. Handler:

```rust
stripe.charge(amount, &ctx.idempotency_key.to_string()).await?;
```

### Builder knobs (każdy z behavioral testem przy 2 wartościach)

| Knob | Default | Validation | Effect |
|---|---|---|---|
| `queue(&str)` | required | non-empty, ≤ 64 chars | nazwa queue |
| `poll_interval(Duration)` | 1s | ≥ 10 ms | deterministyczny cycle |
| `concurrency(usize)` | num_cpus | 1..=`floor((pool.options().get_max_connections() - 2) / 2)` | max parallel handlers; **pool budget: `max_connections >= concurrency × 2 + 2`** (każdy handler może trzymać 1 conn podczas pracy + 1 dla mark_*; +2 dla poll/reaper). **MUSI używać `pool.options().get_max_connections()`, NIE `pool.size()`** (size jest lazy, =0 dla świeżego pool'a — fresh pool always-fail bug). |
| `mark_timeout(Duration)` | `lease_timeout − handler_timeout − 1s` (margin pre-lease-expiry) | ≥ 100ms, ≤ lease_timeout − handler_timeout | timeout dla `mark_done`/`mark_retry`/`mark_dead` SQL wrapped w `tokio::time::timeout`. Pod pool starvation mark_* może czekać na connection sekundy → lease wygasa zanim mark commit'uje → fence-out + duplicate side-effects. Timeout fire → `Stats::mark_timed_out++`, leave row `running`, reaper przejmie (Anti-pattern #14). |
| `max_attempts(u32)` | 3 | ≥ 1, ≤ `i32::MAX` | przed dead-letter |
| `lease_timeout(Duration)` | 5min | ≥ 1s, ≥ poll_interval × 5, ≥ handler_timeout + 1s | **reaper threshold dla process-death recovery** (crash/OOM/partycja worker↔DB); handler-level cancellation idzie przez `handler_timeout`, nie przez ten knob. Domyślnie ratio: `handler_timeout = lease_timeout × 80%` |
| `handler_timeout(Duration)` | `lease_timeout × 80%` (clamped ≥ `MIN_HANDLER_TIMEOUT`) | ≥ 1s (`MIN_HANDLER_TIMEOUT`), ≤ lease_timeout − 1s | per-handler wall clock; library wraps invokację w `tokio::time::timeout`. Elapsed → `mark_retry { reason: "handler_timeout" }` z normalnym backoff'em policy; if `attempts ≥ max_attempts` → upgrade do `mark_dead`. **Gotcha:** cancel'i handler tylko przy `.await` point — CPU-bound work musi iść przez `spawn_blocking` |
| `reaper_interval(Duration)` | lease_timeout/4 | ≥ 1s, ≤ lease_timeout/2 | reaper tick cadence (tylko process-death recovery; rare path) |
| `batch_size(usize)` | 10 | 1..=1_000 | rows per claim_batch |
| `retry_backoff(BackoffPolicy)` | `Exponential { 1s, 2.0, 5min, 0.2 }` | jitter ∈ [0,1], cap ≤ 24h | used when `JobError::Retry { retry_in: None, .. }` |
| `panic_policy(PanicPolicy)` | `Retry` | enum: `Retry` \| `Dead` | what to do when handler panics |
| `codec(impl Codec)` | `JsonCodec` | trait-bound | payload serialization |

Każdy knob ma **integracyjny test który mierzy observable behavior przy
2 wartościach** (np. `poll_interval(100ms)` vs `poll_interval(500ms)` →
różnica latency mierzalna).

## Resource limits (`pgwq::limits` module)

```rust
pub mod limits {
    /// Max payload size pushable (DB CHECK enforces same).
    pub const MAX_PAYLOAD_BYTES: usize = 1 * 1024 * 1024; // 1 MiB

    /// Max items per `Pusher::push_batch` call. Larger batches must chunk.
    pub const MAX_BATCH_SIZE: usize = 10_000;

    /// Max queue name length (DB CHECK enforces same).
    pub const MAX_QUEUE_LEN: usize = 64;

    /// Max length of `last_error` text. Library truncates at this; DB CHECK backstops.
    pub const MAX_LAST_ERROR_LEN: usize = 8 * 1024; // 8 KiB

    /// Minimum poll_interval allowed in builder.
    pub const MIN_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

    /// Minimum handler_timeout. Below this the handler cannot reliably finish
    /// trivial work (parse + 1 DB query) and `mark_retry` commit before being
    /// aborted. Lower bound for builder validation.
    pub const MIN_HANDLER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

    /// Reaper sweep batch size (rows per tick). Wcześniej 256 z mylącym
    /// uzasadnieniem o `max_locks_per_transaction` — TO BYŁA NIEPRAWDA:
    /// `max_locks_per_transaction` (default 64) limituje lock manager entries
    /// dla relation/object/advisory locks, NIE row locks (heap tuple xmax,
    /// które FOR UPDATE SKIP LOCKED tu używa). 1024 OK pod normal i disaster
    /// recovery. Pod data-center power cycle z 1M rows w `running`, single
    /// reaper drainuje ~13 rows/sec przy reaper_interval=75s — 21h recovery.
    /// Z N replikami + adaptive ticking (poll loop nie czeka na tick gdy
    /// reaped == LIMIT) — minutes do hours zamiast days.
    pub const REAPER_BATCH_SIZE: usize = 1024;

    /// Aggregate cap na całkowity batch payload bytes — chroni przed
    /// 10k items × 1 MiB = 10 GB single transaction (S3 z review).
    pub const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

    /// Purge function chunk size (rows per `DELETE ... LIMIT N` iteration).
    pub const PURGE_CHUNK_SIZE: usize = 10_000;
}
```

Wszystkie te wartości mają DB-side CHECK lub builder-side validation
jako defense-in-depth. Testy `tests/resource_limits.rs` weryfikują że:
- `payload.len() > MAX_PAYLOAD_BYTES` → `PushError::PayloadTooLarge`
- `batch.len() > MAX_BATCH_SIZE` → `PushError::BatchTooLarge`
- `poll_interval(< MIN_POLL_INTERVAL)` → `BuildError::PollIntervalTooShort`
- itd.

## Schema (DB layout)

Schema **`pgwq`** (krótka, nie `pg_work_queue` bo `pg_` prefix jest
reserved przez PG dla schemy systemowej).

```sql
CREATE SCHEMA IF NOT EXISTS pgwq;

-- updated_at touch trigger function (namespaced to pgwq, not public).
CREATE OR REPLACE FUNCTION pgwq.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TYPE pgwq.job_status AS ENUM (
    'queued', 'running', 'awaiting_retry', 'done', 'dead'
);

CREATE TABLE pgwq.jobs (
    id                 BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    public_id          UUID        NOT NULL DEFAULT uuidv7() UNIQUE,
    queue              TEXT        COLLATE "C" NOT NULL,
    payload            BYTEA       NOT NULL,
    status             pgwq.job_status NOT NULL DEFAULT 'queued',
    attempts           INTEGER     NOT NULL DEFAULT 0,
    max_attempts       INTEGER     NOT NULL DEFAULT 0,  -- per-row, stamped by claim_batch (rolling-deploy safe)
    lease_token        UUID,
    lease_expires_at   TIMESTAMPTZ,             -- set by claim_batch (now()+lease_timeout); reaper compares per-row
    last_error         TEXT,
    last_attempted_at  TIMESTAMPTZ,
    first_attempted_at TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    run_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT jobs_queue_nonempty        CHECK (length(queue) > 0),
    CONSTRAINT jobs_queue_max_len         CHECK (length(queue) <= 64),
    CONSTRAINT jobs_payload_max_size      CHECK (octet_length(payload) <= 1048576),
    CONSTRAINT jobs_last_error_max_len    CHECK (last_error IS NULL OR length(last_error) <= 8192),
    CONSTRAINT jobs_attempts_nonneg       CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_nonneg   CHECK (max_attempts >= 0),
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
            AND first_attempted_at IS NULL
            AND finished_at IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL)
        OR (status = 'running'
            AND attempts > 0
            AND last_attempted_at IS NOT NULL
            AND first_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL)
        OR (status = 'awaiting_retry'
            AND attempts > 0
            AND last_attempted_at IS NOT NULL
            AND first_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL)
        OR (status IN ('done', 'dead')
            AND finished_at IS NOT NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL)
    )
);

-- fillfactor=90: **HOT updates są NIEosiągalne** w tym schemacie — każda
-- mainstream tranzycja (queued→running, running→awaiting_retry, →done/dead,
-- reaper) crossuje partial-index predicate (jobs_claim_idx / jobs_reap_idx /
-- jobs_terminal_idx), więc każdy UPDATE alokuje nowy tuple niezależnie od
-- fillfactor. 10% slack pomaga jednak na non-HOT path: szybszy autovacuum
-- dead-tuple reclaim + locality nowych tuples (mniej cross-block jumps).
-- 80 było overkill (poprzedni komentarz mylił "HOT in-place" — nieprawda).
ALTER TABLE pgwq.jobs SET (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);

-- Poll claim hot path
CREATE INDEX jobs_claim_idx
    ON pgwq.jobs (queue, run_at, id)
    WHERE status IN ('queued', 'awaiting_retry');

-- Reaper hot path
-- (queue, lease_expires_at) — leading `queue` daje per-queue range scan
-- precyzyjnie na slice tej kolejki. Bez prefixu: scan w order
-- lease_expires_at iteruje przez running rows wszystkich queue, heap-side
-- filter queue=$1, LIMIT stosowane po filtrze → przy 10 queue'kach i
-- jednej z dużym crash backlog'iem, reaper innej queue robi w każdym
-- ticku linear scan cudzych rows. lease_expires_at jako secondary
-- zachowuje per-row deadline dla heterogeneous deploy (Worker A lease=30s
-- vs Worker B lease=10min na tej samej queue → A nie reapuje B-running).
CREATE INDEX jobs_reap_idx
    ON pgwq.jobs (queue, lease_expires_at)
    WHERE status = 'running';

-- Purge functions hot path
CREATE INDEX jobs_terminal_idx
    ON pgwq.jobs (finished_at)
    WHERE status IN ('done', 'dead');

CREATE TRIGGER touch_jobs
    BEFORE UPDATE ON pgwq.jobs
    FOR EACH ROW EXECUTE FUNCTION pgwq.set_updated_at();
```

Decyzje:
- `BIGINT IDENTITY` internal + `public_id UUID` external (uuidv7 default
  → time-ordered B-tree locality).
- `attempts INTEGER` (nie SMALLINT) — `u32` API space-fits.
- `lease_token UUID` — fencing token w mark_* WHERE.
- `CHECK` na payload/queue/last_error length jako defense-in-depth +
  library-side truncation przed insert.
- `first_attempted_at` w status invariants (symmetric z `last_attempted_at`).
- ENUM `pgwq.job_status` zamiast TEXT+CHECK.
- `pgwq.set_updated_at` namespace'd, nie w `public`, żeby nie kolidować
  z user'owym helperem.
- `touch_jobs` trigger → wszystkie SQL queries w pgwq **NIE** ustawiają
  explicit `updated_at = now()`. Single source of truth.
- 3 partial indexes na hot paths.
- Wymaga **PostgreSQL 18+** (`uuidv7()` natywne; CHECK w `numeric()` itd).

## Internal architecture

```
                            ┌──────────────────┐
                            │  Pusher::push    │  (in user's tx)
                            │  ::push_batch    │
                            └──────────────────┘
                                     │
                            ┌────────▼────────┐
                            │   pgwq.jobs     │
                            └────────┬────────┘
                                     │
                ┌────────────────────┼────────────────────┐
                │                    │                    │
        ┌───────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
        │  Poll Loop     │  │  Reaper Loop    │  │ (user-invoked   │
        │  every N ms    │  │  SKIP LOCKED    │  │  purge_done /   │
        │  CTE+UPDATE    │  │  CASE WHEN      │  │  purge_dead)    │
        │  SKIP LOCKED   │  │  attempts >=    │  │                 │
        │  RETURNING *   │  │  max_attempts   │  │                 │
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
         handler return → mark_done(id, lease_token)
                        / mark_retry(id, lease_token, ...)
                        / mark_dead(id, lease_token, ...)
                        / panic → mark_retry albo mark_dead (per policy)
```

**Worker spawn'uje tylko 2 background tasks: poll loop + reaper.**
Retention sweeper z planu v1 — usunięty. User wywołuje `pgwq::purge_*`
ręcznie ze swojego cron / scheduler.

### Poll loop (heart)

**Architectural rule: acquire permits FIRST, then claim only what permits
allow.** Bez tego batch_size > free_permits powoduje że ostatnie claim'ed
rows siedzą `running` z attempts++ i `lease_expires_at` tykającym, czekając
na permit — pod heterogenicznymi czasami handlerów (jeden 400s, reszta 100s)
reaper przejmuje row zanim handler ruszy → mass fence-out, side-effects
duplikują się przez retry-on-other-worker (Anti-pattern #13).

```rust
async fn poll_loop<T>(state: Arc<WorkerState<T>>) {
    let mut ticker = tokio::time::interval(state.poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            _ = state.shutdown.cancelled() => break,
        }

        // Gate na capacity: blokujący acquire pierwszego permitu (z shutdown
        // shortcut). To ZANIM ruszymy claim_batch — w ten sposób mamy gwarancję
        // że co najmniej 1 row claim'ed będzie miał permit od razu, a lease
        // zacznie tykać dopiero kiedy handler faktycznie biegnie.
        let p1 = tokio::select! {
            r = state.semaphore.clone().acquire_owned() => r,
            _ = state.shutdown.cancelled() => break,
        };
        let Ok(p1) = p1 else { break };  // semaphore closed → handle dropped
        let mut permits = vec![p1];

        // Greedy try_acquire reszty do batch_size — bez blokowania.
        // Pod heavy load (wszystkie inne permity zajęte) zostaniemy z 1 permit,
        // claim_batch zwróci ≤ 1 row, ale każdy claimed row ma permit immediately.
        // Pod idle/uniform load — permits.len() ≈ batch_size, efficient.
        while permits.len() < state.batch_size {
            match state.semaphore.clone().try_acquire_owned() {
                Ok(p) => permits.push(p),
                Err(tokio::sync::TryAcquireError::NoPermits) => break,
                Err(tokio::sync::TryAcquireError::Closed) => return,
            }
        }

        let want = permits.len();
        let span = tracing::info_span!("pgwq.poll_tick",
            queue = %state.queue, permits = want, batch_size_max = state.batch_size);
        let _enter = span.enter();

        // CRITICAL: claim_batch SQL AWAIT musi być wrapowany w shutdown select.
        // Bez tego pod pool starvation (sqlx acquire_timeout default 30s)
        // shutdown cancel czeka cały acquire_timeout zanim się zauważy — łamie
        // dokumentowany "shutdown exits immediately" kontrakt. Drop future =
        // sqlx server-side cancel, connection released.
        let claim_result = tokio::select! {
            r = claim_batch(&state.pool, &state.queue, want, state.max_attempts) => r,
            _ = state.shutdown.cancelled() => break,
        };

        match claim_result {
            Ok(rows) if rows.is_empty() => {
                // Nothing to do; permits drop automatycznie → other workers/przyszłe
                // ticki dostaną zwrócone permity.
                continue;
            }
            Ok(rows) => {
                tracing::info!(claimed = rows.len(), wanted = want, "batch claimed");
                // CRITICAL invariant: rows.len() ≤ permits.len() (claim respects LIMIT).
                // Każdy row dostaje permit immediately — spawn nie czeka na semaphore.
                // Nadmiarowe permity (jeśli claim zwrócił mniej niż want) dropped
                // automatycznie poniżej.
                for (row, permit) in rows.into_iter().zip(permits.drain(..)) {
                    state.tasks.spawn(handle_job(row, state.clone(), permit));
                }
                // Pozostałe permits w `permits` dropped tutaj — zwracają sloty.
            }
            Err(e) if is_fatal_sqlx(&e) => {
                tracing::error!(error = %e, "fatal DB error in claim_batch; shutting down worker");
                let _ = state.last_fatal.set(Arc::new(e));
                state.shutdown.cancel();
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "claim batch failed; will retry next tick");
                // permits dropped (drain at end of scope) → other workers dostaną
                // sloty z powrotem.
            }
        }
    }
}
```

**Dlaczego to jest poprawne:**

1. **Brak claim-without-permit window.** Permity są secured PRZED claim'em.
   Każdy claimed row ma gwarantowany dostępny permit → handle_job spawn
   immediately, lease zaczyna tykać dopiero w handlerze, nie w queue.
2. **Brak shutdown race** w spawn loop. `permits.drain(..)` daje
   `OwnedSemaphorePermit` które move'ujemy do handle_job — nie wołamy
   `acquire_owned().await` w środku spawn loop, więc shutdown nie ma
   gdzie nas przerwać między claim'em a spawn'em.
3. **Heterogeniczne czasy handlerów** (mix 100s+400s) — pod load'em
   try_acquire zwraca NoPermits szybko, permits.len() jest małe (np. 1),
   claim'amy tylko tyle ile możemy obsłużyć teraz. Nie zalegamy
   claim'ed-ale-niespawn'owanymi rows.
4. **Pod idle load** — wszystkie permity wolne → permits.len() = batch_size,
   claim'amy maksymalny batch, efficient SQL roundtrip.

**Trade-off:** więcej claim queries pod heavy contention (np. 1 row per
claim zamiast 10). Akceptowalne — claim_batch jest cheap (1 partial index
scan + UPDATE FROM CTE).

`is_fatal_sqlx` distinguishes `PoolClosed` / `WorkerCrashed` / `Migrate`
(fatal → self-shutdown) od `Database` / `Io` / `Tls` (transient → retry
next tick).

### `claim_batch` SQL

```sql
WITH claimed AS (
    SELECT id FROM pgwq.jobs
    WHERE queue = $1
      AND status IN ('queued', 'awaiting_retry')
      AND run_at <= now()
    ORDER BY run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pgwq.jobs j
SET status = 'running',
    attempts = j.attempts + 1,
    max_attempts = $4,                        -- $4 = THIS worker's max_attempts,
                                              -- stamped per-row → rolling-deploy
                                              -- safe (reaper i mark_retry używają
                                              -- j.max_attempts, nie worker-local).
    last_attempted_at = now(),
    first_attempted_at = COALESCE(j.first_attempted_at, now()),
    lease_token = gen_random_uuid(),
    lease_expires_at = now() + $3::interval,  -- $3 = THIS worker's lease_timeout
    last_error = NULL                         -- start fresh: poprzedni attempt's
                                              -- error nie zostaje widoczny podczas
                                              -- aktualnego `running` window'a;
                                              -- mark_retry/dead/reaper i tak overwritują
FROM claimed
WHERE j.id = claimed.id
RETURNING j.id, j.public_id, j.queue, j.payload, j.attempts, j.max_attempts,
          j.first_attempted_at, j.lease_token, j.lease_expires_at;
```

(`updated_at` ustawia trigger, nie explicit SET.)

### Reaper (single-CTE, no race window)

**Plan v1 miał two-step reaper z drugim UPDATE bez status/lease_token
guard'a — łamał własną regułę #6 (każdy UPDATE w state-machine musi
mieć dodatkowy guard). v2 łączy w jeden CTE z `CASE WHEN`:**

```sql
WITH stale AS (
    SELECT id, attempts, max_attempts FROM pgwq.jobs
    WHERE queue = $1                          -- per-queue isolation
      AND status = 'running'
      AND lease_expires_at < now()            -- per-row deadline (set at claim)
    ORDER BY lease_expires_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pgwq.jobs j
SET status = CASE
        -- s.max_attempts jest per-row (stamped przez claim_batch) — rolling-deploy
        -- safe: Worker A (max=3) i Worker B (max=5) widzą TĘ SAMĄ wartość z DB,
        -- werdykt deterministyczny niezależnie od which-replica-wins-the-lock.
        WHEN s.attempts >= s.max_attempts THEN 'dead'::pgwq.job_status
        ELSE 'awaiting_retry'::pgwq.job_status
    END,
    finished_at = CASE
        WHEN s.attempts >= s.max_attempts THEN now()
        ELSE NULL
    END,
    last_error = CASE
        -- Overwrite, NIE COALESCE — reaper jest najnowszym signalem o powodzie
        -- transition'a. Stare last_error (z poprzedniego mark_retry zanim claim
        -- wyzerował) byłby mylący w dead-letter `tracing::error!`: operator
        -- widzi 'stripe_declined' z attempta 1 zamiast 'lease_expired' (worker
        -- crash w attempt 2). claim_batch ustawia last_error=NULL na start
        -- attempta, ale defense-in-depth: tu zawsze overwrite.
        WHEN s.attempts >= s.max_attempts THEN 'lease_expired_max_attempts'
        ELSE 'lease_expired'
    END,
    lease_token = NULL,
    lease_expires_at = NULL
FROM stale s
WHERE j.id = s.id
  AND j.status = 'running'                    -- defense-in-depth (matches partial index)
RETURNING j.id, j.public_id, j.status, j.attempts;
```

Atomic single-statement. SKIP LOCKED w CTE = stale rows trzymane workerem
nie są reapowane. Brak race window. **`queue = $1`** krytyczne — bez tego
Worker A (queue=`email`, lease_timeout=30s) mógłby reapować długo-running
joba Worker B (queue=`billing`, handler trwa 10min). Plus N replik tej
samej kolejki → SKIP LOCKED naturalnie partycjonuje, ale każdy widzi
tylko swój queue.

Reaper task:

```rust
async fn reaper_loop(state: Arc<WorkerState>) {
    let mut ticker = tokio::time::interval(state.reaper_interval);
    let mut skip_next_tick = false;  // adaptive: drain backlog without waiting
    loop {
        if !skip_next_tick {
            tokio::select! {
                _ = ticker.tick() => {},
                _ = state.shutdown.cancelled() => return,
            }
        }
        skip_next_tick = false;

        let span = tracing::info_span!("pgwq.reap_tick", queue = %state.queue);
        let _enter = span.enter();

        // reap() signature: (pool, queue, batch_limit) — max_attempts NOT passed,
        // reaper SQL używa j.max_attempts per-row (rolling-deploy safe, #1).
        // lease_timeout też NOT passed — per-row j.lease_expires_at już compare'owany SQL-side.
        // CRITICAL: reap SQL await wrapowany w shutdown select (same reason
        // jak claim_batch — pool starvation nie może blokować shutdown).
        let reap_result = tokio::select! {
            r = reap(&state.pool, &state.queue, limits::REAPER_BATCH_SIZE) => r,
            _ = state.shutdown.cancelled() => return,
        };
        match reap_result
        {
            Ok(reaped) if reaped.is_empty() => {}
            Ok(reaped) => {
                let dead_count = reaped.iter().filter(|r| r.status == "dead").count();
                let retry_count = reaped.len() - dead_count;
                // Adaptive: jeśli zwróciliśmy pełen LIMIT, sygnalizuje to backlog
                // — następna iteracja skip'uje ticker.tick() i lecimy at-SQL-speed
                // aż drain. Pod normal load reaped << LIMIT, normalna cadence
                // zachowana. Pod disaster recovery (1M stale rows) — drain bez
                // 75s pauz między batch'ami.
                if reaped.len() >= limits::REAPER_BATCH_SIZE {
                    skip_next_tick = true;
                }
                tracing::warn!(
                    reaped_total = reaped.len(),
                    reaped_dead = dead_count,
                    reaped_retry = retry_count,
                    backlog_continues = skip_next_tick,
                    "stale jobs reaped"
                );
                // Per-row pgwq.state.transition events tak samo jak
                // handler-driven. source="reaper" w attrs.
                for row in &reaped {
                    let to = if row.status == "dead" { "dead" } else { "awaiting_retry" };
                    emit_transition(
                        TransitionFrom::Running,
                        if to == "dead" { TransitionTo::Dead } else { TransitionTo::AwaitingRetry },
                        TransitionCtx {
                            job_id: row.id,
                            public_id: row.public_id,
                            queue: &state.queue,
                            attempts: row.attempts,
                            source: "reaper",
                            reason: Some(if to == "dead" { "lease_expired_max_attempts" } else { "lease_expired" }),
                        },
                    );
                }
            }
            Err(e) if is_fatal_sqlx(&e) => {
                tracing::error!(error = %e, "fatal DB error in reaper; shutting down");
                state.shutdown.cancel();
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "reap tick failed; will retry");
            }
        }
    }
}
```

### Reaper panic recovery

**Decyzja (W10 z review):** reaper tick wrapped w `tokio::spawn` →
`JoinHandle` → `JoinError::is_panic()`. **Nie** używamy `futures::catch_unwind`
(brak `futures-util` w deps; plus `catch_unwind` na async fn miałby
wymagać `UnwindSafe` bounds problematycznych dla `&PgPool`). `JoinSet` /
`JoinHandle::await` natywnie surface'uje panic via `JoinError`. K=3
consecutive panic ticks → worker shutdown:

```rust
const REAPER_PANIC_ESCALATION_THRESHOLD: u32 = 3;

let mut consecutive_panics = 0;
loop {
    let state = state.clone();
    let tick = tokio::spawn(async move {
        reap(&state.pool, &state.queue, ...).await
    });

    match tick.await {
        Ok(Ok(reaped)) => { consecutive_panics = 0; emit_events(&reaped); }
        Ok(Err(e)) if is_fatal_sqlx(&e) => {
            let _ = state.last_fatal.set(Arc::new(e));
            state.shutdown.cancel();
            return;
        }
        Ok(Err(e)) => { consecutive_panics = 0; tracing::warn!(error = %e, "reap tick failed"); }
        Err(je) if je.is_panic() => {
            consecutive_panics += 1;
            let msg = extract_panic_message(je);
            tracing::error!(panic = %msg, consecutive = consecutive_panics,
                "reaper tick panicked");
            if consecutive_panics >= REAPER_PANIC_ESCALATION_THRESHOLD {
                tracing::error!(threshold = REAPER_PANIC_ESCALATION_THRESHOLD,
                    "reaper exceeded panic threshold; shutting down worker");
                state.shutdown.cancel();
                return;
            }
        }
        Err(_je_cancelled) => return, // task aborted via abort_handle
    }
}

fn extract_panic_message(je: tokio::task::JoinError) -> String {
    match je.try_into_panic() {
        Ok(payload) => match payload.downcast::<&'static str>() {
            Ok(s) => s.to_string(),
            Err(payload) => match payload.downcast::<String>() {
                Ok(s) => *s,
                Err(_) => "<unknown panic payload>".to_string(),
            }
        }
        Err(_) => "<task cancelled before panic>".to_string(),
    }
}
```

Codec decode w worker (`handle_job`) używa innego patternu niż reaper:
**sync** `std::panic::catch_unwind(AssertUnwindSafe(|| codec.decode(...)))`
zamiast `tokio::spawn`. Powody:

1. Codec decode jest synchroniczny i CPU-bound; `tokio::spawn` per claimed
   row dorzuca scheduling overhead (10us × 10k claim/s = 100ms/s waste).
2. Closure'a `|| codec.decode(payload)` nie capture'uje `&PgPool` ani innych
   non-UnwindSafe refs (tylko bajty payloadu) — `AssertUnwindSafe` jest
   poprawnie applied, bez problemów z reaper'em (linia 745-748).
3. Codec decode jest **w środku handle_job**, który sam jest w JoinSet —
   gdyby tu użyć `tokio::spawn`, leak per Anti-pattern #12 (outer abort_all
   → JoinHandle dropped → decode task detached). Sync catch_unwind nie ma
   tego problemu (no async cancellation point).

Panic w codec → mark_dead z `reason = "codec panic: ..."`.

Test: `tests/reaper_recovers_from_tick_panic.rs` (inject panic via test-only
hook, assert worker survives ≤2 panics, dies on 3rd).

### Handler invocation (`handle_job`)

Każdy claim'ed row dostaje invokację handler'a wrapowaną w `tokio::time::timeout`
i izolowaną przez **lokalny `JoinSet`** — nie przez `tokio::spawn`. Powód
omówiony niżej.

```rust
async fn handle_job<T>(
    row: Job<T>,
    state: Arc<WorkerState<T>>,
    _permit: OwnedSemaphorePermit,
) {
    let ctx = JobContext { /* id, public_id, idempotency_key, ... */ };

    // Panic isolation + proper cascade cancel: local JoinSet's Drop aborti
    // pending tasks. Outer abort (state.tasks.abort_all()) → handle_job
    // unwind → local `set` dropped → handler future poprawnie cancelled.
    let mut set = tokio::task::JoinSet::new();
    let handler_fut = (state.handler)(row.payload.clone(), ctx.clone());
    set.spawn(tokio::time::timeout(state.handler_timeout, handler_fut));

    let outcome = set.join_next().await;  // Option<Result<Result<HandlerResult, Elapsed>, JoinError>>

    // Helper: każde mark_* SQL wrapowane w `mark_timeout`. Pool starvation +
    // brak timeout'u = mark_* czeka sekundy, lease wygasa, reaper przejmuje,
    // mark_* w końcu zwraca 0 rows (fenced), side-effect duplikuje się przez
    // retry-on-other-worker. Z timeoutem: bounded wait, Stats::mark_timed_out++,
    // leave row 'running', reaper przejmie cleanly (Anti-pattern #14).
    let mark = |fut| async move {
        match tokio::time::timeout(state.mark_timeout, fut).await {
            Ok(r) => r,
            Err(_elapsed) => {
                state.stats.mark_timed_out.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(job.id = ctx.id, mark_timeout_ms = ?state.mark_timeout,
                    "mark_* timed out under pool pressure; leaving row 'running' for reaper recovery");
            }
        }
    };

    match outcome {
        Some(Ok(Ok(Ok(())))) => {
            mark(mark_done(&state.pool, ctx.id, ctx.lease_token)).await;
        }
        Some(Ok(Ok(Err(JobError::Retry { reason, retry_in })))) => {
            mark(mark_retry_or_upgrade_dead(&state, &ctx,
                fmt_err_trimmed(&reason),
                retry_in.unwrap_or_else(|| state.backoff.next(ctx.attempt)))).await;
        }
        Some(Ok(Ok(Err(JobError::Abort { reason })))) => {
            mark(mark_dead(&state.pool, ctx.id, ctx.lease_token,
                      fmt_err_trimmed(&reason))).await;
        }
        Some(Ok(Err(_elapsed))) => {
            // handler_timeout fired. Inner future już dropped przez timeout
            // wrapper; mark_retry leci jak Retry { reason: "handler_timeout" }.
            state.stats.timed_out.fetch_add(1, Ordering::Relaxed);
            mark(mark_retry_or_upgrade_dead(&state, &ctx,
                "handler_timeout",
                state.backoff.next(ctx.attempt))).await;
        }
        Some(Err(je)) if je.is_panic() => {
            let msg = extract_panic_message(je);
            match state.panic_policy {
                PanicPolicy::Retry => mark(mark_retry_or_upgrade_dead(&state, &ctx,
                    &format!("panic: {}", fmt_err_trimmed(&msg)),
                    state.backoff.next(ctx.attempt))).await,
                PanicPolicy::Dead => mark(mark_dead(&state.pool, ctx.id, ctx.lease_token,
                    &format!("panic: {}", fmt_err_trimmed(&msg)))).await,
            }
        }
        Some(Err(_cancelled)) => {
            // Wewnętrzny task w `set` został abort'ed PRZED join_next() —
            // ścieżka teoretyczna (nigdy nie wołamy set.abort_all() z wnętrza
            // handle_job). Jeśli kiedyś się pojawi: leave row 'running',
            // reaper recover'i po lease_expires_at.
            unreachable!("inner task can only be cancelled via JoinSet::drop, which happens after handle_job returns")
        }
        None => unreachable!("set had exactly one task spawned"),
    }
    // _permit dropped here → semaphore slot freed.
    // `set` dropped here — no-op if join_next() returned; aborti pending
    // tasks jeśli handle_job sam jest cancelled (cascade).
}
```

**Dlaczego lokalny `JoinSet`, nie `tokio::spawn`:**

`tokio::spawn` zwraca `JoinHandle`, którego **`Drop` nie aborti** spawned
tasku — tylko detach'uje go. Jeśli `handle_job` jest w głównym `JoinSet`
worker'a i `abort_all()` zostanie wywołane (shutdown timeout, fatal SQL,
panic escalation), to:

1. `handle_job` task dostaje abort signal → unwind przy najbliższym `.await`.
2. Lokalne zmienne dropped — w tym `joined: JoinHandle` (gdyby był).
3. **`JoinHandle::drop` detach'uje, nie aborti** → wewnętrzny spawn żyje
   dalej, kontynuuje handler aż do natural completion.
4. mark_*** nigdy nie zostaje wywołany (kod po `joined.await` umarł razem
   z handle_job).
5. Side-effects handler'a wykonują się dalej; row stoi `running` do
   `lease_expires_at`; reaper przejmuje.

Z `JoinSet` zamiast `JoinHandle`: **`JoinSet::drop` aborti wszystkie
pending tasks** (udokumentowane tokio invariant). Cascade działa
poprawnie — outer abort handle_job → lokalny set dropped → inner handler
task aborted → handler future dropped at next `.await`, Drop runs.

**Trade-off vs `catch_unwind`:** plan świadomie nie używa
`futures::catch_unwind` (linia 745-748: brak `futures-util` w deps +
UnwindSafe issues dla `&PgPool` w reaperze). `JoinSet` daje to samo
(`JoinError::is_panic()` jako panic surface) bez nowego deps i bez
UnwindSafe bounds na user-controlled handler state.

**Cancellation semantics handler'a:** kiedy `handler_timeout` elapsed,
`tokio::time::timeout` zwraca `Err(Elapsed)` a inner future (handler)
zostało dropped at `.await` point. Owned resources (sqlx connections,
hyper HTTP w toku) zwolnione przez Drop. Side-effects już issued
serverside (HTTP POST wysłany przed cancel'em) **survive** — patrz
§ Delivery semantics dla `idempotency_key` contract.

**Helper `mark_retry_or_upgrade_dead`:** central'izuje logikę
"if attempts >= max_attempts → upgrade Retry path do mark_dead". Wszystkie
3 call sites (JobError::Retry, handler_timeout, panic w PanicPolicy::Retry)
używają tego samego helper'a żeby decyzja nie drift'owała.

### Mark queries (fencing token w WHERE)

```sql
-- mark_done
UPDATE pgwq.jobs
SET status = 'done', finished_at = now(), last_error = NULL,
    lease_token = NULL, lease_expires_at = NULL
WHERE id = $1 AND status = 'running' AND lease_token = $2;

-- mark_retry
UPDATE pgwq.jobs
SET status = 'awaiting_retry', last_error = $3, run_at = $4,
    lease_token = NULL, lease_expires_at = NULL
WHERE id = $1 AND status = 'running' AND lease_token = $2;

-- mark_dead
-- WHERE status = 'running' only — worker calls mark_dead tylko dla wierszy
-- które właśnie wykonał (handler zwrócił JobError::Abort lub retry-upgrade
-- po max_attempts). Path awaiting_retry → dead należy do reaper'a (single
-- CTE z CASE WHEN attempts >= max_attempts), nie do worker'a.
UPDATE pgwq.jobs
SET status = 'dead', finished_at = now(), last_error = $3,
    lease_token = NULL, lease_expires_at = NULL
WHERE id = $1 AND status = 'running' AND lease_token = $2;
```

**0-rows-affected reactions** (explicit):

- `mark_done` 0 rows → reaper już flipnął (lease expired) lub szczególny
  race. Action: `warn!(job.id, idempotency_key, "mark_done lost race —
  side-effect may have already been retried by other worker")`, increment
  `Stats::fenced_out`, emit structured event `pgwq.state.transition` z
  `lost_race=true` attr. Worker continues — next claim picks up.
- `mark_retry` 0 rows → analogously. `warn!` + `fenced_out++`. Continue.
- `mark_dead` 0 rows → analogously. `warn!` + `fenced_out++`. Continue.

W żadnym wypadku worker NIE re-attempts; reaper-spawned retry przejmuje
dalszą logikę.

**Edge case dokumentowany w § Delivery semantics:** udany handler z
mark_done fenced-out → kolejny retry → może skończyć `dead` jeśli
`attempts >= max_attempts`. Dead-letter `tracing::error!` jest mylący
w tym przypadku — system widzi failure ale externally side-effect
zaszedł. `Stats::fenced_out` umożliwia detekcję.

### Manual cleanup — `pgwq::purge_*`

User wywołuje gdy chce (cron, scheduled task, manual operations).
Chunked DELETE, SKIP LOCKED na wszelki wypadek:

```rust
/// Delete `done` rows older than `age`. Returns count deleted.
pub async fn purge_done(pool: &sqlx::PgPool, age: Duration) -> Result<u64, PurgeError>;

/// Delete `dead` rows older than `age`. Returns count deleted.
pub async fn purge_dead(pool: &sqlx::PgPool, age: Duration) -> Result<u64, PurgeError>;
```

SQL (per function):

```sql
WITH victims AS (
    SELECT id FROM pgwq.jobs
    WHERE status = $1::pgwq.job_status
      AND finished_at < now() - $2::interval
    ORDER BY finished_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
DELETE FROM pgwq.jobs WHERE id IN (SELECT id FROM victims);
```

Funkcja w pętli z `LIMIT limits::PURGE_CHUNK_SIZE` aż batch zwróci 0
(no more matches). Sumuje deleted count.

Tracing:
```rust
tracing::info!(status = %status, age = ?age, deleted = total, "purge complete");
```

### Batch push

```sql
-- run_at omitted — DB DEFAULT now() fires per row.
-- Bez RETURNING ani WITH ORDINALITY: Postgres docs explicit'ie nie gwarantują
-- order RETURNING, a public_id i tak generujemy client-side PRZED INSERT'em.
-- Zwracamy z Rust'a client-side Vec<Uuid> w input order, no DB roundtrip
-- needed dla result set.
INSERT INTO pgwq.jobs (queue, payload, public_id)
SELECT $1, payload, public_id
FROM unnest($2::bytea[], $3::uuid[]) AS u(payload, public_id);
```

`Pusher` client-side:
1. Validate `items.len() > 0 && items.len() <= limits::MAX_BATCH_SIZE` →
   else `PushError::BatchTooLarge { size, max }` lub `BatchEmpty`.
2. Encode loop **short-circuited**: encode item i → validate
   `payload.len() <= MAX_PAYLOAD_BYTES` (else
   `PushError::PayloadTooLarge { index: i, size, max }`) → accumulate
   `total_bytes`; jeśli `total_bytes > MAX_BATCH_BYTES` → bail z
   `PushError::BatchPayloadTooLarge { total_bytes, max }`. Bez tego
   5 GB transient buffor zanim item 4999 fails.
3. Generate `public_id = Uuid::now_v7()` per item (client-side dla
   outbox correlation-in-same-tx; deterministic order, **canonical handle**
   zwracany z Pusher'a).
4. Single `INSERT...SELECT FROM unnest(...)` round-trip; **no RETURNING**.
5. Verify `rows_affected == public_ids.len()`; mismatch (CHECK violation
   on jeden item, etc.) → `PushError::BatchPartial { inserted, expected }`.
   Pod normalnym INSERT'em wszystko-albo-nic (single statement) — mismatch
   teoretycznie nieosiągalny bo CHECK fail rollback'uje całą statement.
   Defense-in-depth dla future cases (np. ON CONFLICT DO NOTHING jeśli
   kiedyś dodamy push-side dedup w v0.2).
6. Return client-side `Vec<Uuid>` w input order — to są te same UUIDy
   które wstawiliśmy do DB, w identycznej kolejności jak `payloads` input.

```rust
// Pusher::push_batch (sketch)
async fn push_batch<T: Serialize>(
    &self, tx: &mut PgConnection, payloads: &[T],
) -> Result<Vec<Uuid>, PushError> {
    // (validations + encode loop omitted)
    let public_ids: Vec<Uuid> = payloads.iter().map(|_| Uuid::now_v7()).collect();
    let payload_bytes: Vec<Vec<u8>> = /* encode each */;

    let rows = sqlx::query!(
        "INSERT INTO pgwq.jobs (queue, payload, public_id)
         SELECT $1, payload, public_id
         FROM unnest($2::bytea[], $3::uuid[]) AS u(payload, public_id)",
        &self.queue, &payload_bytes, &public_ids,
    )
    .execute(&mut **tx)
    .await?
    .rows_affected();

    if rows as usize != public_ids.len() {
        return Err(PushError::BatchPartial {
            inserted: rows as usize,
            expected: public_ids.len(),
        });
    }
    Ok(public_ids)
}
```

Return: `Vec<Uuid>` w **input order**. Drugi wariant
`push_batch_at(tx, &[(T, Option<DateTime<Utc>>)])` jako follow-up
v0.2 jeśli user supply use case dla per-item scheduled run_at.

API:

```rust
pub struct Pusher<C = JsonCodec> {
    queue: String,
    codec: C,
}

impl Pusher<JsonCodec> {
    pub fn new(queue: impl Into<String>) -> Self;
}

impl<C: Codec> Pusher<C> {
    pub fn with_codec<C2: Codec>(self, codec: C2) -> Pusher<C2>;

    pub async fn push<T: Serialize>(
        &self, tx: &mut PgConnection, payload: &T,
    ) -> Result<Uuid, PushError>;

    pub async fn push_at<T: Serialize>(
        &self, tx: &mut PgConnection, payload: &T, run_at: DateTime<Utc>,
    ) -> Result<Uuid, PushError>;

    pub async fn push_batch<T: Serialize>(
        &self, tx: &mut PgConnection, payloads: &[T],
    ) -> Result<Vec<Uuid>, PushError>;
}
```

### Public API surface (`pub use` w `lib.rs`)

```rust
pub use crate::backoff::BackoffPolicy;
pub use crate::codec::{Codec, JsonCodec};
pub use crate::error::{BuildError, JobError, PurgeError, PushError, ShutdownError};
pub use crate::pusher::Pusher;
pub use crate::purge::{purge_dead, purge_done, queue_stats, QueueStats};
pub use crate::worker::{
    JobContext, PanicPolicy, Stats, Worker, WorkerBuilder, WorkerHandle,
};

pub mod limits;  // public module — users reference constants

// Migracja re-export:
pub fn migrator() -> sqlx::Migrator { sqlx::migrate!("./migrations") }
```

Bez tego `use pg_work_queue::{Worker, JobError, ...}` w przykładach
głównych nie skompiluje się (S20 z review).

### Codec

```rust
pub trait Codec: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Self::Error>;
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Self::Error>;
}

pub struct JsonCodec;

impl Codec for JsonCodec {
    type Error = serde_json::Error;
    fn encode<T: Serialize>(&self, v: &T) -> Result<Vec<u8>, _> { serde_json::to_vec(v) }
    fn decode<T: DeserializeOwned>(&self, b: &[u8]) -> Result<T, _> { serde_json::from_slice(b) }
}
```

Default `JsonCodec`. User implementuje swój `Codec` jeśli chce CBOR /
bincode / etc. `serde_json` dodajemy do deps.

## Retry backoff policy

```rust
pub enum BackoffPolicy {
    Linear {
        base: Duration,
        increment: Duration,
        cap: Duration,
    },
    Exponential {
        base: Duration,
        factor: f64,    // > 1.0
        cap: Duration,
        jitter: f64,    // ratio 0.0..=1.0
    },
}

impl BackoffPolicy {
    pub fn exponential(base: Duration, factor: f64, cap: Duration, jitter: f64) -> Self;
    pub fn fixed(d: Duration) -> Self;   // → Linear { base: d, inc: 0, cap: d }
    pub fn next(&self, attempt: u32) -> Duration {
        let raw_secs = match self {
            BackoffPolicy::Linear { base, increment, cap } => {
                base.as_secs_f64() + increment.as_secs_f64() * attempt as f64
            }
            BackoffPolicy::Exponential { base, factor, cap, .. } => {
                // factor.powi(attempt as i32) może overflow'ować do INFINITY
                // przy dużych attempt + factor>2 (np. factor=10, attempt=400).
                // Duration::from_secs_f64(INFINITY) PANICUJE — clamp przed
                // konwersją. `panic = "deny"` w Cargo.toml dotyczy explicit
                // panic! macro, NIE runtime stdlib panics.
                base.as_secs_f64() * factor.powi(attempt as i32)
            }
        };
        let cap_secs = self.cap().as_secs_f64();
        // Clamp na non-finite (NaN/INF) + cap. Plus saturating max(0.0).
        let clamped_secs = if raw_secs.is_finite() {
            raw_secs.min(cap_secs).max(0.0)
        } else {
            cap_secs
        };
        // Jitter zastosowany do clamped value (jitter też może rozszerzyć
        // poza cap, ale bounded by ratio ≤ 1.0).
        let jittered = self.apply_jitter(clamped_secs);
        Duration::from_secs_f64(jittered.min(cap_secs).max(0.0))
    }
}
```

`Fixed` variant usunięty (degenerate Linear). `fixed()` constructor
zachowany dla convenience.

Default: `Exponential { 1s, 2.0, 5min, 0.2 }` → ~1s, 2s, 4s, 8s, ... 5min
(±20% jitter).

Jitter ważny przy thundering herd (10 jobs fails równocześnie → bez jittera
wszystkie wracają w tym samym ticku).

User per-call override: `Err(JobError::Retry { retry_in: Some(d), .. })`.
**Clamp:** `d ∈ [max(poll_interval, 100ms), 24h]`. Powód dolnego ograniczenia:
`Duration::ZERO` w retry-loop'ującym handlerze + `poll_interval=10ms` =
~100×N retries/sec hammering DB (S1 z review). Cliampujemy do co najmniej
100ms; przy clampie emit `warn!(requested_ms, applied_ms, "retry_in clamped")`.
Górny clamp 24h zapobiega `Duration::MAX` jako footgun.

## Error semantics & handling

### Public error enums

```rust
#[derive(thiserror::Error, Debug)]
pub enum PushError {
    // Caller bugs — NOT retriable. Don't propagate to backoff loops.
    #[error("payload too large: {size} bytes > {max}")]
    PayloadTooLarge { index: usize, size: usize, max: usize },
    #[error("batch too large: {size} > {max}")]
    BatchTooLarge { size: usize, max: usize },
    #[error("batch aggregate payload exceeds {max} bytes")]
    BatchPayloadTooLarge { total_bytes: usize, max: usize },
    #[error("batch is empty")]
    BatchEmpty,
    #[error("queue name invalid: {0}")]
    QueueNameInvalid(String),
    #[error("codec error: {0}")]
    Codec(#[source] BoxError),
    #[error("codec error at batch index {index}: {source}")]
    BatchCodec { index: usize, #[source] source: BoxError },

    // Deterministic DB errors (CHECK violation, FK, integrity) — also caller bug.
    #[error("database constraint violation: {0}")]
    Constraint(#[source] sqlx::Error),
    // Transient DB errors (connection, IO) — caller may retry.
    #[error("database error (transient): {0}")]
    Transient(#[source] sqlx::Error),
    // Defense-in-depth: rows_affected != expected po INSERT. Pod current
    // single-statement INSERT...SELECT...unnest, CHECK violation rollback'uje
    // całą statement → unreachable. Wariant zarezerwowany na future cases
    // (np. ON CONFLICT DO NOTHING gdyby push-side dedup pojawił się w v0.2).
    #[error("batch partial: inserted {inserted} of {expected} expected rows")]
    BatchPartial { inserted: usize, expected: usize },
}

impl PushError {
    /// True if calling code may retry (transient infra). False if request itself
    /// is invalid (caller bug — fix the input, don't loop).
    pub fn is_retriable(&self) -> bool {
        matches!(self, PushError::Transient(_))
    }
}

impl From<sqlx::Error> for PushError {
    fn from(e: sqlx::Error) -> Self {
        // Classify SQLSTATE 23xxx (integrity constraint violation) as deterministic.
        if let sqlx::Error::Database(db) = &e {
            if db.code().as_deref().is_some_and(|c| c.starts_with("23")) {
                return PushError::Constraint(e);
            }
        }
        PushError::Transient(e)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BuildError {
    #[error("poll_interval must be >= {min:?}")]
    PollIntervalTooShort { min: Duration },
    #[error("concurrency must be >= 1")]
    ConcurrencyZero,
    #[error("pool too small: need max_connections >= concurrency × 2 + 2 (handler + mark_* per concurrent slot + poll + reaper); have {actual}, need {required}. Use pool.options().get_max_connections() (NOT pool.size() — lazy, 0 dla świeżego pool'a).")]
    PoolTooSmall { actual: u32, required: u32 },
    #[error("max_attempts must be >= 1")]
    MaxAttemptsZero,
    #[error("lease_timeout must be >= 1s (MIN_LEASE_TIMEOUT)")]
    LeaseTimeoutBelowFloor,
    #[error("lease_timeout must be >= 5 * poll_interval")]
    LeaseTimeoutTooShort,
    #[error("lease_timeout + reaper_interval combination impossible: lease={lease:?}, need reaper <= lease/2 but >= 1s, so lease must be >= 2s")]
    LeaseTimeoutTooShortForReaper { lease: Duration },
    #[error("handler_timeout must be >= {min:?} (MIN_HANDLER_TIMEOUT)")]
    HandlerTimeoutBelowFloor { min: Duration },
    #[error("handler_timeout ({handler:?}) + 1s must be <= lease_timeout ({lease:?}); mark_retry needs margin to commit before lease expires")]
    HandlerTimeoutTooLong { handler: Duration, lease: Duration },
    #[error("mark_timeout must be >= 100ms")]
    MarkTimeoutTooShort,
    #[error("mark_timeout ({mark:?}) must be <= lease_timeout − handler_timeout ({budget:?}); else mark_* może przekroczyć lease before retry-via-reaper takes over")]
    MarkTimeoutTooLong { mark: Duration, budget: Duration },
    #[error("reaper_interval must be <= lease_timeout / 2")]
    ReaperIntervalTooLong,
    #[error("queue name invalid: {0}")]
    QueueNameInvalid(String),
    #[error("handler not set")]
    HandlerMissing,
}

#[derive(thiserror::Error, Debug)]
pub enum ShutdownError {
    #[error("worker already shut down")]
    AlreadyShutdown,
    #[error("worker failed with fatal error: {0}")]
    Fatal(sqlx::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum PurgeError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}
```

### Handler signature & error semantics

Handler zwraca **`Result<(), JobError>`** — idiomatic Rust, kompozycja
z `?` operatorem na user error types przez `From` impl.

```rust
pub type HandlerResult = Result<(), JobError>;

#[derive(thiserror::Error, Debug)]
pub enum JobError {
    /// Job miał transient issue — library decyduje retry-vs-dead na
    /// bazie `attempts < max_attempts`. Optional `retry_in` override
    /// backoff policy dla tego konkretnego retry.
    #[error("retry: {reason}")]
    Retry {
        reason: String,
        retry_in: Option<Duration>,
    },
    /// Job permanently can't proceed — mark_dead direct (bypass retry
    /// budget). Użyj gdy retry nigdy nie pomoże (invalid input,
    /// permissions denied, etc.).
    #[error("abort: {reason}")]
    Abort { reason: String },
}

impl JobError {
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry { reason: reason.into(), retry_in: None }
    }
    pub fn retry_in(reason: impl Into<String>, retry_in: Duration) -> Self {
        Self::Retry { reason: reason.into(), retry_in: Some(retry_in) }
    }
    pub fn abort(reason: impl Into<String>) -> Self {
        Self::Abort { reason: reason.into() }
    }
}
```

User implementuje `From<MyError> for JobError` żeby propagować przez `?`.
**Ważne:** zgodnie z §"Sensitive data warning" — log full error
internally (gdzie operator może zobaczyć stack), pass **opaque category**
do `JobError`. Przykład:

```rust
impl From<sqlx::Error> for JobError {
    fn from(e: sqlx::Error) -> Self {
        // Log full error to operator log only — never persist e.to_string()
        // bo może zawierać connection strings, payload fragmenty, PII.
        tracing::error!(error = ?e, "handler db error");
        JobError::retry("db_error")  // opaque category, no e.to_string()
    }
}

impl From<StripeError> for JobError {
    fn from(e: StripeError) -> Self {
        tracing::error!(error = ?e, "handler stripe error");
        match e {
            StripeError::CardDeclined(_) => JobError::abort("card_declined"),
            _ => JobError::retry("stripe_error"),
        }
    }
}
```

Library mapping na DB state:

| Handler return | DB action |
|---|---|
| `Ok(())` | `mark_done` (fencing-guarded) |
| `Err(JobError::Retry { reason, retry_in })` | `mark_retry` z `run_at = now() + retry_in.unwrap_or_else(\|\| backoff.next(attempts))`; if `attempts >= max_attempts` → upgrade do `mark_dead` |
| `Err(JobError::Abort { reason })` | `mark_dead` natychmiast (bypass retry budget) |
| Timeout (`handler_timeout` elapsed) | future dropped na `.await` point; library traktuje jak `Err(JobError::Retry { reason: "handler_timeout", retry_in: None })` → `mark_retry` z backoff'em policy; if `attempts >= max_attempts` → upgrade do `mark_dead`. Side-effects already-issued **survive cancellation** (mitigacja: `ctx.idempotency_key`). |
| Panic | per `PanicPolicy`: `Retry` → `mark_retry`; `Dead` → `mark_dead` z `reason = "panic: <msg>"` |
| Codec decode error (przed wywołaniem handlera) | `mark_dead` z `reason = "payload decode: <err>"` (handler nigdy nie called — retry by miał ten sam decode-error). **Jeśli mark_dead samo zawiedzie** (DB error) — `warn!` + leave row `running`; reaper recover'i przez lease expiration → kolejne attempts spalą `max_attempts` → final `dead` z `last_error = 'lease_expired_max_attempts'`. Bounded loop, ale slow. |

Pełny handler example z `?`:

```rust
.handler(|task: ChargeTask, ctx: JobContext| async move {
    let user = db.find_user(task.user_id).await?;             // sqlx::Error → Retry
    if user.banned {
        return Err(JobError::abort("user banned"));
    }
    stripe.charge(task.amount, &ctx.idempotency_key.to_string()).await?;  // StripeError → Retry/Abort
    db.record_charge(task.user_id, task.amount).await?;
    Ok(())
})
```

### Library-side string truncation

Wszystkie user-supplied `reason` strings (`JobError::Retry/Abort`, panic
message) **truncate'owane na library boundary** do `limits::MAX_LAST_ERROR_LEN`
**character units** (nie bajtów — Postgres `length(TEXT)` zwraca chars).
Trim by char boundary safe (rust-safe-string-truncation skill):

```rust
fn trim_reason(s: &str) -> String {
    s.chars().take(limits::MAX_LAST_ERROR_LEN).collect()
}
```

DB CHECK na `length(last_error) <= 8192` jako backstop.

Centralizowane przez `fmt_err_trimmed(e: &dyn Error)`. Wszystkie sites
(mark_retry reason, mark_dead reason, panic message extraction, codec
decode error) używają tej samej fn.

### Sensitive data warning

`reason` field w `JobError::Retry/Abort` jest **persisted w `last_error`
column + emitted via `tracing::warn!`**. Library nie sanitizuje. Handlers
muszą uważać:

- `e.to_string()` może zawierać connection strings, API tokens, fragmenty
  payload, PII (HIPAA/GDPR).
- Recommended pattern: log **full error internally**, pass **opaque
  category** do `JobError`:
  ```rust
  Err(e) => {
      tracing::error!(job.id = ctx.id, error = ?e, "internal handler error");
      Err(JobError::retry("internal_error"))  // not e.to_string()
  }
  ```
- Library-side: per-row `state.transition` events z `reason` w demoted
  do `DEBUG`; `ERROR` level events (dead-letter) zawierają tylko
  `reason_length: usize` + `reason_present: bool`.

To jest **handler responsibility** — library zapewnia narzędzie, użycie
jest po stronie usera. Doc-comment na `JobError` warianty będzie miał
ten warning.

### Sqlx error classification

```rust
fn is_fatal_sqlx(e: &sqlx::Error) -> bool {
    matches!(e,
        // Pool / runtime — no recovery possible.
        sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Configuration(_)
        | sqlx::Error::Migrate(_)
        // Schema/wire-level — retry won't fix; loud crash forces operator
        // to redeploy / re-migrate. Apalis anti-pattern was infinite-warn-
        // loop on missing schema.
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_)
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::Protocol(_)
    )
}
```

Fatal → worker self-shutdown via cancellation token, surfaced przez
`ShutdownError::Fatal(Arc<sqlx::Error>)` w `WorkerHandle::shutdown` result.
Transient (`Database` / `Io` / `Tls` / `PoolTimedOut`) → `warn!` + retry
next tick.

`PoolTimedOut` jest **transient** (worker temporarily over-subscribed),
ale persistent PoolTimedOut suggeruje misconfiguration (W8: concurrency
+ 3 ≤ pool.size). Logged at `warn!`.

## Observability spec

Tracing-first observability. Każda critical operacja ma:
- **Span** wokół całej operacji z `name` i podstawowymi attrs.
- **Event** na każde state-machine transition.
- **Worker identity attribute** dla multi-replica debugging.

### Span vocabulary

| Span name | When | Attrs |
|---|---|---|
| `pgwq.poll_tick` | każdy poll cycle | `worker.id`, `queue`, `batch_size`, `claimed` (after) |
| `pgwq.claim_batch` | claim SQL query | `worker.id`, `queue`, `batch_size`, `rows_returned`, `duration_ms` |
| `pgwq.handle_job` | per handler invocation | `worker.id`, `queue`, `job.id`, `job.public_id`, `job.attempt` |
| `pgwq.mark_done` | mark_done SQL | `worker.id`, `job.id`, `rows_affected` |
| `pgwq.mark_retry` | mark_retry SQL | `worker.id`, `job.id`, `job.attempt`, `retry_in_ms`, `rows_affected` |
| `pgwq.mark_dead` | mark_dead SQL | `worker.id`, `job.id`, `job.attempts`, `rows_affected` |
| `pgwq.reap_tick` | reaper cycle | `worker.id`, `queue`, `reaped_total`, `reaped_dead`, `reaped_retry` |
| `pgwq.push` | Pusher::push | `queue`, `public_id`, `payload_size` |
| `pgwq.push_batch` | Pusher::push_batch | `queue`, `count`, `total_payload_size` |
| `pgwq.purge` | purge_done / purge_dead | `status`, `age`, `deleted` |
| `pgwq.shutdown` | WorkerHandle::shutdown | `worker.id`, `timeout_ms`, `completed`, `failed`, `aborted` |

### State transition events

Każde state transition emituje structured event przez **`emit_transition`
single-source helper** — wszystkie 5+ call sites (mark_done, mark_retry,
mark_dead, reaper transitions, purge) muszą tej fn używać żeby attrs
nie drift'owały:

```rust
pub(crate) enum TransitionSource { Worker, Reaper, Purge }

pub(crate) struct TransitionCtx<'a> {
    pub job_id: i64,
    pub public_id: Uuid,
    pub queue: &'a str,
    pub attempts: u32,
    pub source: TransitionSource,
    pub reason: Option<&'a str>,    // not logged at ERROR level
    pub lost_race: bool,            // mark_* 0-rows-affected
}

pub(crate) fn emit_transition(from: Option<&str>, to: &str, ctx: TransitionCtx<'_>) {
    let level = match to {
        "dead" => tracing::Level::ERROR,
        "done" | "awaiting_retry" => tracing::Level::INFO,
        _ => tracing::Level::DEBUG,
    };
    // OTel convention: event name jako tracing `target`, nie message body.
    // Plus `messaging.*` semantic conventions.
    tracing::event!(
        target: "pgwq.state.transition",
        level,
        job.id = ctx.job_id,
        job.public_id = %ctx.public_id,
        queue = ctx.queue,
        job.attempts = ctx.attempts,
        status.from = from.unwrap_or("none"),
        status.to = to,
        source = ?ctx.source,
        lost_race = ctx.lost_race,
        // reason only at INFO/DEBUG; ERROR gets metadata only (PII safety).
        reason_length = ctx.reason.map(|r| r.len()),
        reason_present = ctx.reason.is_some(),
        reason = ctx.reason,  // tracing.event handles None gracefully
    );
}
```

Tabela:

| From → To | Level | Event volume / strategy |
|---|---|---|
| → `queued` (single push) | `info` | 1 event per `Pusher::push` |
| → `queued` (push_batch) | `info` aggregate + `trace` per-row | **1 summary event** (`count`, `first_public_id`, `last_public_id`) — per-row would log-DoS przy 10k batchach. Per-row na `trace` jeśli operator chce drill-down. |
| `queued` → `running` | `debug` | 1 event per row claimed |
| `awaiting_retry` → `running` | `debug` | 1 event per row claimed |
| `running` → `done` | `info` | 1 event per mark_done success |
| `running` → `awaiting_retry` | `info` | mark_retry (worker) or reaper |
| `running` → `dead` | **`error`** | mark_dead (max_attempts) — **dead-letter**; `reason_length: usize` + `reason_present: bool` attrs (no `reason` body at ERROR level for PII reasons) |
| `dead`/`done` → ∅ | `info` aggregate | purge_done / purge_dead: 1 event per call z `deleted: count` |

### Worker identity

Każdy `Worker::start()` przypisuje `worker.id = Uuid::now_v7()` używany
jako `worker.id` attribute w każdym span/event. Pozwala multi-replica
debug: "który replikator claim'ował job X?".

### OpenTelemetry compatibility

Span names i attrs zgodne z OpenTelemetry messaging semantic conventions
gdzie pasują:
- `messaging.system` = `"pg_work_queue"`
- `messaging.destination.name` = queue name
- `messaging.operation` = `"send"` (push) / `"receive"` (claim) / `"process"` (handle)
- `messaging.message.id` = public_id

User z `tracing-opentelemetry` integracją dostaje OTel metrics/traces
out-of-the-box.

### Tracing initialization w testach

`tests/common/mod.rs` zawiera `init_tracing()` helper z env-filter
`RUST_LOG=pgwq=debug,test=info` (default). Każdy test file `mod.rs` calls
go w `#[tokio::test]` setup.

## Shutdown semantics

`WorkerHandle::shutdown(timeout: Duration) -> Result<Stats, ShutdownError>`:

**`WorkerHandle` MUSI trzymać `AbortHandle` poll loop'a i reaper'a osobno
od `state.tasks: JoinSet` (handlery).** Bez tego: `tokio::spawn(poll_loop)`
+ drop handle = detached task per Anti-pattern #12, shutdown(timeout) zwraca
ale poll/reaper żyją w runtime aż naturalnie skończą `.await`. Konkret:

```rust
pub struct WorkerHandle {
    state: Arc<WorkerState>,
    poll_abort: tokio::task::AbortHandle,    // poll loop task
    reaper_abort: tokio::task::AbortHandle,  // reaper loop task
    poll_join: tokio::task::JoinHandle<()>,  // dla graceful wait
    reaper_join: tokio::task::JoinHandle<()>,
}

impl WorkerHandle {
    pub async fn shutdown(self, timeout: Duration) -> Result<Stats, ShutdownError> {
        // 1. Soft signal: cancellation token → poll/reaper SQL await wrapped
        //    w select widzą cancel, exit gracefully na najbliższym .await point.
        self.state.shutdown.cancel();

        // 2. Soft drain: czekaj timeout/2 na poll+reaper graceful exit.
        //    Pod normalnych warunkach to <100ms (select wychodzi natychmiast).
        let half = timeout / 2;
        let _ = tokio::time::timeout(half, async {
            let _ = self.poll_join.await;
            let _ = self.reaper_join.await;
        }).await;

        // 3. Hard abort: poll/reaper jeśli nadal żyją (np. SQL drop nie
        //    rozwiązał się w czasie — rzadkie ale możliwe pod pool deadlock).
        self.poll_abort.abort();
        self.reaper_abort.abort();

        // 4. Handler drain: state.tasks JoinSet, drugi half timeout'a.
        let drain_deadline = tokio::time::Instant::now() + (timeout - half);
        while let Some(_) = tokio::time::timeout_at(drain_deadline,
            self.state.tasks.lock().await.join_next()).await.ok().flatten() {}

        // 5. Hard abort handlers via cascade: state.tasks.abort_all() →
        //    handle_job unwind → local JoinSet drop → handler cancelled
        //    (Anti-pattern #12 cascade).
        self.state.tasks.lock().await.abort_all();
        while let Some(_) = self.state.tasks.lock().await.join_next().await {}

        // 6. Build Stats.
        self.collect_stats()
    }
}
```

Krok 1 daje **immediate** observability shutdown'u (poll/reaper widzą cancel
przy najbliższym `.await` — claim_batch/reap SQL future drop ⇒ sqlx
server-side cancel sygnał). Krok 3 jest defense-in-depth dla case'a kiedy
SQL wisi w pool wait beyond half-timeout.

```rust
pub struct Stats {
    pub completed: u64,       // handlers returned Ok(()) (mark_done ack'd, rows_affected > 0)
    pub failed: u64,          // handlers returned Err(JobError::Retry/Abort) OR panicked
    pub timed_out: u64,       // handlers cancelled via handler_timeout elapsed; mark_retry'd z reason="handler_timeout"
    pub mark_timed_out: u64,  // mark_* SQL przekroczył mark_timeout (#3); leave row 'running', reaper przejmie
    pub aborted: u64,         // handlers aborted via JoinSet::abort_all (worker SHUTDOWN timeout, nie handler_timeout)
    pub fenced_out: u64,      // mark_* returned 0 rows (lease lost to reaper)
    pub pending_recovery: u64,// rows still 'running' at drain (reaper will recover via lease_timeout)
}
```

Implementacja: `AtomicU64` per field z `Ordering::Relaxed` (one writer
per field; reader just snapshots at shutdown). `last_fatal:
std::sync::OnceLock<Arc<sqlx::Error>>` w `WorkerState` — stdlib set-once
primitive bez poison semantics (Mutex z `unwrap_used = deny` byłby
awkward). First fatal wins:

```rust
let _ = state.last_fatal.set(Arc::new(e)); // Err if already set, ignored
// At shutdown:
state.last_fatal.get().cloned()  // Option<Arc<sqlx::Error>>
```

`ShutdownError::Fatal(Arc<sqlx::Error>)` propaguje to user'owi.

**Aborted handlers semantics:** ich rows zostają `status='running'` z
aktualnym `lease_token`. Reaper-side recovery (po `lease_timeout`) flipuje
do `awaiting_retry` lub `dead` (na bazie attempts). Czyli **graceful
shutdown po timeout zachowuje correctness, kosztem opóźnionego retry
(do `lease_timeout`)**.

**Edge case:** handler `mark_done` w-locie podczas `abort_all`. Jeśli
SQL UPDATE committed server-side ale tokio future cancelled przed
zwrotem do worker'a: row jest `done`, handler nie zliczany do completed.
Stats może być off by 1-2. To fundamentalny limit non-cooperative abort.
**Dokumentowane jako accepted.**

## Test strategy (TDD-first, każdy case testcontainers)

**Wszystkie behavioral testy używają `testcontainers` PG18.** Każdy
test spawn'uje swój własny pool, applikuje migracje, runs scenariusz,
cleanup container. Test parallel-safe (każdy test = osobny container
albo osobny schema w shared container — TBD per fixture cost).

`tests/common/mod.rs` zawiera:
- `init_tracing()` — env-filter setup
- `pg18_pool()` → `(PgPool, testcontainers::Container<Postgres>)` —
  spawn + migrate
- helper assertions: `assert_job_status`, `assert_job_attempts`, etc.

### Behavioral tests (każdy knob, 2 wartości)

1. `tests/poll_interval_behavior.rs` — pickup latency 100ms vs 500ms.
2. `tests/concurrency_behavior.rs` — N=1 sequential vs N>1 parallel.
3. `tests/max_attempts_behavior.rs` — fail loop, dead po N attempts.
4. `tests/lease_timeout_behavior.rs` — reaper z 1s vs 10s.
5. `tests/handler_timeout_behavior.rs` — handler `tokio::sleep(5s)`; `handler_timeout=1s` → `mark_retry { reason: "handler_timeout" }` po ~1s; `handler_timeout=10s` → `mark_done` po ~5s. Mierzy DB-observable transition + timing.
6. `tests/reaper_interval_behavior.rs` — reaper z 1s vs 5s tick.
7. `tests/batch_size_behavior.rs` — claim shape przy 10 vs 50.
   Plus: weryfikuje że `claim_batch(N)` z N>free_permits zwraca rows = free_permits (permit-gated claim, Anti-pattern #13).
8. `tests/poll_acquires_permits_before_claim.rs` — push 100 jobów,
   `concurrency=4`, `batch_size=50`, każdy handler `tokio::sleep(1s)`.
   Captures per-row delta `(first_attempted_at, handler_start_real_time)` przez
   handler-side instrumentation. Assert: dla **każdego** row delta < 100ms
   (handler spawn natychmiast po claim). Regresja Anti-pattern #13 —
   bez permit-gated claim test by widział delta rosnące do (batch_size/concurrency-1) × handler_duration.
9. `tests/scheduled_run_at.rs` — push z run_at = now()+2s.
10. `tests/retry_backoff_behavior.rs` — Fixed vs Exponential run_at delta.
11. `tests/retry_in_override.rs` — `Err(JobError::Retry { retry_in: Some(5s), .. })`.
12. `tests/panic_policy_behavior.rs` — PanicPolicy::Retry vs ::Dead.
13. `tests/codec_swappable.rs` — JsonCodec vs custom CborCodec.

### Crash safety / correctness

14. `tests/skip_locked_no_double_claim.rs` — 2 workery, 100 jobs, suma = 100.
15. `tests/stale_running_reaped.rs` — manual UPDATE stale, reaper flipuje.
16. `tests/reaper_to_dead_when_max_attempts.rs` — reaper z attempts=N+1
    flipuje do dead (nie awaiting_retry).
17. `tests/reaper_single_cte_no_race.rs` — verify że single-CTE reaper
    nie produkuje (running, awaiting_retry, dead) inconsistencies pod
    concurrent claim+reap (regresja po W1 z review).
18. `tests/fencing_token_no_double_run.rs` — claim → manual stale → reaper
    → stary handler mark_done ze starym tokenem → 0 rows.
19. `tests/shutdown_graceful.rs` — handler trwa krócej niż timeout, drain OK.
20. `tests/shutdown_abort_after_timeout.rs` — handler trwa dłużej, abort +
    reaper recovery + correctness.
21. `tests/shutdown_aborts_handler_no_leak.rs` — handler robi `sleep(60s)` +
    side-effect counter (np. AtomicU64). Worker shutdown(timeout=1s) → abort_all.
    Po 5s sprawdza że counter NIE wzrósł i task nie żyje (verify że lokalny
    JoinSet cascade-aborted, nie detached jak `tokio::spawn` by zrobił). Regresja
    Anti-pattern #12.
22. `tests/shutdown_cancels_poll_loop.rs` — mid-poll-sleep shutdown
    wychodzi natychmiast.
23. `tests/migrator_schema.rs` — schema CREATE'd, CHECKs fire, fillfactor
    w `pg_class.reloptions`.
24. `tests/reaper_no_advisory_lock_leak.rs` — 3 reapery parallel,
    `pg_locks` clean post-test.
25. `tests/fatal_sqlx_triggers_shutdown.rs` — PgPool close mid-poll,
    worker self-shutdown z error w stats.

### Resource limits

26. `tests/resource_limits.rs` — payload > 1MiB rejected, batch > 10k
    rejected, last_error truncate, queue name length CHECK.
27. `tests/builder_validation.rs` — wszystkie `BuildError::*` variants
    rzucane na nieprawidłowy config (włącznie z `HandlerTimeoutBelowFloor`,
    `HandlerTimeoutTooLong`).

### Idempotency / at-least-once

28. `tests/idempotency_key_stable_across_retries.rs` — handler fail 3x,
    captured `ctx.idempotency_key` identical każdy attempt.
29. `tests/at_least_once_semantics.rs` — simulate mark_done loss
    (manual rollback), reaper recover, handler called 2x ten sam job
    z tym samym idempotency_key.

### Push & purge

30. `tests/push_batch_throughput.rs` — 1000 single push vs batch,
    batch ≥ 5x szybszy.
31. `tests/push_batch_order_preserved.rs` — push_batch zwraca client-side
    Vec<Uuid> w input order. Insert robi rzędy w DB (kolejność wstawiania
    nieistotna, public_id stabilne); test weryfikuje że **return value
    Rust function'a** matches input order, **bez polegania na RETURNING**
    (Postgres docs explicit'ie unspecified order).
32. `tests/purge_done_chunked.rs` — 50k done rows, purge_done(0s)
    deletuje wszystkie chunkami.
33. `tests/purge_dead_separate.rs` — purge_done nie tyka dead, vice versa.

### Observability

34. `tests/tracing_events_emitted.rs` — capture tracing events via
    custom subscriber, assert że dla każdej transition emitted event
    z expected attrs (job.id, status.from, status.to).
35. `tests/dead_letter_logged.rs` — job hits max_attempts → reaper or
    handler emits `tracing::error!` z dead-letter context.

### No-DB / unit

36. `tests/backoff_policy_unit.rs` — `BackoffPolicy::next(attempt)`.
37. `tests/codec_json_roundtrip.rs` — Serialize → Vec<u8> → Deserialize.
38. `tests/sqlx_error_classification.rs` — `is_fatal_sqlx` cases.
39. `tests/truncate_safe_string.rs` — UTF-8 boundary safety w trim.

### v3.5 regression tests

40. `tests/max_attempts_rolling_deploy.rs` — push job. Worker A z `max_attempts=3`
    claim'uje (stamp'uje max=3 w wierszu), handler hangsze, lease wygasa.
    Worker B z `max_attempts=5` startuje (rolling deploy) i jego reaper widzi
    row jako stale. Assert: row → `dead` (bo `j.max_attempts=3` z claim'a A,
    nie B's max=5). Werdykt deterministyczny. Regresja #1 v3.5.
41. `tests/shutdown_immediate_with_pool_starvation.rs` — pool wyczerpany (user
    trzyma wszystkie conn'y), poll loop / reaper czekają na acquire. Worker
    `shutdown(timeout=2s)`. Assert: shutdown wraca ≤ 200ms (cancel sygnał
    interceptowany przez `tokio::select` na SQL await, sqlx future drop =
    server-side cancel). Bez fix: shutdown czeka pool acquire_timeout
    (~30s default). Regresja #2 v3.5.
42. `tests/mark_timeout_under_pool_pressure.rs` — `pool.max_connections = concurrency × 2 + 2`,
    handlery + user kod celowo wyczerpują pool. Handler kończy `Ok(())`,
    mark_done nie dostaje conn'a, `mark_timeout=500ms` → fire. Assert:
    `Stats::mark_timed_out > 0`, row pozostaje `running`, reaper przejmuje
    po `lease_timeout`. Regresja #3 v3.5.
43. `tests/backoff_extreme_attempt_no_panic.rs` — `BackoffPolicy::Exponential
    { factor: 10.0, cap: 24h, .. }`. Wywołaj `next(400)` → assert że zwraca
    `cap` (24h), nie panic. `next(u32::MAX)` też. Regresja #4 v3.5.
44. `tests/builder_validation_fresh_pool.rs` — `PgPool::connect(url).await?`
    bezpośrednio potem `Worker::builder(pool).concurrency(4).build()`. Pool
    `size()` jest 0 (lazy), ale `options().get_max_connections() = 10`
    (default). Assert: build sukces. Bez fix #6: build fail z `PoolTooSmall
    { actual: 0 }`. Regresja #6 v3.5.
45. `tests/reaper_drains_backlog_adaptive.rs` — 10k rows manual UPDATE'd do
    `running` z lease_expires_at='past'. Reaper startuje. Assert: zdrainowane
    w < 30s (adaptive ticking; bez fix: 10k / 1024 = 10 ticks × 75s = 12.5min).
    Regresja #7 v3.5.

### Anti-pattern guard (rule, nie test)

Każdy test musi mierzyć **observable behavior** widoczny w DB lub przez
tracing-subscriber capture. Test który asserts na shape config (np.
`assert_eq!(builder.poll_interval, 500ms)`) jest **PR-blocker**.

## Implementation phases

### Faza 0 — repo init ✅ DONE
Skeleton, Cargo.toml z pinned deps, initial migration verified na PG18.
Commit `d9c4dc7`.

### Faza 1 — Pusher + codec + migrator

- `Codec` trait + `JsonCodec` impl.
- `Pusher::new` (infallible — matches API sketch §"Public API"; queue-name
  validation is fail-late w `push`/`push_batch`, zwraca
  `PushError::QueueNameInvalid`), `with_codec`, `push`, `push_at`,
  `push_batch`.
- `pg_work_queue::migrator()` re-export `sqlx::migrate!("./migrations")`.
- Resource validation (per-item + aggregate batch bytes).
- `PushError` enum z `BatchPayloadTooLarge` + `BatchCodec { index, source }`
  + per-item `PayloadTooLarge { index }`.
- Tests: `migrator_schema.rs`, `migrator_pg17_fails_loud.rs` (S5),
  `push_batch_throughput.rs`, `push_batch_order_preserved.rs`,
  `resource_limits_push.rs`, `codec_json_roundtrip.rs`,
  `codec_swappable.rs`, `truncate_safe_string.rs`.

### Faza 2 — claim_batch SQL + Job<T> + JobContext

- `claim_batch` SQL function.
- `pub struct Job<T>` + `pub struct JobContext` (includes idempotency_key).
- Codec decode na claim time; decode error → mark_dead **z catch_unwind
  wokół decode call** (custom Codec impl może panicować).
- Tests: `skip_locked_no_double_claim.rs`, `batch_size_behavior.rs`,
  `codec_decode_error_marks_dead.rs`, `codec_panic_marks_dead.rs`.

### Faza 3 — single-shot worker + mark queries z fencing

- `Worker::tick_once(...)` — fetch batch, run handlers sequential,
  mark_done/retry/dead z fencing.
- Library-side char-safe truncation w `last_error` (`fmt_err_trimmed`).
- `unreachable_pub` guards.
- Tests: end-to-end smoke, `fencing_token_no_double_run.rs`,
  `builder_validation.rs` (cross-knob rules), `mark_done_loses_to_reaper.rs`
  (W6 — fenced_out stat).

### Faza 4 — poll loop + concurrency + handler_timeout + worker identity

- `Worker::start()` → spawn poll loop + JoinSet. Schema check przy
  `start()` (SELECT 1 FROM pgwq.jobs LIMIT 0).
- `worker.id = Uuid::now_v7()` w span attrs.
- `CancellationToken` plumbing. Poll loop **kompletuje spawn batch
  unconditionally** po claim (W9 fix — żeby claimed rows nie były
  abandoned przy shutdown mid-batch).
- `is_fatal_sqlx()` classification z schema-level error variants
  (W3).
- **`handle_job` wraps handler future w `tokio::time::timeout(handler_timeout, ...)`
  spawn'ed do **lokalnego `JoinSet`** (panic isolation via
  `JoinError::is_panic()` + cascade abort via `JoinSet::drop` —
  Anti-pattern #12: `tokio::spawn` by leak'ował handler po outer
  `abort_all`). Timeout elapsed → `mark_retry { reason: "handler_timeout" }`
  z backoff'em policy; if `attempts ≥ max_attempts` → upgrade do `mark_dead`.
  Default `handler_timeout = lease_timeout × 80%`, builder validates
  `handler_timeout + 1s ≤ lease_timeout`.**
- **Codec decode** wrapowany w `std::panic::catch_unwind(AssertUnwindSafe(...))`
  (sync, no async overhead, no JoinHandle leak). Panic → mark_dead z
  `reason = "codec panic: ..."`.
- Tracing spans: `pgwq.poll_tick`, `.claim_batch`, `.handle_job`,
  `.mark_*`. `handle_job` span attrs: `timeout_ms`, plus event
  `pgwq.handler.timeout_elapsed` przy timeout fire. State transition
  events przez `emit_transition` helper (single source, all sites).
- Tests: `poll_interval_behavior.rs`, `concurrency_behavior.rs`,
  `handler_timeout_behavior.rs`, `tracing_events_emitted.rs`,
  `sqlx_error_classification.rs`, `fatal_sqlx_triggers_shutdown.rs`,
  `schema_missing_fails_loud.rs`, `shutdown_drains_claimed_batch.rs` (W9).

### Faza 5 — reaper (single-CTE, SKIP LOCKED, catch_unwind)

- Reaper task spawn'ed parallel z poll loop.
- Single-CTE z `queue = $1` filter + `CASE WHEN attempts >= max_attempts`.
- `tracing::warn!` na reaped count, per-row `pgwq.state.transition`
  events z `source="reaper"`, `tracing::error!` na dead-letter.
- `AssertUnwindSafe(...).catch_unwind()` per tick, escalate po K=3
  consecutive panics → worker shutdown.
- Tests: `stale_running_reaped.rs`, `reaper_to_dead_when_max_attempts.rs`,
  `reaper_single_cte_no_race.rs`, `reaper_no_advisory_lock_leak.rs`,
  `reaper_per_queue_isolation.rs` (regression dla W4),
  `reaper_recovers_from_tick_panic.rs` (W10),
  `lease_timeout_behavior.rs`, `reaper_interval_behavior.rs`,
  `dead_letter_logged.rs`, `reaper_transition_events.rs` (W7),
  `at_least_once_semantics.rs`, `idempotency_key_stable_across_retries.rs`.

### Faza 6 — retry semantics + BackoffPolicy + panic policy

- `JobError::Retry { reason, retry_in }` z fallback do policy + clamp `retry_in`.
- `JobError::Abort { reason }` → mark_dead direct (bypass retry budget).
- `BackoffPolicy::{Linear, Exponential}` z jitter.
- `From<E> for JobError` example impls dla typowych error types (sqlx, reqwest).
- `mark_retry` ustawia `run_at = now() + duration`.
- `PanicPolicy::{Retry, Dead}` + JoinError::is_panic handling.
- Tests: `max_attempts_behavior.rs`, `scheduled_run_at.rs`,
  `retry_backoff_behavior.rs`, `retry_in_override.rs`,
  `panic_policy_behavior.rs`, `backoff_policy_unit.rs`.

### Faza 7 — shutdown semantics

- `WorkerHandle::shutdown(timeout)`.
- Stats { completed, failed, aborted }.
- `pgwq.shutdown` span.
- Tests: `shutdown_graceful.rs`, `shutdown_abort_after_timeout.rs`,
  `shutdown_cancels_poll_loop.rs`, `fatal_sqlx_triggers_shutdown.rs`.

### Faza 8 — manual purge functions

- `pgwq::purge_done(pool, age) -> u64`.
- `pgwq::purge_dead(pool, age) -> u64`.
- Chunked with `LIMIT limits::PURGE_CHUNK_SIZE` per iteration.
- `tracing::info!` na deleted count.
- Tests: `purge_done_chunked.rs`, `purge_dead_separate.rs`.

### Faza 9 — docs + README + idempotency_key contract

- README z quick start + at-least-once doc + idempotency_key example.
- Doc comments na każdym public knob z "observable effect" + link do
  testu.
- Cargo.toml metadata.
- Optional: tag v0.1.0 + crates.io.

### Faza 10 — integracja z `rust_event_outbox` (osobny plan)

Szczegóły migracji rust_event_outbox z apalis na pg_work_queue
**MOVE TO `rust_event_outbox` repo plan**. Tutaj tylko jednolinijka:
"v0.6.0 outboxa zamienia apalis → pg_work_queue; szczegóły w
`rust_event_outbox/PLAN_v06.md`."

## Open questions / decisions TBD

1. **MSRV**: 1.85+ (edition 2024). Settled.
2. **Multi-tenant via `queue` column** — single table. Settled.
3. **Plugin/tower middleware** — NIE. Settled.
4. **Worker registration table** — NIE. Settled.
5. **Push-side idempotency column** (`unique_key TEXT UNIQUE`) — NIE w v0.1.
6. **Multi-queue worker** — one-queue-per-worker w v0.1. `Worker::queues(&[...])`
   follow-up.
7. **PgBouncer compat** — verify w CI matrix.
8. **License** — MIT. Settled.
9. **PgQ/Kraken-style two-table** — far future, not v0.1.
10. **Lease renewal API (`JobContext::renew_lease`)** — DEFERRED to v0.2.
    v0.1 stand-in: hard floor `MIN_LEASE_TIMEOUT=1s` + `tracing::warn!`
    w builderze gdy `lease_timeout < 10s` + loud rustdoc warning ("set
    lease_timeout ≥ p99 handler duration × 3").
11. **Reaper task panic recovery** — SETTLED: catch_unwind per tick,
    log + counter, escalate do worker shutdown po K=3 consecutive
    panic ticks. Test `tests/reaper_recovers_from_tick_panic.rs`.
12. **Tracing-subscriber default**: nie installuje subscribera by
    default (library best practice).
13. **PG version check** — DO-block w migracji rejecting `< PG18`
    (S5 z review):
    ```sql
    DO $$
    BEGIN
        IF current_setting('server_version_num')::int < 180000 THEN
            RAISE EXCEPTION 'pgwq requires PostgreSQL 18+ (uuidv7() native), got %', current_setting('server_version');
        END IF;
    END$$;
    ```
14. **Fast-fail schema check** (S7 z review) — `Worker::start()` jako
    first action runs `SELECT 1 FROM pgwq.jobs LIMIT 0` żeby loud-fail
    przy brakującej migracji. Error → `BuildError::SchemaMissing { details }`.
15. **last_error history** — overwritten każdy retry (single source). Pełna
    historia wymaga centralized log retention keyed on `public_id`.
    `pgwq.job_attempts` table jako v0.2 roadmap (S23).
16. **`queue_stats(pool) -> QueueStats`** — v0.1 helper read-only function
    dla operator cookbook (zwraca queued/running/done/dead counts).
17. **`handler_timeout` knob** — SETTLED: required builder knob (default
    `lease_timeout × 80%` clamped do `MIN_HANDLER_TIMEOUT=1s`; hard constraint
    `handler_timeout + 1s ≤ lease_timeout`). Library wraps invokację w
    `tokio::time::timeout` inside **lokalnego `JoinSet`** (panic isolation
    przez `JoinError::is_panic()` + proper cascade cancel przez
    `JoinSet::drop`; **nie** `tokio::spawn` — `JoinHandle::drop` detach'uje,
    nie aborti, co leak'owałoby handler po outer abort_all — patrz Anti-pattern
    #12). Elapsed → `mark_retry { reason: "handler_timeout" }` z normalnym
    backoff'em policy; if `attempts ≥ max_attempts` → upgrade do `mark_dead`.
    Reaper pozostaje **wyłącznie dla process-death recovery** (crash/OOM/
    partycja); immediate retry bez backoff'u na tej ścieżce jest poprawne
    (infrastruktura, nie content jobu). **Cancellation gotcha:**
    `tokio::time::timeout` cancel'i tylko przy `.await` point — CPU-bound
    work musi iść przez `spawn_blocking` albo periodic `yield_now`. Tests:
    `handler_timeout_behavior.rs`, `builder_validation.rs` (cross-knob
    `HandlerTimeoutTooLong`), `shutdown_aborts_handler_no_leak.rs` (regresja
    Anti-pattern #12).

## Anti-patterns z których wyciągnęliśmy lekcje

Hard rules:

1. **Każdy public knob musi mieć behavioral test 2-wartościowy.**
2. **Nie wystawiaj knoba dopóki nie zweryfikowałeś że jest READ w
   hot-path.** (apalis `Config::with_poll_interval` bug.)
3. **Nie ufaj nazwie struct'a, dopóki nie przeczytałeś poll/Future
   impl.**
4. **Test który passował od pierwszego compile'a to red flag.** TDD
   wymaga RED.
5. **Verify-before-completion przed każdym claim "fix done".**
6. **Każdy state-machine UPDATE musi mieć status guard + fencing
   token guard.** (apalis ack race + plan v1 reaper W1 bug.)
7. **Nie używamy advisory locków.** Cała klasa bugów (apalis
   `register.sql` leak).
8. **Zero commented-out kodu w state-machine logic.** PR-blocker.
   (apalis `AbortError` branch.)
9. **`unwrap_used = deny`, `expect_used = deny`, `panic = deny`** w
   `Cargo.toml`. Wszystkie error paths explicit. (apalis `Shared`
   driver `.unwrap()` panics.)
10. **All user-facing strings truncated at library boundary** zanim
    wrzucą do DB lub logu. Plus DB CHECK jako backstop.
11. **Background tasks: panic isolation per tick przez `tokio::spawn` +
    `JoinError::is_panic()` + threshold escalation.** Reaper tick wrapped
    w `tokio::spawn`, `JoinError` surface'uje panic; log + counter. Po
    K=3 consecutive panic ticks → worker shutdown (loud failure, surfaced
    przez `ShutdownError::Fatal`). Faza 5. **Nie używamy** `catch_unwind`
    — brak `futures-util` deps + UnwindSafe issues z `&PgPool` w closurze
    reapera (omówione PLAN.md:745-748).
12. **Supervisor wrapping handler future MUSI używać `JoinSet` (lokalnego
    albo zewnętrznego), NIE `tokio::spawn`.** Powód: `JoinHandle::drop`
    detach'uje task (kontynuuje run), `JoinSet::drop` aborti pending tasks.
    Jeśli supervisor sam może zostać `abort_all`'owany (handle_job w głównym
    workerowym JoinSet podczas shutdown timeout / fatal escalation), to
    `tokio::spawn` wewnątrz leak'uje detached handler — side-effects lecą
    dalej, mark_* się nigdy nie wywołuje, row stoi `running` aż lease wygaśnie.
    Z lokalnym `JoinSet` cascade abort działa poprawnie: outer abort → supervisor
    unwind → lokalny `JoinSet` dropped → inner task aborted → handler future
    dropped at next `.await`, Drop runs. Test regresyjny:
    `shutdown_aborts_handler_no_leak.rs`. Faza 4.
13. **Nigdy nie ustawiaj `lease_expires_at` (= nie inkrementuj attempts
    przez claim_batch) zanim masz GWARANCJĘ że handler ruszy natychmiast.**
    Konkretnie: poll loop musi acquire'ować concurrency permits PRZED
    claim_batch, claim'ować ≤ permits, spawn'ować bez `.await` na semaphore
    w środku spawn-after-claim loop'a. Bez tego: batch_size > free_permits
    daje rows siedzące `running` z attempts++ i lease tykającym, czekające
    na semaphore — przy heterogenicznych czasach handlerów (mix 100s+400s)
    reaper przejmuje row zanim handler ruszy → mass fence-out → side-effect
    duplikuje się przez retry-on-other-worker. Generalizacja: claim'ed row
    = obietnica natychmiastowego handler start. Złamanie tej obietnicy
    = at-least-once amplification proportional to (batch_size − concurrency)
    × p99 handler duration. Tests: `poll_acquires_permits_before_claim.rs`,
    `batch_size_behavior.rs`. Faza 4.
14. **Każdy worker-decision SQL (reaper, library mark_retry upgrade-to-dead)
    MUSI używać PER-ROW wartości stamped przez claim_batch, NIE worker-local
    state.** Konkret: rolling deploy z Worker A (max_attempts=3) i Worker B
    (max_attempts=5) na tej samej kolejce — jeśli werdykt dead-vs-retry
    używa `state.max_attempts` (worker-local), ten sam stale row dostaje
    różny verdict zależnie od which-replica-wins-the-lock. Non-deterministyczne
    dead-letter dla identycznego inputu. Fix: stamp `max_attempts` jako kolumnę
    przez claim_batch, reaper/mark_retry porównują z `j.max_attempts`.
    Generalizacja: każdy stateful per-job config knob (max_attempts,
    potencjalnie inne future-knoby jak retry_policy_id) MUSI być stamped
    per-row przy claim time. Test: `max_attempts_rolling_deploy.rs`. Faza 5.

## Roadmap

1. **pg_work_queue v0.1.0** — Fazy 1-9. ~3-4 dni roboty (więcej niż v1
   estimate bo dochodzi observability spec + resource limits + idempotency
   testing).
2. **rust_event_outbox v0.6.0** — Faza 10 (osobny plan).
3. **(later) OSS publish** — crates.io publish jeśli wartościowe.

## Co dalej

Następny krok: **Faza 1** (Pusher + codec + migrator). TDD red-first
— najpierw `tests/migrator_schema.rs` jako RED (asserts that schema
exists with CHECK constraints, indexes, fillfactor), potem implementacja
`migrator()`. Behavioral-first, zawsze.
