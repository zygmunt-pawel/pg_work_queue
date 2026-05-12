# pg_work_queue — plan i przemyślenia (v2, post-multi-agent-review)

> Status: design draft, pre-implementation. Konwencja po polsku
> (kod/identyfikatory po angielsku) — zgodnie z `rust_event_outbox`.
> Wersja v2: zaktualizowana po **9-agentowym review** (failure-rollback,
> race-conditions, reinventing-wheels, verbose-design, resource-exhaustion,
> audit-trail, error-handling, security, duplication) + Opus 4.7 review.
> Backup wersji: PLAN_v0_original.md, PLAN_v1_pre_review.md.

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
| RetryAfter duration ignored (#4) | `Outcome::Retry { in_: Some(d) }` przekłada się 1:1 na `run_at = now() + d` w `mark_retry` SQL. Test: `tests/retry_in_override.rs`. |
| Triple retry counter (#5) | **DB-side `attempts` jest single source of truth.** Brak in-memory retry budget. Reaper i mark_* używają tej samej kolumny. |
| AbortError commented out (#6a) | Code review hard-rule: zero commented-out kodu w state-machine logic (PR-blocker). |
| Advisory lock leak (#6b) | **Nie używamy advisory locków w ogóle.** Reaper przez SKIP LOCKED. |
| Reaper join race (#6c) | **Brak `pgwq.workers` table.** Reaper widzi tylko `jobs.last_attempted_at` + `lease_token`. |
| metrics 24 full scans (#6d) | **Brak `metrics::global` API.** Observability przez `tracing` events + DB queries po queue table. User buduje swój dashboard. |
| `.unwrap()` w hot path (#6e,f) | `Cargo.toml`: `unwrap_used = "deny"`, `expect_used = "deny"`, `panic = "deny"`. Nie skompiluje się. |
| Broken stats.sql (#6g) | TDD + verify-before-completion: każda query exercised w teście. |
| No CHECK on status (#7) | `pgwq.job_status` ENUM + `jobs_status_invariants CHECK` z explicit `(status, attempts, *_at, lease_token)` invariantami. |
| `Backend` trait abstraction (#8) | **Brak Backend trait.** Library jest postgres-only, no abstraction layer. ~1.5–3 k LOC vs 13.8 k LOC apalis. |
| `service_fn` macro (#10) | Handler jest po prostu `async fn(T, JobContext) -> Outcome`. Zero macro, zero lifetime puzzles. |
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
- **Brak typed retry strategies w handler API.** Tylko `Outcome::Retry { reason, in_: Option<Duration> }` lub `Outcome::Dead { reason }`.
- **Brak cross-worker priorities / fairness.**
- **Brak automatycznego retention sweepera.** User wywołuje `pgwq::purge_done` / `pgwq::purge_dead` ręcznie kiedy chce (cron, tokio interval, manual). Library nie spawn'uje background cleanup task — user ma pełną kontrolę kiedy DELETE leci.
- **Brak push-side dedup column (`unique_key TEXT UNIQUE`)** w v0.1. User może zrobić własny `INSERT...ON CONFLICT` przed `Pusher::push` jeśli potrzeba.
- **Brak Worker registration table** (`pgwq.workers`). Lessons z apalisa.

## Delivery semantics & idempotency

**`pg_work_queue` daje at-least-once delivery.** Handler **MOŻE** być
zawołany ≥1 raz dla tego samego logicznego jobu. Konkretnie:

- Handler kończy się sukcesem, `mark_done` query traci connection
  zanim commit się zarejestruje → reaper flipuje, kolejny worker
  re-execute.
- Reaper flipuje row do `awaiting_retry` przez `lease_timeout` (worker
  cię handler trwa dłużej niż lease) → stary handler kończy się i
  jego `mark_done` 0-rows-affected (fencing token mismatch), ale
  side-effect już się wykonał. Reaper-spawned retry wykonuje go ponownie.
- Worker proces crashuje po handler success ale przed `mark_done`
  commit → reaper recovery, nowy worker retry.

### Handler-side idempotency contract

`JobContext` zawiera pole **`idempotency_key: Uuid`** które:

1. Jest **stabilne across retries** dla tego samego jobu (= `public_id`
   ustawione na push).
2. Jest **unikalne per logical operation** (UUIDv7, near-zero collision).
3. Jest **przekazywane do handlera w każdym attempt**.

Handler który **musi** wykonać external side-effect dokładnie raz
(np. SMTP send, payment charge, webhook POST) **musi** używać
`idempotency_key` do dedup'u:

```rust
.handler(|task: ChargeTask, ctx: JobContext| async move {
    // Dedupe via idempotency_key — repeated invocations are no-ops.
    if redis.get(format!("charge:{}", ctx.idempotency_key)).await?.is_some() {
        return Outcome::Done;
    }
    stripe.charge(task.amount, &ctx.idempotency_key.to_string()).await?;
    redis.set(format!("charge:{}", ctx.idempotency_key), "1").await?;
    Outcome::Done
})
```

Library **nie ma sposobu** dać exactly-once external side-effects.
To jest fundamentalny limit każdej polling queue. **Handler dostaje
narzędzie (`idempotency_key`); użycie jest jego odpowiedzialnością.**

External APIs (Stripe, AWS, etc.) standardowo wspierają `idempotency_key`
header — nasz UUID v7 jest perfect fit.

## Public API — sketch

```rust
use pg_work_queue::{Worker, Outcome, JobContext, Pusher, BackoffPolicy, JsonCodec};
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
            match send_smtp(&task, &ctx.idempotency_key).await {
                Ok(_) => Outcome::Done,
                Err(e) if e.is_transient() => Outcome::Retry {
                    reason: e.to_string(),
                    in_: None, // backoff from policy
                },
                Err(e) => Outcome::Dead { reason: e.to_string() },
            }
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
    pub public_id: Uuid,               // = idempotency_key (alias)
    pub idempotency_key: Uuid,         // stable across retries
    pub queue: String,                 // queue name (cloned for handler)
    pub attempt: u32,                  // 1-indexed, current attempt
    pub first_attempted_at: DateTime<Utc>,
    pub lease_token: Uuid,             // current claim's fencing token
}
```

`idempotency_key` i `public_id` to ta sama wartość (alias dla readability).
`public_id` ekspozuje implementację (UUID v7 zegarodatowy), `idempotency_key`
ekspozuje intencję (use this for dedup). Handler używa `idempotency_key`.

### Builder knobs (każdy z behavioral testem przy 2 wartościach)

| Knob | Default | Validation | Effect |
|---|---|---|---|
| `queue(&str)` | required | non-empty, ≤ 64 chars | nazwa queue |
| `poll_interval(Duration)` | 1s | ≥ 10 ms | deterministyczny cycle |
| `concurrency(usize)` | num_cpus | 1..=pool.size | max parallel handlers |
| `max_attempts(u32)` | 3 | ≥ 1, ≤ `i32::MAX` | przed dead-letter |
| `lease_timeout(Duration)` | 5min | ≥ poll_interval × 5 | stale-running threshold |
| `reaper_interval(Duration)` | lease_timeout/4 | ≥ 1s, ≤ lease_timeout/2 | reaper tick cadence |
| `batch_size(usize)` | 10 | 1..=1_000 | rows per claim_batch |
| `retry_backoff(BackoffPolicy)` | `Exponential { 1s, 2.0, 5min, 0.2 }` | jitter ∈ [0,1], cap ≤ 24h | used when `Outcome::Retry { in_: None }` |
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

    /// Reaper sweep batch size (rows per tick).
    pub const REAPER_BATCH_SIZE: usize = 1_000;

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
    lease_token        UUID,
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
            AND lease_token IS NULL)
        OR (status = 'running'
            AND attempts > 0
            AND last_attempted_at IS NOT NULL
            AND first_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NOT NULL)
        OR (status = 'awaiting_retry'
            AND attempts > 0
            AND last_attempted_at IS NOT NULL
            AND first_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NULL)
        OR (status IN ('done', 'dead')
            AND finished_at IS NOT NULL
            AND lease_token IS NULL)
    )
);

-- High-churn queue tables bloatują na default settings. fillfactor=80
-- zostawia 20% wolnego per block → HOT updates in-place; agresywniejszy
-- autovacuum → dead tuples reclamowane szybciej.
ALTER TABLE pgwq.jobs SET (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);

-- Poll claim hot path
CREATE INDEX jobs_claim_idx
    ON pgwq.jobs (queue, run_at, id)
    WHERE status IN ('queued', 'awaiting_retry');

-- Reaper hot path
CREATE INDEX jobs_reap_idx
    ON pgwq.jobs (last_attempted_at)
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

```rust
async fn poll_loop<T>(state: Arc<WorkerState<T>>) {
    let mut ticker = tokio::time::interval(state.poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            _ = state.shutdown.cancelled() => break,
        }

        let permit = tokio::select! {
            r = state.semaphore.clone().acquire_owned() => r,
            _ = state.shutdown.cancelled() => break,
        };
        let Ok(permit) = permit else { break };

        let span = tracing::info_span!("pgwq.poll_tick",
            queue = %state.queue, batch_size = state.batch_size);
        let _enter = span.enter();

        match claim_batch(&state.pool, &state.queue, state.batch_size).await {
            Ok(rows) if rows.is_empty() => { drop(permit); continue; }
            Ok(rows) => {
                tracing::info!(claimed = rows.len(), "batch claimed");
                let mut iter = rows.into_iter();
                if let Some(row) = iter.next() {
                    state.tasks.spawn(handle_job(row, state.clone(), permit));
                }
                for row in iter {
                    let p = tokio::select! {
                        r = state.semaphore.clone().acquire_owned() => r,
                        _ = state.shutdown.cancelled() => return,
                    };
                    let Ok(p) = p else { return };
                    state.tasks.spawn(handle_job(row, state.clone(), p));
                }
            }
            Err(e) if is_fatal_sqlx(&e) => {
                tracing::error!(error = %e, "fatal DB error in claim_batch; shutting down worker");
                state.shutdown.cancel();
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "claim batch failed; will retry next tick");
                drop(permit);
            }
        }
    }
}
```

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
    last_attempted_at = now(),
    first_attempted_at = COALESCE(j.first_attempted_at, now()),
    lease_token = gen_random_uuid()
FROM claimed
WHERE j.id = claimed.id
RETURNING j.id, j.public_id, j.queue, j.payload, j.attempts,
          j.first_attempted_at, j.lease_token;
```

(`updated_at` ustawia trigger, nie explicit SET.)

### Reaper (single-CTE, no race window)

**Plan v1 miał two-step reaper z drugim UPDATE bez status/lease_token
guard'a — łamał własną regułę #6 (każdy UPDATE w state-machine musi
mieć dodatkowy guard). v2 łączy w jeden CTE z `CASE WHEN`:**

```sql
WITH stale AS (
    SELECT id, attempts FROM pgwq.jobs
    WHERE status = 'running'
      AND last_attempted_at < now() - $1::interval
    ORDER BY last_attempted_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pgwq.jobs j
SET status = CASE
        WHEN s.attempts >= $3 THEN 'dead'::pgwq.job_status
        ELSE 'awaiting_retry'::pgwq.job_status
    END,
    finished_at = CASE
        WHEN s.attempts >= $3 THEN now()
        ELSE NULL
    END,
    last_error = COALESCE(j.last_error, CASE
        WHEN s.attempts >= $3 THEN 'lease_expired_max_attempts'
        ELSE 'lease_expired'
    END),
    lease_token = NULL
FROM stale s
WHERE j.id = s.id
RETURNING j.id, j.status, j.attempts;
```

Atomic single-statement. SKIP LOCKED w CTE = stale rows trzymane workerem
nie są reapowane. Brak race window.

Reaper task:

```rust
async fn reaper_loop(state: Arc<WorkerState>) {
    let mut ticker = tokio::time::interval(state.reaper_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            _ = state.shutdown.cancelled() => return,
        }

        let span = tracing::info_span!("pgwq.reap_tick", queue = %state.queue);
        let _enter = span.enter();

        match reap(&state.pool, &state.queue, state.lease_timeout,
                   limits::REAPER_BATCH_SIZE, state.max_attempts).await
        {
            Ok(reaped) if reaped.is_empty() => {}
            Ok(reaped) => {
                let dead_count = reaped.iter().filter(|r| r.status == "dead").count();
                let retry_count = reaped.len() - dead_count;
                tracing::warn!(
                    reaped_total = reaped.len(),
                    reaped_dead = dead_count,
                    reaped_retry = retry_count,
                    "stale jobs reaped"
                );
                for row in &reaped {
                    if row.status == "dead" {
                        tracing::error!(
                            job.id = row.id, job.attempts = row.attempts,
                            "job dead-lettered (lease expired, max_attempts exhausted)"
                        );
                    }
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

Reaper task wrapped w `JoinSet::spawn` z `tracing::error!` na panic via
`JoinError`.

### Mark queries (fencing token w WHERE)

```sql
-- mark_done
UPDATE pgwq.jobs
SET status = 'done', finished_at = now(), last_error = NULL, lease_token = NULL
WHERE id = $1 AND status = 'running' AND lease_token = $2;

-- mark_retry
UPDATE pgwq.jobs
SET status = 'awaiting_retry', last_error = $3, run_at = $4, lease_token = NULL
WHERE id = $1 AND status = 'running' AND lease_token = $2;

-- mark_dead
UPDATE pgwq.jobs
SET status = 'dead', finished_at = now(), last_error = $3, lease_token = NULL
WHERE id = $1
  AND status IN ('running', 'awaiting_retry')
  AND lease_token = $2;
```

**0-rows-affected reactions** (explicit):

- `mark_done` 0 rows → reaper już flipnął (lease expired) lub szczególny
  race. Log `warn!(job.id, idempotency_key, "mark_done lost race — side-effect
  may have already been retried by other worker")`. Worker continues —
  next claim picks up.
- `mark_retry` 0 rows → analogously. `warn!`. Continue.
- `mark_dead` 0 rows → analogously. `warn!`. Continue.

W żadnym wypadku worker NIE re-attempts; reaper-spawned retry przejmuje
dalszą logikę.

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
INSERT INTO pgwq.jobs (queue, payload, public_id, run_at)
SELECT $1, unnest($2::bytea[]), unnest($3::uuid[]),
       COALESCE(unnest($4::timestamptz[]), now())
ORDER BY ordinality
RETURNING id, public_id;
```

`Pusher` client-side:
1. Validate `items.len() <= limits::MAX_BATCH_SIZE` → else `PushError::BatchTooLarge`.
2. Generate `public_id = Uuid::now_v7()` per item (deterministic ordering;
   client-side dla outbox correlation-in-same-tx).
3. Validate `payload.len() <= limits::MAX_PAYLOAD_BYTES` per item → else
   `PushError::PayloadTooLarge { index, size }`.
4. Single `INSERT...SELECT unnest()` round-trip.

Return: `Vec<Uuid>` w **input order** (zachowane przez `ORDER BY ordinality`).

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
    pub fn next(&self, attempt: u32) -> Duration;
}
```

`Fixed` variant usunięty (degenerate Linear). `fixed()` constructor
zachowany dla convenience.

Default: `Exponential { 1s, 2.0, 5min, 0.2 }` → ~1s, 2s, 4s, 8s, ... 5min
(±20% jitter).

Jitter ważny przy thundering herd (10 jobs fails równocześnie → bez jittera
wszystkie wracają w tym samym ticku).

User per-call override: `Outcome::Retry { in_: Some(d), .. }`.
**Cap**: `d` clamp'owany do `[Duration::ZERO, 24h]` żeby nie przyjmować
`Duration::MAX` jako footgun.

## Error semantics & handling

### Public error enums

```rust
#[derive(thiserror::Error, Debug)]
pub enum PushError {
    #[error("payload too large: {size} bytes > {max}")]
    PayloadTooLarge { index: usize, size: usize, max: usize },
    #[error("batch too large: {size} > {max}")]
    BatchTooLarge { size: usize, max: usize },
    #[error("queue name invalid: {0}")]
    QueueNameInvalid(String),
    #[error("codec error: {0}")]
    Codec(#[source] BoxError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum BuildError {
    #[error("poll_interval must be >= {min:?}")]
    PollIntervalTooShort { min: Duration },
    #[error("concurrency must be >= 1")]
    ConcurrencyZero,
    #[error("max_attempts must be >= 1")]
    MaxAttemptsZero,
    #[error("lease_timeout must be >= 5 * poll_interval")]
    LeaseTimeoutTooShort,
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

### Handler outcome semantics

```rust
pub enum Outcome {
    Done,
    Retry { reason: String, in_: Option<Duration> },
    Dead { reason: String },
}
```

Library applies:
- `Done` → `mark_done` (fencing-guarded).
- `Retry` → `mark_retry` z `run_at = now() + in_.unwrap_or_else(|| backoff.next(attempts))`.
  Jeśli `attempts >= max_attempts` → upgrade do `mark_dead`.
- `Dead` → `mark_dead` natychmiast (bypass attempts check).
- **Panic** → per `PanicPolicy`:
  - `PanicPolicy::Retry` (default) → `mark_retry` z `reason = "panic: <msg>"`.
  - `PanicPolicy::Dead` → `mark_dead` z `reason = "panic: <msg>"`.
- **Codec decode error** (worker can't deserialize payload) → `mark_dead`
  natychmiast z `reason = "payload decode: <err>"`. NIE wywołuje handler.
  Każdy retry attempt by ten sam decode-error miał → terminal natychmiast.

### Library-side string truncation

Wszystkie user-supplied `reason` strings (Outcome::Retry/Dead, panic
message) **truncate'owane na library boundary** do `limits::MAX_LAST_ERROR_LEN`
zanim go do `last_error`. Trim by char boundary safe (rust-safe-string-truncation
skill). DB CHECK na 8 KiB jako backstop.

Także `tracing::warn!(reason = %trim(reason))` — nie logujemy nieograniczonych
user strings.

### Sqlx error classification

```rust
fn is_fatal_sqlx(e: &sqlx::Error) -> bool {
    matches!(e,
        sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Configuration(_)
        | sqlx::Error::Migrate(_)
    )
}
```

Fatal → worker self-shutdown via cancellation token. Transient
(Database / Io / Tls / Protocol) → warn + retry next tick.

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

Każde state transition emituje `tracing::info!` (lub `tracing::error!`
dla dead-letter) event z structured attrs:

```rust
tracing::info!(
    job.id = id,
    job.public_id = %public_id,
    queue = %queue,
    status.from = "running",
    status.to = "done",
    "pgwq.state.transition"
);
```

Tabela:

| From → To | Level | When |
|---|---|---|
| → `queued` | `info` | push, push_batch (1 event per row inserted) |
| `queued` → `running` | `debug` | claim_batch (1 event per row claimed) |
| `awaiting_retry` → `running` | `debug` | claim_batch (re-attempt) |
| `running` → `done` | `info` | mark_done success |
| `running` → `awaiting_retry` | `info` | mark_retry success (handler retry or reaper) |
| `running` → `dead` | **`error`** | mark_dead (max_attempts exhausted) — **dead-letter** |
| `dead`/`done` → ∅ | `info` | purge_done / purge_dead delete |

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

1. Cancel `shutdown` cancellation token → poll loop + reaper exit
   immediately (na `tokio::select` z token).
2. Drop semaphore permit-acquire — nowe handlery się nie spawn'ują.
3. `JoinSet::join_next` w pętli z `tokio::time::timeout(timeout)`.
4. Po timeout: `JoinSet::abort_all()`, czekaj na ostatnie join. Stats
   summary.

```rust
pub struct Stats {
    pub completed: u64,    // handlers returned Outcome::Done (mark_done ack'd)
    pub failed: u64,       // handlers returned Outcome::Retry/Dead OR panicked
    pub aborted: u64,      // handlers aborted via JoinSet::abort_all (timeout)
}
```

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
5. `tests/reaper_interval_behavior.rs` — reaper z 1s vs 5s tick.
6. `tests/batch_size_behavior.rs` — claim shape przy 10 vs 50.
7. `tests/scheduled_run_at.rs` — push z run_at = now()+2s.
8. `tests/retry_backoff_behavior.rs` — Fixed vs Exponential run_at delta.
9. `tests/retry_in_override.rs` — Outcome::Retry { in_: Some(5s) }.
10. `tests/panic_policy_behavior.rs` — PanicPolicy::Retry vs ::Dead.
11. `tests/codec_swappable.rs` — JsonCodec vs custom CborCodec.

### Crash safety / correctness

12. `tests/skip_locked_no_double_claim.rs` — 2 workery, 100 jobs, suma = 100.
13. `tests/stale_running_reaped.rs` — manual UPDATE stale, reaper flipuje.
14. `tests/reaper_to_dead_when_max_attempts.rs` — reaper z attempts=N+1
    flipuje do dead (nie awaiting_retry).
15. `tests/reaper_single_cte_no_race.rs` — verify że single-CTE reaper
    nie produkuje (running, awaiting_retry, dead) inconsistencies pod
    concurrent claim+reap (regresja po W1 z review).
16. `tests/fencing_token_no_double_run.rs` — claim → manual stale → reaper
    → stary handler mark_done ze starym tokenem → 0 rows.
17. `tests/shutdown_graceful.rs` — handler trwa krócej niż timeout, drain OK.
18. `tests/shutdown_abort_after_timeout.rs` — handler trwa dłużej, abort +
    reaper recovery + correctness.
19. `tests/shutdown_cancels_poll_loop.rs` — mid-poll-sleep shutdown
    wychodzi natychmiast.
20. `tests/migrator_schema.rs` — schema CREATE'd, CHECKs fire, fillfactor
    w `pg_class.reloptions`.
21. `tests/reaper_no_advisory_lock_leak.rs` — 3 reapery parallel,
    `pg_locks` clean post-test.
22. `tests/fatal_sqlx_triggers_shutdown.rs` — PgPool close mid-poll,
    worker self-shutdown z error w stats.

### Resource limits

23. `tests/resource_limits.rs` — payload > 1MiB rejected, batch > 10k
    rejected, last_error truncate, queue name length CHECK.
24. `tests/builder_validation.rs` — wszystkie `BuildError::*` variants
    rzucane na nieprawidłowy config.

### Idempotency / at-least-once

25. `tests/idempotency_key_stable_across_retries.rs` — handler fail 3x,
    captured `ctx.idempotency_key` identical każdy attempt.
26. `tests/at_least_once_semantics.rs` — simulate mark_done loss
    (manual rollback), reaper recover, handler called 2x ten sam job
    z tym samym idempotency_key.

### Push & purge

27. `tests/push_batch_throughput.rs` — 1000 single push vs batch,
    batch ≥ 5x szybszy.
28. `tests/push_batch_order_preserved.rs` — push_batch zwraca uuidy
    w input order.
29. `tests/purge_done_chunked.rs` — 50k done rows, purge_done(0s)
    deletuje wszystkie chunkami.
30. `tests/purge_dead_separate.rs` — purge_done nie tyka dead, vice versa.

### Observability

31. `tests/tracing_events_emitted.rs` — capture tracing events via
    custom subscriber, assert że dla każdej transition emitted event
    z expected attrs (job.id, status.from, status.to).
32. `tests/dead_letter_logged.rs` — job hits max_attempts → reaper or
    handler emits `tracing::error!` z dead-letter context.

### No-DB / unit

33. `tests/backoff_policy_unit.rs` — `BackoffPolicy::next(attempt)`.
34. `tests/codec_json_roundtrip.rs` — Serialize → Vec<u8> → Deserialize.
35. `tests/sqlx_error_classification.rs` — `is_fatal_sqlx` cases.
36. `tests/truncate_safe_string.rs` — UTF-8 boundary safety w trim.

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
- `Pusher::new`, `with_codec`, `push`, `push_at`, `push_batch`.
- `pg_work_queue::migrator()` re-export `sqlx::migrate!("./migrations")`.
- Resource validation (payload size, batch size, queue length).
- `PushError` enum.
- Tests: `migrator_schema.rs`, `push_batch_throughput.rs`,
  `push_batch_order_preserved.rs`, `resource_limits.rs` (push parts).

### Faza 2 — claim_batch SQL + Job<T> + JobContext

- `claim_batch` SQL function.
- `pub struct Job<T>` + `pub struct JobContext`.
- Codec decode na claim time; decode error → mark_dead.
- Tests: `skip_locked_no_double_claim.rs`.

### Faza 3 — single-shot worker + mark queries z fencing

- `Worker::tick_once(...)` — fetch batch, run handlers sequential,
  mark_done/retry/dead z fencing.
- Library-side string truncation w `last_error`.
- `unreachable_pub` guards.
- Tests: end-to-end smoke, `fencing_token_no_double_run.rs`.

### Faza 4 — poll loop + concurrency + worker identity

- `Worker::start()` → spawn poll loop + JoinSet.
- `worker.id = Uuid::now_v7()` w span attrs.
- `CancellationToken` plumbing.
- `is_fatal_sqlx()` classification.
- Tracing spans: `pgwq.poll_tick`, `.claim_batch`, `.handle_job`,
  `.mark_*`. State transition events.
- Tests: `poll_interval_behavior.rs`, `concurrency_behavior.rs`,
  `tracing_events_emitted.rs`.

### Faza 5 — reaper (single-CTE, SKIP LOCKED)

- Reaper task spawn'ed parallel z poll loop.
- Single-CTE z CASE WHEN attempts >= max_attempts.
- `tracing::warn!` na reaped count, `tracing::error!` na dead-letter.
- Reaper task wrapped w panic-recovery (`JoinSet` + on-panic restart? TBD).
- Tests: `stale_running_reaped.rs`, `reaper_to_dead_when_max_attempts.rs`,
  `reaper_single_cte_no_race.rs`, `reaper_no_advisory_lock_leak.rs`,
  `lease_timeout_behavior.rs`, `reaper_interval_behavior.rs`,
  `dead_letter_logged.rs`.

### Faza 6 — retry semantics + BackoffPolicy + panic policy

- `Outcome::Retry { reason, in_ }` z fallback do policy + clamp `in_`.
- `BackoffPolicy::{Linear, Exponential}` z jitter.
- `mark_retry` ustawia `run_at = now() + duration`.
- `Outcome::Dead` → mark_dead direct.
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
2. **Multi-tenant via `queue` column** — single table na start. Settled.
3. **Plugin/tower middleware** — NIE. Settled.
4. **Worker registration table** — NIE. Settled.
5. **Push-side idempotency column** (`unique_key TEXT UNIQUE`) — NIE w v0.1.
6. **Multi-queue worker** — one-queue-per-worker w v0.1. `Worker::queues(&[...])`
   to follow-up.
7. **PgBouncer compat** — verify w CI matrix (no LISTEN/NOTIFY, no
   session advisory locks → powinno działać; testcontainers `pgbouncer`
   image w dedicated test).
8. **License** — MIT. Settled.
9. **PgQ/Kraken-style two-table** — far future, not v0.1.
10. **`reaper_interval` validation cross-knob** — `<= lease_timeout/2`.
    Hard validation in builder.
11. **Reaper task panic recovery** — TBD: monitor + restart vs
    log + dead worker? Phase 5 decision.
12. **Tracing-subscriber default**: nie installuje subscribera by
    default (library best practice — to user's app's responsibility).

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
11. **Background tasks restart-aware lub explicit-shutdown.** Reaper
    task panic = worker shutdown (faza 5 decyzja).

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
