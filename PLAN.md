# pg_work_queue — plan i przemyślenia (v1, post-research)

> Status: design draft, pre-implementation. Konwencja po polsku
> (kod/identyfikatory po angielsku) — zgodnie z `rust_event_outbox`.
> Wersja v1 zaktualizowana po cross-referencu źródła apalisa (rc.9 monorepo
> + rc.8 split repo `apalis-dev/apalis-postgres`) i analizie ekosystemu
> (River, gue, pg-boss, Solid Queue, Que, neoq). Szczegóły researchu w
> `research/SYNTHESIS.md` + trzech raportach pomocniczych.

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

## Motywacja — dlaczego nie apalis

`apalis-postgres` jest w aktywnej re-architekturze: PR #586 (rc.1) wyciął
backendy z monorepo do osobnych repo. Aktualny stan:

- `apalis-dev/apalis-postgres` — repo utworzone 2025-08-19, pierwsza alpha
  2025-10-25, obecnie w cyklu rc.1 (grudzień 2025) → rc.8 (maj 2026).
- Wcześniej Postgres żył w `apalis-sql` w starym monorepo. Te dwie wersje
  mają silently-breaking API differences (np. payload column JSONB → BYTEA).

Przy tej turbulencji, plus seria niezamkniętych issues, krybsujący użytkownik
ma trzy opcje: (a) zostać na rc.7 z znanymi bugami, (b) skoczyć na ruchomy
cel rc.x bez gwarancji że bugi są fixnięte, (c) napisać własne. Wybieramy
(c).

Konkretne, zweryfikowane na źródle problemy w apalis-postgres rc.8:

1. **`PgPollFetcher::next_backoff` hardcoduje `1s → 5min` exponential cap.**
   `Config::with_poll_interval(MultiStrategy)` jest zapisywany do
   `self.config`, ale fetcher go **nigdy nie czyta**
   (`apalis-postgres/src/fetcher.rs:84,160-163`). Dead-code w rc.7 i rc.8.
2. **Trigger `pg_notify('apalis::job::insert', ...)` emitowany per INSERT**
   (`migrations/20251018165121_notify_run_at.sql`). NOTIFY przy commit
   bierze `LWLock NotifyQueueLock` (≡ `AccessExclusiveLock` na locktype=
   `database` w `pg_locks`) — serializuje wszystkie NOTIFY-issuing
   commits cluster-wide. Recall.ai publicznie udokumentował tę
   patologię w `postgres-listen-notify-does-not-scale` (marzec 2025); ich
   case jest library-agnostic ale opisany pattern (NOTIFY-per-INSERT przy
   write-heavy workload) pasuje 1:1 do tego co apalis robi. Outbox jest
   write-heavy z natury → unikalibyśmy NOTIFY na zasadzie precedence.
3. **`ack=UPDATE` zamiast DELETE** (`queries/task/ack.sql`). Konsekwencja:
   row accumulation w queue table, user musi cron'ować `vacuum()`.
4. **`RetryAfterError(_, duration)` — `get_duration()` ma zero callers
   w całym crate.** Duration jest dead-code. Plain `Transient` retry semantics.
5. **Triple retry budget** — apalis `RetryPolicy::retries(N)` (in-memory)
   + DB `attempts` column + DB `max_attempts` w shared logic. In-memory
   counter **resetuje się** per worker lease, czyli crashed worker
   zeruje retry budget. Authoritative source-of-truth jest niejasny.
6. **Live bugs w rc.8:**
   - `AbortError` branch w `calculate_status` (`src/ack.rs:70`) jest
     **literalnie zakomentowany** — aborty po cichu stają się `Failed`
     i są retry'owane do `max_attempts`.
   - Worker registration leakuje session-scoped advisory locki —
     `pg_try_advisory_lock(hashtext(workers.id))` w `register.sql` nigdy
     nie jest released; trzymane przez sqlx pool connection
     indefinitely.
   - Reaper join-to-workers race — `reenqueue_orphaned` requires
     `INNER JOIN apalis.workers`; jeśli worker row jest purged (manual
     cleanup), jego locked jobs **stuck permanently**.
   - `metrics::global` wykonuje 24 full-table scans `apalis.jobs` per
     call.
   - `Shared` driver `.unwrap()` na listener-connect / listen / send —
     panic na transient connection issue.
   - `wait_for` to 500ms sleep-poll z `.unwrap()` panics w hot path.
   - `queries/backend/stats.sql` jest SQLite syntax (`?1` placeholdery)
     w postgres queries dir. Unused dead code — sygnał słabej discipline.
7. **Schema warts:** brak `CHECK`/`ENUM` na `status` column; redundant
   indexes; brak composite indeksu na hot fetch path `(job_type, status,
   run_at)`; PRIMARY KEY-e dodane dopiero w rc.1 (po 5 latach lifetime).

Wynik audytu: każdy z tych warts wymagałby workaroundu / hacka w
`rust_event_outbox`. `pg_work_queue` eliminuje wszystkie naraz przez
nie używanie apalis w ogóle.

## Co `pg_work_queue` świadomie NIE robi (anti-features)

- **Brak `LISTEN/NOTIFY`.** Commit-NOTIFY serializuje cluster-wide. Dla
  write-heavy queue to nieakceptowalne ryzyko. Zawsze poll.
- **Brak adaptive / exponential backoff na pollerze.** Cadence jest
  deterministyczna (`poll_interval`). User chce 500ms → poll co 500ms.
  Trade-off load-vs-latency robi user explicit. (Retry backoff dla
  handler errors to osobna sprawa — patrz `Outcome::Retry`.)
- **Brak multi-backend abstraction.** Postgres-only by design.
- **Brak worker dashboard / GUI / metrics endpoint.** Observability
  przez `tracing` spans + DB queries po queue table.
- **Brak typed retry strategies w handler API.** Handler zwraca
  `Outcome::Retry { reason, in_: Option<Duration> }` lub
  `Outcome::Dead { reason }`. Library decyduje retry-vs-dead po
  `attempts < max_attempts`. Backoff dla retry: jeśli `in_` = `Some(d)`
  user override; jeśli `None` używamy `retry_backoff` policy z konfiguracji
  (default: exponential z jitter).
- **Brak cross-worker priorities, fairness, multi-tenant isolation
  beyond `queue` column.** Trzymamy się prostoty. Jeden queue name =
  jeden FIFO stream tasks.
- **Brak Tower middleware stack.** Apalis to robi (timeout/retry/limit/
  catch-panic jako tower layers); my po prostu wywołujemy `handler.call`
  w `JoinSet` ze `tokio::time::timeout` na lease_timeout i `AssertUnwindSafe`
  catch-panic. ~30 LOC zamiast 300+ apalis-core.

## Public API — sketch

```rust
use pg_work_queue::{Worker, Outcome, JobContext, Pusher, BackoffPolicy};
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
        .retry_backoff(BackoffPolicy::exponential(
            Duration::from_secs(1),  // base
            2.0,                      // factor
            Duration::from_secs(300), // cap
            0.2,                      // jitter ratio
        ))
        .done_retention(Duration::from_secs(7 * 24 * 3600))
        .dead_retention(None) // ∞ — keep forever
        .handler(|task: EmailTask, ctx: JobContext| async move {
            tracing::info!(to = %task.to, attempt = ctx.attempt, "sending");
            match send_smtp(&task).await {
                Ok(_) => Outcome::Done,
                Err(e) if e.is_transient() => Outcome::Retry {
                    reason: e.to_string(),
                    in_: None, // use policy
                },
                Err(e) => Outcome::Dead { reason: e.to_string() },
            }
        })
        .build()?;

    let handle = worker.start(); // spawns poll loop + reaper + retention sweeper

    // Push side (in your own transaction):
    let mut tx = pool.begin().await?;
    Pusher::new("email_send")
        .push(&mut tx, &EmailTask { to: "x@y".into(), body: "hi".into() })
        .await?;
    tx.commit().await?;

    // Batch push (perf for outbox-style workloads):
    let mut tx = pool.begin().await?;
    Pusher::new("email_send")
        .push_batch(&mut tx, &[task1, task2, task3])
        .await?;
    tx.commit().await?;

    // Graceful shutdown:
    tokio::signal::ctrl_c().await?;
    handle.shutdown(Duration::from_secs(10)).await?;
    Ok(())
}
```

### Builder knobs (każdy z behavioral testem przy 2 wartościach)

| Knob | Default | Effect |
|---|---|---|
| `queue(&str)` | required | nazwa queue (PG column lookup) |
| `poll_interval(Duration)` | 1s | **deterministyczny** cycle (nie backoff) |
| `concurrency(usize)` | num_cpus | max parallel handlers per worker |
| `max_attempts(u32)` | 3 | przed dead-letter |
| `lease_timeout(Duration)` | 5min | po tym czasie stale-running wiersz reapowany |
| `reaper_interval(Duration)` | 60s | jak często sprawdzać stale-running |
| `batch_size(usize)` | 10 | ile wierszy claim'ować per poll |
| `retry_backoff(BackoffPolicy)` | `Exponential { base: 1s, factor: 2, cap: 5min, jitter: 0.2 }` | używany gdy `Outcome::Retry { in_: None }` |
| `done_retention(Duration)` | 7 days | po tym czasie `done` rows są DELETE'd |
| `dead_retention(Option<Duration>)` | `None` (∞) | dead rows trzymane forever — diagnostyka |
| `retention_interval(Duration)` | 1h | jak często sweeper czyści stare rows |

Każdy z tych ma **integracyjny test który sprawdza observable behavior
przy dwóch różnych wartościach** (np. `poll_interval(100ms)` vs
`poll_interval(500ms)` → różnica latency mierzalna).

## Schema (DB layout)

Schema **`pgwq`** (krótka, nie `pg_work_queue` bo `pg_` prefix jest
**reserved przez PG** dla schemy systemowej — Postgres odrzuca
`CREATE SCHEMA pg_work_queue` z `ERROR: unacceptable schema name`).
Crate nazywa się `pg_work_queue`; tylko nazwa schemy DB jest `pgwq`.

```sql
CREATE SCHEMA IF NOT EXISTS pgwq;

CREATE TYPE pgwq.job_status AS ENUM (
    'queued', 'running', 'awaiting_retry', 'done', 'dead'
);

CREATE TABLE pgwq.jobs (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    public_id          UUID NOT NULL UNIQUE,
    queue              TEXT COLLATE "C" NOT NULL,
    payload            BYTEA NOT NULL,
    status             pgwq.job_status NOT NULL DEFAULT 'queued',
    attempts           SMALLINT NOT NULL DEFAULT 0,
    lease_token        UUID,           -- fencing token, NOT NULL gdy status='running'
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
            AND finished_at IS NULL
            AND lease_token IS NULL)
        OR (status = 'running'
            AND attempts > 0
            AND last_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NOT NULL)
        OR (status = 'awaiting_retry'
            AND attempts > 0
            AND last_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NULL)
        OR (status IN ('done', 'dead')
            AND finished_at IS NOT NULL
            AND lease_token IS NULL)
    )
);

-- Bloat resistance: queue tables są high-churn. fillfactor=80 zostawia
-- 20% wolnego miejsca w blokach na HOT updates; autovacuum agresywniej.
ALTER TABLE pgwq.jobs SET (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);

-- Hot path: poll claim (queued | awaiting_retry, sortowane po run_at, id).
CREATE INDEX jobs_claim_idx
    ON pgwq.jobs (queue, run_at, id)
    WHERE status IN ('queued', 'awaiting_retry');

-- Reaper hot path
CREATE INDEX jobs_reap_idx
    ON pgwq.jobs (last_attempted_at)
    WHERE status = 'running';

-- Retention sweeper hot path
CREATE INDEX jobs_terminal_idx
    ON pgwq.jobs (finished_at)
    WHERE status IN ('done', 'dead');
```

Decyzje względem `rust_event_outbox` lessons + research:
- `BIGINT IDENTITY` internal PK + `public_id UUID` external (compact FK/index,
  sortable wire format).
- Named CHECK constraints — defense-in-depth przeciw buggy code; `status`
  invariants enforce'owane na DB level.
- `lease_token UUID` — fencing token przeciw double-execution race między
  reaperem a stary workerem (apalis tego NIE ma — żywy bug).
- Partial indexes na hot paths (claim, reap, terminal). `WHERE status IN`
  trzyma indeks mały (terminal rows dominują w długim runtime).
- `COLLATE "C"` na `queue` (byte-exact `=` lookup).
- ENUM zamiast CHECK constraint na status (rygorystyczniej; apalis nie ma).
- Schema namespacing (`pgwq.*`).
- `run_at` — pozwala scheduled jobs (push z `run_at = now() + 5min`).
- `payload BYTEA` (nie `JSONB`) — biblioteka nie wnika w format.
  (JSONB można dodać jako optional feature później jeśli ktoś chce
  filtrować po payload w SQL.)
- `fillfactor=80` + agresywny autovacuum — high-churn queue tables
  bloatują standardowymi defaultami (problem opisany przez Brandura
  w "Building Robust Systems"). Tani fix dnia zerowego.

## Internal architecture

```
                            ┌──────────────────┐
                            │  Pusher::push    │  (in user's tx)
                            │  ::push_batch    │
                            └──────────────────┘
                                     │
                            ┌────────▼────────┐
                            │    pgwq.jobs    │
                            └────────┬────────┘
                                     │
            ┌─────────────────┬──────┴───────┬──────────────────┐
            │                 │              │                  │
    ┌───────▼────────┐ ┌──────▼───────┐ ┌────▼─────────┐ ┌──────▼─────────┐
    │  Poll Loop     │ │ Reaper Loop  │ │ Retention    │ │ (handler pool, │
    │  every N ms    │ │ SKIP LOCKED  │ │ Sweeper      │ │  JoinSet)      │
    │  CTE + UPDATE  │ │ flip stale   │ │ DELETE       │ │                │
    │  SKIP LOCKED   │ │ → retry      │ │ done/dead    │ │                │
    │  RETURNING *   │ │ (no advisory)│ │ > TTL        │ │                │
    └───────┬────────┘ └──────────────┘ └──────────────┘ └────────────────┘
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

        // Acquire one permit upfront — gates whole tick.
        let permit = tokio::select! {
            r = state.semaphore.clone().acquire_owned() => r,
            _ = state.shutdown.cancelled() => break,
        };
        let Ok(permit) = permit else { break }; // semaphore closed

        let batch = claim_batch(&state.pool, &state.queue, state.batch_size).await;
        match batch {
            Ok(rows) if rows.is_empty() => {
                drop(permit);
                continue;
            }
            Ok(rows) => {
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
            Err(e) => {
                tracing::warn!(error = %e, "claim batch failed; will retry next tick");
                drop(permit);
            }
        }
    }
}
```

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
    lease_token = gen_random_uuid(),
    updated_at = now()
FROM claimed
WHERE j.id = claimed.id
RETURNING j.id, j.public_id, j.queue, j.payload, j.attempts,
          j.first_attempted_at, j.lease_token;
```

Załatwia:
- Atomic claim (`FOR UPDATE SKIP LOCKED` — multi-worker safe).
- Inkrementacja attempts w tej samej query.
- Scheduled jobs (`run_at <= now()`).
- Fresh `lease_token` per claim — fencing token w `mark_*` queries.

### Reaper (SKIP LOCKED, no advisory lock)

Apalis używa `INNER JOIN apalis.workers` + advisory locki = kompleks i race
conditions. My idziemy prościej: SKIP LOCKED na stale-running rows.
N replik **naturalnie** partycjonuje pracę.

```sql
WITH reaped AS (
    SELECT id FROM pgwq.jobs
    WHERE status = 'running'
      AND last_attempted_at < now() - $1::interval
    ORDER BY last_attempted_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pgwq.jobs j
SET status = 'awaiting_retry',
    last_error = COALESCE(last_error, 'lease_expired'),
    lease_token = NULL,
    updated_at = now()
FROM reaped
WHERE j.id = reaped.id
RETURNING j.id, j.attempts;
```

Reaper-side decyzja czy flip → `awaiting_retry` czy `dead` (gdy
`attempts >= max_attempts`) — drugi update w tej samej transakcji
po zwracanym set:

```sql
UPDATE pgwq.jobs
SET status = 'dead',
    finished_at = now(),
    last_error = COALESCE(last_error, 'lease_expired_max_attempts'),
    updated_at = now()
WHERE id = ANY($1::bigint[]) AND attempts >= $2;
```

Brak advisory locków → brak leak'u → brak stuck rows.

### Mark queries (fencing token w WHERE)

```sql
-- mark_done
UPDATE pgwq.jobs
SET status = 'done',
    finished_at = now(),
    last_error = NULL,
    lease_token = NULL,
    updated_at = now()
WHERE id = $1 AND status = 'running' AND lease_token = $2;

-- mark_retry (po Err)
UPDATE pgwq.jobs
SET status = 'awaiting_retry',
    last_error = $3,
    run_at = $4,  -- now() + backoff(attempts) lub user-supplied in_
    lease_token = NULL,
    updated_at = now()
WHERE id = $1 AND status = 'running' AND lease_token = $2;

-- mark_dead (Permanent error lub max_attempts exhausted)
UPDATE pgwq.jobs
SET status = 'dead',
    finished_at = now(),
    last_error = $3,
    lease_token = NULL,
    updated_at = now()
WHERE id = $1
  AND status IN ('running', 'awaiting_retry')
  AND lease_token = $2;
```

Każda zachowuje `WHERE status = ... AND lease_token = $2`. Stary worker
który próbuje mark_done po reaperze nie znajdzie row'a (lease_token się
nie zgadza) → 0 rows affected → handler się dowiaduje, loguje warning,
nie commituje side-effect. Apalis ma tylko status guard, nie ma fencing
token → race window jest realny.

### Retention sweeper

Osobny tokio task, interval = `retention_interval` (default 1h):

```sql
-- Done — krótszy TTL (default 7d), to noise.
DELETE FROM pgwq.jobs
WHERE status = 'done' AND finished_at < now() - $1::interval;

-- Dead — opcjonalny TTL (default ∞), to diagnostic gold.
DELETE FROM pgwq.jobs
WHERE status = 'dead' AND finished_at < now() - $2::interval;
```

Drugie query odpalane tylko gdy `dead_retention` != None.

### Batch push (perf — outbox use case)

```sql
INSERT INTO pgwq.jobs (queue, payload, public_id, run_at)
SELECT $1, unnest($2::bytea[]), unnest($3::uuid[]), unnest($4::timestamptz[])
RETURNING id, public_id;
```

Lub dla bardzo dużych batches → `COPY FROM STDIN` przez sqlx
`copy_in_raw`. API:

```rust
impl Pusher {
    pub async fn push_batch<T: Serialize>(
        &self,
        tx: &mut PgConnection,
        items: &[T],
    ) -> Result<Vec<Uuid>, PushError>;
}
```

Apalis ma `unnest($1::text[],...)` pattern w `sink.sql` — kopiujemy
i dodajemy opcjonalne `ON CONFLICT DO NOTHING` (gdy user supplyuje
external idempotency key — TBD jako follow-up feature).

## Retry backoff policy

```rust
pub enum BackoffPolicy {
    Fixed(Duration),
    Linear { base: Duration, increment: Duration, cap: Duration },
    Exponential {
        base: Duration,
        factor: f64,
        cap: Duration,
        jitter: f64,  // ratio 0.0..=1.0
    },
}

impl BackoffPolicy {
    pub fn next(&self, attempt: u32) -> Duration { /* ... */ }
}
```

Plan v0 mówił "domyślnie 0 (next poll cycle)". To footgun — flapping
handler spali `max_attempts` w jednej sekundzie. Default v1:
`Exponential { base: 1s, factor: 2.0, cap: 5min, jitter: 0.2 }`. Daje
sequence ~1s, 2s, 4s, 8s, ... 5min (z ±20% jitter).

User może override per-call w `Outcome::Retry { in_: Some(d) }`. Jitter
ważny przy thundering herd (10 jobs fails równocześnie → bez jittera
wszystkie wracają w tym samym ticku).

## Shutdown semantics

`WorkerHandle::shutdown(timeout: Duration) -> Result<Stats, ShutdownError>`:

1. Cancel `state.shutdown` token → wszystkie pętle (poll, reaper,
   retention) wychodzą natychmiast z tokio::select.
2. Drop semaphore permits acquire — nowe handler'y się nie spawn'ują.
3. Czekaj na `JoinSet::join_next` w pętli z `timeout`.
4. Po timeout: abort wszystkich pozostałych tasks, return Stats z
   `aborted_count`, `completed_count`, `failed_count`.

Aborted handlers nie wywołują `mark_done`/`mark_retry` → ich rows
zostają `status='running'` z aktualnym `lease_token`. Reaper zauważy
po `lease_timeout` i flipnie → `awaiting_retry`. Czyli graceful
shutdown po timeout zachowuje correctness, kosztem opóźnionego retry.

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
   N tasków idą sekwencyjnie, `concurrency(N)` że wykonują się
   parallel.
3. **`tests/max_attempts_behavior.rs`** — handler zawsze fail Retry.
   Po `max_attempts(3)` row jest `dead`, po `max_attempts(5)` row
   jest `dead` po 5 próbach.
4. **`tests/lease_timeout_behavior.rs`** — symulacja crashed worker
   (manual UPDATE status='running' z stale last_attempted_at), reaper
   z `lease_timeout(1s)` flipuje wcześniej niż z `lease_timeout(10s)`.
5. **`tests/batch_size_behavior.rs`** — push 100 jobs, `batch_size(10)`
   vs `batch_size(50)` daje różny shape claim batches.
6. **`tests/scheduled_run_at.rs`** — push z `run_at = now() + 2s`,
   worker nie pickupuje przed t+2s.
7. **`tests/retry_backoff_behavior.rs`** — handler zawsze fail z
   `Outcome::Retry { in_: None }`. Policy `Fixed(1s)` → kolejne
   `run_at` różnią się ~1s. Policy `Exponential { base: 100ms, factor:
   2.0, ... }` → kolejne `run_at` rosną geometrycznie. Jitter ±20%
   testowany przez 50 prób + assert że standard deviation > 0.
8. **`tests/retry_in_override.rs`** — handler zwraca `Outcome::Retry
   { in_: Some(5s) }`. Niezależnie od policy, `run_at` = ~now + 5s.
9. **`tests/done_retention_behavior.rs`** — `done_retention(1s)`:
   done rows znikają po 1s + interval. `done_retention(1h)`: trzymane
   dłużej.
10. **`tests/dead_retention_forever.rs`** — `dead_retention(None)`:
    dead rows trwają przez kilka sweep cycles. Z `dead_retention(Some(1s))`
    znikają.

### Crash safety / correctness tests

11. **`tests/skip_locked_no_double_claim.rs`** — 2 workery równolegle
    pollują, push 100 jobs, suma claimed == 100 (no double-claim).
12. **`tests/stale_running_reaped.rs`** — analog do
    `rust_event_outbox::stale_running_reaper.rs`.
13. **`tests/fencing_token_no_double_run.rs`** — claim job → ręcznie
    flipuje `last_attempted_at` w przeszłość → reaper flipuje status
    + zeruje lease_token → stary handler kończy się i próbuje
    `mark_done` ze starym tokenem → 0 rows affected. Row pozostaje
    `awaiting_retry` (nie skacze do done).
14. **`tests/shutdown_graceful.rs`** — `shutdown(5s)` czeka na drain;
    jeśli handler trwa > 5s, abort'owany; stats poprawne.
15. **`tests/shutdown_cancels_poll_loop.rs`** — mid-poll-sleep
    shutdown wychodzi natychmiast.
16. **`tests/migrator_schema.rs`** — schema CREATE'd correctly,
    indexes obecne, CHECK constraints fire na invalid input
    (try INSERT z `status='running'` bez `lease_token` → reject),
    `fillfactor` ustawione (sprawdzić w `pg_class.reloptions`).
17. **`tests/reaper_no_advisory_lock_leak.rs`** — uruchom 3 reapery
    równolegle, push 100 stale-running rows, suma reaped == 100,
    żaden nie został z dwóch reaperów jednocześnie (SKIP LOCKED
    behavior). Po teście `pg_locks` nie ma session locks z naszych
    connection ids.
18. **`tests/batch_push_throughput.rs`** — push 1000 jobs jako
    single batch vs 1000 single push. Batch musi być wyraźnie
    szybszy (≥ 5x).

### No-DB / unit tests

19. **`tests/builder_validation.rs`** — config validation
    (`poll_interval == 0` rejected, `concurrency == 0` rejected, etc.)
20. **`tests/payload_codec.rs`** — round-trip serde + bytea.
21. **`tests/backoff_policy_unit.rs`** — `BackoffPolicy::next(attempt)`
    dla każdej wariantu — pure-fn testy bez DB.

### Anti-pattern guard

Nie dodawać testów które testują **identyczność struktury**
(np. `assert_eq!(builder.build().poll_interval, Duration::from_millis(500))`).
Każdy test musi mierzyć **behavior** widoczny w DB lub przez observable
side-effect.

## Implementation phases

### Faza 0 — repo init

- `cargo init --lib` w `pg_work_queue/`.
- `Cargo.toml`: `sqlx 0.8` (postgres, runtime-tokio-rustls, uuid,
  chrono, json, macros, migrate), `tokio` (full), `tracing`, `serde`,
  `thiserror`, `async-trait`, `chrono`, `uuid` (v4 + v7 + serde),
  `anyhow`, `rand` (jitter), `tokio-util` (sync feature dla
  `CancellationToken`).
- Dev: `testcontainers`, `testcontainers-modules` (postgres),
  `tracing-subscriber`.
- `migrations/20260513000000_v01_init.sql` z schemą wyżej.
- Skeleton `lib.rs`: `pub mod migrator; pub mod worker; pub mod
  pusher; pub mod codec; pub mod backoff;`.

### Faza 1 — push + migracja + manual claim (no worker yet)

- `Pusher::push<T: Serialize>(tx, payload, run_at)` → INSERT.
- `Pusher::push_batch<T>(tx, &[T])` — unnest variant.
- `pg_work_queue::migrator()` re-export sqlx::Migrator.
- Test: `migrator_schema.rs`, `batch_push_throughput.rs`.

### Faza 2 — claim_batch SQL + Job/JobContext types

- `claim_batch(pool, queue, batch_size, now)` → `Vec<JobRow>`.
- `pub struct Job<T>` + `pub struct JobContext { attempt, lease_token, ... }`.
- Codec generic.
- Test: `skip_locked_no_double_claim.rs`.

### Faza 3 — single-shot worker + mark queries z fencing

- `Worker::tick_once(...)` — fetches batch, runs handlers sekwencyjnie,
  `mark_done`/`mark_retry`/`mark_dead` z `lease_token`.
- Test: end-to-end smoke.

### Faza 4 — poll loop + concurrency

- `Worker::start()` → spawn poll loop + JoinSet.
- `CancellationToken` shutdown plumbing.
- Test: `poll_interval_behavior.rs`, `concurrency_behavior.rs`.

### Faza 5 — reaper (SKIP LOCKED)

- Spawned alongside poll loop, **no advisory lock**.
- Reaper-side check `attempts >= max_attempts` → flip do `dead` zamiast
  `awaiting_retry`.
- Test: `stale_running_reaped.rs`, `lease_timeout_behavior.rs`,
  `reaper_no_advisory_lock_leak.rs`.

### Faza 6 — retry semantics + BackoffPolicy

- `Outcome::Retry { in_: Option<Duration> }` z fallback do policy.
- `BackoffPolicy::{Fixed, Linear, Exponential}` z jitter.
- `mark_retry` ustawia `run_at = now() + backoff_or_override`.
- Test: `max_attempts_behavior.rs`, `scheduled_run_at.rs`,
  `retry_backoff_behavior.rs`, `retry_in_override.rs`,
  `backoff_policy_unit.rs`.

### Faza 7 — shutdown semantics

- `WorkerHandle::shutdown(timeout)`.
- Test: `shutdown_graceful.rs`, `shutdown_cancels_poll_loop.rs`,
  `fencing_token_no_double_run.rs`.

### Faza 8 — retention sweeper

- Osobny tokio task: `done_retention`, `dead_retention`,
  `retention_interval`.
- Test: `done_retention_behavior.rs`, `dead_retention_forever.rs`.

### Faza 9 — docs + README

- README z quick start.
- Doc comments na każdym public knob z "observable effect"
  wzmianką + link do testu.
- Cargo.toml `description`, `repository`, `documentation`,
  `categories`, `keywords`.
- Możliwe: tag `v0.1.0`. Crates.io publish: TBD.

### Faza 10 — integracja z `rust_event_outbox`

- W `rust_event_outbox` v0.6:
  - Drop `apalis`, `apalis-postgres` z deps.
  - Dodaj `pg_work_queue = "0.1"` (lub path dep w workspace na start).
  - W `dispatch_in_tx`: zamiast `apalis_postgres::sink::push_tasks`
    użyj `pg_work_queue::Pusher::push_batch` z payload =
    `DeliveryJob { delivery_id: i64, mode: Handler|Channel }`.
  - W `outbox.rs::start_workers`: użyj `pg_work_queue::Worker::builder`.
  - Drop `spawn_stale_reaper` — `pg_work_queue` ma własny reaper na
    queue table. **Ale:** `outbox.*_deliveries.status='running'` lease
    nadal wymaga osobnego mechanizmu (te tabele nie są zarządzane
    przez pg_work_queue) — możliwie zachować outbox-side reaper jako
    defense-in-depth dla deliveries.
  - Drop `purge_apalis_done_jobs` + `apalis_done_retention` config —
    `pg_work_queue` ma własny retention sweeper.
  - Update wszystkie testy DB — `apalis.jobs` znika, zastąpione
    `pgwq.jobs`.
  - Update `pg::run_all_migrations` — usuń apalis migrator, dodaj
    `pg_work_queue::migrator()`.
  - Bump 0.5.0 → 0.6.0, breaking change, fresh schema, dokumentacja.

## Open questions / decisions TBD

1. **MSRV** — Rust 2024 edition (1.85+). Konsystencja z apalis-postgres
   i `rust_event_outbox`.
2. **`payload BYTEA` vs `JSONB`** — BYTEA na start. JSONB jako opt-in
   feature flag jeśli ktoś chce SQL-side filtering.
3. **Multi-tenant via `queue` column** — single table, OK na start.
4. **Plugin layer architecture (tower middleware)?** NIE. Handler to
   prosta `async fn(T, JobContext) -> Outcome`.
5. **Worker registration table (`pgwq.workers`)?** NIE.
   Reaper używa `last_attempted_at` + `lease_token` jako proxy. Plus
   lessons z apalisa (`apalis.workers` + advisory locks → leak +
   stuck rows).
6. **Idempotency key column (`unique_key TEXT UNIQUE NULL`)?** Niski-
   -koszt nice-to-have, **nie na v0.1**. Add gdy user supply use case.
7. **Tracing span structure** — `pg_work_queue.poll_tick`,
   `pg_work_queue.claim_batch`, `pg_work_queue.handle_job`,
   `pg_work_queue.reap_tick`, `pg_work_queue.retention_tick`. Każdy
   z attrs: `queue`, `job.id`, `job.attempts`, `claimed_count`.
8. **Multi-queue worker** — one-queue-per-worker na v0.1.
9. **PgBouncer transaction-pooling compatibility** — brak
   `LISTEN/NOTIFY`, brak session-scoped advisory locks → powinno
   działać. Dodać test w CI matrix.
10. **Generic vs concrete payload** — `Worker<T>` generic.
11. **License** — MIT.
12. **`Outcome::Pause` / `Outcome::Abort`?** Niepotrzebne na start.
13. **PgQ/Kraken-style two-table design (jobs + jobs_archive)?**
    Plan v1 trzyma jedną tabelę + retention sweeper. Two-table można
    rozważyć jeśli `done` row volume + retention TTL daje > 10M rows
    w hot table. Far future problem.

## Wpływ na `rust_event_outbox` (bez zmian względem v0)

### Public API zmiany (v0.5 → v0.6)

- **`Outbox::start_workers(monitor)` → `Outbox::start_workers()`** —
  zwraca `OutboxHandle { shutdown(timeout) }` zamiast `apalis Monitor`.
  Breaking dla wszystkich konsumentów.
- **`OutboxConfigBuilder` knoby zmiana**: dochodzi
  `handler_poll_interval(Duration)` + `channel_poll_interval(Duration)`
  (oba domyślnie 1s). Reszta (`max_attempts`, `concurrency`, retry
  backoff) **bez zmian semantycznie** — pg_work_queue obsługuje to
  samo.
- **Drop `apalis_done_retention` + `purge_apalis_done_jobs`** — orphan
  Done rows znikają wraz z apalis.
- **Drop `ApalisConfig` reexport**.
- **Wewnętrznie**: dispatch.rs używa `pg_work_queue::Pusher::push_batch`
  zamiast `apalis_postgres::sink::push_tasks`. Reaper w outbox.rs
  prawdopodobnie usuwalny dla queue jobs; outbox-side
  `*_deliveries.status='running'` lease może wymagać osobnego mechanizmu.

### Co zostaje bez zmian

- Schema `outbox.*` (events, dispatch_keys, handler_deliveries,
  channel_subscriptions, channel_deliveries).
- Public API dispatch (`Outbox::dispatch<E: DomainEvent>(...)`),
  history, subscriptions, channel impls, handler trait.
- Handler context (`HandlerContext.delivery_id` — wciąż public_id UUID).
- Retry semantics z perspektywy user'a (Transient/Permanent return
  values — mapowane na `Outcome::Retry`/`Outcome::Dead`).
- Reaper threshold + interval defaults.

### Risk + mitigation

- **Wszystkie obecne 44 testy DB to safety net.** Po migracji każdy
  musi nadal przechodzić.
- **Behavioral testy w pg_work_queue są red-first** (TDD od początku).

## Anti-patterns z których wyciągnęliśmy lekcje

Te zasady są **hard rules** w pg_work_queue:

1. **Każdy public knob musi mieć integracyjny test który sprawdza
   observable effect przy dwóch różnych wartościach.** Lesson z
   `rust_event_outbox` v0.4 `handler_max_poll_backoff` bug'a.
2. **Nie wystawiaj knoba dopóki nie zweryfikowałeś że jest READ w
   hot-path.** Lesson z apalis `Config::with_poll_interval`.
3. **Nie ufaj nazwie struct'a, dopóki nie przeczytałeś `Stream::poll_next`
   / `Future::poll` implementation.** Lesson z apalis `PgPollFetcher`.
4. **Test który passował od pierwszego compile'a to red flag.** TDD
   wymaga RED przed GREEN.
5. **Verify-before-completion przed każdym claim "fix done".** Cargo
   test passes ≠ fix works.
6. **(nowy)** **Każdy `WHERE id = $1` UPDATE w state-machine musi mieć
   dodatkowy guard.** Status guard (`AND status = ...`) nie wystarcza —
   race window między reaper a worker. Fencing token (`AND lease_token
   = $N`) wymagany. Lesson z apalis ack race.
7. **(nowy)** **Każdy `pg_advisory_lock*` musi mieć udokumentowany
   release path.** Lesson z apalis `register.sql` leak. (Stąd: my w
   ogóle nie używamy advisory locks — eliminujemy klasę bugów.)
8. **(nowy)** **Każdy commented-out kod w state-machine logic to bug
   waiting to happen.** Lesson z apalis `AbortError` branch (literalnie
   zakomentowany w `calculate_status`). Code review hard-blokuje
   ten pattern.

## Roadmap

1. **pg_work_queue v0.1.0** — Fazy 0-9 wyżej. ~2-3 dni roboty.
2. **rust_event_outbox v0.6.0** — Faza 10. ~1-2 dni roboty.
3. **(later) OSS publish** — crates.io publish dla pg_work_queue jeśli
   wartościowe. README z demo + benchmark vs apalis-postgres.

## Co dalej

Następny krok: faza 0 (repo init + skeleton + first migration).
TDD od pierwszej linijki — nie commitujemy fazy 1 bez green
`migrator_schema.rs` + `batch_push_throughput.rs`. Behavior-first,
zawsze.
