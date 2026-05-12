# Synteza researchu — apalis vs PLAN.md vs ekosystem

> Skompilowane z trzech równoległych agentów:
> - `apalis_online.md` — GitHub issues, blog posts, Recall.ai
> - `apalis_source_analysis.md` — clone rc.9 monorepo + rc.8 split repo
> - `postgres_queue_patterns.md` — River, gue, pg-boss, Solid Queue, Que, neoq

## Sytuacja apalisa (kontekst którego plan nie ma)

`apalis-postgres` nie istnieje już w monorepo `geofmureithi/apalis`. Został
**wycięty do osobnego repo `apalis-dev/apalis-postgres`** w PR #586 (rc.1).

- `apalis-dev/apalis-postgres` repo utworzono **2025-08-19**.
- Pierwsza alpha: **2025-10-25**.
- Cykl rc.1 (grudzień 2025) → rc.8 (maj 2026).
- Mniej niż rok od pierwszego release. Strukturalnie młody i niespokojny.
- Multiple silently-breaking changes (JSONB→BYTEA payload column, brakujące
  PRIMARY KEY-e dodane dopiero w rc.1).

Plan referuje paths typu `apalis-postgres-1.0.0-rc.7/src/lib.rs:87` —
to crates.io publish layout. Konceptualnie OK, ale plan nie wzmiankuje
że apalis przechodzi multi-repo re-architecture i że jego "stable target"
to ruchomy cel.

## Verdict na każdy claim z PLAN.md

| # | Claim z planu | Verdict |
|---|---|---|
| 1 | `PgPollFetcher::next_backoff` hardcodes `1s→5min`, `Config::with_poll_interval` zapisywany ale nieczytany | **POTWIERDZONE** (`fetcher.rs:84,160-163`) |
| 2 | `pg_notify('apalis::job::insert', …)` per INSERT bierze `AccessExclusiveLock` na cały klaster | **CZĘŚCIOWO** — trigger istnieje i jest per-row (`migrations/20251018165121_notify_run_at.sql`); **lock semantics technicznie wrong** — to nie `AccessExclusiveLock` na tabeli, to `AccessExclusiveLock` na locktype=`database` (per Postgres `async.c` source) lub równoważnie `LWLock NotifyQueueLock` w pamięci. Praktyczny efekt **jest taki sam** (serializacja wszystkich NOTIFY-issuing commits cluster-wide), ale wording "lock na cały klaster" trzeba zaostrzyć technicznie |
| 3 | `ack = UPDATE` zamiast DELETE → rows accumulate, fresh ULID per push w outboxie | **POTWIERDZONE** (`queries/task/ack.sql`); manual `vacuum()` only |
| 4 | `RetryAfterError(_, duration)` — duration nie honored przez ack | **POTWIERDZONE** — `get_duration()` ma **zero callers w całym crate** |
| 5 | `LockTaskLayer`, `PgAck`, `initial_heartbeat`, `keep_alive_stream`, `reenqueue_orphaned_stream` są `pub(crate)` → niemożliwy custom Backend impl | **OBALONE** — wszystkie są `pub` i re-eksportowane (`lib.rs:25-33`); **zero `pub(crate)` w crate**. **Plan musi to wyciąć.** |
| 6 | Double retry budget (apalis `RetryPolicy::retries(N)` + DB `max_attempts`) | **POTWIERDZONE i gorzej** — to **triple counter**: in-memory `RetryPolicy`, DB `attempts`, DB `max_attempts`. In-memory budget **resetuje się** per worker lease — czyli crashed worker = retry budget zerowany |

## Claimy poboczne planu wymagające poprawki

- **"Recall.ai miał 3 outage'e i wymigrowali w 1 dzień"** — blog post Recall.ai
  (`postgres-listen-notify-does-not-scale`) jest **library-agnostic**. Nigdzie
  nie nazywa apalisa. HN thread (44490510) też nie. To jest **inference autora
  planu, nie udokumentowany fakt**. Trzeba zmiękczyć ("podobne wzorce w
  branży", "publiczny case Recall.ai pokazuje że…"). Bez tego claim jest
  rhetorically slabszy bo łatwo go zweryfikować jako overreach.

- **AccessExclusiveLock wording** — "globalny lock przy commit" jest
  technicznie nieścisłe. Precyzyjniej: "NOTIFY przy commit bierze
  `LWLock NotifyQueueLock` (równoważnie `AccessExclusiveLock` na locktype=
  `database` w `pg_locks`). Sam pg_notify nie tknie user tables, ale
  **serializuje wszystkie inne NOTIFY-issuing commits cluster-wide**. Dla
  apalis (trigger NOTIFY per INSERT) efekt = wszystkie commity-via-apalis
  się serializują. Recall.ai udokumentował publicznie tę patologię na
  write-heavy workload w marcu 2025." Konkluzja planu (no LISTEN/NOTIFY) jest
  **valid**, tylko reasoning trzeba poprawić.

## Issues w apalis których plan **nie** złapał (a powinien się ich strzec)

1. **Worker registration leaks session-scoped advisory locks.**
   `pg_try_advisory_lock(hashtext(workers.id))` w `register.sql` nigdy nie
   jest released. Trzymane przez connection w sqlx pool indefinitely. To
   memory leak na pg side + ryzyko że pool connection recycle = lock
   "transfer" do innego workera.

2. **Reaper join-to-workers race.** `reenqueue_orphaned` requires
   `INNER JOIN apalis.workers`. Jeśli worker row jest purged (np. manual
   cleanup), jego locked jobs **stuck permanently**. Brak fencing token =
   ryzyko double-execution między reaperem a recovering workerem.

3. **`AbortError` branch literalnie zakomentowany w `calculate_status`**
   (`src/ack.rs:70`). Aborty po cichu stają się `Failed` i są retry'owane
   do `max_attempts`. To live bug w rc.8.

4. **Brak `CHECK`/`ENUM` na `status` column** w schemie. Każda wartość
   string jest valid z perspektywy DB. Plan słusznie używa ENUM.

5. **Brak composite index na hot fetch path** `(job_type, status, run_at)`.
   Redundant indexes elsewhere. Plan używa partial index z trzema kolumnami
   — lepiej.

6. **PRIMARY KEY-e dodane 5 lat po release.** Schema design rot.

7. **`AccessExclusiveLock` ack workflow** — `ack=UPDATE` + brak DELETE +
   `vacuum()` jako separate user API = wymaga cron'u po stronie usera.
   Plan tego unika (`done` rows usuwane przez `purge_terminal`).

8. **Stats query (`queries/backend/stats.sql`) jest SQLite syntax** (`?1`
   placeholdery) w postgres queries dir. Unused dead code — sygnał slabej
   discipline.

9. **`wait_for` to 500ms sleep-poll z `.unwrap()` panics** w hot path.

10. **`metrics::global` robi 24 full-table scans `apalis.jobs` per call.**
    Tragiczne na większych queue'ach.

11. **`Shared` driver `.unwrap()` na listener-connect / listen / send** —
    single-point-of-failure, panic na transient connection issue.

## Co plan robi **dobrze** (zostawić — broader research potwierdził):

✅ **`SELECT ... FOR UPDATE SKIP LOCKED` + CTE + `UPDATE ... RETURNING`** —
mainstream pattern (River, gue, pg-boss, Solid Queue, neoq). Que używa
advisory locków co **zabija PgBouncer transaction-mode compat**. Plan
słusznie tego unika.

✅ **Single `jobs` table + `queue` column + partial indexes na hot
status sets + `BIGINT IDENTITY` PK + `UUID public_id`** — match z River
i gue exactly. pg-boss robi partycjonowanie per-queue, ale to overkill
dla ≤dozens kolejek.

✅ **Polling-only, no LISTEN/NOTIFY** — validated przez Recall.ai outage data.
Nawet River (który **ma** NOTIFY) i tak polluje jako fallback (issue #960
pokazuje userów uderzających w 1s polling floor). Pure polling jest
honest o swoim latency floor i pomija commit-serialization cliff.

✅ **ENUM `job_status` + named CHECK constraints + `COLLATE "C"` na
queue + `BYTEA` payload** — schema rygorystyczniejsza niż apalis.

✅ **`ack=DELETE` (przez `purge_terminal`) zamiast `UPDATE`-and-leave** —
unika apalis-style row accumulation.

✅ **Authoritative DB-side `max_attempts`, no in-memory retry budget** —
unika apalis triple-counter footgun.

✅ **Każdy public knob ma behavioral test przy 2 wartościach** — to
hard rule z `rust_event_outbox` lessons. Bezpośrednio chroni przed
apalis-style "config stored but unread" bug.

## Co plan POWINIEN dodać (gaps z researchu)

### Wysokie priorytetowe

**P1. Default retry backoff dla `Outcome::Retry { in_: None }`.**
Plan mówi "domyślnie 0 (następny poll cycle)". To **footgun**: flapping
handler spali `max_attempts` w jednej sekundzie. Default `base * 2^attempts`
z jitter (jak każdy mainstream lib). Plan może to wystawić jako
`Worker::retry_backoff(BackoffPolicy)` knob z domyślnym `Exponential { base:
1s, factor: 2, cap: 5min, jitter: 0.2 }`. User może override w
`Outcome::Retry { in_: Some(d) }`.

**P2. Reaper przez `SELECT ... FOR UPDATE SKIP LOCKED` zamiast advisory lock.**
Plan używa `pg_advisory_lock` żeby multi-replica nie reapowała równolegle.
Ale `SKIP LOCKED` na stale-running rows naturalnie partycjonuje pracę
między replikami — bez extra primitive, bez extra testów, bez ryzyka
stuck-lock. River i gue to robią. Plan zmienić.

**P3. `fillfactor=80` + per-table autovacuum tuning** w migracji.
Bez tego hot queue table bloatuje szybko (klasyczny problem Brandura
w "Building Robust Systems with ACID and Constraints"). Tani fix
dnia dzisiejszego — dodać do migracji:

```sql
ALTER TABLE pg_work_queue.jobs SET (
  fillfactor = 80,
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05
);
```

**P4. Dwa retention knoby: `done_retention` (default 7d) vs `dead_retention`
(default ∞).** Plan ma jeden `purge_terminal(ttl)`. `done` rows to noise,
ale `dead` rows to diagnostic gold (czemu konkretny job zginął miesiąc
temu). Nie chcesz ich usuwać tym samym TTL.

### Średnie priorytetowe

**P5. Batch INSERT API (`Pusher::push_batch`).** River's main perf
claim to `COPY FROM`. Outbox consumer (główny use case planu) będzie
wstawiał N wierszy per event → per-row round-trips zdominują. Implementacja:
`INSERT INTO ... SELECT * FROM unnest($1::bytea[], $2::text[], ...)` lub
sqlx `copy_in_raw`. Apalis to robi (`sink.sql` z `unnest($1::text[],...)`)
i to **jest dobry pattern** — warto skopiować, dodać `ON CONFLICT DO
NOTHING` dla idempotency.

**P6. Fencing token przeciw double-execution.** Plan ma reaper który
flipuje `running → awaiting_retry` po `lease_timeout`. Ale jeśli stary
worker żyje i kończy job po reaper'ze, `mark_done` zaktualizuje row
który już jest claim'owany przez nowego workera (race). Plan ma `WHERE
status = 'running'` guard — to częściowy fix, ale nie chroni przed
sytuacją "stary worker mark_done po nowym worker claim_batch". Trzeba
albo fencing tokenu (`attempts` jako logical token — claim wymaga match
`attempts`), albo lease_token UUID w `mark_done WHERE id=$1 AND
lease_token=$2`. Apalis-postgres tego NIE ma — to faktyczny bug, plan
ma szansę go uniknąć.

**P7. Multi-queue worker (jeden worker, N queue names).** Plan decyduje
"one-queue-per-worker, simpler". OK, ale wiele real-world deployments
ma 5-15 niskoruchowych queue'ów i odpalanie 15 connection pools to
overhead. Alternatywa: `claim_batch` z `queue = ANY($1::text[])`,
priorytetyzacja round-robin po queue. To +50 LOC, nie dramat. Zachować
"one-queue-per-worker as default" + opcjonalnie `.queues(&[...])`.

### Niskie priorytetowe / TBD

**P8. Idempotency key column** (`unique_key TEXT UNIQUE NULL`)?
Apalis dodał to dopiero w rc.8 (#736). Niski-koszt nice-to-have.

**P9. Tracing span structure.** Plan wymienia (`pg_work_queue.poll_tick`,
`.claim_batch`, `.job`). Apalis ma OpenTelemetry context propagation
(#716). Worth budować with first-class otel attrs (job.id, job.attempts,
job.queue) zamiast tylko message names.

## Korekty do PLAN.md — TODO

Konkretna lista poprawek do zrobienia w planie zanim cokolwiek napiszemy:

1. **§ Motywacja, claim #5** — usunąć cały bullet o `pub(crate)`.
   Refuted. Możliwe że apalis-postgres ma inne realne integration warts
   (custom Backend impl wymaga reimplementacji ~2.2k LOC i tighty
   sprzężonych traits) — ale nie z powodu visibility.
2. **§ Motywacja, claim #2** — przepisać AccessExclusiveLock wording
   do `LWLock NotifyQueueLock` / `locktype=database`. Konkluzja zostaje.
3. **§ Motywacja, claim #6** — zamienić "Recall.ai migrated w 1 dzień"
   na "publiczny case Recall.ai (`postgres-listen-notify-does-not-scale`)
   pokazuje że NOTIFY-per-INSERT degraduje write-heavy workloads. Library-
   -agnostic, ale pattern apalisa pasuje do tej kategorii."
4. **§ Anti-features** — pozostawić.
5. **§ Schema** — dodać `fillfactor=80` + autovacuum tuning + lease_token UUID
   column (fencing).
6. **§ Reaper** — zmienić advisory lock na `SELECT FOR UPDATE SKIP LOCKED`.
7. **§ Mark queries** — `mark_done`, `mark_retry`, `mark_dead` muszą
   testować `lease_token=$N` w `WHERE`, nie tylko status.
8. **§ Builder knobs** — dodać `retry_backoff(BackoffPolicy)`, dodać
   `done_retention(Duration)` + `dead_retention(Duration)`, opcjonalnie
   `.queues(&[...])` jako follow-up.
9. **§ Outcome enum** — `Outcome::Retry { in_: Option<Duration>, ... }`.
   Domyślnie `None` = backoff policy z konfiguracji, nie "next tick".
10. **§ Public API** — dodać `Pusher::push_batch(&[T])` z `unnest()` lub
    `COPY FROM`.
11. **§ Test strategy** — dodać:
    - `tests/retry_backoff_behavior.rs` — `Outcome::Retry None` z policy
      `Exponential` daje rosnące delays.
    - `tests/fencing_token_no_double_run.rs` — reaper flipuje, stary
      worker próbuje `mark_done` ze starym tokenem → fails, no DB update.
    - `tests/done_vs_dead_retention.rs` — różne TTL.
    - `tests/push_batch_throughput.rs` — N batch insert vs N single
      inserts (czas).

## Sumarycznie: plan jest **broadly correct, surgically off**

- 4/6 motywacji-claimów verified. 1 needs sharpening. 1 must be removed.
- Core architectural choices (no NOTIFY, SKIP LOCKED, single table,
  partial indexes, separate ENUM, BIGINT+UUID) — wszystkie validated
  przeciw River/gue/pg-boss best practices.
- Brakujące rzeczy: retry backoff default, fencing token, reaper-via-
  skip-locked, fillfactor, batch push, dwa retention knoby.
- Apalis-postgres jest w **young+turbulent** stanie (split do osobnego
  repo 2025-08, rc.1→rc.8 w pół roku, breaking changes silently). To
  **wzmacnia** motywację do własnego crate, ale plan może to wykorzystać
  silniej w README/marketing (zamiast Recall.ai narrative).
- Realistic LOC szacunek: pg_work_queue v0.1 = ~1.5-3 k LOC Rust + ~100
  LOC SQL. Apalis-postgres ma ~2.2k LOC Rust + 1.5k LOC SQL/migrations
  na backendzie + ~11k LOC apalis-core które trzeba ucztać żeby zrobić
  custom Backend. Nasza redukcja ~5-10x w domain złożoności.
