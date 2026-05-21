# Per-Key Concurrency Limiting — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-key concurrency limit to `pg_work_queue` — a job carries an optional `concurrency_key`, and the Worker claims at most N jobs of a given key at a time, gated at claim time, under a hard single-instance assumption.

**Architecture:** A nullable `concurrency_key` column + a per-key claim CTE. The Worker keeps an in-memory per-key counter of live handler tasks (an `Arc<std::sync::Mutex<HashMap<String,u32>>>`), decremented by an RAII `KeySlotGuard` on every task exit. Each poll tick computes `headroom = limit − count` per key and passes it to the claim SQL, which claims at most `headroom` rows per key via a `LATERAL` join. No DB seeding; no cross-process coordination.

**Tech Stack:** Rust, `sqlx` (Postgres 18), `tokio`, `testcontainers` integration tests. The full design is in `docs/superpowers/specs/2026-05-21-per-key-concurrency-design.md` — read it before starting.

**Ground rules (from CLAUDE.md):**
- `cargo clippy --all-targets -- -D warnings` must pass — `pedantic`/`nursery` warn, `unwrap_used`/`expect_used`/`panic` deny in `src/`. No `.unwrap()`/`.expect()` in `src/`; tests are exempt.
- Docker must be running — every integration test boots its own PG18 container.
- README.md must move in lockstep (Task 14).
- Pinned exact dependency versions in `Cargo.toml` are intentional — do not loosen.

---

## File Structure

**Created:**
- `migrations/20260521000000_v01_concurrency_key.sql` — schema change.
- `src/key_slot.rs` — `KeySlotGuard` RAII counter guard (new focused module).
- `tests/concurrency_key_schema.rs` — schema-shape assertions.
- `tests/per_key_concurrency_limit.rs` — limit enforced (paired, two values).
- `tests/per_key_unlimited.rs` — NULL key and unconfigured key are unlimited.
- `tests/per_key_no_head_of_line.rs` — saturated key does not block other keys.
- `tests/per_key_counter_no_leak.rs` — counter returns to 0, incl. abort path.
- `tests/per_key_no_seed_restart.rs` — fresh Worker ignores ghost `running` rows.
- `tests/concurrency_key_immutable.rs` — the column cannot be UPDATEd.
- `tests/concurrency_key_pusher.rs` — push/push_at/push_batch carry the key.

**Modified:**
- `src/limits.rs` — `MAX_CONCURRENCY_KEY_LEN` constant.
- `src/error.rs` — `PushError::ConcurrencyKeyInvalid`, `#[non_exhaustive]` on `PushError`, `BuildError::{ConcurrencyKeyInvalid, ConcurrencyLimitInvalid}`.
- `src/pusher.rs` — `concurrency_key` argument on all three push methods; char-count validation.
- `src/claim.rs` — `concurrency_key` in `RawClaimedRow`, the keyed claim SQL, `claim_and_decode` gains a `headroom` parameter.
- `src/job.rs` — `concurrency_key` field on `Job<T>`.
- `src/worker.rs` — `WorkerBuilder::concurrency_limits`, the `concurrency_limits` field threaded through `WorkerBuilder`/`Worker`/`WorkerState`, the poll-loop increment + headroom snapshot, `handle_job` guard parameter, char-count queue fix.
- `src/lib.rs` — module declaration for `key_slot`.
- `README.md` — lockstep documentation (Task 14).
- ~17 `tests/*.rs` — `push_batch` call-site updates.
- 5 `tests/*.rs` — `claim_and_decode` call-site updates.
- `tests/migrator_schema.rs` — new column/index/constraint/trigger assertions.

---

## Task 1: Schema migration + `MAX_CONCURRENCY_KEY_LEN`

**Files:**
- Create: `migrations/20260521000000_v01_concurrency_key.sql`
- Modify: `src/limits.rs` (after line 16, the `MAX_QUEUE_LEN` const)
- Test: `tests/concurrency_key_schema.rs`

- [ ] **Step 1: Write the migration SQL**

Create `migrations/20260521000000_v01_concurrency_key.sql`:

```sql
-- pg_work_queue v0.1 — per-key concurrency limiting.
--
-- Adds the nullable `concurrency_key` column, a covering INCLUDE on the
-- claim index, a per-key claim index, a length CHECK (NOT VALID — all
-- pre-existing rows are NULL, hence provably valid), and an immutability
-- trigger. See docs/superpowers/specs/2026-05-21-per-key-concurrency-design.md.

-- NULL = no limit. COLLATE "C" for byte-exact, locale-free comparison
-- (matches the `queue` column).
ALTER TABLE pgwq.jobs
    ADD COLUMN concurrency_key TEXT COLLATE "C";

-- NOT VALID: metadata-only, no validating scan. Correct because every
-- existing row has concurrency_key = NULL, which satisfies the first
-- disjunct. The CHECK is still enforced on every future INSERT/UPDATE.
ALTER TABLE pgwq.jobs
    ADD CONSTRAINT jobs_concurrency_key_len
    CHECK (concurrency_key IS NULL
           OR (length(concurrency_key) >= 1 AND length(concurrency_key) <= 128))
    NOT VALID;

-- Rebuild jobs_claim_idx with concurrency_key as an INCLUDE (covering)
-- column. The (queue, run_at, id) KEY prefix is byte-identical to the old
-- index — empty-limits claim_batch range scan and FIFO order unaffected.
-- INCLUDE lets the keyed claim's `eligible_unlimited` anti-join filter on
-- concurrency_key without a heap fetch.
DROP INDEX pgwq.jobs_claim_idx;
CREATE INDEX jobs_claim_idx
    ON pgwq.jobs (queue, run_at, id) INCLUDE (concurrency_key)
    WHERE status IN ('queued', 'awaiting_retry');

-- Per-key bounded claim. Leading (queue, concurrency_key) equality makes
-- each per-key LATERAL a tight (run_at, id) range scan with no Sort node.
CREATE INDEX jobs_claim_conc_idx
    ON pgwq.jobs (queue, concurrency_key, run_at, id)
    WHERE status IN ('queued', 'awaiting_retry');

-- Immutability guard — a SEPARATE single-purpose trigger (not folded into
-- pgwq.set_updated_at). BEFORE UPDATE only, so INSERT (NULL -> value at
-- enqueue) is allowed. claim/mark/reaper never SET concurrency_key, so for
-- them OLD IS NOT DISTINCT FROM NEW holds and this trigger is inert. Its
-- real purpose is to reject external `UPDATE ... SET concurrency_key`.
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

- [ ] **Step 2: Add the `MAX_CONCURRENCY_KEY_LEN` constant**

In `src/limits.rs`, insert after the `MAX_QUEUE_LEN` constant (line 16):

```rust
/// Max length of a job's concurrency key, in **character** units
/// (Postgres `length(TEXT)`). Matches the `jobs_concurrency_key_len` DB
/// CHECK. Validated character-wise on the push and builder sides.
pub const MAX_CONCURRENCY_KEY_LEN: usize = 128;
```

- [ ] **Step 3: Write the failing schema test**

Create `tests/concurrency_key_schema.rs`:

```rust
//! Asserts the per-key concurrency migration applied the expected schema.
mod common;

use common::pg18_pool;

#[tokio::test]
async fn concurrency_key_column_and_objects_present() {
    let (pool, _container) = pg18_pool().await;

    // Column exists, nullable, type text.
    let (data_type, is_nullable): (String, String) = sqlx::query_as(
        "SELECT data_type, is_nullable FROM information_schema.columns
         WHERE table_schema = 'pgwq' AND table_name = 'jobs'
           AND column_name = 'concurrency_key'",
    )
    .fetch_one(&pool)
    .await
    .expect("concurrency_key column must exist");
    assert_eq!(data_type, "text");
    assert_eq!(is_nullable, "YES");

    // Both claim indexes exist.
    for idx in ["jobs_claim_idx", "jobs_claim_conc_idx"] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes
             WHERE schemaname = 'pgwq' AND indexname = $1)",
        )
        .bind(idx)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exists, "index {idx} must exist");
    }

    // Length CHECK constraint exists.
    let check_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_constraint
         WHERE conname = 'jobs_concurrency_key_len')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(check_exists, "jobs_concurrency_key_len CHECK must exist");

    // Immutability trigger exists.
    let trigger_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_trigger
         WHERE tgname = 'assert_concurrency_key_immutable')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(trigger_exists, "immutability trigger must exist");
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cargo test --test concurrency_key_schema`
Expected: FAIL — the migration file does not exist yet, or `sqlx::migrate!()` has not picked it up. (If it unexpectedly passes, the migration was already embedded; re-check the file name.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test --test concurrency_key_schema`
Expected: PASS — `sqlx::migrate!()` embeds every `.sql` in `migrations/` automatically; no Rust change needed beyond the file existing.

- [ ] **Step 6: Verify the build and lints**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add migrations/20260521000000_v01_concurrency_key.sql src/limits.rs tests/concurrency_key_schema.rs
git commit -m "feat(schema): add concurrency_key column, indexes, immutability trigger"
```

---

## Task 2: Pusher — `concurrency_key` on `push` / `push_at`

**Files:**
- Modify: `src/error.rs:22` (add `#[non_exhaustive]`), `src/error.rs:62` (after `QueueNameInvalid`)
- Modify: `src/pusher.rs` — `validate_queue`, new `validate_concurrency_key`, `push`, `push_at`
- Test: `tests/concurrency_key_pusher.rs`

- [ ] **Step 1: Write the failing test**

Create `tests/concurrency_key_pusher.rs`:

```rust
//! Pusher carries concurrency_key through to the row; validation rejects
//! out-of-range keys.
mod common;

use common::pg18_pool;
use pg_work_queue::{Pusher, PushError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

#[tokio::test]
async fn push_with_key_persists_it() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("q")
        .push(&mut tx, &T { n: 1 }, Some("handler-x"))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let key: Option<String> = sqlx::query_scalar(
        "SELECT concurrency_key FROM pgwq.jobs WHERE public_id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(key.as_deref(), Some("handler-x"));
}

#[tokio::test]
async fn push_with_none_key_is_null() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("q").push(&mut tx, &T { n: 1 }, None).await.unwrap();
    tx.commit().await.unwrap();
    let key: Option<String> = sqlx::query_scalar(
        "SELECT concurrency_key FROM pgwq.jobs WHERE public_id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(key, None);
}

#[tokio::test]
async fn push_with_empty_key_rejected() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let err = Pusher::new("q")
        .push(&mut tx, &T { n: 1 }, Some(""))
        .await
        .unwrap_err();
    assert!(matches!(err, PushError::ConcurrencyKeyInvalid(_)));
}

#[tokio::test]
async fn push_with_oversize_key_rejected() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    // 129 characters > MAX_CONCURRENCY_KEY_LEN (128).
    let long = "a".repeat(129);
    let err = Pusher::new("q")
        .push(&mut tx, &T { n: 1 }, Some(&long))
        .await
        .unwrap_err();
    assert!(matches!(err, PushError::ConcurrencyKeyInvalid(_)));
}

#[tokio::test]
async fn push_with_multibyte_key_at_limit_accepted() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    // 128 Polish characters — 256 bytes. Must pass: validation counts chars.
    let key = "ą".repeat(128);
    let ok = Pusher::new("q")
        .push(&mut tx, &T { n: 1 }, Some(&key))
        .await;
    assert!(ok.is_ok(), "128-char multibyte key must be accepted");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test concurrency_key_pusher`
Expected: FAIL to compile — `push` takes 2 args, not 3; `PushError::ConcurrencyKeyInvalid` does not exist.

- [ ] **Step 3: Add the `PushError` variant and `#[non_exhaustive]`**

In `src/error.rs`, add `#[non_exhaustive]` to the `PushError` enum (line 22-23 area):

```rust
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PushError {
```

Add this variant after `QueueNameInvalid` (after line 62):

```rust
    /// `concurrency_key` is empty or exceeds `limits::MAX_CONCURRENCY_KEY_LEN`
    /// characters.
    #[error("concurrency_key invalid: {0:?}")]
    ConcurrencyKeyInvalid(String),
```

- [ ] **Step 4: Update `pusher.rs` — validation + the two methods**

In `src/pusher.rs`, change the import line 15 to add `MAX_CONCURRENCY_KEY_LEN`:

```rust
use crate::limits::{
    MAX_BATCH_BYTES, MAX_BATCH_SIZE, MAX_CONCURRENCY_KEY_LEN, MAX_PAYLOAD_BYTES, MAX_QUEUE_LEN,
};
```

Replace `validate_queue` (lines 52-58) — switch byte `.len()` to character count:

```rust
    /// Validate the queue name once per push call (fail-late per the API sketch).
    fn validate_queue(&self) -> Result<(), PushError> {
        let len = self.queue.chars().count();
        if len == 0 || len > MAX_QUEUE_LEN {
            return Err(PushError::QueueNameInvalid(self.queue.clone()));
        }
        Ok(())
    }

    /// Validate an optional `concurrency_key` — character count in
    /// `1..=MAX_CONCURRENCY_KEY_LEN`. Called before the codec encode step.
    fn validate_concurrency_key(key: Option<&str>) -> Result<(), PushError> {
        if let Some(k) = key {
            let len = k.chars().count();
            if len == 0 || len > MAX_CONCURRENCY_KEY_LEN {
                return Err(PushError::ConcurrencyKeyInvalid(k.to_string()));
            }
        }
        Ok(())
    }
```

Replace `push` (lines 90-107) with the 3-argument form:

```rust
    /// Enqueue a single job. `run_at` defaults to `now()` server-side.
    ///
    /// `concurrency_key` — optional per-key concurrency bucket. `None` =
    /// unlimited. See the README "Per-key concurrency" section.
    ///
    /// # Errors
    /// - [`PushError::QueueNameInvalid`] — queue is empty or too long.
    /// - [`PushError::ConcurrencyKeyInvalid`] — key empty or too long.
    /// - [`PushError::Codec`] — codec rejected `payload`.
    /// - [`PushError::PayloadTooLarge`] — encoded size > `MAX_PAYLOAD_BYTES`.
    /// - [`PushError::Constraint`] / [`PushError::Transient`] — DB faults.
    #[tracing::instrument(skip(self, tx, payload), fields(queue = %self.queue))]
    pub async fn push<T: Serialize + Sync>(
        &self,
        tx: &mut PgConnection,
        payload: &T,
        concurrency_key: Option<&str>,
    ) -> Result<Uuid, PushError> {
        self.validate_queue()?;
        Self::validate_concurrency_key(concurrency_key)?;
        let bytes = self.encode_one(payload, 0)?;
        let public_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO pgwq.jobs (queue, payload, public_id, concurrency_key)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&self.queue)
        .bind(&bytes)
        .bind(public_id)
        .bind(concurrency_key)
        .execute(&mut *tx)
        .await?;
        Ok(public_id)
    }
```

Replace `push_at` (lines 113-134) similarly — add the `concurrency_key` argument, the validation call, and the column:

```rust
    /// Enqueue a single job with an explicit scheduled `run_at` (UTC).
    ///
    /// # Errors
    /// Same as [`Pusher::push`].
    #[tracing::instrument(skip(self, tx, payload), fields(queue = %self.queue))]
    pub async fn push_at<T: Serialize + Sync>(
        &self,
        tx: &mut PgConnection,
        payload: &T,
        run_at: DateTime<Utc>,
        concurrency_key: Option<&str>,
    ) -> Result<Uuid, PushError> {
        self.validate_queue()?;
        Self::validate_concurrency_key(concurrency_key)?;
        let bytes = self.encode_one(payload, 0)?;
        let public_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO pgwq.jobs (queue, payload, public_id, run_at, concurrency_key)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&self.queue)
        .bind(&bytes)
        .bind(public_id)
        .bind(run_at)
        .bind(concurrency_key)
        .execute(&mut *tx)
        .await?;
        Ok(public_id)
    }
```

- [ ] **Step 5: Update existing `push` / `push_at` call sites**

The signature change breaks every `push(` / `push_at(` call site outside `push_batch`. Find them:

Run: `grep -rln '\.push(' tests/ src/ ; grep -rln '\.push_at(' tests/ src/`

In every hit, add `, None` as the final argument (these call sites do not use per-key concurrency). Also update the crate-root doctest in `src/lib.rs:45` (`.push(&mut tx, &EmailTask { .. })` → `.push(&mut tx, &EmailTask { .. }, None)`). Do **not** touch `push_batch` call sites — Task 3 handles those.

- [ ] **Step 6: Run the tests**

Run: `cargo test --test concurrency_key_pusher`
Expected: PASS (all five cases).

Run: `cargo build --all-targets`
Expected: PASS — all `push`/`push_at` callers updated.

- [ ] **Step 7: Commit**

```bash
git add src/error.rs src/pusher.rs src/lib.rs tests/ src/
git commit -m "feat(pusher): add concurrency_key to push/push_at + char-count validation"
```

---

## Task 3: Pusher — `push_batch` tuple form

**Files:**
- Modify: `src/pusher.rs` — `push_batch` (lines 148-204)
- Modify: ~17 `tests/*.rs` call sites
- Test: extend `tests/concurrency_key_pusher.rs`

- [ ] **Step 1: Write the failing test**

Append to `tests/concurrency_key_pusher.rs`:

```rust
#[tokio::test]
async fn push_batch_carries_per_item_keys_in_order() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = vec![
        (T { n: 0 }, Some("a".to_string())),
        (T { n: 1 }, None),
        (T { n: 2 }, Some("b".to_string())),
    ];
    let ids = Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(ids.len(), 3);

    for (id, expected) in ids.iter().zip(["a", "", "b"]) {
        let key: Option<String> = sqlx::query_scalar(
            "SELECT concurrency_key FROM pgwq.jobs WHERE public_id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let want = if expected.is_empty() { None } else { Some(expected.to_string()) };
        assert_eq!(key, want);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test concurrency_key_pusher push_batch_carries`
Expected: FAIL to compile — `push_batch` takes `&[T]`, not `&[(T, Option<String>)]`.

- [ ] **Step 3: Rewrite `push_batch`**

Replace `push_batch` in `src/pusher.rs` (lines 148-204) with:

```rust
    /// Enqueue many jobs in a single round-trip. Each item is a
    /// `(payload, concurrency_key)` pair. Returns `Vec<Uuid>` in **input
    /// order** (client-side `Uuid::now_v7()` per item — no `RETURNING`).
    ///
    /// # Errors
    /// - [`PushError::BatchEmpty`] — `items.is_empty()`.
    /// - [`PushError::BatchTooLarge`] — `items.len() > MAX_BATCH_SIZE`.
    /// - [`PushError::ConcurrencyKeyInvalid`] — an item's key is out of range.
    /// - [`PushError::PayloadTooLarge`] — one payload exceeds `MAX_PAYLOAD_BYTES`.
    /// - [`PushError::BatchPayloadTooLarge`] — total bytes exceed `MAX_BATCH_BYTES`.
    /// - [`PushError::BatchCodec`] — codec rejected an item.
    /// - [`PushError::QueueNameInvalid`].
    /// - DB faults (`Constraint`/`Transient`).
    #[tracing::instrument(skip(self, tx, items), fields(queue = %self.queue, count = items.len()))]
    pub async fn push_batch<T: Serialize + Sync>(
        &self,
        tx: &mut PgConnection,
        items: &[(T, Option<String>)],
    ) -> Result<Vec<Uuid>, PushError> {
        self.validate_queue()?;
        if items.is_empty() {
            return Err(PushError::BatchEmpty);
        }
        if items.len() > MAX_BATCH_SIZE {
            return Err(PushError::BatchTooLarge {
                size: items.len(),
                max: MAX_BATCH_SIZE,
            });
        }

        // Encode + validate keys, short-circuiting on the first failure.
        let mut payload_bytes: Vec<Vec<u8>> = Vec::with_capacity(items.len());
        let mut keys: Vec<Option<String>> = Vec::with_capacity(items.len());
        let mut total_bytes: usize = 0;
        for (i, (payload, key)) in items.iter().enumerate() {
            Self::validate_concurrency_key(key.as_deref())?;
            let bytes = self.encode_one(payload, i)?;
            total_bytes = total_bytes.saturating_add(bytes.len());
            if total_bytes > MAX_BATCH_BYTES {
                return Err(PushError::BatchPayloadTooLarge {
                    total_bytes,
                    max: MAX_BATCH_BYTES,
                });
            }
            payload_bytes.push(bytes);
            keys.push(key.clone());
        }

        let public_ids: Vec<Uuid> = (0..items.len()).map(|_| Uuid::now_v7()).collect();

        let result = sqlx::query(
            "INSERT INTO pgwq.jobs (queue, payload, public_id, concurrency_key)
             SELECT $1, payload, public_id, concurrency_key
             FROM unnest($2::bytea[], $3::uuid[], $4::text[])
                  AS u(payload, public_id, concurrency_key)",
        )
        .bind(&self.queue)
        .bind(&payload_bytes)
        .bind(&public_ids)
        .bind(&keys)
        .execute(&mut *tx)
        .await?;

        let inserted = usize::try_from(result.rows_affected()).unwrap_or(usize::MAX);
        if inserted != public_ids.len() {
            return Err(PushError::BatchPartial {
                inserted,
                expected: public_ids.len(),
            });
        }
        Ok(public_ids)
    }
```

Note: `encode_one`'s `index == 0` branch routes the first item's codec error to `PushError::Codec` rather than `BatchCodec`. That pre-existing quirk is unchanged — leave `encode_one` as-is.

- [ ] **Step 4: Update every `push_batch` call site**

Run: `grep -rln 'push_batch(' tests/`

Expected ~17 files. In each, the call passes `&[T]` (e.g. `&payloads` or `&[a, b]`). Convert each element to a `(payload, None)` tuple. Two mechanical patterns:

- Literal slice `&[a, b, c]` → `&[(a, None), (b, None), (c, None)]`.
- A `Vec<T>` named `payloads` built then passed as `&payloads` → build it as `Vec<(T, Option<String>)>` instead: change each `T { .. }` constructed for the batch to `(T { .. }, None)`, or map at the call site: `&payloads.into_iter().map(|p| (p, None)).collect::<Vec<_>>()`.

Prefer the map-at-call-site form when the `Vec` is reused; prefer inline tuples for small literal slices. Every batch in these tests is unkeyed, so the second tuple element is always `None`.

- [ ] **Step 5: Run the tests**

Run: `cargo test --test concurrency_key_pusher`
Expected: PASS.

Run: `cargo build --all-targets`
Expected: PASS — all 17 `push_batch` callers updated. Fix any remaining compile errors (they will all be the same tuple transformation).

- [ ] **Step 6: Run clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/pusher.rs tests/
git commit -m "feat(pusher): push_batch takes (payload, concurrency_key) pairs"
```

---

## Task 4: `concurrency_key` on `Job<T>` and `RawClaimedRow`

**Files:**
- Modify: `src/job.rs:17-40` (`Job<T>` struct)
- Modify: `src/claim.rs` — `RawClaimedRow`, `claim_batch_raw` RETURNING + extraction, `Job` construction
- Test: extend `tests/concurrency_key_pusher.rs`

This task only adds the field and populates it from the *existing* `claim_batch` SQL. The keyed claim path lands in Task 6.

- [ ] **Step 1: Write the failing test**

Append to `tests/concurrency_key_pusher.rs`:

```rust
#[tokio::test]
async fn claimed_job_exposes_concurrency_key() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    Pusher::new("q").push(&mut tx, &T { n: 1 }, Some("k")).await.unwrap();
    tx.commit().await.unwrap();

    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool,
        &pg_work_queue::JsonCodec,
        "q",
        10,
        std::time::Duration::from_secs(30),
        3,
        &std::collections::HashMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].concurrency_key.as_deref(), Some("k"));
}
```

Note this test already uses the Task 5 signature (`&HashMap::new()` as the 7th arg). It will not compile until Task 5. Mark it `#[ignore = "needs Task 5 claim_and_decode signature"]` for now; Task 6 removes the `#[ignore]`.

- [ ] **Step 2: Add the field to `Job<T>`**

In `src/job.rs`, add to the `Job<T>` struct after the `queue` field (after line 23):

```rust
    /// Optional per-key concurrency bucket, stamped at enqueue. `None` =
    /// unlimited. Immutable for the job's lifetime.
    pub concurrency_key: Option<String>,
```

- [ ] **Step 3: Add the field to `RawClaimedRow` + claim SQL**

In `src/claim.rs`, add to `RawClaimedRow` after `queue` (after line 37):

```rust
    pub(crate) concurrency_key: Option<String>,
```

In `claim_batch_raw`, add `j.concurrency_key` to the `RETURNING` list (line 87-88) — append `, j.concurrency_key`:

```rust
         RETURNING j.id, j.public_id, j.queue, j.payload, j.attempts, j.max_attempts,
                   j.first_attempted_at, j.lease_token, j.lease_expires_at, j.concurrency_key",
```

In the row-extraction loop (after line 109, `first_attempted_at`), add:

```rust
            concurrency_key: r.try_get("concurrency_key")?,
```

`try_get` infers `Option<String>` from the field type — SQL `NULL` maps to `None`.

In `claim_and_decode`, the `Job` construction (lines 165-175) — add the field after `queue`:

```rust
                    concurrency_key: raw.concurrency_key,
```

- [ ] **Step 4: Verify the build**

Run: `cargo build --lib`
Expected: PASS — `Job` is constructed only in `claim.rs`; the poll loop and `tick_once` read named fields and do not destructure exhaustively.

- [ ] **Step 5: Run clippy**

Run: `cargo clippy --lib -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/job.rs src/claim.rs tests/concurrency_key_pusher.rs
git commit -m "feat(claim): thread concurrency_key from row into Job"
```

---

## Task 5: `claim_and_decode` gains the `headroom` parameter + keyed claim SQL

**Files:**
- Modify: `src/claim.rs` — `claim_batch_raw`, `claim_and_decode`
- Modify: `src/worker.rs:887` (`tick_once`), `src/worker.rs:1509` (`poll_loop`)
- Modify: 5 `tests/*.rs` calling `claim_and_decode`
- Test: covered by `tests/per_key_concurrency_limit.rs` in Task 6

This task is behavior-preserving: every caller passes an **empty** headroom map for now, which selects the unchanged `claim_batch` SQL. Task 7 wires the real headroom into `poll_loop`.

- [ ] **Step 1: Rewrite `claim_batch_raw` to branch on headroom**

In `src/claim.rs`, add the import at the top:

```rust
use std::collections::HashMap;
```

Replace the `claim_batch_raw` function signature and body. It gains a `headroom: &HashMap<String, u32>` parameter and selects one of two SQL strings. Keep the existing query as the empty-headroom branch verbatim (with the Task 4 `RETURNING` change). The keyed branch:

```rust
async fn claim_batch_raw(
    pool: &PgPool,
    queue: &str,
    batch_size: u32,
    lease_timeout: Duration,
    max_attempts: u32,
    headroom: &HashMap<String, u32>,
) -> Result<Vec<RawClaimedRow>, sqlx::Error> {
    let i32_max_u32: u32 = i32::MAX as u32;
    let batch_size_i32: i32 = i32::try_from(batch_size.min(i32_max_u32)).unwrap_or(i32::MAX);
    let max_attempts_i32: i32 = i32::try_from(max_attempts.min(i32_max_u32)).unwrap_or(i32::MAX);

    // RETURNING list shared by both SQL variants.
    const RETURNING: &str = "j.id, j.public_id, j.queue, j.payload, j.attempts, \
        j.max_attempts, j.first_attempted_at, j.lease_token, j.lease_expires_at, \
        j.concurrency_key";

    let rows = if headroom.is_empty() {
        // Empty-limits fast path — the original claim_batch SQL.
        sqlx::query(&format!(
            "WITH claimed AS (
                 SELECT id FROM pgwq.jobs
                 WHERE queue = $1
                   AND status IN ('queued', 'awaiting_retry')
                   AND run_at <= now()
                 ORDER BY run_at, id
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             UPDATE pgwq.jobs j
             SET status = 'running', attempts = j.attempts + 1, max_attempts = $4,
                 last_attempted_at = now(),
                 first_attempted_at = COALESCE(j.first_attempted_at, now()),
                 lease_token = gen_random_uuid(),
                 lease_expires_at = now() + $3::interval, last_error = NULL
             FROM claimed
             WHERE j.id = claimed.id
             RETURNING {RETURNING}"
        ))
        .bind(queue)
        .bind(batch_size_i32)
        .bind(lease_timeout)
        .bind(max_attempts_i32)
        .fetch_all(pool)
        .await?
    } else {
        // Keyed claim — per-key headroom bound. $5 is the headroom map as a
        // jsonb object {key: headroom}. See the design spec §5.
        let headroom_json = serde_json::to_string(headroom).unwrap_or_else(|_| "{}".to_string());
        sqlx::query(&format!(
            "WITH
             hr AS (
                 SELECT key AS concurrency_key, value::int AS h
                 FROM jsonb_each_text($5::jsonb)
             ),
             eligible_keyed AS (
                 SELECT e.id
                 FROM hr
                 CROSS JOIN LATERAL (
                     SELECT j.id FROM pgwq.jobs j
                     WHERE j.queue = $1
                       AND j.status IN ('queued', 'awaiting_retry')
                       AND j.concurrency_key = hr.concurrency_key
                       AND j.run_at <= now()
                     ORDER BY j.run_at, j.id
                     LIMIT LEAST(GREATEST(hr.h, 0), $2)
                 ) e
             ),
             eligible_unlimited AS (
                 SELECT j.id FROM pgwq.jobs j
                 LEFT JOIN hr ON hr.concurrency_key = j.concurrency_key
                 WHERE j.queue = $1
                   AND j.status IN ('queued', 'awaiting_retry')
                   AND j.run_at <= now()
                   AND hr.concurrency_key IS NULL
                 ORDER BY j.run_at, j.id
                 LIMIT $2
             ),
             locked AS (
                 SELECT j.id FROM pgwq.jobs j
                 WHERE j.id IN (SELECT id FROM eligible_keyed
                                UNION ALL
                                SELECT id FROM eligible_unlimited)
                   AND j.status IN ('queued', 'awaiting_retry')
                   AND j.run_at <= now()
                 ORDER BY j.run_at, j.id
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             UPDATE pgwq.jobs j
             SET status = 'running', attempts = j.attempts + 1, max_attempts = $4,
                 last_attempted_at = now(),
                 first_attempted_at = COALESCE(j.first_attempted_at, now()),
                 lease_token = gen_random_uuid(),
                 lease_expires_at = now() + $3::interval, last_error = NULL
             FROM locked
             WHERE j.id = locked.id
             RETURNING {RETURNING}"
        ))
        .bind(queue)
        .bind(batch_size_i32)
        .bind(lease_timeout)
        .bind(max_attempts_i32)
        .bind(headroom_json)
        .fetch_all(pool)
        .await?
    };

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let attempts_i32: i32 = r.try_get("attempts")?;
        let max_attempts_i32: i32 = r.try_get("max_attempts")?;
        out.push(RawClaimedRow {
            id: r.try_get("id")?,
            public_id: r.try_get("public_id")?,
            queue: r.try_get("queue")?,
            concurrency_key: r.try_get("concurrency_key")?,
            payload: r.try_get("payload")?,
            attempts: u32::try_from(attempts_i32).unwrap_or(0),
            max_attempts: u32::try_from(max_attempts_i32).unwrap_or(0),
            first_attempted_at: r.try_get("first_attempted_at")?,
            lease_token: r.try_get("lease_token")?,
            lease_expires_at: r.try_get("lease_expires_at")?,
        });
    }
    Ok(out)
}
```

Note `serde_json::to_string` cannot fail for a `HashMap<String, u32>`; the `unwrap_or_else` keeps `src/` panic-free per the lint posture.

- [ ] **Step 2: Add the `headroom` parameter to `claim_and_decode`**

In `src/claim.rs`, change `claim_and_decode`'s signature to take `headroom: &HashMap<String, u32>` as the final parameter, and forward it to `claim_batch_raw`:

```rust
pub async fn claim_and_decode<T, C>(
    pool: &PgPool,
    codec: &C,
    queue: &str,
    batch_size: u32,
    lease_timeout: Duration,
    max_attempts: u32,
    headroom: &HashMap<String, u32>,
) -> Result<Vec<Job<T>>, sqlx::Error>
where
    T: serde::de::DeserializeOwned,
    C: Codec,
{
    let raws =
        claim_batch_raw(pool, queue, batch_size, lease_timeout, max_attempts, headroom).await?;
    // ... unchanged decode loop ...
```

- [ ] **Step 3: Update the two `worker.rs` call sites**

`tick_once` (`src/worker.rs:887`) — add `&std::collections::HashMap::new()` as the final argument:

```rust
        let claimed = crate::claim::claim_and_decode::<T, C>(
            &self.pool,
            &self.codec,
            &self.queue,
            self.batch_size,
            self.lease_timeout,
            self.max_attempts,
            &std::collections::HashMap::new(),
        )
        .await?;
```

`poll_loop` (`src/worker.rs:1509`) — add `&std::collections::HashMap::new()` as the final argument inside the `tokio::select!` claim arm. (Task 7 replaces this with the real headroom.)

- [ ] **Step 4: Update the 5 `claim_and_decode` test call sites**

Run: `grep -rln 'claim_and_decode' tests/`

Expected: `skip_locked_no_double_claim.rs`, `fencing_token_no_double_run.rs`, `batch_size_behavior.rs`, `codec_panic_marks_dead.rs`, `codec_decode_error_marks_dead.rs`. In each, append `, &std::collections::HashMap::new()` as the final argument to every `claim_and_decode(` call.

- [ ] **Step 5: Verify build + lints + the existing suite for claim**

Run: `cargo build --all-targets`
Expected: PASS.

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS.

Run: `cargo test --test skip_locked_no_double_claim --test batch_size_behavior`
Expected: PASS — empty headroom → unchanged `claim_batch` SQL, no behavior change.

- [ ] **Step 6: Commit**

```bash
git add src/claim.rs src/worker.rs tests/
git commit -m "feat(claim): add headroom parameter + keyed claim SQL (dormant)"
```

---

## Task 6: Verify the keyed claim SQL directly

**Files:**
- Test: `tests/per_key_concurrency_limit.rs` (claim-SQL-level portion)
- Modify: `tests/concurrency_key_pusher.rs` — remove the `#[ignore]` from Task 4 Step 1

- [ ] **Step 1: Un-ignore the Task 4 test**

In `tests/concurrency_key_pusher.rs`, delete the `#[ignore = "needs Task 5 claim_and_decode signature"]` attribute on `claimed_job_exposes_concurrency_key`.

- [ ] **Step 2: Write the keyed-claim SQL test**

Create `tests/per_key_concurrency_limit.rs`:

```rust
//! The keyed claim SQL claims at most `headroom` rows per key.
mod common;

use common::pg18_pool;
use pg_work_queue::{JsonCodec, Pusher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

async fn push_n_keyed(pool: &sqlx::PgPool, queue: &str, key: &str, n: u32) {
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..n).map(|i| (T { n: i }, Some(key.to_string()))).collect();
    Pusher::new(queue).push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn keyed_claim_respects_headroom_limit_2() {
    let (pool, _c) = pg18_pool().await;
    push_n_keyed(&pool, "q", "k", 10).await;

    let headroom: HashMap<String, u32> = [("k".to_string(), 2u32)].into();
    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool, &JsonCodec, "q", 32, Duration::from_secs(30), 3, &headroom,
    )
    .await
    .unwrap();
    // headroom 2 -> at most 2 rows of key "k" claimed, even though batch_size 32.
    assert_eq!(jobs.len(), 2);
}

#[tokio::test]
async fn keyed_claim_saturated_key_claims_zero() {
    let (pool, _c) = pg18_pool().await;
    push_n_keyed(&pool, "q", "k", 10).await;

    let headroom: HashMap<String, u32> = [("k".to_string(), 0u32)].into();
    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool, &JsonCodec, "q", 32, Duration::from_secs(30), 3, &headroom,
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 0);
}

#[tokio::test]
async fn keyed_claim_unconfigured_key_is_unlimited() {
    let (pool, _c) = pg18_pool().await;
    push_n_keyed(&pool, "q", "other", 10).await;

    // headroom configured for "k" only — "other" is not in the map.
    let headroom: HashMap<String, u32> = [("k".to_string(), 1u32)].into();
    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool, &JsonCodec, "q", 32, Duration::from_secs(30), 3, &headroom,
    )
    .await
    .unwrap();
    // "other" is unconfigured -> unlimited -> all 10 (batch_size 32) claimed.
    assert_eq!(jobs.len(), 10);
}

#[tokio::test]
async fn keyed_claim_null_key_is_unlimited() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..10).map(|i| (T { n: i }, None)).collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let headroom: HashMap<String, u32> = [("k".to_string(), 1u32)].into();
    let jobs = pg_work_queue::__test_exports::claim_and_decode::<T, _>(
        &pool, &JsonCodec, "q", 32, Duration::from_secs(30), 3, &headroom,
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 10);
}
```

- [ ] **Step 3: Run the tests**

Run: `cargo test --test per_key_concurrency_limit --test concurrency_key_pusher`
Expected: PASS — the keyed claim SQL from Task 5 is exercised directly.

- [ ] **Step 4: Commit**

```bash
git add tests/per_key_concurrency_limit.rs tests/concurrency_key_pusher.rs
git commit -m "test(claim): verify keyed claim SQL respects per-key headroom"
```

---

## Task 7: `WorkerBuilder::concurrency_limits` + plumbing

**Files:**
- Modify: `src/error.rs` — add two `BuildError` variants
- Modify: `src/worker.rs` — `WorkerBuilder` field + `new()` + `.codec()` + `.handler()` + `build()` + `Worker` field
- Test: `tests/builder_validation.rs` (extend) or a new `tests/per_key_builder.rs`

- [ ] **Step 1: Write the failing test**

Create `tests/per_key_builder.rs`:

```rust
//! WorkerBuilder::concurrency_limits validation.
mod common;

use common::pg18_pool;
use pg_work_queue::{BuildError, JobContext, JobError, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

fn builder(pool: sqlx::PgPool) -> pg_work_queue::WorkerBuilder<T, pg_work_queue::JsonCodec, ()> {
    Worker::<T>::builder().pool(pool).queue("q")
}

async fn ok_handler(_t: T, _c: JobContext) -> Result<(), JobError> { Ok(()) }

#[tokio::test]
async fn concurrency_limit_zero_rejected() {
    let (pool, _c) = pg18_pool().await;
    let err = builder(pool)
        .concurrency_limits([("k".to_string(), 0u32)])
        .handler(ok_handler)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::ConcurrencyLimitInvalid { .. }));
}

#[tokio::test]
async fn concurrency_key_empty_rejected() {
    let (pool, _c) = pg18_pool().await;
    let err = builder(pool)
        .concurrency_limits([(String::new(), 2u32)])
        .handler(ok_handler)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::ConcurrencyKeyInvalid(_)));
}

#[tokio::test]
async fn valid_concurrency_limits_build_ok() {
    let (pool, _c) = pg18_pool().await;
    let built = builder(pool)
        .concurrency_limits([("a".to_string(), 2u32), ("b".to_string(), 5u32)])
        .handler(ok_handler)
        .build();
    assert!(built.is_ok());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test per_key_builder`
Expected: FAIL to compile — `.concurrency_limits` does not exist; `BuildError::ConcurrencyLimitInvalid` / `ConcurrencyKeyInvalid` do not exist.

- [ ] **Step 3: Add the `BuildError` variants**

In `src/error.rs`, add to the `BuildError` enum (after `BackoffInvalid`, before the closing brace, ~line 331):

```rust
    /// A `concurrency_limits` key is empty or exceeds
    /// `limits::MAX_CONCURRENCY_KEY_LEN` characters.
    #[error("concurrency_limits key invalid: {0:?}")]
    ConcurrencyKeyInvalid(String),

    /// A `concurrency_limits` value is outside `1..=i32::MAX`.
    #[error("concurrency limit for key {key:?} must be in 1..=2147483647, got {limit}")]
    ConcurrencyLimitInvalid {
        /// The offending key.
        key: String,
        /// The offending limit value.
        limit: u32,
    },
```

- [ ] **Step 4: Add the field + builder method + plumbing**

In `src/worker.rs`:

Add `use std::collections::HashMap;` to the imports.

Add the field to `WorkerBuilder` (after `concurrency` at line 231):

```rust
    concurrency_limits: HashMap<String, u32>,
```

In `WorkerBuilder::new()` (after `concurrency: None,` at line 253):

```rust
            concurrency_limits: HashMap::new(),
```

In `.codec()`'s `WorkerBuilder { .. }` literal (after `concurrency: self.concurrency,` ~line 555) and in `.handler()`'s literal (after `concurrency: self.concurrency,` ~line 607), add to **both**:

```rust
            concurrency_limits: self.concurrency_limits,
```

Add the builder method inside `impl<T, C, H> WorkerBuilder<T, C, H>` (e.g. after `concurrency`, line 446):

```rust
    /// Per-key concurrency limits — `key → max concurrently-running jobs`.
    ///
    /// A job carrying a `concurrency_key` (set via `Pusher::push`) present in
    /// this map is limited to at most `limit` concurrently-running handler
    /// tasks. A `None` key, or a key absent from this map, is unlimited.
    ///
    /// Accumulates across calls; a duplicate key takes the last value (a
    /// `tracing::warn!` is emitted on overwrite at `build()` time).
    ///
    /// Validated on `build()`: each key `1..=MAX_CONCURRENCY_KEY_LEN`
    /// characters → [`BuildError::ConcurrencyKeyInvalid`]; each limit
    /// `1..=i32::MAX` → [`BuildError::ConcurrencyLimitInvalid`].
    ///
    /// **Single-instance only** — the limit is enforced via an in-process
    /// counter. See the README "Per-key concurrency" section.
    #[must_use]
    pub fn concurrency_limits(
        mut self,
        limits: impl IntoIterator<Item = (String, u32)>,
    ) -> Self {
        for (k, v) in limits {
            if let Some(prev) = self.concurrency_limits.insert(k.clone(), v) {
                tracing::warn!(
                    target: "pgwq.builder",
                    key = %k,
                    previous = prev,
                    replacement = v,
                    "concurrency_limits: duplicate key overwritten",
                );
            }
        }
        self
    }
```

In `build()`, add validation before the `Ok(Worker { .. })` (after the backoff validation, ~line 763):

```rust
        // Per-key concurrency limits validation.
        for (key, &limit) in &self.concurrency_limits {
            let key_len = key.chars().count();
            if key_len == 0 || key_len > MAX_CONCURRENCY_KEY_LEN {
                return Err(BuildError::ConcurrencyKeyInvalid(key.clone()));
            }
            if limit == 0 || limit > i32::MAX as u32 {
                return Err(BuildError::ConcurrencyLimitInvalid {
                    key: key.clone(),
                    limit,
                });
            }
        }
```

Add `MAX_CONCURRENCY_KEY_LEN` to the `crate::limits` import on line 50:

```rust
use crate::limits::{
    MAX_CONCURRENCY_KEY_LEN, MAX_QUEUE_LEN, MIN_HANDLER_TIMEOUT, MIN_MARK_TIMEOUT,
    MIN_POLL_INTERVAL,
};
```

In the `Ok(Worker { .. })` literal (after `concurrency,` ~line 774):

```rust
            concurrency_limits: self.concurrency_limits,
```

Add the field to the `Worker` struct (after `concurrency: usize,` line 815):

```rust
    concurrency_limits: HashMap<String, u32>,
```

Also fix the queue char/byte bug in `build()` — line 630, change `queue.len()` to `queue.chars().count()`:

```rust
        let queue_len = queue.chars().count();
        if queue.is_empty() || queue_len > MAX_QUEUE_LEN {
            return Err(BuildError::QueueNameInvalid(queue));
        }
```

- [ ] **Step 5: Run the tests**

Run: `cargo test --test per_key_builder`
Expected: PASS.

Run: `cargo build --all-targets && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/error.rs src/worker.rs tests/per_key_builder.rs
git commit -m "feat(worker): WorkerBuilder::concurrency_limits + validation"
```

---

## Task 8: `KeySlotGuard` module

**Files:**
- Create: `src/key_slot.rs`
- Modify: `src/lib.rs` — declare the module

- [ ] **Step 1: Write the failing unit test + module**

Create `src/key_slot.rs`:

```rust
//! `KeySlotGuard` — RAII guard for the in-memory per-key concurrency counter.
//!
//! The Worker keeps an `Arc<Mutex<HashMap<String, u32>>>` of live handler
//! tasks per concurrency key. One `KeySlotGuard` owns exactly one slot:
//! `acquire` increments, `Drop` decrements. Because `Drop` runs on every
//! task exit — normal return, panic, and `JoinSet` abort-cancellation — the
//! decrement is exhaustive by construction.
//!
//! `std::sync::Mutex` (not `tokio::sync::Mutex`): the decrement runs inside a
//! synchronous `Drop`. The critical sections are O(1), panic-free `HashMap`
//! work and never hold the lock across an `.await`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Shared per-key live-task counter.
pub(crate) type ConcurrencyCounter = Arc<Mutex<HashMap<String, u32>>>;

/// Owns one per-key slot. Increments on `acquire`, decrements on `Drop`.
/// A no-op for jobs without a configured-limit key (`none()`).
pub(crate) struct KeySlotGuard {
    slot: Option<(ConcurrencyCounter, String)>,
}

impl KeySlotGuard {
    /// Increment the counter for `key` and return a guard owning that slot.
    /// On a poisoned mutex (unreachable — the critical section is panic-free)
    /// returns a slot-less guard: increment succeeded ⟺ the guard owns a slot.
    pub(crate) fn acquire(counter: ConcurrencyCounter, key: String) -> Self {
        match counter.lock() {
            Ok(mut map) => {
                let n = map.entry(key.clone()).or_insert(0);
                *n = n.saturating_add(1);
                drop(map);
                Self {
                    slot: Some((counter, key)),
                }
            }
            Err(_poisoned) => Self { slot: None },
        }
    }

    /// A guard owning no slot — for unkeyed / unconfigured-key jobs.
    pub(crate) const fn none() -> Self {
        Self { slot: None }
    }
}

impl Drop for KeySlotGuard {
    fn drop(&mut self) {
        if let Some((counter, key)) = &self.slot {
            if let Ok(mut map) = counter.lock() {
                if let Some(n) = map.get_mut(key) {
                    *n = n.saturating_sub(1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counter_with(keys: &[&str]) -> ConcurrencyCounter {
        let map: HashMap<String, u32> = keys.iter().map(|k| ((*k).to_string(), 0)).collect();
        Arc::new(Mutex::new(map))
    }

    fn count(counter: &ConcurrencyCounter, key: &str) -> u32 {
        *counter.lock().unwrap().get(key).unwrap()
    }

    #[test]
    fn acquire_increments_drop_decrements() {
        let counter = counter_with(&["k"]);
        {
            let _g = KeySlotGuard::acquire(counter.clone(), "k".to_string());
            assert_eq!(count(&counter, "k"), 1);
        }
        assert_eq!(count(&counter, "k"), 0);
    }

    #[test]
    fn two_guards_stack() {
        let counter = counter_with(&["k"]);
        let g1 = KeySlotGuard::acquire(counter.clone(), "k".to_string());
        let g2 = KeySlotGuard::acquire(counter.clone(), "k".to_string());
        assert_eq!(count(&counter, "k"), 2);
        drop(g1);
        assert_eq!(count(&counter, "k"), 1);
        drop(g2);
        assert_eq!(count(&counter, "k"), 0);
    }

    #[test]
    fn none_guard_is_noop() {
        let counter = counter_with(&["k"]);
        {
            let _g = KeySlotGuard::none();
        }
        assert_eq!(count(&counter, "k"), 0);
    }
}
```

- [ ] **Step 2: Declare the module**

In `src/lib.rs`, add after `pub(crate) mod job;` (line 76):

```rust
pub(crate) mod key_slot;
```

- [ ] **Step 3: Run the unit tests**

Run: `cargo test --lib key_slot`
Expected: PASS (3 tests).

- [ ] **Step 4: Run clippy**

Run: `cargo clippy --lib -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/key_slot.rs src/lib.rs
git commit -m "feat(worker): add KeySlotGuard RAII per-key counter guard"
```

---

## Task 9: Wire the counter into the poll loop

**Files:**
- Modify: `src/worker.rs` — `WorkerState` struct + fields, `start()`, `poll_loop`, `handle_job`

- [ ] **Step 1: Add the counter fields to `WorkerState`**

In `src/worker.rs`, add to `WorkerState` (after `semaphore` ~line 1034):

```rust
    pub(crate) concurrency_limits: HashMap<String, u32>,
    pub(crate) concurrency_running: crate::key_slot::ConcurrencyCounter,
```

In `start()`'s `WorkerState { .. }` literal (after `semaphore: Arc::new(...)` ~line 988), add — the counter is initialized with every configured key at `0` (no DB seed; see the design spec §4):

```rust
            concurrency_running: {
                let initial: std::collections::HashMap<String, u32> =
                    self.concurrency_limits.keys().map(|k| (k.clone(), 0)).collect();
                std::sync::Arc::new(std::sync::Mutex::new(initial))
            },
            concurrency_limits: self.concurrency_limits,
```

- [ ] **Step 2: Add the `headroom` parameter to `handle_job` and drop the guard there**

Change `handle_job`'s signature (line 1589) to take a `KeySlotGuard`:

```rust
async fn handle_job<T, C>(
    job: Job<T>,
    state: Arc<WorkerState<T, C>>,
    _permit: OwnedSemaphorePermit,
    _slot: crate::key_slot::KeySlotGuard,
) where
```

`_slot` is dropped at the end of `handle_job` exactly like `_permit` — on every `match` arm, on panic, and on `JoinSet::abort_all` cancellation. No body change is needed.

- [ ] **Step 3: Compute headroom + increment in the poll loop**

In `poll_loop`, replace the empty-headroom claim call (the `&std::collections::HashMap::new()` argument added in Task 5, ~line 1509-1516) with a real per-tick headroom snapshot computed before the claim:

```rust
        // Per-tick headroom snapshot: limit − live-task count, per configured
        // key. The map always contains every configured key (headroom >= 0).
        let headroom: std::collections::HashMap<String, u32> = {
            let running = state.concurrency_running.lock();
            match running {
                Ok(counts) => state
                    .concurrency_limits
                    .iter()
                    .map(|(k, &limit)| {
                        let used = counts.get(k).copied().unwrap_or(0);
                        (k.clone(), limit.saturating_sub(used))
                    })
                    .collect(),
                // Poisoned (unreachable — counter critical sections are
                // panic-free): fall back to empty -> claim_batch fast path.
                Err(_) => std::collections::HashMap::new(),
            }
        };
```

Then change the claim call to pass `&headroom` instead of `&std::collections::HashMap::new()`.

- [ ] **Step 4: Increment + spawn with the guard**

Replace the spawn loop (lines 1535-1539) so the guard is acquired and the task spawned in the same iteration:

```rust
                let mut tasks = state.tasks.lock().await;
                for (row, permit) in rows.into_iter().zip(permits) {
                    let s = state.clone();
                    let slot = match &row.concurrency_key {
                        Some(k) if state.concurrency_limits.contains_key(k) => {
                            crate::key_slot::KeySlotGuard::acquire(
                                state.concurrency_running.clone(),
                                k.clone(),
                            )
                        }
                        _ => crate::key_slot::KeySlotGuard::none(),
                    };
                    tasks.spawn(handle_job(row, s, permit, slot));
                }
```

- [ ] **Step 5: Verify build + lints**

Run: `cargo build --all-targets && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Run a smoke test**

Run: `cargo test --test worker_tick_once_smoke --test concurrency_behavior`
Expected: PASS — no behavior change for workers without `concurrency_limits` (headroom map is empty → `claim_batch` fast path).

- [ ] **Step 7: Commit**

```bash
git add src/worker.rs
git commit -m "feat(worker): in-memory per-key counter wired into the poll loop"
```

---

## Task 10: End-to-end behavioral tests

**Files:**
- Test: `tests/per_key_no_head_of_line.rs`, `tests/per_key_counter_no_leak.rs`, `tests/per_key_no_seed_restart.rs`, `tests/per_key_unlimited.rs`

- [ ] **Step 1: Write the limit-enforced + no-head-of-line test**

Create `tests/per_key_no_head_of_line.rs`:

```rust
//! A saturated key does not block other keys; the limit is enforced
//! end-to-end through Worker::start.
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T { key: String }

#[tokio::test]
async fn limit_enforced_and_other_keys_progress() {
    let (pool, _c) = pg18_pool().await;

    // 6 jobs of "slow" (limited to 2), 3 jobs of "fast" (unlimited).
    let mut tx = pool.begin().await.unwrap();
    let mut items: Vec<(T, Option<String>)> = Vec::new();
    for _ in 0..6 {
        items.push((T { key: "slow".into() }, Some("slow".to_string())));
    }
    for _ in 0..3 {
        items.push((T { key: "fast".into() }, Some("fast".to_string())));
    }
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let slow_peak = Arc::new(AtomicU32::new(0));
    let slow_now = Arc::new(AtomicU32::new(0));
    let fast_done = Arc::new(AtomicU32::new(0));
    let (sp, sn, fd) = (slow_peak.clone(), slow_now.clone(), fast_done.clone());

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .concurrency(8)
        .concurrency_limits([("slow".to_string(), 2u32)])
        .handler(move |t: T, _c: JobContext| {
            let (sp, sn, fd) = (sp.clone(), sn.clone(), fd.clone());
            async move {
                if t.key == "slow" {
                    let now = sn.fetch_add(1, Ordering::SeqCst) + 1;
                    sp.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    sn.fetch_sub(1, Ordering::SeqCst);
                } else {
                    fd.fetch_add(1, Ordering::SeqCst);
                }
                Ok::<(), JobError>(())
            }
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    // Give the worker time to drain the fast key while slow is throttled.
    tokio::time::sleep(Duration::from_millis(400)).await;
    // All 3 fast jobs done well before the slow jobs (limited to 2 at a time).
    assert_eq!(fast_done.load(Ordering::SeqCst), 3, "fast key not head-of-line blocked");

    let _ = handle.shutdown(Duration::from_secs(10)).await;
    // The slow key never exceeded its limit of 2.
    assert!(slow_peak.load(Ordering::SeqCst) <= 2, "slow key exceeded limit 2");
}
```

- [ ] **Step 2: Write the counter-no-leak test (incl. abort path)**

Create `tests/per_key_counter_no_leak.rs`:

```rust
//! After all jobs of a key complete (or are aborted at shutdown), the key's
//! headroom is fully restored — the in-memory counter does not leak.
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

#[tokio::test]
async fn counter_restored_after_all_jobs_complete() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..5).map(|i| (T { n: i }, Some("k".to_string()))).collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async { Ok::<(), JobError>(()) })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    // Wait for all 5 to be processed (limit 2, trivial handler — fast).
    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    let done: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgwq.jobs WHERE status = 'done'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(done, 5, "all keyed jobs completed -> counter did not wedge");
}

#[tokio::test]
async fn counter_decrements_on_shutdown_abort() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..2).map(|i| (T { n: i }, Some("k".to_string()))).collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .lease_timeout(Duration::from_secs(60))
        .handler_timeout(Duration::from_secs(30))
        .concurrency_limits([("k".to_string(), 2u32)])
        // Handler sleeps long; shutdown will abort it mid-run.
        .handler(|_t: T, _c: JobContext| async {
            tokio::time::sleep(Duration::from_secs(120)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    // shutdown aborts the in-flight handlers; KeySlotGuard::drop must still run.
    let stats = handle.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(stats.aborted >= 1, "expected aborted handlers");
    // No assertion on the counter directly (it dies with the process); the
    // test asserts shutdown completes cleanly — a guard that paniced on a
    // missing decrement would abort the process. Reaching here = guard ran.
}
```

- [ ] **Step 3: Write the no-seed restart test**

Create `tests/per_key_no_seed_restart.rs`:

```rust
//! A fresh Worker does not seed its counter from `running` rows left by a
//! crashed process — it claims up to the full limit immediately.
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

#[tokio::test]
async fn fresh_worker_ignores_ghost_running_rows() {
    let (pool, _c) = pg18_pool().await;

    // Simulate a crashed process: a row already in `status='running'` with a
    // concurrency_key, an expired lease, and a stale lease_token.
    sqlx::query(
        "INSERT INTO pgwq.jobs
             (queue, payload, status, concurrency_key, attempts, max_attempts,
              last_attempted_at, first_attempted_at, lease_token, lease_expires_at)
         VALUES ('q', '\\x00', 'running', 'k', 1, 3,
                 now(), now(), gen_random_uuid(), now() - interval '1 hour')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Push 2 fresh jobs of the same key; limit is 2.
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> =
        (0..2).map(|i| (T { n: i }, Some("k".to_string()))).collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        .concurrency_limits([("k".to_string(), 2u32)])
        .handler(|_t: T, _c: JobContext| async { Ok::<(), JobError>(()) })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    // Both fresh jobs reached `done` — the ghost row did NOT consume headroom.
    let done: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgwq.jobs WHERE status = 'done'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(done, 2, "ghost running row must not block fresh claims");
}
```

- [ ] **Step 4: Write the unlimited-keys end-to-end test**

Create `tests/per_key_unlimited.rs`:

```rust
//! Jobs with NULL key or an unconfigured key run unlimited (worker-wide
//! `concurrency` is the only cap).
mod common;

use common::pg18_pool;
use pg_work_queue::{JobContext, JobError, Pusher, Worker};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

#[tokio::test]
async fn null_key_jobs_all_complete() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let items: Vec<(T, Option<String>)> = (0..10).map(|i| (T { n: i }, None)).collect();
    Pusher::new("q").push_batch(&mut tx, &items).await.unwrap();
    tx.commit().await.unwrap();

    let handle = Worker::<T>::builder()
        .pool(pool.clone())
        .queue("q")
        // A configured key that no job uses — must not affect NULL-key jobs.
        .concurrency_limits([("unused".to_string(), 1u32)])
        .handler(|_t: T, _c: JobContext| async { Ok::<(), JobError>(()) })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = handle.shutdown(Duration::from_secs(10)).await;

    let done: i64 = sqlx::query_scalar("SELECT count(*) FROM pgwq.jobs WHERE status = 'done'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(done, 10);
}
```

- [ ] **Step 5: Run all new behavioral tests**

Run: `cargo test --test per_key_no_head_of_line --test per_key_counter_no_leak --test per_key_no_seed_restart --test per_key_unlimited`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/per_key_no_head_of_line.rs tests/per_key_counter_no_leak.rs tests/per_key_no_seed_restart.rs tests/per_key_unlimited.rs
git commit -m "test(worker): end-to-end per-key concurrency behavioral tests"
```

---

## Task 11: `concurrency_key` immutability test

**Files:**
- Test: `tests/concurrency_key_immutable.rs`

- [ ] **Step 1: Write the test**

Create `tests/concurrency_key_immutable.rs`:

```rust
//! The immutability trigger rejects any UPDATE that changes concurrency_key,
//! and the normal claim/mark lifecycle preserves it.
mod common;

use common::pg18_pool;
use pg_work_queue::Pusher;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct T { n: u32 }

#[tokio::test]
async fn direct_update_of_concurrency_key_is_rejected() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("q").push(&mut tx, &T { n: 1 }, Some("k")).await.unwrap();
    tx.commit().await.unwrap();

    let err = sqlx::query("UPDATE pgwq.jobs SET concurrency_key = 'other' WHERE public_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("immutable"), "expected immutability error, got: {msg}");
}

#[tokio::test]
async fn setting_concurrency_key_from_null_is_rejected() {
    let (pool, _c) = pg18_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let id = Pusher::new("q").push(&mut tx, &T { n: 1 }, None).await.unwrap();
    tx.commit().await.unwrap();

    let err = sqlx::query("UPDATE pgwq.jobs SET concurrency_key = 'x' WHERE public_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("immutable"));
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test --test concurrency_key_immutable`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/concurrency_key_immutable.rs
git commit -m "test(schema): concurrency_key immutability trigger"
```

---

## Task 12: Observability — `pgwq.claim` saturation event

**Files:**
- Modify: `src/worker.rs` — `poll_loop`

- [ ] **Step 1: Add edge-triggered saturation logging**

In `poll_loop`, add a poll-loop local before the `loop {` (next to `consecutive_claim_errors`, ~line 1466):

```rust
    // Edge-triggered saturation logging: emit only when the set of
    // headroom-0 keys changes between ticks.
    let mut prev_saturated: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
```

After the `headroom` snapshot is computed (Task 9 Step 3), add:

```rust
        let saturated: std::collections::BTreeSet<String> = headroom
            .iter()
            .filter(|(_, &h)| h == 0)
            .map(|(k, _)| k.clone())
            .collect();
        if saturated != prev_saturated {
            if !saturated.is_empty() {
                tracing::debug!(
                    target: "pgwq.claim",
                    worker.id = %state.worker_id,
                    queue = %state.queue,
                    saturated_keys = ?saturated,
                    "per-key concurrency saturated",
                );
            }
            prev_saturated = saturated;
        }
```

- [ ] **Step 2: Verify build + lints**

Run: `cargo build --all-targets && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/worker.rs
git commit -m "feat(worker): edge-triggered pgwq.claim saturation tracing event"
```

---

## Task 13: Update `migrator_schema.rs`

**Files:**
- Modify: `tests/migrator_schema.rs`

- [ ] **Step 1: Read the file and extend the assertions**

Read `tests/migrator_schema.rs`. Make these changes:
- Add `"concurrency_key"` to the required-columns list asserted by the column-presence test.
- Add `"jobs_claim_conc_idx"` to the index-presence assertions. If a test named `three_partial_indexes_present` (or similar) asserts a closed set of exactly three index names, rename it (e.g. `claim_partial_indexes_present`) and add the fourth index; if it only checks each of three is present (subset check) the rename is still warranted for honesty.
- Add an assertion that the `jobs_concurrency_key_len` constraint exists (`SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname = 'jobs_concurrency_key_len')`).
- Add an assertion that the `assert_concurrency_key_immutable` trigger exists (`SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = 'assert_concurrency_key_immutable')`).

Follow the existing query/assertion style in the file.

- [ ] **Step 2: Run the test**

Run: `cargo test --test migrator_schema`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/migrator_schema.rs
git commit -m "test(schema): assert concurrency_key column/index/constraint/trigger"
```

---

## Task 14: README + rustdoc lockstep

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the README**

Read `README.md` and the design spec §8. Make every change:
- `## Quick start` — `Pusher::push` call gains `, None`.
- `### Pusher` section: all three `push*` signatures; the `push_batch` method-table row signature `&[(T, Option<String>)]`; the `unnest(...)` SQL string gains `$4::text[]`; the per-push "Validation order" list gains the concurrency-key step (after queue-name, before encode).
- `#### Builder methods` table: new `concurrency_limits` row.
- `### State machine and schema`: the `concurrency_key` column; `jobs_claim_idx` now `INCLUDE (concurrency_key)`; new `jobs_claim_conc_idx`; the `assert_concurrency_key_immutable` trigger.
- `### Error types`: `PushError::ConcurrencyKeyInvalid` (and `PushError` now `#[non_exhaustive]`); `BuildError::ConcurrencyKeyInvalid`; `BuildError::ConcurrencyLimitInvalid`.
- `### Resource limits`: `MAX_CONCURRENCY_KEY_LEN` (character units).
- `### Tracing / observability`: the edge-triggered `pgwq.claim` saturation event.
- New `### Per-key concurrency` section: the model; claim-time gating; the guarantee surface is *live handler tasks*, not `running` rows; the single-instance / single-`Worker`-object assumption and its consequences; no-seed restart behavior (transiently stale-high `running` count); the non-cooperative-handler slot-hold; the `tick_once` limitation.
- `## Architecture`: the claim-path branch (empty headroom → `claim_batch`; keyed → CTE).
- `## Known limitations`: the single-instance assumption; the large-table migration index-build write-stall; the head-of-queue skew.

- [ ] **Step 2: Verify the doctest still compiles**

Run: `cargo test --doc`
Expected: PASS — the crate-root example in `src/lib.rs` (already fixed in Task 2 Step 5) and any README doctests compile.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document per-key concurrency (README lockstep)"
```

---

## Task 15: Full suite + final gate

- [ ] **Step 1: Clippy across all targets**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS — no warnings.

- [ ] **Step 2: Full integration suite**

Run: `cargo test --no-fail-fast`
Expected: PASS — all ~120 existing tests plus the new ones. Investigate and fix any failure before proceeding.

- [ ] **Step 3: Rustdoc build**

Run: `cargo doc --no-deps`
Expected: PASS — no broken intra-doc links.

- [ ] **Step 4: Bump the crate version**

In `Cargo.toml`, bump the version (`0.1.3` → `0.1.4`) — consistent with the repo's per-feature version-bump convention (see recent commits).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml
git commit -m "chore: bump version to 0.1.4"
```

---

## Self-Review

**Spec coverage:** §1 schema → Task 1. §2 Pusher → Tasks 2, 3. §3 WorkerBuilder → Task 7. §4 counter → Tasks 8, 9. §5 claim SQL → Tasks 5, 6. §6 observability → Task 12. §7 non-goals → respected (no Stats fields; reaper/mark untouched; `tick_once` passes empty headroom). §8 README → Task 14. §9 tests → Tasks 1, 6, 10, 11, 13.

**Type consistency:** `concurrency_key: Option<String>` used uniformly in `Job`, `RawClaimedRow`, and the SQL. `concurrency_limits: HashMap<String, u32>` uniform in `WorkerBuilder`/`Worker`/`WorkerState`. `ConcurrencyCounter = Arc<std::sync::Mutex<HashMap<String, u32>>>` defined in Task 8, used in Task 9. `claim_and_decode`'s 7th parameter is `&HashMap<String, u32>` everywhere (Task 5 callers; Task 6/Task 9 producers).

**Ordering:** signature-breaking changes (`push_batch` Task 3, `claim_and_decode` Task 5, `push`/`push_at` Task 2) each update all call sites in the same task so the crate compiles after every task. Task 5 is behavior-preserving (empty headroom) so the suite stays green until Task 9 activates the counter.

**Known deferral:** the head-of-queue skew (spec §5 / open-risk 2) is documented in Task 14, not fixed — matches the spec's "documented, not fixed" decision.
