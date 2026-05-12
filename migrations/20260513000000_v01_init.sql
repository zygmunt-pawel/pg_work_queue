-- pg_work_queue v0.1 initial schema.
--
-- Type C (Internal Mutable) table per postgres-schema-design.md:
-- - Internal queue state, hard-delete OK (retention sweeper does DELETE)
-- - Status state machine: queued → running → done | dead | awaiting_retry → running
-- - public_id exposed externally (returned from Pusher::push for correlation/logging)
-- - No FKs (queue rows are self-contained; user's parent rows reference public_id naked)
--
-- Requires PostgreSQL 18+ (for native uuidv7()).

-- Loud-fail on PG < 18 instead of cryptic "function uuidv7() does not exist".
DO $$
BEGIN
    IF current_setting('server_version_num')::int < 180000 THEN
        RAISE EXCEPTION 'pgwq requires PostgreSQL 18+ (uuidv7() native), got %',
            current_setting('server_version');
    END IF;
END$$;

CREATE SCHEMA IF NOT EXISTS pgwq;

-- updated_at touch trigger function, namespaced to pg_work_queue (not public)
-- to avoid colliding with consumer apps' own set_updated_at helpers.
CREATE OR REPLACE FUNCTION pgwq.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

-- Stable enum: the 5 states are part of the public state machine and adding
-- new ones is a major version event for the crate. ENUM > lookup table here.
CREATE TYPE pgwq.job_status AS ENUM (
    'queued',
    'running',
    'awaiting_retry',
    'done',
    'dead'
);

CREATE TABLE pgwq.jobs (
    -- Internal PK: compact int8, used by reaper/poll batching, never user-facing.
    id                 BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- External handle: returned from Pusher::push; users correlate via this.
    -- uuidv7 gives time-ordered B-tree locality and sortable client-side logs.
    public_id          UUID        NOT NULL DEFAULT uuidv7() UNIQUE,
    -- COLLATE "C": byte-exact equality, no locale-dependent comparison cost.
    queue              TEXT        COLLATE "C" NOT NULL,
    -- Opaque blob — codec lives in app; library does not introspect payload.
    payload            BYTEA       NOT NULL,
    status             pgwq.job_status NOT NULL DEFAULT 'queued',
    -- INTEGER (not SMALLINT) — builder accepts max_attempts: u32, overflow
    -- at i16::MAX (32767) would otherwise be reachable + cause numeric_value_
    -- out_of_range in claim_batch's `attempts = j.attempts + 1`, putting the
    -- row in a stuck-running loop the reaper can't recover.
    attempts           INTEGER     NOT NULL DEFAULT 0,
    -- Fencing token: set by claim_batch via gen_random_uuid (v4 — no time leak,
    -- ephemeral, never indexed). Cleared on every transition out of 'running'.
    -- Required for mark_done/retry/dead WHERE clauses to prevent the
    -- reaper-vs-old-worker race that apalis-postgres has.
    lease_token        UUID,
    last_error         TEXT,
    last_attempted_at  TIMESTAMPTZ,
    first_attempted_at TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    -- Scheduled jobs: pusher may set run_at = now() + delay; claim_batch
    -- requires run_at <= now(). Retry path also uses this for backoff.
    run_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT jobs_queue_nonempty
        CHECK (length(queue) > 0),
    -- Defense-in-depth backstops; library also validates pre-INSERT and
    -- truncates on write. DB CHECK catches buggy clients bypassing the lib.
    CONSTRAINT jobs_queue_max_len
        CHECK (length(queue) <= 64),
    CONSTRAINT jobs_payload_max_size
        CHECK (octet_length(payload) <= 1048576),  -- 1 MiB; matches limits::MAX_PAYLOAD_BYTES
    CONSTRAINT jobs_last_error_max_len
        CHECK (last_error IS NULL OR length(last_error) <= 8192),  -- matches limits::MAX_LAST_ERROR_LEN
    CONSTRAINT jobs_attempts_nonneg
        CHECK (attempts >= 0),
    -- Temporal monotonicity. Catches reordering bugs in claim/mark/reap paths.
    -- run_at >= created_at rejects "backfill into the past" — set created_at
    -- explicitly if you need that.
    CONSTRAINT jobs_temporal CHECK (
        (first_attempted_at IS NULL OR first_attempted_at >= created_at)
        AND (last_attempted_at IS NULL OR last_attempted_at >= COALESCE(first_attempted_at, created_at))
        AND (finished_at IS NULL OR finished_at >= COALESCE(last_attempted_at, created_at))
        AND updated_at >= created_at
        AND run_at >= created_at
    ),
    -- State machine invariants. Any code path producing a logically impossible
    -- (status, fields) combination fails loudly here instead of corrupting state.
    -- Critically: lease_token NOT NULL iff status='running' — this is what
    -- makes the fencing-token guard in mark_* queries meaningful.
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

-- High-churn queue table: leave 20% free per block so updates can HOT in-place
-- instead of allocating new tuples elsewhere. Plus more aggressive autovacuum
-- so dead tuples (from done→DELETE and HOT chains) get reclaimed fast.
ALTER TABLE pgwq.jobs SET (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);

-- Poll claim hot path. Partial keeps the index small (terminal rows dominate
-- in long runtime). ORDER BY run_at, id matches the claim_batch query.
CREATE INDEX jobs_claim_idx
    ON pgwq.jobs (queue, run_at, id)
    WHERE status IN ('queued', 'awaiting_retry');

-- Reaper hot path. Scan only rows that COULD be stale (status='running');
-- WHERE clause keeps the index tiny relative to total table size.
CREATE INDEX jobs_reap_idx
    ON pgwq.jobs (last_attempted_at)
    WHERE status = 'running';

-- Purge functions hot path. `pgwq::purge_done(pool, age)` and
-- `pgwq::purge_dead(pool, age)` are user-invoked (no auto-sweeper); both
-- DELETE WHERE status = ? AND finished_at < now() - ?.
CREATE INDEX jobs_terminal_idx
    ON pgwq.jobs (finished_at)
    WHERE status IN ('done', 'dead');

CREATE TRIGGER touch_jobs
    BEFORE UPDATE ON pgwq.jobs
    FOR EACH ROW EXECUTE FUNCTION pgwq.set_updated_at();
