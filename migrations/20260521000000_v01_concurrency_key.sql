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
