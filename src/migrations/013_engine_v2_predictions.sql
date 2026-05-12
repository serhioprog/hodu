-- =============================================================
-- Migration 013 — engine_v2_predictions table
--
-- Purpose: Phase 1-2 shadow-mode output for engine v2. Old engine
-- continues to write property_clusters; new engine writes here only,
-- enabling diff analysis without dual-writes to canonical state.
-- Dropped after Phase 3 cut-over (RESEARCH §12.5.7 + §12.6 retention).
--
-- Sources:
--   * RESEARCH.md §12.5.7 (proposed table #4 — DDL verbatim)
--   * RESEARCH.md §12.5.10 (run_full_dedup writes here in Phase 1-2)
--
-- UNIQUE (pair_key, scored_at): one row per pair per scrape run.
-- Re-running run_full_dedup at the same NOW() instant would conflict;
-- in practice scored_at advances per-row so no real collision.
--
-- Idempotent: re-running this migration is a no-op.
-- =============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS engine_v2_predictions (
    id            UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    pair_key      TEXT          NOT NULL,                              -- "lower_uuid:greater_uuid"
    a_id          UUID          NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    b_id          UUID          NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    verdict       TEXT          NOT NULL,                              -- duplicate | different | uncertain
    confidence    FLOAT         NULL,
    reasoning     TEXT          NULL,
    tier_emitted  SMALLINT      NOT NULL,                              -- 0 | 1 | 2 | 3
    cost_usd      NUMERIC(10,6) NOT NULL DEFAULT 0,
    scored_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uix_engine_v2_predictions_pair_scored UNIQUE (pair_key, scored_at)
);

CREATE INDEX IF NOT EXISTS idx_engine_v2_predictions_scored_at
    ON engine_v2_predictions (scored_at);

CREATE INDEX IF NOT EXISTS idx_engine_v2_predictions_verdict
    ON engine_v2_predictions (verdict);

COMMIT;
