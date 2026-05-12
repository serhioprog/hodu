-- =============================================================
-- Migration 011 — engine_pair_cache table
--
-- Purpose: per-pair scoring cache for engine v2 (Pass 6 Day 2).
-- Cache key = canonical pair_key ("lower_uuid:greater_uuid").
-- Invalidated on either property's content_hash change OR
-- engine_version bump.
--
-- Sources:
--   * RESEARCH.md §12.5.7 (4 new tables proposed; this is #2)
--   * RESEARCH.md §12.5.13 (engine writes; UI never reads)
--   * research/HYBRID_DESIGN.md §4.2 (full schema spec — verbatim)
--
-- No FK to properties — verbatim HYBRID_DESIGN spec. Orphan rows
-- after property deletion are harmless (next lookup invalidates
-- via content_hash mismatch). Architect-confirmed Pass 6 Day 2.
--
-- Idempotent: re-running this migration is a no-op.
-- =============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS engine_pair_cache (
    pair_key        TEXT          PRIMARY KEY,                 -- "lower_uuid:greater_uuid"
    engine_version  TEXT          NOT NULL,                    -- e.g. "v2.1.0"; bump invalidates cache
    a_content_hash  TEXT          NOT NULL,                    -- properties.content_hash at scoring time
    b_content_hash  TEXT          NOT NULL,
    verdict         TEXT          NOT NULL,                    -- duplicate | different | uncertain
    confidence      FLOAT         NULL,
    reasoning       TEXT          NULL,
    tier_emitted    SMALLINT      NOT NULL,                    -- 0 | 1 | 2 | 3
    cost_usd        NUMERIC(10,6) NOT NULL DEFAULT 0,
    scored_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ   NULL                         -- NULL = no TTL
);

CREATE INDEX IF NOT EXISTS idx_engine_pair_cache_engine_version
    ON engine_pair_cache (engine_version);

CREATE INDEX IF NOT EXISTS idx_engine_pair_cache_content_hashes
    ON engine_pair_cache (a_content_hash, b_content_hash);

COMMIT;
