-- =============================================================
-- Migration 015 — Dual-engine support (Sprint 7 Phase B)
--
-- Purpose: enable engine v2 to write its own clusters parallel to
-- engine v1, with per-engine admin views and shared feedback learning.
--
-- Sources:
--   * Sprint 7 architectural decision: Option A (junction table for v2)
--   * User decision: per-engine Dissolved DISPLAY, shared learning
--
-- Changes:
--   1. property_clusters.engine_version — marks which engine owns
--      the cluster. Default '1' so all existing rows are legacy
--      engine 1 (old detector). Engine v2 writer sets '2' on new rows.
--
--   2. cluster_v2_members — junction table for engine v2 only.
--      Property.cluster_id FK stays exclusive to engine 1 (no schema
--      churn там); engine 2 uses this junction for many-to-many-ish
--      membership (each property can be in at most one v2 cluster,
--      but the table allows the same property in v1 cluster AND v2
--      cluster simultaneously — that's the whole point of dual mode).
--
--   3. ai_duplicate_feedbacks.source_engine_version — tracks WHICH
--      engine's cluster admin was rejecting when this row was written.
--      Default '1' for legacy (matches the migration tag we already
--      use). Read path is engine-agnostic (both engines read all rows)
--      — this column is ONLY for display filtering on Dissolved tabs.
--
-- Idempotent: re-running this migration is a no-op.
-- Metadata-only ALTERs (no row rewrite), safe on production.
-- =============================================================

BEGIN;

-- 1) Add engine_version to property_clusters
-- Default '1' so existing rows + new old-engine inserts are tagged
-- legacy v1. Engine v2 writer sets '2' explicitly.
ALTER TABLE property_clusters
    ADD COLUMN IF NOT EXISTS engine_version VARCHAR(1) NOT NULL DEFAULT '1';

CREATE INDEX IF NOT EXISTS idx_property_clusters_engine_version
    ON property_clusters (engine_version);

COMMENT ON COLUMN property_clusters.engine_version IS
    'Which engine produced this cluster: ''1'' = legacy InternalDuplicateDetector, '
    '''2'' = HybridEngine (v2). Default ''1'' covers all pre-Sprint-7 rows. '
    'Engine v2 writer explicitly sets ''2''.';

-- 2) Junction table for engine v2 cluster membership
-- Engine v1 keeps using Property.cluster_id FK (no churn there).
-- Engine v2 uses this junction so a single property can be in
-- BOTH a v1 cluster AND a v2 cluster simultaneously without
-- conflict — the whole point of running engines in parallel.
CREATE TABLE IF NOT EXISTS cluster_v2_members (
    cluster_id  UUID NOT NULL REFERENCES property_clusters(id) ON DELETE CASCADE,
    property_id UUID NOT NULL REFERENCES properties(id)        ON DELETE CASCADE,
    added_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cluster_id, property_id)
);

-- Lookup by property — "which v2 cluster is this property in?"
CREATE INDEX IF NOT EXISTS idx_cluster_v2_members_property
    ON cluster_v2_members (property_id);

COMMENT ON TABLE cluster_v2_members IS
    'Engine v2 cluster membership junction. Engine 1 uses Property.cluster_id; '
    'engine 2 uses THIS table. Same property can appear in v1 cluster (via '
    'Property.cluster_id) AND a v2 cluster (via this junction) at the same '
    'time — that is the whole point of dual-engine parallel operation.';

-- 3) Add source_engine_version to ai_duplicate_feedbacks
-- Tracks WHICH engine's cluster admin was rejecting when this row
-- was written. Used for DISPLAY filtering on per-engine Dissolved
-- tabs ONLY. Both engines read ALL rows for learning — admin's
-- "NOT duplicate" verdict is engine-agnostic ground truth.
ALTER TABLE ai_duplicate_feedbacks
    ADD COLUMN IF NOT EXISTS source_engine_version VARCHAR(1)
        NOT NULL DEFAULT '1';

CREATE INDEX IF NOT EXISTS idx_feedback_source_engine_version
    ON ai_duplicate_feedbacks (source_engine_version);

COMMENT ON COLUMN ai_duplicate_feedbacks.source_engine_version IS
    'Which engine''s cluster was rejected when this feedback was written: '
    '''1'' = engine 1 (legacy), ''2'' = engine 2 (HybridEngine). Default ''1'' '
    'covers all pre-Sprint-7 rows. DISPLAY-ONLY filter on Dissolved tabs — '
    'learning reads all rows regardless of this column.';

COMMIT;