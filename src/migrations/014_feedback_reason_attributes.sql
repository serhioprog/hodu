-- Migration 014: Add structured feedback reasoning
-- Sprint 6 Phase A — collect per-rejection attribute deltas + free-text
-- notes so Phase C ML training can learn from WHY admin rejected pairs.
--
-- Engine v2 continues to use this table at T0 (spec §3.4) — existing rows
-- without these new fields work fine (defaults applied). No data backfill
-- needed; old rejections simply have empty reason_attributes and a
-- feedback_source of 'migration' to distinguish them from new structured
-- feedback rows.
--
-- Note: `updated_at` already exists on this table from migration 003
-- (NOT NULL DEFAULT CURRENT_TIMESTAMP). The ADD COLUMN IF NOT EXISTS
-- below is a documented no-op — the upsert writer in
-- src/database/feedback_repository.py sets updated_at = NOW() explicitly
-- when an admin enriches an existing row.

BEGIN;

ALTER TABLE ai_duplicate_feedbacks
    ADD COLUMN IF NOT EXISTS reason_attributes JSONB
        NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS reason_text TEXT,
    ADD COLUMN IF NOT EXISTS feedback_source VARCHAR(20)
        NOT NULL DEFAULT 'admin_reject',
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

-- Index for analytics queries (Phase B will aggregate by attribute):
-- "show me all rejections that cited 'location' as a reason"
CREATE INDEX IF NOT EXISTS idx_feedback_reason_attributes
    ON ai_duplicate_feedbacks USING gin (reason_attributes);

-- Index for filtering by source on dissolved page
CREATE INDEX IF NOT EXISTS idx_feedback_source
    ON ai_duplicate_feedbacks (feedback_source);

-- Document valid feedback_source values via comment
COMMENT ON COLUMN ai_duplicate_feedbacks.feedback_source IS
    'Where the rejection came from: admin_reject (cluster reject button), '
    'manual_split (property removed from cluster), cluster_dissolve '
    '(cluster dissolved via shrinkage — currently no UI for this), '
    'migration (legacy rows from before Sprint 6 Phase A).';

-- One-shot: existing rows get feedback_source='migration' to distinguish
-- from new structured feedback. New inserts will use proper source values.
UPDATE ai_duplicate_feedbacks
SET feedback_source = 'migration'
WHERE feedback_source = 'admin_reject';  -- which is the default we just set

COMMIT;
