-- =============================================================
-- Migration 010 — Add `notes` column to property_clusters
--
-- Purpose: audit trail for clusters created via admin actions.
--
-- Primary use case (Sprint 4 / Group D — Manual Merge):
--   When an admin manually merges properties from existing clusters
--   into a new APPROVED cluster, we record human-readable provenance
--   here, e.g.:
--     "Manual merge of 4 properties from clusters [uuid1, uuid2]"
--
-- Future use cases:
--   * Operator notes on PENDING clusters during review
--   * Reasons for verdict locks beyond the (verdict_locked_by, _at) audit
--
-- Idempotent: re-running this migration is a no-op.
-- Metadata-only ALTER (no row rewrite), safe on production.
-- =============================================================

BEGIN;

ALTER TABLE property_clusters
    ADD COLUMN IF NOT EXISTS notes TEXT NULL;

COMMIT;
