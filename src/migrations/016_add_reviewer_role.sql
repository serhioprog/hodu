-- =============================================================
-- Migration 016 — Reviewer role (Sprint 7 Phase C)
--
-- Purpose: introduce a third user role between Agent and Admin.
-- Reviewer can interact with Properties + Duplicates tabs (manage
-- clusters: approve/reject/merge/dissolve/remove), but CANNOT
-- access Users / Settings / Scrapers / Email tabs.
--
-- Admin implicitly has all reviewer permissions — no need to set
-- both flags True. The OR-check in get_current_reviewer() handles
-- this in code.
--
-- Sources:
--   * Sprint 7 architectural decision: extend boolean-flag pattern
--     (is_admin → is_admin + is_reviewer) rather than introduce
--     role enum. KISS — one ALTER, no data migration.
--
-- Changes:
--   1. agents.is_reviewer — boolean flag, default FALSE so all
--      existing rows are unaffected. Admin sets it via the
--      Add Agent modal in /admin Users tab.
--
-- Idempotent: re-running this migration is a no-op.
-- Metadata-only ALTER (no row rewrite), safe on production.
-- =============================================================

BEGIN;

-- 1) Add is_reviewer to agents
-- Default FALSE so all existing accounts retain their current
-- permission level. New Reviewer accounts get is_reviewer=TRUE
-- explicitly via the Add Agent form.
ALTER TABLE agents
    ADD COLUMN IF NOT EXISTS is_reviewer BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN agents.is_reviewer IS
    'Sprint 7 Phase C: Reviewer role. Grants access to /admin Properties + '
    'Duplicates tabs and all cluster management endpoints (approve / reject / '
    'manual-merge / recurrence-check / bulk-verdict / remove member / dissolved). '
    'Does NOT grant access to Users / Settings / Scrapers / Email tabs. '
    'Admin role (is_admin=TRUE) implicitly has all reviewer permissions — the '
    'OR-check in get_current_reviewer() means admins never need both flags set.';

COMMIT;