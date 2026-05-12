-- =============================================================
-- Migration 012 — mismerge_flags table
--
-- Purpose: engine reports of pairs in APPROVED clusters that
-- violate hard rules (year_diff > 10, T0 disagrees, multi-cluster
-- bridge). Engine flags but NEVER auto-dissolves (spec §11).
-- Admin reviews via weekly Telegram digest and sets admin_action.
--
-- Sources:
--   * RESEARCH.md §12.5.11 (full schema + detection algorithm — verbatim)
--   * RESEARCH.md §12.5.9 (multi_cluster_bridge flag_type from
--     cluster-ID-collision rule)
--
-- 4 flag_types:
--   year_diff_outlier    — pair in approved cluster, year_diff > 10
--   engine_t0_disagrees  — pair in approved cluster, T0 fires DIFFERENT
--   multi_cluster_bridge — new property bridges ≥2 approved clusters
--   pattern              — reserved for future patterns
--
-- UNIQUE(cluster_id, pair_a_id, pair_b_id, flag_type) makes
-- detection idempotent: repeated run_full_dedup runs do not
-- re-emit existing pending flags.
--
-- Idempotent: re-running this migration is a no-op.
-- =============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS mismerge_flags (
    id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id       UUID          NOT NULL REFERENCES property_clusters(id) ON DELETE CASCADE,
    pair_a_id        UUID          NOT NULL REFERENCES properties(id)        ON DELETE CASCADE,
    pair_b_id        UUID          NOT NULL REFERENCES properties(id)        ON DELETE CASCADE,
    flag_type        TEXT          NOT NULL,    -- year_diff_outlier | engine_t0_disagrees | multi_cluster_bridge | pattern
    flag_reason      TEXT          NOT NULL,
    detected_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    admin_action     TEXT          NULL,        -- confirm_keep | dissolve | ignore | NULL (pending)
    admin_action_at  TIMESTAMPTZ   NULL,
    admin_action_by  TEXT          NULL,        -- admin user id / handle
    CONSTRAINT uix_mismerge_flag_pair UNIQUE (cluster_id, pair_a_id, pair_b_id, flag_type)
);

-- Partial index: pending flags only (admin's worklist).
CREATE INDEX IF NOT EXISTS idx_mismerge_flags_pending
    ON mismerge_flags (cluster_id, admin_action)
    WHERE admin_action IS NULL;

CREATE INDEX IF NOT EXISTS idx_mismerge_flags_detected_at
    ON mismerge_flags (detected_at);

COMMIT;
