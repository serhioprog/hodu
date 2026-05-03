-- =============================================================
-- Applied migrations registry.
--
-- Self-bootstrapping: creates the tracking table AND registers
-- all previous migrations as already-applied. After this runs once,
-- init_db.py will skip any migration whose filename is in this table.
--
-- Strategy: insert each known historical filename. ON CONFLICT DO NOTHING
-- so re-runs don't break.
-- =============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS applied_migrations (
    filename   TEXT        PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Register all historical migrations as applied (so init_db.py won't re-run them).
-- We assume that if 006 is being applied, 001-005 have been applied at some
-- point already (manually or via earlier init_db.py runs).
INSERT INTO applied_migrations (filename) VALUES
    ('001_mdm_architecture.sql'),
    ('002_post_tz_fixes.sql'),
    ('003_ai_feedback.sql'),
    ('004_system_logs.sql'),
    ('005_core_ai_metrics.sql'),
    ('006_applied_migrations.sql')
ON CONFLICT (filename) DO NOTHING;

COMMIT;