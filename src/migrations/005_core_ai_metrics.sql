-- =============================================================
-- Core AI Metrics columns.
-- 
-- These columns are referenced from ORM (domain.py) and from the
-- internal_duplicate_detector code, but were never added to the
-- physical schema in 001-004. This migration closes that gap.
--
-- Added columns:
--   property_clusters.ai_score         (FLOAT)   max similarity among edges in cluster
--   property_clusters.phash_matches    (INTEGER) max pHash matches among edges in cluster  
--   power_properties.enhanced_media_urls (TEXT[]) AI-enhanced image URLs (filled by external service)
--
-- Idempotent: safe to re-run.
-- =============================================================

BEGIN;

-- 1. property_clusters AI scoring -------------------------------
ALTER TABLE property_clusters
    ADD COLUMN IF NOT EXISTS ai_score      FLOAT   NULL,
    ADD COLUMN IF NOT EXISTS phash_matches INTEGER NULL;

-- Index on ai_score for fast "show me high-confidence clusters first" queries
CREATE INDEX IF NOT EXISTS ix_clusters_ai_score
    ON property_clusters(ai_score DESC NULLS LAST)
    WHERE ai_score IS NOT NULL;

-- 2. power_properties enhanced images ---------------------------
ALTER TABLE power_properties
    ADD COLUMN IF NOT EXISTS enhanced_media_urls TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];

COMMIT;