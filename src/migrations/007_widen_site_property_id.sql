-- =============================================================
-- Widen site_property_id from varchar(50) to varchar(255).
--
-- Reason: greekexclusiveproperties.com (and likely others in the
-- future) returns SEO slugs as site_property_id, e.g.
-- "new-luxury-sea-view-villa-with-pool-for-sale-in-vourvourou-sithonia-halkidiki"
-- which exceed 50 chars.
--
-- ALTER COLUMN TYPE varchar(50) → varchar(255) is a metadata-only
-- change in PostgreSQL (no row rewrite), safe even on large tables.
--
-- Idempotent: re-running is a no-op.
-- =============================================================

BEGIN;

ALTER TABLE properties
    ALTER COLUMN site_property_id TYPE VARCHAR(255);

COMMIT;