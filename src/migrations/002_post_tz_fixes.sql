-- =============================================================
-- Post-TZ fixes: enum status, lifecycle columns, manual verdict,
-- additional indexes. Runs AFTER 001_mdm_architecture.sql.
-- Idempotent: safe to re-run.
-- =============================================================

BEGIN;

-- 1. property_status ENUM ---------------------------------------
DO $$ BEGIN
    CREATE TYPE property_status AS ENUM (
        'NEW', 'ACTIVE', 'PRICE_CHANGED', 'DELISTED', 'SOLD'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Sanitize any rows that might have non-enum values (before type cast)
UPDATE properties
SET status = 'ACTIVE'
WHERE status IS NULL
   OR status NOT IN ('NEW', 'ACTIVE', 'PRICE_CHANGED', 'DELISTED', 'SOLD');

-- Cast column. Safe because all current values are known-valid above.
DO $$ BEGIN
    ALTER TABLE properties
        ALTER COLUMN status TYPE property_status
        USING status::property_status;
EXCEPTION WHEN others THEN
    -- Already converted on previous run → skip.
    NULL;
END $$;

ALTER TABLE properties
    ALTER COLUMN status SET DEFAULT 'NEW'::property_status,
    ALTER COLUMN status SET NOT NULL;

-- 2. Lifecycle columns for re-fetch cooldown --------------------
ALTER TABLE properties
    ADD COLUMN IF NOT EXISTS details_fetch_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_details_fetch_at  TIMESTAMPTZ;

-- 3. Russian literal cleanup from calc_* (one-time) -------------
UPDATE properties SET calc_prefecture   = NULL WHERE calc_prefecture   = 'Не определено';
UPDATE properties SET calc_municipality = NULL WHERE calc_municipality = 'Не определено';
UPDATE properties SET calc_area         = NULL WHERE calc_area         = 'Не определено';

-- 4. Manual admin verdict lock on clusters ----------------------
ALTER TABLE property_clusters
    ADD COLUMN IF NOT EXISTS verdict_locked BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS verdict_locked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS verdict_locked_by UUID REFERENCES agents(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS ix_clusters_verdict_locked
    ON property_clusters(verdict_locked) WHERE verdict_locked = TRUE;

-- 5. Additional missing indexes (TZ domain 2.7) -----------------
CREATE INDEX IF NOT EXISTS ix_properties_created_at  ON properties(created_at);
CREATE INDEX IF NOT EXISTS ix_properties_updated_at  ON properties(updated_at);
CREATE INDEX IF NOT EXISTS ix_properties_price       ON properties(price);
CREATE INDEX IF NOT EXISTS ix_properties_source_status
    ON properties(source_domain, status);

-- Functional index for daily-report date filter
CREATE INDEX IF NOT EXISTS ix_properties_created_date
    ON properties (( (created_at AT TIME ZONE 'UTC')::date ));
CREATE INDEX IF NOT EXISTS ix_properties_updated_date
    ON properties (( (updated_at AT TIME ZONE 'UTC')::date ));

-- 6. Expired auth_tokens — index for cleanup --------------------
CREATE INDEX IF NOT EXISTS ix_auth_tokens_expires ON auth_tokens(expires_at);

COMMIT;