-- =============================================================
-- MDM + Power Object Architecture Migration
-- Idempotent where possible. No data loss.
-- =============================================================

BEGIN;

-- 1. Extensions --------------------------------------------------
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- для gen_random_uuid()

-- 2. Cluster status ENUM ----------------------------------------
DO $$ BEGIN
    CREATE TYPE cluster_status AS ENUM ('PENDING', 'APPROVED', 'REJECTED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 3. property_clusters ------------------------------------------
CREATE TABLE IF NOT EXISTS property_clusters (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status                    cluster_status NOT NULL DEFAULT 'PENDING',
    member_count              INTEGER NOT NULL DEFAULT 0,
    last_external_is_unique   BOOLEAN,
    last_external_check_at    TIMESTAMPTZ,
    power_generated_at        TIMESTAMPTZ,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_clusters_status ON property_clusters(status);
CREATE INDEX IF NOT EXISTS ix_clusters_unique_flag
    ON property_clusters(last_external_is_unique) WHERE last_external_is_unique IS NOT NULL;

-- 4. properties — ADD COLUMNS (no data loss) --------------------
ALTER TABLE properties
    ADD COLUMN IF NOT EXISTS embedding      vector(1536),
    ADD COLUMN IF NOT EXISTS content_hash   VARCHAR(64),
    ADD COLUMN IF NOT EXISTS cluster_id     UUID REFERENCES property_clusters(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS image_phashes  TEXT[] DEFAULT ARRAY[]::TEXT[];

CREATE INDEX IF NOT EXISTS ix_properties_content_hash       ON properties(content_hash);
CREATE INDEX IF NOT EXISTS ix_properties_cluster_id         ON properties(cluster_id);
CREATE INDEX IF NOT EXISTS ix_properties_source_domain      ON properties(source_domain);
CREATE INDEX IF NOT EXISTS ix_properties_cat_muni           ON properties(category, calc_municipality);
CREATE INDEX IF NOT EXISTS ix_properties_active_status      ON properties(is_active, status);

-- HNSW on embedding, cosine ops, partial (only indexed rows with embedding)
CREATE INDEX IF NOT EXISTS ix_properties_embedding_hnsw
    ON properties USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL;

-- 5. external_property_cache ------------------------------------
CREATE TABLE IF NOT EXISTS external_property_cache (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_source  VARCHAR(100) NOT NULL,
    external_id      VARCHAR(255) NOT NULL,
    canonical_text   TEXT         NOT NULL,
    content_hash     VARCHAR(64)  NOT NULL,
    embedding        vector(1536),
    raw_payload      JSONB        NOT NULL,
    expires_at       TIMESTAMPTZ  NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (external_source, external_id)
);
CREATE INDEX IF NOT EXISTS ix_extcache_expires ON external_property_cache(expires_at);
CREATE INDEX IF NOT EXISTS ix_extcache_hash    ON external_property_cache(content_hash);
CREATE INDEX IF NOT EXISTS ix_extcache_embedding_hnsw
    ON external_property_cache USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL;

-- 6. power_properties (master) ----------------------------------
CREATE TABLE IF NOT EXISTS power_properties (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id           UUID NOT NULL UNIQUE REFERENCES property_clusters(id) ON DELETE CASCADE,

    description          TEXT NOT NULL,
    features             JSONB NOT NULL DEFAULT '{}'::JSONB,

    price                INTEGER,
    size_sqm             FLOAT,
    land_size_sqm        FLOAT,
    bedrooms             INTEGER,
    bathrooms            INTEGER,
    year_built           INTEGER,
    category             VARCHAR(100),

    calc_prefecture      VARCHAR(255),
    calc_municipality    VARCHAR(255),
    calc_area            VARCHAR(255),
    latitude             FLOAT,
    longitude            FLOAT,

    image_urls           TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    image_local_paths    TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],

    source_property_ids  UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
    source_domains       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],

    generated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    regenerated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_power_price        ON power_properties(price);
CREATE INDEX IF NOT EXISTS ix_power_muni         ON power_properties(calc_municipality);
CREATE INDEX IF NOT EXISTS ix_power_category     ON power_properties(category);

COMMIT;