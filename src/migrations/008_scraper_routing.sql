-- ====================================================================
-- Migration 008 — Fetch Funnel routing & telemetry
-- ====================================================================
-- Adds two tables that the FetchFunnel orchestrator uses to learn which
-- fetch stage works best for each domain over time.
--
-- scraper_routing:
--     One row per scraped domain. Stores the currently preferred stage
--     and which stages are enabled. Updated by the funnel after several
--     consecutive successes/failures.
--
-- fetch_attempts:
--     Append-only log of every fetch attempt (success or failure).
--     Used for daily telemetry rollups in the Telegram report and for
--     post-hoc debugging when a domain starts misbehaving.
--
-- Both tables are non-essential — if they were dropped, the funnel
-- would still work using in-memory defaults. They make the system
-- observable and self-tuning, but are not on the critical path.
-- ====================================================================

CREATE TABLE IF NOT EXISTS scraper_routing (
    domain              VARCHAR(100)   PRIMARY KEY,

    -- Stage to start fetch attempts on. 0 = curl_cffi (cheapest).
    -- Promoted upward by the funnel after consecutive successes on a
    -- higher stage. Demoted by the periodic probe job (Sprint 3+).
    preferred_stage     INT            NOT NULL DEFAULT 0,

    -- Which stages are allowed for this domain. Default = all (0..4).
    -- Admins can override e.g. to ARRAY[0,1] for cost-sensitive domains
    -- where we never want paid stages 3-4 to fire.
    enabled_stages      INT[]          NOT NULL DEFAULT ARRAY[0,1,2,3,4],

    -- Bookkeeping for observability
    last_success_at     TIMESTAMPTZ    NULL,
    last_attempt_at     TIMESTAMPTZ    NULL,
    last_probe_at       TIMESTAMPTZ    NULL,
    notes               TEXT           NULL,

    created_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fetch_attempts (
    id           BIGSERIAL    PRIMARY KEY,
    domain       VARCHAR(100) NOT NULL,
    url          TEXT         NOT NULL,
    stage        INT          NOT NULL,
    success      BOOLEAN      NOT NULL,
    duration_ms  INT          NOT NULL DEFAULT 0,

    -- Populated only on failure. Values are stable strings for grouping
    -- in analytics (cloudflare_block, captcha_detected, timeout,
    -- http_error, empty_response, all_stages_failed, unknown).
    error_code   VARCHAR(50)  NULL,
    error_text   TEXT         NULL,

    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Speed up the typical analytics query: "show me failures for $domain
-- in the last 24 hours grouped by stage and error_code".
CREATE INDEX IF NOT EXISTS idx_fetch_attempts_domain_created_at
    ON fetch_attempts (domain, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_fetch_attempts_success_stage
    ON fetch_attempts (success, stage);

-- Bookkeeping: keep a hard cap on the table size by deleting attempts
-- older than 30 days. We only need recent data for routing decisions
-- and current dashboards. Run this manually or in a weekly cron job.
--
-- Example: DELETE FROM fetch_attempts WHERE created_at < NOW() - INTERVAL '30 days';