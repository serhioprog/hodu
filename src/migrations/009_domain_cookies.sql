-- ====================================================================
-- Migration 009 — Per-domain cookie persistence
-- ====================================================================
-- When Playwright successfully solves a Cloudflare JS challenge, the
-- site issues a `cf_clearance` cookie that's valid for ~30 minutes.
-- Subsequent requests carrying this cookie skip the challenge entirely.
--
-- Without persistence, every container restart loses the cookie and we
-- have to re-solve the challenge from scratch — expensive (~5-10s per
-- domain) and wasteful (CF tracks repeated solves and may block).
--
-- This table caches cookies between scrape runs. The funnel reads the
-- row for the target domain, injects cookies into the browser context
-- BEFORE navigating, then writes any updated cookies back after the
-- request completes.
--
-- Schema notes:
--   * cookies stored as JSONB array of {name, value, domain, path,
--     expires, httpOnly, secure, sameSite} objects — the exact shape
--     Playwright's BrowserContext.add_cookies() accepts.
--   * One row per domain (PRIMARY KEY). Last-write-wins semantics.
--   * Cookies have their own internal expiry; we still track
--     last_updated_at to expire stale rows server-side as a backstop.
-- ====================================================================

CREATE TABLE IF NOT EXISTS domain_cookies (
    domain          VARCHAR(100) PRIMARY KEY,

    -- Playwright-format cookie array. See:
    -- https://playwright.dev/python/docs/api/class-browsercontext#browser-context-add-cookies
    cookies         JSONB        NOT NULL DEFAULT '[]'::jsonb,

    -- Bookkeeping for stale-cookie eviction. Cookies in `cookies` may
    -- have their own expiry timestamps; this is a coarser fallback.
    last_updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- We almost always read by domain (PK lookup is cheap), so no
-- additional indexes needed.
