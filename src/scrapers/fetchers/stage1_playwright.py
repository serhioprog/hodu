"""
Stage 1 — Playwright with stealth + persistent cookies.

This is the first stage in the funnel that can actually defeat
Cloudflare's JS challenge. It works by launching a real Chromium browser,
patching out the most obvious bot-detection signals, and waiting for the
page to "settle" — by which time CF has issued a `cf_clearance` cookie
and we're free to read the rendered HTML.

Architecture:

    fetcher.get(url)
        ├── load saved cookies for domain (if any)
        ├── browser_pool.acquire() ⟶ get warm Chromium
        │    └── new BrowserContext (isolated profile)
        │         ├── add_cookies(saved)         ← skip CF if still valid
        │         ├── Stealth.apply(page)        ← patch bot fingerprints
        │         ├── page.goto(url)
        │         ├── wait for CF challenge to complete
        │         │   - check for `Just a moment...` text → wait & retry
        │         │   - check for cf_clearance cookie → success
        │         ├── extract HTML
        │         └── save updated cookies
        └── return FetchResult

Key anti-detection measures (handled by playwright-stealth):
  * navigator.webdriver = false
  * Mock missing plugins, languages, deviceMemory
  * Patch chrome.runtime, WebGL vendor strings
  * Hide automation-specific window properties

POST support:
  realestatecenter.gr collects URLs by POSTing form-data to a WP
  admin-ajax.php endpoint. This stage supports POST by intercepting
  the request via page.evaluate(fetch(...)) — runs the call FROM
  INSIDE the rendered page context, which means the AJAX call carries
  the same cf_clearance cookie that the page just earned.

Failure modes:
  * Browser pool unavailable        → CaptchaDetected (so funnel escalates)
  * page.goto() times out            → FetcherTimeout (don't blame the stage)
  * Page still shows CF challenge after wait → CloudflareBlock
  * 4xx / 5xx                        → HttpError
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Mapping
from urllib.parse import urlparse

from loguru import logger
from sqlalchemy import text as sql_text

from src.database.db                       import async_session_maker
from src.scrapers.fetchers.base_fetcher    import BaseFetcher, FetchResult
from src.scrapers.fetchers.browser_pool    import browser_pool
from src.scrapers.fetchers.exceptions      import (
    CaptchaDetected, CloudflareBlock, EmptyResponse, FetcherTimeout, HttpError,
)


# Try to import stealth — graceful if missing, falls back to manual flags
try:
    from playwright_stealth import Stealth
    _STEALTH_AVAILABLE = True
except ImportError:                                  # pragma: no cover
    _STEALTH_AVAILABLE = False
    logger.warning(
        "[Stage1] playwright_stealth not installed — using minimal anti-detection only"
    )


# ── Tuning constants ─────────────────────────────────────────────────

DEFAULT_PAGE_TIMEOUT_MS:   int = 45_000
"""Total budget for page.goto() to settle. CF challenges typically resolve
in 5-10s; we allow generous headroom."""

CHALLENGE_POLL_INTERVAL:   float = 0.5
"""How often we re-check if the CF challenge page has navigated away."""

CHALLENGE_MAX_WAIT_SECONDS: float = 20.0
"""Max time to wait sitting on `Just a moment...` before giving up.
Some legitimate slow networks need 10-15s; bot detections will reject
us almost instantly so this loop only really matters in the slow case."""

# Plausible viewport — common widescreen size, NOT 1920x1080 which is
# stereotypically headless. CF tracks this.
_VIEWPORT  = {"width": 1366, "height": 768}
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


# ─── Cookie persistence helpers ──────────────────────────────────────

async def _load_cookies(domain: str) -> list[dict] | None:
    """Read saved cookies for domain. Returns None if not present or DB error."""
    try:
        async with async_session_maker() as session:
            row = (await session.execute(sql_text("""
                SELECT cookies FROM domain_cookies WHERE domain = :d
            """), {"d": domain})).first()
            if row and row.cookies:
                # asyncpg returns JSONB as already-decoded list
                return list(row.cookies) if isinstance(row.cookies, list) else json.loads(row.cookies)
    except Exception as e:
        logger.debug(f"[Stage1] cookie load failed for {domain}: {e}")
    return None


async def _save_cookies(domain: str, cookies: list[dict]) -> None:
    """Upsert cookies for domain. Best-effort — never raises."""
    try:
        async with async_session_maker() as session:
            await session.execute(sql_text("""
                INSERT INTO domain_cookies (domain, cookies, last_updated_at)
                VALUES (:d, CAST(:c AS jsonb), NOW())
                ON CONFLICT (domain) DO UPDATE
                  SET cookies = EXCLUDED.cookies,
                      last_updated_at = NOW()
            """), {"d": domain, "c": json.dumps(cookies)})
            await session.commit()
    except Exception as e:
        logger.debug(f"[Stage1] cookie save failed for {domain}: {e}")


def _domain_of(url: str) -> str:
    """Extract bare hostname for cookie scoping."""
    return urlparse(url).hostname or ""


# ─── CF challenge detection ──────────────────────────────────────────

def _looks_like_cf_challenge(html: str) -> bool:
    """Heuristic: page is still showing CF's wait-page, not the real content."""
    if not html or len(html) > 100_000:
        # Real CF challenge pages are tiny (~1-5KB).
        return False
    lower = html.lower()
    return (
        "just a moment..." in lower
        or "cf-browser-verification" in lower
        or "cf-challenge" in lower
        or "_cf_chl_opt" in lower
    )


async def _wait_for_cf_to_resolve(page) -> None:
    """
    Poll the page DOM until the CF challenge gives way to real content.
    Raises CloudflareBlock if we're still on the challenge after the
    deadline — in practice this means CF has rejected us.
    """
    deadline = time.monotonic() + CHALLENGE_MAX_WAIT_SECONDS
    while time.monotonic() < deadline:
        try:
            html = await page.content()
        except Exception:
            # Page might be navigating, just sleep and try again
            await asyncio.sleep(CHALLENGE_POLL_INTERVAL)
            continue
        if not _looks_like_cf_challenge(html):
            return
        await asyncio.sleep(CHALLENGE_POLL_INTERVAL)
    raise CloudflareBlock(
        f"CF challenge did not resolve within {CHALLENGE_MAX_WAIT_SECONDS}s"
    )


# ─── The fetcher ─────────────────────────────────────────────────────

class Stage1PlaywrightFetcher(BaseFetcher):
    """Headless Chromium fetcher with stealth + cookie persistence."""

    stage_number    = 1
    name            = "playwright"
    default_timeout = 45  # seconds

    # ── Public API ────────────────────────────────────────────────

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params:  Mapping[str, Any] | None = None,
        timeout: int | None              = None,
    ) -> FetchResult:
        if params:
            # Build query string in URL — page.goto() doesn't take params.
            from urllib.parse import urlencode, urlsplit, urlunsplit
            parts = urlsplit(url)
            existing = parts.query
            new_q = urlencode(dict(params))
            qs = f"{existing}&{new_q}" if existing else new_q
            url = urlunsplit((parts.scheme, parts.netloc, parts.path, qs, parts.fragment))

        return await self._fetch_via_browser(url, method="GET", headers=headers, timeout=timeout)

    async def post(
        self,
        url: str,
        *,
        data:    Any                      = None,
        headers: Mapping[str, str] | None = None,
        timeout: int | None               = None,
    ) -> FetchResult:
        return await self._fetch_via_browser(
            url, method="POST", data=data, headers=headers, timeout=timeout,
        )

    async def close(self) -> None:
        """Pool is shared module-level — closing one fetcher does NOT
        kill the pool. The actual teardown happens via app shutdown
        hook calling browser_pool.close_all()."""
        return None

    # ── WordPress AJAX (single-session nonce + POST) ─────────────

    async def wp_ajax(
        self,
        ajax_url:      str,
        *,
        referer_url:   str,
        action:        str,
        nonce_pattern: str | None              = None,
        nonce_js_path: str | None              = None,
        extra_data:    Mapping[str, Any] | None = None,
        headers:       Mapping[str, str] | None = None,
        timeout:       int | None               = None,
    ) -> FetchResult:
        """
        See BaseFetcher.wp_ajax for the contract.

        Implementation: opens ONE Playwright context. Inside it:
          1. page.goto(referer_url) — earn WP session cookies + run JS.
          2. Extract nonce — either from window globals (preferred:
             `nonce_js_path` like "halkiGrid3Config.filterNonce") or
             via regex on page HTML (`nonce_pattern`). At least one
             must be supplied.
          3. fetch() the AJAX URL FROM the page context — cookies +
             Referer + Origin all match what WP expects.

        Returns the AJAX response as a FetchResult.
        """
        if not browser_pool.available:
            raise CaptchaDetected("Stage 1 Playwright unavailable", url=ajax_url)

        if not nonce_pattern and not nonce_js_path:
            raise EmptyResponse(
                "wp_ajax requires either nonce_pattern or nonce_js_path",
                url=ajax_url,
            )

        import re as _re
        domain        = _domain_of(ajax_url)
        timeout_ms    = (timeout or self.default_timeout) * 1000
        saved_cookies = await _load_cookies(domain)
        started       = time.monotonic()

        async with browser_pool.acquire() as browser:
            context = await browser.new_context(
                viewport=_VIEWPORT,
                user_agent=_USER_AGENT,
                locale="en-US",
                timezone_id="Europe/Athens",
                java_script_enabled=True,
                extra_http_headers={
                    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            try:
                if saved_cookies:
                    try:
                        await context.add_cookies(saved_cookies)
                    except Exception:
                        pass

                page = await context.new_page()
                if _STEALTH_AVAILABLE:
                    await Stealth().apply_stealth_async(page)

                # ── Step 1: visit referer (loads JS + sets cookies) ──
                # Use 'domcontentloaded' for the navigation itself — fast.
                # If we need a JS global, we'll poll for it specifically
                # below rather than waiting for full networkidle (which
                # can take 45+ seconds on ad-heavy pages).
                try:
                    await page.goto(
                        referer_url, wait_until="domcontentloaded",
                        timeout=timeout_ms,
                    )
                except asyncio.TimeoutError as e:
                    raise FetcherTimeout(f"referer goto timeout: {e}", url=ajax_url) from e

                if _looks_like_cf_challenge(await page.content()):
                    logger.info(f"[Stage1] {referer_url}: CF challenge detected, waiting…")
                    await _wait_for_cf_to_resolve(page)

                # ── Step 2: extract nonce ────────────────────────
                nonce: str | None = None
                if nonce_js_path:
                    # Walk the dotted path on window. Returns None if any
                    # segment is missing (gracefully fall back to regex).
                    js_lookup = (
                        f"() => {{ "
                        f"  const path = {json.dumps(nonce_js_path.split('.'))}; "
                        f"  let cur = window; "
                        f"  for (const seg of path) {{ "
                        f"    if (cur == null || typeof cur !== 'object') return null; "
                        f"    cur = cur[seg]; "
                        f"  }} "
                        f"  return typeof cur === 'string' ? cur : null; "
                        f"}}"
                    )
                    # Poll for the JS global — most plugins set it within
                    # a few hundred ms of DOM ready, but we give 8s headroom.
                    # This is much faster than waiting for full networkidle.
                    poll_deadline = time.monotonic() + 8.0
                    while time.monotonic() < poll_deadline:
                        try:
                            nonce = await page.evaluate(js_lookup)
                            if nonce:
                                logger.debug(
                                    f"[Stage1] {domain}: nonce from JS path "
                                    f"'{nonce_js_path}' = {nonce}"
                                )
                                break
                        except Exception as e:
                            logger.debug(f"[Stage1] {domain}: JS nonce lookup failed: {e}")
                        await asyncio.sleep(0.2)

                if not nonce and nonce_pattern:
                    referer_html = await page.content()
                    m = _re.search(nonce_pattern, referer_html)
                    if m:
                        nonce = m.group(1)
                        logger.debug(f"[Stage1] {domain}: nonce from HTML regex = {nonce}")

                if not nonce:
                    raise EmptyResponse(
                        f"nonce not found via js_path={nonce_js_path!r} or pattern={nonce_pattern!r}",
                        url=referer_url,
                    )

                # ── Step 3: POST the AJAX call from inside the page ──
                from urllib.parse import urlencode as _urlenc
                payload_dict: dict[str, Any] = {"action": action, "nonce": nonce}
                if extra_data:
                    payload_dict.update(extra_data)
                payload_str = _urlenc(payload_dict)

                hdr_dict = dict(headers) if headers else {}
                hdr_dict.setdefault("Content-Type",     "application/x-www-form-urlencoded; charset=UTF-8")
                hdr_dict.setdefault("X-Requested-With", "XMLHttpRequest")

                js = f"""
                    (async () => {{
                        const r = await fetch({json.dumps(ajax_url)}, {{
                            method: 'POST',
                            credentials: 'include',
                            headers: {json.dumps(hdr_dict)},
                            body: {json.dumps(payload_str)}
                        }});
                        const text = await r.text();
                        return {{ status: r.status, url: r.url, text }};
                    }})()
                """
                try:
                    result = await page.evaluate(js)
                except Exception as e:
                    raise FetcherTimeout(f"in-page fetch failed: {e}", url=ajax_url) from e

                status     = int(result.get("status") or 0)
                final_url  = result.get("url") or ajax_url
                body       = result.get("text") or ""

                # Save cookies for next run.
                try:
                    fresh = [c for c in await context.cookies() if c.get("name") not in ("__cf_bm",)]
                    if fresh:
                        await _save_cookies(domain, fresh)
                except Exception:
                    pass

                # ── Validate ─────────────────────────────────────
                if body.strip() == "-1":
                    raise HttpError(
                        status or 403,
                        f"WP nonce check failed (body=-1)", url=ajax_url,
                    )

                if status in (403, 429, 503):
                    raise CloudflareBlock(
                        f"WAF/CF block on AJAX (HTTP {status})", url=ajax_url,
                    )

                if status not in (200, 201):
                    raise HttpError(status, f"HTTP {status} for AJAX {ajax_url}", url=ajax_url)

                if not body:
                    raise EmptyResponse("empty AJAX body", url=ajax_url)

                elapsed_ms = int((time.monotonic() - started) * 1000)
                return FetchResult(
                    text=body,
                    status_code=status,
                    url=final_url,
                    headers={},
                    elapsed_ms=elapsed_ms,
                )

            finally:
                try:
                    await context.close()
                except Exception:
                    pass

    # ── Internal ──────────────────────────────────────────────────

    async def _fetch_via_browser(
        self,
        url:     str,
        *,
        method:  str,
        data:    Any                      = None,
        headers: Mapping[str, str] | None = None,
        timeout: int | None               = None,
    ) -> FetchResult:

        if not browser_pool.available:
            # Stage 1 effectively disabled (Playwright not installed).
            # Raise CaptchaDetected so funnel knows to try a higher stage
            # rather than retrying us.
            raise CaptchaDetected(
                "Stage 1 Playwright unavailable (not installed)", url=url,
            )

        domain        = _domain_of(url)
        timeout_ms    = (timeout or self.default_timeout) * 1000
        saved_cookies = await _load_cookies(domain)
        started       = time.monotonic()

        async with browser_pool.acquire() as browser:
            context = await browser.new_context(
                viewport=_VIEWPORT,
                user_agent=_USER_AGENT,
                locale="en-US",
                timezone_id="Europe/Athens",
                java_script_enabled=True,
                # Important: same headers regardless of `headers` kwarg —
                # we don't want to send extra Accept-Language etc that
                # could fingerprint us. Caller-supplied headers go on the
                # individual request below.
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )

            try:
                # Inject saved cookies BEFORE the page navigates.
                if saved_cookies:
                    try:
                        await context.add_cookies(saved_cookies)
                        logger.debug(f"[Stage1] {domain}: loaded {len(saved_cookies)} saved cookies")
                    except Exception as e:
                        # Stale/malformed cookies — drop and continue cold.
                        logger.debug(f"[Stage1] {domain}: failed to add saved cookies: {e}")

                page = await context.new_page()

                # Apply stealth patches BEFORE any navigation.
                if _STEALTH_AVAILABLE:
                    await Stealth().apply_stealth_async(page)

                # ── Navigate ─────────────────────────────────────────
                if method == "GET":
                    await self._do_get(page, url, headers, timeout_ms)
                    final_url = page.url
                    html      = await page.content()
                    status    = 200  # Playwright doesn't surface status easily; assume OK if we got here
                else:
                    # POST: do an in-page fetch() so we ride on the cf_clearance
                    # cookie that goto() just earned.
                    final_url, html, status = await self._do_post(
                        page, url, data, headers, timeout_ms,
                    )

                # ── Validate result ──────────────────────────────────
                if status >= 400:
                    raise HttpError(status, f"HTTP {status} for {method} {url}", url=url)

                if not html or len(html) < 200:
                    raise EmptyResponse(
                        f"body too short ({len(html) if html else 0} bytes)",
                        url=url,
                    )

                if _looks_like_cf_challenge(html):
                    raise CloudflareBlock("page still on CF challenge after wait", url=url)

                # ── Persist cookies for next run ────────────────────
                try:
                    fresh_cookies = await context.cookies()
                    # Filter to ones useful to us — CF clearance, session, csrf-like.
                    useful = [c for c in fresh_cookies if c.get("name") not in ("__cf_bm",)]
                    if useful:
                        await _save_cookies(domain, useful)
                except Exception as e:
                    logger.debug(f"[Stage1] {domain}: cookie save skipped: {e}")

                elapsed_ms = int((time.monotonic() - started) * 1000)
                return FetchResult(
                    text=html,
                    status_code=status,
                    url=final_url,
                    headers={},  # Playwright doesn't easily expose response headers from goto()
                    elapsed_ms=elapsed_ms,
                )

            finally:
                # Always close the context. Browser stays in pool.
                try:
                    await context.close()
                except Exception:
                    pass

    # ── GET path ─────────────────────────────────────────────────

    async def _do_get(self, page, url: str, headers, timeout_ms: int) -> None:
        """Navigate and wait for CF to resolve."""
        try:
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=timeout_ms,
                # extra_http_headers go via context, but we honour any
                # caller-supplied headers as a one-shot override.
                **({"referer": headers["Referer"]} if headers and "Referer" in headers else {}),
            )
        except asyncio.TimeoutError as e:
            raise FetcherTimeout(f"page.goto timeout: {e}", url=url) from e
        except Exception as e:
            # Map common navigation errors
            err = str(e).lower()
            if "timeout" in err:
                raise FetcherTimeout(str(e), url=url) from e
            raise

        # If the response shows CF challenge, sit and wait for it to clear.
        try:
            initial_html = await page.content()
        except Exception:
            initial_html = ""

        if _looks_like_cf_challenge(initial_html):
            logger.info(f"[Stage1] {url}: CF challenge detected, waiting…")
            await _wait_for_cf_to_resolve(page)

    # ── POST path (in-page fetch) ────────────────────────────────

    async def _do_post(
        self, page, url: str, data: Any, headers, timeout_ms: int,
    ) -> tuple[str, str, int]:
        """
        For POST we first navigate to the site root to earn cf_clearance
        (if not already cached), then fire fetch() from within the page —
        the cookie rides along automatically.
        """
        # 1. Visit root to ensure we have a clearance cookie. If saved
        # cookies were valid, this is fast (no challenge shown).
        parts = urlparse(url)
        root = f"{parts.scheme}://{parts.netloc}/"
        try:
            await page.goto(root, wait_until="domcontentloaded", timeout=timeout_ms)
        except asyncio.TimeoutError as e:
            raise FetcherTimeout(f"warmup goto timeout: {e}", url=url) from e

        try:
            warmup_html = await page.content()
        except Exception:
            warmup_html = ""
        if _looks_like_cf_challenge(warmup_html):
            await _wait_for_cf_to_resolve(page)

        # 2. Make the AJAX call FROM the page context. Body must be a
        # string (form-urlencoded already serialised by caller) or a
        # dict (we'll JSON.stringify).
        if isinstance(data, dict):
            body_js   = json.dumps(data)
            body_arg  = f"JSON.stringify({body_js})"
            content_type = "application/json"
        else:
            body_arg     = json.dumps(str(data) if data is not None else "")
            content_type = (
                headers.get("Content-Type") if headers else None
            ) or "application/x-www-form-urlencoded; charset=UTF-8"

        # Build extra-headers JS object literal
        hdr_dict = dict(headers) if headers else {}
        hdr_dict.setdefault("Content-Type", content_type)
        hdr_dict.setdefault("X-Requested-With", "XMLHttpRequest")
        headers_js = json.dumps(hdr_dict)

        js = f"""
            (async () => {{
                const r = await fetch({json.dumps(url)}, {{
                    method: 'POST',
                    credentials: 'include',
                    headers: {headers_js},
                    body: {body_arg}
                }});
                const text = await r.text();
                return {{ status: r.status, url: r.url, text }};
            }})()
        """
        try:
            result = await page.evaluate(js)
        except Exception as e:
            raise FetcherTimeout(f"in-page fetch failed: {e}", url=url) from e

        return (
            result.get("url") or url,
            result.get("text") or "",
            int(result.get("status") or 0),
        )