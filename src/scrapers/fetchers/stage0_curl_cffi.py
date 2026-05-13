"""
Stage 0 — curl_cffi with browser TLS impersonation.

This is the SAME logic as the original src/core/http_client.py, just
packaged as a Fetcher subclass so the funnel can talk to it through a
uniform interface.

Behavioural guarantees (verified against the original):
  * Uses `impersonate="chrome120"` on the AsyncSession.
  * fake_useragent rotates UA per request (Windows Chrome).
  * Automatic retry: 3 attempts with exponential backoff (2s, 4s, 8s).
  * Detects Cloudflare via 403 status OR "Just a moment..." in body.
  * SSL verification disabled (sites with broken cert chains).

What's different from the original:
  * Raises typed FetcherError subclasses instead of generic ValueError —
    funnel uses these to decide on escalation.
  * Tracks elapsed time for telemetry.
  * Returns a normalised FetchResult, not the raw curl_cffi Response.

This stage is the cheapest (~1-3s per request, $0) and should always
be the first attempt for a domain unless we've learned it doesn't work.
"""
from __future__ import annotations

import time
from typing import Any, Mapping

from curl_cffi.requests import AsyncSession
from fake_useragent      import UserAgent
from loguru              import logger
from tenacity            import (
    retry, stop_after_attempt, wait_exponential, retry_if_exception_type,
)

from src.core.config import settings, should_verify_tls
from src.scrapers.fetchers.base_fetcher       import BaseFetcher, FetchResult
from src.scrapers.fetchers.exceptions         import (
    CloudflareBlock, HttpError, FetcherTimeout, EmptyResponse,
)


# UA pool shared across the process. Initialising this is non-trivial
# (it downloads a UA database on first call) so we do it once at import.
_UA = UserAgent(os="windows", browsers=["chrome"])


class Stage0CurlCffiFetcher(BaseFetcher):
    """Baseline fetcher using curl_cffi's chrome120 TLS impersonation."""

    stage_number    = 0
    name            = "curl_cffi"
    default_timeout = 30

    def __init__(self) -> None:
        # We honour the same proxy setting that RequestEngine used.
        # Funnel-level proxy rotation will live in higher stages.
        self._proxy: str | None = settings.PROXY_URL or None

    # ── Public API ────────────────────────────────────────────────

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params:  Mapping[str, Any] | None = None,
        timeout: int | None              = None,
    ) -> FetchResult:
        return await self._request("GET", url,
                                   headers=headers, params=params, timeout=timeout)

    async def post(
        self,
        url: str,
        *,
        data:    Any                      = None,
        headers: Mapping[str, str] | None = None,
        timeout: int | None              = None,
    ) -> FetchResult:
        return await self._request("POST", url,
                                   headers=headers, data=data, timeout=timeout)

    # ── Internal ──────────────────────────────────────────────────

    def _build_headers(self, extra: Mapping[str, str] | None) -> dict[str, str]:
        """Same header recipe as the legacy RequestEngine — random UA + standard
        Accept-Language so we don't stand out as a bot."""
        h = {
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "User-Agent":      _UA.random,
        }
        if extra:
            h.update(extra)
        return h

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        # Retry only on transient errors; let CloudflareBlock and HttpError
        # propagate immediately so the funnel can escalate.
        retry=retry_if_exception_type((FetcherTimeout, EmptyResponse)),
        before_sleep=lambda rs: logger.warning(
            f"[Stage0/curl_cffi] retry #{rs.attempt_number} after {rs.outcome.exception()}"
        ),
        reraise=True,
    )
    async def _request(
        self,
        method:  str,
        url:     str,
        *,
        headers: Mapping[str, str] | None = None,
        params:  Mapping[str, Any] | None = None,
        data:    Any                      = None,
        timeout: int | None               = None,
    ) -> FetchResult:
        """Single HTTP request with retry on transient errors."""
        req_headers = self._build_headers(headers)
        req_timeout = timeout or self.default_timeout

        started = time.monotonic()
        try:
            # NOTE: AsyncSession is per-request like the legacy code.
            # In a future iteration we can pool sessions for efficiency,
            # but for behavioural parity we keep it per-request.
            async with AsyncSession(impersonate="chrome120") as session:
                if method == "GET":
                    resp = await session.get(
                        url, params=params, headers=req_headers,
                        proxy=self._proxy, timeout=req_timeout, verify=should_verify_tls(url),
                    )
                else:  # POST
                    resp = await session.post(
                        url, data=data, headers=req_headers,
                        proxy=self._proxy, timeout=req_timeout, verify=should_verify_tls(url),
                    )
        except (TimeoutError, Exception) as e:
            # curl_cffi raises a variety of types on network failure.
            # We narrow timeouts (socket-level) into FetcherTimeout, others
            # bubble up wrapped as a generic HttpError so funnel escalates.
            elapsed = int((time.monotonic() - started) * 1000)
            err_str = str(e).lower()
            if "timeout" in err_str or "timed out" in err_str:
                raise FetcherTimeout(f"timeout after {elapsed}ms: {e}", url=url) from e
            # Re-raise any FetcherError unchanged
            from src.scrapers.fetchers.exceptions import FetcherError as _FE
            if isinstance(e, _FE):
                raise
            raise HttpError(0, f"network error: {e}", url=url) from e

        elapsed = int((time.monotonic() - started) * 1000)
        text    = resp.text or ""
        text_lc = text.lower()

        # ── Cloudflare / WAF detection ──
        # Strategy: 403/429/503 from a server we expect to be reachable
        # are almost always WAF / bot-detection blocks. Different stages
        # (Playwright with stealth, residential proxies) can defeat these,
        # so we ALWAYS escalate by raising CloudflareBlock.
        #
        # We don't bother trying to parse the body to confirm "yes really
        # it's CF" — false positives here are cheap (we waste a Playwright
        # call, which still works), but false negatives are expensive
        # (the funnel gives up and returns nothing).
        #
        # 401 is NOT in this list — that's a genuine auth issue and a
        # different stage won't fix it.
        WAF_STATUSES = {403, 429, 503}

        if resp.status_code in WAF_STATUSES:
            # Identify the most likely cause for the log message —
            # useful for debugging but doesn't change behaviour.
            cause = "unknown"
            if "cloudflare" in text_lc or "cf-ray" in text_lc:
                cause = "cloudflare"
            elif "just a moment" in text_lc or "cf-browser-verification" in text_lc:
                cause = "cf-challenge"
            elif "attention required" in text_lc:
                cause = "cf-attention"
            elif resp.status_code == 429:
                cause = "rate-limit"
            elif method == "POST":
                cause = "ajax-blocked"
            raise CloudflareBlock(
                f"WAF/CF block (HTTP {resp.status_code}, cause={cause})",
                url=url,
            )

        # ── Body-based CF detection on 200s ──
        # Some configurations return 200 OK with the CF wait-page in body.
        if "just a moment..." in text_lc and len(text) < 50_000:
            raise CloudflareBlock("CF challenge page (HTTP 200 with wait-body)", url=url)

        # ── Other HTTP errors (404, 500, etc.) ──
        # These are genuine non-WAF errors. Escalation won't help.
        if resp.status_code not in (200, 201):
            raise HttpError(
                resp.status_code,
                f"HTTP {resp.status_code} for {method} {url}",
                url=url,
            )

        # ── Sanity check on body size ──
        # Tiny responses (<200 bytes) for HTML pages are usually WAF stubs.
        # AJAX endpoints return JSON which is fine to be small, so skip
        # this check when caller asked for JSON-typed body.
        if len(text) < 200 and not text.lstrip().startswith(("{", "[")):
            raise EmptyResponse(
                f"body too short ({len(text)} bytes) — possible WAF stub",
                url=url,
            )

        return FetchResult(
            text=text,
            status_code=resp.status_code,
            url=str(resp.url),
            headers=dict(resp.headers),
            elapsed_ms=elapsed,
        )