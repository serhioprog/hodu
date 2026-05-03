"""
Warm browser pool for Stage 1 (Playwright) fetcher.

Why a pool:
    Cold-starting Chromium takes ~3 seconds (process spawn + JS engine
    init + extension load). With 250+ realestatecenter.gr URLs to fetch
    per sync, that's 12+ minutes of pure overhead.

    A pool of 3-5 long-lived Chromium processes amortises that cost
    across the whole run. After the first request, subsequent fetches
    take ~0.5s of overhead instead of 3s.

Why per-context isolation:
    Each acquired browser comes with its OWN BrowserContext (think
    "incognito profile"). Contexts hold cookies, localStorage, and
    page-level state. Sharing a context across requests would let one
    site's auth bleed into another's, which we very much don't want.

    BrowserContext is cheap (sub-100ms to create), so we spin up a fresh
    one per fetch and discard it. The expensive Browser process stays
    alive in the pool.

Lifecycle:
    Pool                   →  on first acquire(): launches first browser
                              on subsequent acquires: reuses idle browser
                              on release(): returns to idle queue
                              on close_all(): tears down everything
                              (called from FastAPI shutdown hook)

Concurrency:
    asyncio.Semaphore limits concurrent in-flight requests (so we don't
    spawn unlimited browsers if many tasks ask at once). Idle browsers
    above the high-water-mark are torn down to bound memory.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from loguru import logger

# Playwright imports — but DEFERRED to first use. We don't want this
# module to crash the whole app if playwright isn't installed (e.g. when
# someone deploys without the right Dockerfile).
try:
    from playwright.async_api import (
        async_playwright, Browser, BrowserContext, Playwright,
    )
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:                          # pragma: no cover
    _PLAYWRIGHT_AVAILABLE = False
    logger.warning(
        "[BrowserPool] playwright not installed — Stage 1 will be disabled. "
        "Install it via 'pip install playwright' and 'playwright install chromium'."
    )


# ─── Config ──────────────────────────────────────────────────────────

POOL_MIN_SIZE: int = 1
"""How many browsers we keep around even when idle. Keeping at least 1
warm shaves ~3s off the first request after a quiet period."""

POOL_MAX_SIZE: int = 3
"""Hard cap on concurrent browser processes. Each Chromium is ~150-300MB
RAM. With ~3 in pool, we use ~750MB — comfortable on a multi-GB host."""

# Chromium launch flags. These mostly mirror what `playwright-stealth`
# expects, plus a few that explicitly help avoid bot-detection
# heuristics on Cloudflare.
_LAUNCH_ARGS: list[str] = [
    "--disable-blink-features=AutomationControlled",  # main giveaway flag
    "--disable-dev-shm-usage",                        # avoids /dev/shm OOM in Docker
    "--no-sandbox",                                   # required when running as non-root in Docker
    "--disable-gpu",                                  # headless never needs GPU
    "--disable-features=IsolateOrigins,site-per-process",
    "--lang=en-US,en",
]


# ─── Pool ────────────────────────────────────────────────────────────

class PlaywrightBrowserPool:
    """
    Singleton-style pool that hands out Browser objects.

    Usage:
        async with pool.acquire() as browser:
            ctx = await browser.new_context(**kwargs)
            page = await ctx.new_page()
            await page.goto("https://...")
            ...
            await ctx.close()
    """

    def __init__(self) -> None:
        self._playwright: Optional["Playwright"] = None
        self._idle:       list["Browser"]        = []
        self._semaphore:  asyncio.Semaphore      = asyncio.Semaphore(POOL_MAX_SIZE)
        self._lock:       asyncio.Lock           = asyncio.Lock()
        self._closed:     bool                   = False

    @property
    def available(self) -> bool:
        """Returns False if Playwright failed to import — funnel uses this
        to gracefully skip Stage 1."""
        return _PLAYWRIGHT_AVAILABLE and not self._closed

    # ── Acquire / release as an async context manager ─────────────

    def acquire(self) -> "_PoolGuard":
        """
        Returns an async context manager that yields a Browser.
        The browser is automatically returned to the pool on exit.
        """
        return _PoolGuard(self)

    async def _take(self) -> "Browser":
        """Internal: pop an idle browser or spawn a new one."""
        if self._closed:
            raise RuntimeError("BrowserPool is closed")

        async with self._lock:
            # Reuse an idle browser if any
            while self._idle:
                browser = self._idle.pop()
                if browser.is_connected():
                    return browser
                # Stale (process died) — drop and try next
                logger.debug("[BrowserPool] discarded dead browser, will spawn fresh")

            # No idle browsers — spawn a new one. Initialise Playwright on
            # first call (lazy, so import-time failures don't break the app).
            if self._playwright is None:
                self._playwright = await async_playwright().start()
                logger.info("[BrowserPool] Playwright runtime started")

            browser = await self._playwright.chromium.launch(
                headless=True,
                args=_LAUNCH_ARGS,
            )
            logger.info(
                f"[BrowserPool] launched browser "
                f"(idle pool: {len(self._idle)}, max: {POOL_MAX_SIZE})"
            )
            return browser

    async def _release(self, browser: "Browser") -> None:
        """Internal: return a browser to the pool, or close it if over capacity."""
        if self._closed or not browser.is_connected():
            try:
                await browser.close()
            except Exception:
                pass
            return

        async with self._lock:
            if len(self._idle) < POOL_MAX_SIZE:
                self._idle.append(browser)
            else:
                # Pool full — close this one to bound memory.
                try:
                    await browser.close()
                except Exception as e:
                    logger.warning(f"[BrowserPool] error closing browser: {e}")

    # ── Lifecycle ────────────────────────────────────────────────

    async def close_all(self) -> None:
        """Tear everything down. Called from FastAPI shutdown hook."""
        self._closed = True

        async with self._lock:
            for browser in self._idle:
                try:
                    await browser.close()
                except Exception:
                    pass
            self._idle.clear()

            if self._playwright is not None:
                try:
                    await self._playwright.stop()
                except Exception as e:
                    logger.warning(f"[BrowserPool] error stopping playwright: {e}")
                self._playwright = None

        logger.info("[BrowserPool] all browsers closed")


class _PoolGuard:
    """Async context manager wrapper around acquire/release.

    Implemented separately rather than as a method on the pool so that the
    semaphore is held only during the actual usage (not during the await
    on _take's lock).
    """

    def __init__(self, pool: PlaywrightBrowserPool) -> None:
        self._pool   = pool
        self._browser: Optional["Browser"] = None

    async def __aenter__(self) -> "Browser":
        await self._pool._semaphore.acquire()
        try:
            self._browser = await self._pool._take()
        except BaseException:
            self._pool._semaphore.release()
            raise
        return self._browser

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if self._browser is not None:
                await self._pool._release(self._browser)
        finally:
            self._pool._semaphore.release()


# Module-level singleton — import this:
#     from src.scrapers.fetchers.browser_pool import browser_pool
browser_pool = PlaywrightBrowserPool()
