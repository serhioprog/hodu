"""
Base scraper class.

Every scraper subclasses BaseScraper and gets:
    self.client     — HTTP client (now backed by the FetchFunnel — see below)
    self.extractor  — DataExtractor with regex/NLP rules
    self.source_domain — set by the subclass

HISTORY (Sprint 1, 2026-05):
    self.client used to be a RequestEngine that wrapped curl_cffi directly.
    It's now a thin _FunnelClientAdapter that routes through the
    FetchFunnel orchestrator.

    Why not just import fetcher_funnel directly in scrapers?
    1. Keeps the scraper API stable (`self.client.get(url)`, no domain arg).
       Scrapers don't have to know about the funnel — they get domain-aware
       behaviour for free via the adapter binding `self.source_domain`.
    2. Makes the migration zero-touch for existing scraper code: no
       diffs in gl_real_estate.py, greek_exclusive_properties.py,
       real_estate_center_SJ.py.
    3. Future: makes it easy to add per-scraper hooks (e.g. cookie
       persistence, custom headers) without leaking funnel internals.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

from loguru import logger

from src.core.extractor                import DataExtractor
from src.scrapers.fetchers             import fetcher_funnel
from src.scrapers.fetchers.base_fetcher import FetchResult


class _FunnelClientAdapter:
    """
    Drop-in replacement for the legacy RequestEngine.

    Exposes the same `.get(url, ...)` and `.post(url, data=...)` methods
    that scrapers already call, but routes them through the FetchFunnel
    bound to the owning scraper's source_domain.

    The result object has `.text`, `.status_code`, `.url`, `.headers`,
    `.json()` — same shape as curl_cffi.Response, so legacy scrapers
    that do `response.text` / `response.json()` keep working.
    """

    def __init__(self, scraper: "BaseScraper") -> None:
        self._scraper = scraper

    @property
    def _domain(self) -> str:
        # Late binding: source_domain is set in subclass __init__ AFTER
        # super().__init__() returns. We resolve it on every call.
        d = self._scraper.source_domain
        if not d:
            # Defensive: should never happen if subclass is well-formed
            logger.warning("[FunnelAdapter] source_domain is empty!")
            return "unknown"
        return d

    async def get(
        self,
        url:     str,
        params:  Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> FetchResult:
        return await fetcher_funnel.get(
            self._domain, url, headers=headers, params=params,
        )

    async def post(
        self,
        url:     str,
        data:    Any                      = None,
        headers: Mapping[str, str] | None = None,
    ) -> FetchResult:
        return await fetcher_funnel.post(
            self._domain, url, data=data, headers=headers,
        )


class BaseScraper(ABC):
    def __init__(self) -> None:
        # NEW: self.client is a funnel-backed adapter. Same interface
        # as the legacy RequestEngine, so subclass code is unchanged.
        self.client    = _FunnelClientAdapter(self)
        self.extractor = DataExtractor()
        self.source_domain = ""

    @abstractmethod
    async def collect_urls(self, min_price: int = 400000):
        """List-page phase: collect URLs (and maybe price/size) of listings."""
        raise NotImplementedError

    @abstractmethod
    async def fetch_details(self, url: str):
        """Detail-page phase: fetch and parse one listing's content."""
        raise NotImplementedError
