"""
Fetch Funnel — multi-stage HTTP fetching with cascading fallback.

Public API:
    from src.scrapers.fetchers import fetcher_funnel

    html = await fetcher_funnel.get(domain, url)
    html = await fetcher_funnel.post(domain, url, data=...)

Architecture:
    Each "stage" implements BaseFetcher and tries to fetch a URL using
    a different technique (curl_cffi, Playwright, flaresolverr, paid APIs).

    The Funnel orchestrator picks a starting stage per-domain (based on
    learned history in `scraper_routing` table) and walks UP the stages
    until one succeeds, then records the attempt for future routing
    decisions.

    Skraper code never knows which stage served their request — they just
    call funnel.get(domain, url). This means swapping in new fetcher
    technology (e.g. adding Playwright stage 1) requires zero changes to
    the existing scraper logic.
"""
from src.scrapers.fetchers.funnel       import fetcher_funnel
from src.scrapers.fetchers.base_fetcher import BaseFetcher, FetchResult
from src.scrapers.fetchers.exceptions   import (
    FetcherError, CloudflareBlock, CaptchaDetected, FetcherTimeout,
    AllStagesFailed,
)

__all__ = [
    "fetcher_funnel",
    "BaseFetcher", "FetchResult",
    "FetcherError", "CloudflareBlock", "CaptchaDetected",
    "FetcherTimeout", "AllStagesFailed",
]
