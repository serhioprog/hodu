"""
BaseFetcher — abstract interface that every funnel stage implements.

Design:
  * Two methods: `get()` and `post()`. Skrapers need both
    (collect_urls often POSTs to AJAX, fetch_details GETs a page).
  * Returns FetchResult, NOT raw httpx/requests.Response. This insulates
    callers from the underlying library and forces stages to normalise
    output (text, status, headers).
  * Raises specific FetcherError subclasses on failure — caller decides
    whether to retry, escalate, or give up based on the exception type.
  * Each fetcher exposes `stage_number` and `name` for telemetry/logging.

Why a dataclass, not just returning text:
  * status_code is needed for AJAX responses (some sites return 200 with
    JSON containing `success: false`)
  * headers carry Set-Cookie that we may want to persist
  * url tracks redirects (final URL after 301/302 chain)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(slots=True)
class FetchResult:
    """Normalised response from any fetcher stage."""
    text:        str
    status_code: int
    url:         str                              # final URL after redirects
    headers:     Mapping[str, str] = field(default_factory=dict)
    elapsed_ms:  int               = 0            # for telemetry

    def json(self) -> Any:
        """Convenience for AJAX endpoints. Stages don't pre-parse JSON
        because the caller might not expect it."""
        import json as _json
        return _json.loads(self.text)


class BaseFetcher(ABC):
    """
    Abstract base class for a fetch stage.

    Subclasses implement _do_get() and _do_post() with the actual fetching
    logic. The base class enforces consistent error handling and timing.

    Stage numbering (lower = preferred / cheaper):
      0 — curl_cffi (this is the baseline)
      1 — Playwright + stealth (free, JS rendering)
      2 — flaresolverr (free, specialised CF bypass)
      3 — Browserless free tier (free, limited quota)
      4 — Paid commercial API (ScrapingBee / Bright Data)
    """

    # Stage subclasses MUST override these two class attributes.
    stage_number: int = -1
    name:         str = "unknown"

    # Default timeout per request (subclasses may override).
    default_timeout: int = 30

    @abstractmethod
    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params:  Mapping[str, Any] | None = None,
        timeout: int | None              = None,
    ) -> FetchResult:
        """HTTP GET. Raises FetcherError subclass on failure."""
        raise NotImplementedError

    @abstractmethod
    async def post(
        self,
        url: str,
        *,
        data:    Any                     = None,
        headers: Mapping[str, str] | None = None,
        timeout: int | None              = None,
    ) -> FetchResult:
        """HTTP POST. Raises FetcherError subclass on failure."""
        raise NotImplementedError

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
        """Specialised method for WordPress AJAX endpoints.

        Many WP plugins guard their admin-ajax.php handlers with a `nonce`
        token that's bound to the visitor's session (via PHP session or
        cookies). If we extract the nonce in one fetch context (e.g.
        curl_cffi) and use it in another (e.g. Playwright), the WP server
        rejects the call with `-1` + 403 because the cookies don't match.

        This method does the whole flow IN ONE SESSION:
          1. Visit `referer_url` to earn whatever cookies WP wants.
          2. Extract the nonce. Two ways, pick one:
             - `nonce_pattern` (regex, with one capture group): match
               against page HTML. Fast but brittle — many WP plugins
               put a "decoy" nonce in HTML and the real one in JS.
             - `nonce_js_path` (e.g. "halkiGrid3Config.filterNonce"):
               read directly from window globals after JS executes.
               This is what jQuery handlers actually use.
          3. POST to `ajax_url` with `action=...&nonce=...&{extra_data}`.

        Default impl fails — stages without a built-in browser cannot
        satisfy the same-session guarantee. Stage 1 (Playwright) overrides
        this with a real implementation.

        Stages that DON'T support this method should raise CaptchaDetected
        (signals "you need a higher stage"), not HttpError (which won't
        escalate). Subclasses that opt out can leave the default.
        """
        from src.scrapers.fetchers.exceptions import CaptchaDetected
        raise CaptchaDetected(
            f"{self.name} cannot guarantee same-session WP AJAX — escalate",
            url=ajax_url,
        )

    async def close(self) -> None:
        """Release resources. Default no-op for stateless stages.
        Browser-based stages override this to close browser pools."""
        return None

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} stage={self.stage_number} name={self.name!r}>"
