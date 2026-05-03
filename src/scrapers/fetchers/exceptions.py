"""
Typed exceptions for the fetch funnel.

Why typed exceptions instead of generic ones:
  * The Funnel orchestrator uses exception type to decide what to do next.
    CloudflareBlock → escalate to next stage immediately.
    FetcherTimeout  → maybe retry same stage, or skip if circuit is open.
    CaptchaDetected → skip stages without captcha-solving capability.
  * Each stage can raise a specific subclass; failure classification then
    happens automatically when it lands in the funnel's try/except.

All exceptions inherit from FetcherError so callers can catch the whole
family with a single `except FetcherError`.
"""
from __future__ import annotations


class FetcherError(Exception):
    """Base for all fetcher-layer failures. Always carries an error_code
    suitable for storing in the fetch_attempts.error_code column."""
    error_code: str = "unknown"

    def __init__(self, message: str = "", *, url: str | None = None) -> None:
        super().__init__(message)
        self.url = url


class CloudflareBlock(FetcherError):
    """HTTP 403/503 with Cloudflare signature in body, or 'Just a moment...'"""
    error_code = "cloudflare_block"


class CaptchaDetected(FetcherError):
    """Page rendered but contains hCaptcha / Turnstile / reCAPTCHA element.
    Stages without captcha-solving capability MUST raise this so funnel
    knows to skip them and jump to a higher stage."""
    error_code = "captcha_detected"


class FetcherTimeout(FetcherError):
    """Request exceeded the stage-specific timeout. Could be slow upstream
    or our circuit overloaded — funnel might retry on the next stage but
    will not penalise the current stage's reliability score."""
    error_code = "timeout"


class HttpError(FetcherError):
    """Generic non-2xx response that's not a CF block (e.g. 404, 500).
    Funnel will NOT escalate to next stage for these — wrong URL is wrong
    URL regardless of which fetcher tries."""
    error_code = "http_error"

    def __init__(self, status_code: int, message: str = "", *, url: str | None = None) -> None:
        super().__init__(message or f"HTTP {status_code}", url=url)
        self.status_code = status_code


class EmptyResponse(FetcherError):
    """Server returned 200 but body is empty / too short to be real content.
    Often a sign of WAF returning a stub page."""
    error_code = "empty_response"


class AllStagesFailed(FetcherError):
    """Raised by the Funnel when every enabled stage has been tried and
    none succeeded. Carries the list of underlying errors for diagnostics."""
    error_code = "all_stages_failed"

    def __init__(self, url: str, errors: list[tuple[int, FetcherError]]) -> None:
        # errors: list of (stage_number, exception_that_was_raised)
        msg = f"All {len(errors)} stages failed for {url}"
        super().__init__(msg, url=url)
        self.errors = errors
