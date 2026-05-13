"""
Fetch Funnel orchestrator.

Public API:
    fetcher_funnel.get(domain, url, headers=..., params=...)
    fetcher_funnel.post(domain, url, data=..., headers=...)

Behaviour:
    1. Reads `preferred_stage` for `domain` from scraper_routing table.
       Defaults to stage 0 for new domains.
    2. Tries that stage. If it raises a typed FetcherError, walks UP the
       chain (stage+1, stage+2, ...) until success or AllStagesFailed.
    3. Records every attempt (success or failure) into fetch_attempts.
    4. After 3 consecutive successes on a stage HIGHER than preferred,
       promotes the domain to that stage. After 5 consecutive failures
       on the preferred stage, eagerly tries the next stage.

Sprint 1 scope:
    Only stage 0 (curl_cffi) is registered. The chain is functionally a
    no-op wrapper for now. The full orchestration logic is in place so
    Sprint 2 can register stage 1 (Playwright) without code changes here.

Resilience:
    Telemetry writes are best-effort — if the DB is unreachable, the
    funnel still returns the fetched HTML to the caller. The scrape job
    is more important than the metrics about it.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing      import Any, Mapping

from loguru        import logger
from sqlalchemy    import text as sql_text

from src.database.db                   import async_session_maker
from src.scrapers.fetchers.base_fetcher import BaseFetcher, FetchResult
from src.scrapers.fetchers.exceptions   import (
    AllStagesFailed, CloudflareBlock, EmptyResponse, FetcherError,
    FetcherTimeout, HttpError,
)
from src.scrapers.fetchers.stage0_curl_cffi import Stage0CurlCffiFetcher

# Stage 1 (Playwright) is imported best-effort. If playwright isn't
# installed yet, we just don't register stage 1 — funnel will skip it
# and other stages still work. This makes the rollout safe: deploying
# the new funnel BEFORE the new Dockerfile won't crash the container.
try:
    from src.scrapers.fetchers.stage1_playwright import Stage1PlaywrightFetcher
    _STAGE1_AVAILABLE = True
except ImportError as _e:
    _STAGE1_AVAILABLE = False
    Stage1PlaywrightFetcher = None  # type: ignore


# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────

# Promotion thresholds — see _maybe_promote / _maybe_demote.
PROMOTE_AFTER_SUCCESSES = 3
"""After this many consecutive successes on a stage HIGHER than the
domain's preferred_stage, we save the new preferred_stage to DB."""

# Errors that should NOT escalate to the next stage. If the URL is
# genuinely 404, no fetcher in the world will fix that.
_NON_ESCALATING_ERRORS: tuple[type[FetcherError], ...] = (HttpError,)
"""HttpError covers 404/500/etc — different stage won't help. Funnel
gives up immediately and raises AllStagesFailed."""

# Errors that DO escalate — we walk up the chain on any of these.
# (Anything not in _NON_ESCALATING_ERRORS escalates by default.)


# ─────────────────────────────────────────────────────────────────────
# Internal state per domain
# ─────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class _DomainState:
    """In-memory cache of routing state. Synced from DB lazily."""
    preferred_stage:           int = 0
    consecutive_success:       int = 0
    enabled_stages:            tuple[int, ...] = (0, 1, 2, 3, 4)


# ─────────────────────────────────────────────────────────────────────
# Funnel
# ─────────────────────────────────────────────────────────────────────

class FetchFunnel:
    """Singleton-style orchestrator. Held as a module-level instance below."""

    def __init__(self) -> None:
        # Stage registry — populated at Sprint boundaries.
        # Sprint 1: only stage 0.
        # Sprint 2: + stage 1 (Playwright).  ← NOW
        # Sprint 4: + stage 2 (flaresolverr).
        # Sprint 5: + stage 3, 4 (Browserless / ScrapingBee).
        self._stages: dict[int, BaseFetcher] = {
            0: Stage0CurlCffiFetcher(),
        }
        if _STAGE1_AVAILABLE:
            self._stages[1] = Stage1PlaywrightFetcher()
            logger.info("[Funnel] Stage 1 (Playwright) registered")
        else:
            logger.warning(
                "[Funnel] Stage 1 (Playwright) NOT available — install playwright deps"
            )

        # In-memory cache so we don't hit DB on every fetch.
        # Reset on process restart — DB is the source of truth.
        self._state_cache: dict[str, _DomainState] = {}
        self._cache_lock = asyncio.Lock()

    # ── Public entrypoints ───────────────────────────────────────

    async def get(
        self,
        domain:  str,
        url:     str,
        *,
        headers: Mapping[str, str] | None = None,
        params:  Mapping[str, Any] | None = None,
        timeout: int | None              = None,
    ) -> FetchResult:
        return await self._fetch(
            "GET", domain, url, headers=headers, params=params, timeout=timeout,
        )

    async def post(
        self,
        domain:  str,
        url:     str,
        *,
        data:    Any                      = None,
        headers: Mapping[str, str] | None = None,
        timeout: int | None               = None,
    ) -> FetchResult:
        return await self._fetch(
            "POST", domain, url, data=data, headers=headers, timeout=timeout,
        )

    async def wp_ajax(
        self,
        domain:        str,
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
        """Specialised entry point for WordPress AJAX endpoints.

        Use this instead of separate get(referer)+post(ajax) when the
        site's AJAX handler verifies a nonce that's bound to the visitor's
        session. Stage 0 (curl_cffi) cannot satisfy this — it makes
        independent connections per call. Stage 1 (Playwright) handles it
        by running both calls inside one BrowserContext.

        See BaseFetcher.wp_ajax for the full contract and nonce options.
        """
        return await self._fetch(
            "WP_AJAX", domain, ajax_url,
            referer_url=referer_url,
            action=action,
            nonce_pattern=nonce_pattern,
            nonce_js_path=nonce_js_path,
            extra_data=extra_data,
            headers=headers,
            timeout=timeout,
        )

    async def close_all(self) -> None:
        """Close all underlying fetchers (e.g. browser pools).
        Should be called on FastAPI shutdown."""
        for stage in self._stages.values():
            try:
                await stage.close()
            except Exception as e:
                logger.warning(f"[Funnel] error closing {stage.name}: {e}")

    # ── Core fetch loop ──────────────────────────────────────────

    async def _fetch(
        self,
        method:  str,
        domain:  str,
        url:     str,
        **kwargs: Any,
    ) -> FetchResult:
        state    = await self._get_state(domain)

        # WP_AJAX has special routing semantics:
        #   - Stage 0 (curl_cffi) cannot guarantee same-session nonce
        #     binding, so it always returns CaptchaDetected. Going through
        #     it just adds latency — skip directly to stage 1.
        #   - Successes here should NOT promote the domain's preferred_stage,
        #     because GET/POST traffic to the same domain may still be fine
        #     on stage 0. Promoting based on AJAX success would force ALL
        #     traffic through Playwright unnecessarily.
        if method == "WP_AJAX":
            plan = [s for s in (1, 2, 3, 4) if s in state.enabled_stages]
        else:
            plan = self._build_chain(state)

        errors:  list[tuple[int, FetcherError]] = []

        for stage_num in plan:
            stage = self._stages.get(stage_num)
            if stage is None:
                # Stage configured but not yet registered (e.g. Playwright in
                # Sprint 1). Skip silently — sentinel until that Sprint lands.
                continue

            try:
                if method == "GET":
                    result = await stage.get(url, **kwargs)
                elif method == "POST":
                    result = await stage.post(url, **kwargs)
                elif method == "WP_AJAX":
                    result = await stage.wp_ajax(url, **kwargs)
                else:
                    raise ValueError(f"unknown funnel method: {method}")
            except FetcherError as e:
                errors.append((stage_num, e))
                # Telemetry — fire and forget.
                asyncio.create_task(self._record_attempt(
                    domain, url, stage_num, success=False,
                    error_code=e.error_code, error_text=str(e), elapsed_ms=0,
                ))

                # Don't escalate on hard HTTP errors (404 etc.)
                if isinstance(e, _NON_ESCALATING_ERRORS):
                    logger.debug(f"[Funnel] {domain}: non-escalating {e.error_code}, giving up")
                    break

                # Otherwise walk up the chain.
                logger.info(
                    f"[Funnel] {domain}: stage {stage_num} ({stage.name}) "
                    f"failed with {e.error_code}, escalating"
                )
                continue

            # ── Success path ──
            asyncio.create_task(self._record_attempt(
                domain, url, stage_num, success=True,
                error_code=None, error_text=None, elapsed_ms=result.elapsed_ms,
            ))
            # Only promote on regular GET/POST success. WP_AJAX successes
            # are expected to be on stage 1+ and don't reflect the domain's
            # general crawlability.
            if method != "WP_AJAX":
                await self._maybe_promote(domain, state, stage_num)
            return result

        # All stages exhausted. Raise composite error with full diagnostic.
        raise AllStagesFailed(url, errors)

    # ── Routing logic ────────────────────────────────────────────

    def _build_chain(self, state: _DomainState) -> list[int]:
        """
        Construct the ordered list of stage numbers to try for this domain.

        Starts at preferred_stage (learned from history), then ascends
        through enabled_stages in numerical order. Stages we don't have
        registered yet are still in the list — _fetch silently skips them.
        """
        chain = [
            s for s in sorted(state.enabled_stages)
            if s >= state.preferred_stage
        ]
        return chain

    async def _maybe_promote(
        self, domain: str, state: _DomainState, success_stage: int,
    ) -> None:
        """
        If we just succeeded on a stage HIGHER than the preferred one for
        this domain, count toward promotion. After PROMOTE_AFTER_SUCCESSES
        in a row, persist new preferred_stage to DB.

        Why threshold-based: a single success on a higher stage might be
        a fluke. Three in a row means the lower stage is genuinely broken.
        """
        if success_stage <= state.preferred_stage:
            # Reset the streak — we're back on baseline.
            state.consecutive_success = 0
            return

        state.consecutive_success += 1
        if state.consecutive_success >= PROMOTE_AFTER_SUCCESSES:
            old_stage = state.preferred_stage
            state.preferred_stage     = success_stage
            state.consecutive_success = 0
            logger.warning(
                f"[Funnel] {domain}: promoted from stage {old_stage} → "
                f"{success_stage} after {PROMOTE_AFTER_SUCCESSES} successes"
            )
            await self._persist_preferred_stage(domain, success_stage)
            await self._fire_routing_alert(
                domain=domain, old_stage=old_stage, new_stage=success_stage,
                direction="promoted", reason="repeated success on higher stage",
            )

    async def probe_demote(self, domain: str) -> dict:
        """
        Test whether a domain promoted to higher stages can be demoted back
        down to stage 0. Idea: CF protections may have been lifted; we
        shouldn't pay the Playwright tax forever just because we got blocked
        once last week.

        Strategy:
          1. If domain is already on stage 0, nothing to do.
          2. Otherwise, fire a real GET via stage 0 (homepage). If it
             succeeds, the lower stage works → demote.
          3. If it fails, leave preferred_stage as-is.

        Returns a small dict suitable for logging or Telegram.

        IMPORTANT: This issues a REAL request to the domain. The probe job
        runs at most weekly, so this is a few requests per week — basically
        invisible to the target site.
        """
        state = await self._get_state(domain)

        if state.preferred_stage == 0:
            return {"domain": domain, "outcome": "noop", "reason": "already on stage 0"}

        stage_zero = self._stages.get(0)
        if stage_zero is None:
            return {"domain": domain, "outcome": "skipped", "reason": "stage 0 not registered"}

        # Probe URL: domain root. Lower side-effects than a real listing page.
        probe_url = f"https://{domain}/"
        try:
            await stage_zero.get(probe_url, timeout=15)
        except FetcherError as e:
            await self._record_probe_attempt(domain, success=False, error_code=e.error_code)
            logger.info(f"[Funnel/Probe] {domain}: stage 0 still failing ({e.error_code}) — keeping stage {state.preferred_stage}")
            return {
                "domain": domain, "outcome": "kept", "stage": state.preferred_stage,
                "reason": f"stage 0 fail: {e.error_code}",
            }

        # Stage 0 worked! Demote.
        old_stage = state.preferred_stage
        state.preferred_stage = 0
        state.consecutive_success = 0
        await self._record_probe_attempt(domain, success=True, error_code=None)
        await self._persist_preferred_stage(domain, 0)
        await self._fire_routing_alert(
            domain=domain, old_stage=old_stage, new_stage=0,
            direction="demoted", reason="weekly probe found stage 0 works",
        )
        logger.warning(f"[Funnel/Probe] {domain}: DEMOTED from stage {old_stage} → 0")
        return {
            "domain": domain, "outcome": "demoted",
            "old_stage": old_stage, "new_stage": 0,
        }

    # ── Alert hook ───────────────────────────────────────────────

    async def _fire_routing_alert(
        self, *, domain: str, old_stage: int, new_stage: int,
        direction: str, reason: str,
    ) -> None:
        """Send a Telegram alert about a routing change.

        Best-effort: imported lazily, errors swallowed. We don't want
        failed Telegram delivery to break the funnel logic, and we don't
        want a hard import dependency on the notifier (keeps funnel
        testable in isolation).
        """
        try:
            from src.services.telegram_notifier import telegram_notifier

            arrow = "⬆️" if direction == "promoted" else "⬇️"
            stage_names = {0: "curl_cffi", 1: "Playwright", 2: "flaresolverr",
                           3: "Browserless", 4: "ScrapingBee"}
            old_name = stage_names.get(old_stage, f"stage {old_stage}")
            new_name = stage_names.get(new_stage, f"stage {new_stage}")

            text = (
                f"{arrow} <b>FUNNEL ROUTING CHANGE</b>\n"
                f"\n"
                f"🌐 <b>{domain}</b>\n"
                f"   {old_name} (stage {old_stage}) → "
                f"{new_name} (stage {new_stage})\n"
                f"\n"
                f"<i>{reason}</i>"
            )
            await telegram_notifier.send(text)
        except Exception as e:
            logger.debug(f"[Funnel] routing alert send failed (non-fatal): {e}")

    # ── DB persistence (best-effort) ─────────────────────────────

    async def _get_state(self, domain: str) -> _DomainState:
        """Load (or create) routing state for a domain. Cached in memory."""
        async with self._cache_lock:
            if domain in self._state_cache:
                return self._state_cache[domain]

        # Fetch from DB. If row doesn't exist, create with defaults.
        try:
            async with async_session_maker() as session:
                row = (await session.execute(sql_text("""
                    SELECT preferred_stage, enabled_stages
                    FROM scraper_routing
                    WHERE domain = :d
                """), {"d": domain})).first()

                if row:
                    state = _DomainState(
                        preferred_stage=int(row.preferred_stage),
                        enabled_stages=tuple(row.enabled_stages),
                    )
                else:
                    # Insert default row so we have routing config visible
                    # in the admin DB inspection.
                    await session.execute(sql_text("""
                        INSERT INTO scraper_routing
                          (domain, preferred_stage, enabled_stages)
                        VALUES (:d, 0, ARRAY[0,1,2,3,4])
                        ON CONFLICT (domain) DO NOTHING
                    """), {"d": domain})
                    await session.commit()
                    state = _DomainState()
        except Exception as e:
            # DB unreachable — use safe defaults, don't crash the scrape.
            logger.warning(f"[Funnel] could not load routing for {domain}: {e}")
            state = _DomainState()

        async with self._cache_lock:
            self._state_cache[domain] = state
        return state

    async def _persist_preferred_stage(self, domain: str, stage: int) -> None:
        """Save promoted/demoted preferred_stage to DB. Best-effort."""
        try:
            async with async_session_maker() as session:
                await session.execute(sql_text("""
                    UPDATE scraper_routing
                       SET preferred_stage = :s,
                           last_success_at = NOW()
                     WHERE domain = :d
                """), {"d": domain, "s": stage})
                await session.commit()
        except Exception as e:
            logger.warning(f"[Funnel] could not persist preferred_stage: {e}")

    async def _record_probe_attempt(
        self, domain: str, *, success: bool, error_code: str | None,
    ) -> None:
        """Update last_probe_at on scraper_routing for visibility.

        Separate from _record_attempt because probe attempts shouldn't
        pollute the per-URL fetch_attempts table — they're not real scrape
        traffic, just diagnostic pings.
        """
        try:
            async with async_session_maker() as session:
                note = None if success else f"probe_fail:{error_code}"
                await session.execute(sql_text("""
                    UPDATE scraper_routing
                       SET last_probe_at = NOW(),
                           notes = COALESCE(:n, notes)
                     WHERE domain = :d
                """), {"d": domain, "n": note})
                await session.commit()
        except Exception as e:
            logger.debug(f"[Funnel] probe telemetry write failed: {e}")

    async def _record_attempt(
        self, domain: str, url: str, stage: int, *,
        success: bool, error_code: str | None,
        error_text: str | None, elapsed_ms: int,
    ) -> None:
        """Insert a fetch_attempts row. Best-effort, never raises."""
        try:
            async with async_session_maker() as session:
                await session.execute(sql_text("""
                    INSERT INTO fetch_attempts
                      (domain, url, stage, success, duration_ms,
                       error_code, error_text)
                    VALUES
                      (:d, :u, :s, :ok, :ms, :ec, :et)
                """), {
                    "d": domain, "u": url[:500], "s": stage, "ok": success,
                    "ms": elapsed_ms, "ec": error_code,
                    "et": (error_text or "")[:500] if error_text else None,
                })
                await session.commit()
        except Exception as e:
            # Telemetry must never break the scrape.
            logger.debug(f"[Funnel] telemetry write failed: {e}")


# Module-level singleton
fetcher_funnel = FetchFunnel()