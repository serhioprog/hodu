"""
Weekly probe job — auto-self-healing of fetch funnel routing.

Why this exists:
    The funnel promotes domains UP the stage ladder when stage 0 fails
    repeatedly. But there's no automatic way DOWN — once a domain is on
    Playwright, it stays on Playwright forever even if the original
    blocker is gone (CF rule lifted, IP unblocked, etc).

    This job runs once a week and pokes each promoted domain to see if
    stage 0 has recovered. If yes, demote → save Playwright costs.

Schedule:
    Mondays 04:00 Europe/Athens (quietest hour, lowest scrape activity).

Behavior:
    - Reads all domains from scraper_routing where preferred_stage > 0.
    - For each, calls fetcher_funnel.probe_demote(domain).
    - probe_demote fires a single GET to https://{domain}/ via stage 0.
    - On success → demote + Telegram alert.
    - On failure → leave as-is, record last_probe_at.

The probe is a single homepage GET per stuck domain. Even with 100
domains stuck on Playwright, that's 100 GETs per week — invisible
load on target sites.
"""
from __future__ import annotations

from loguru import logger
from sqlalchemy import text as sql_text

from src.database.db                 import async_session_maker
from src.scrapers.fetchers           import fetcher_funnel


async def run_probe_job() -> None:
    """Top-level entry — called by APScheduler."""
    logger.info("[ProbeJob] starting weekly demotion probe")

    # Fetch list of currently-promoted domains.
    try:
        async with async_session_maker() as session:
            rows = (await session.execute(sql_text("""
                SELECT domain, preferred_stage
                FROM scraper_routing
                WHERE preferred_stage > 0
                ORDER BY domain
            """))).all()
    except Exception as e:
        logger.error(f"[ProbeJob] could not load routing: {e}")
        return

    if not rows:
        logger.info("[ProbeJob] no domains on higher stages — nothing to probe")
        return

    logger.info(f"[ProbeJob] probing {len(rows)} domain(s)")

    demoted: list[str] = []
    kept:    list[str] = []

    for row in rows:
        domain = row.domain
        try:
            result = await fetcher_funnel.probe_demote(domain)
            outcome = result.get("outcome", "unknown")
            if outcome == "demoted":
                demoted.append(domain)
            elif outcome == "kept":
                kept.append(domain)
            # 'noop' / 'skipped' aren't actionable — just log
            logger.info(f"[ProbeJob] {domain}: {outcome}")
        except Exception as e:
            logger.warning(f"[ProbeJob] {domain}: error during probe: {e}")
            kept.append(domain)

    logger.info(
        f"[ProbeJob] done — demoted {len(demoted)}, kept {len(kept)}"
    )

    # Send a summary if anything changed (otherwise stay silent — no news
    # is good news; we don't want to spam the channel weekly with
    # "0 changes today").
    if demoted:
        try:
            from src.services.telegram_notifier import telegram_notifier
            lines = [f"⬇️ <b>WEEKLY PROBE — {len(demoted)} demotion(s)</b>", ""]
            for d in demoted:
                lines.append(f"  🌐 {d}")
            await telegram_notifier.send("\n".join(lines))
        except Exception as e:
            logger.debug(f"[ProbeJob] summary send failed: {e}")