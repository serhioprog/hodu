"""
Orchestrator for the full daily sync.

Phases:
  1. Scraping       (per-domain: collect → diff → revive DELISTED → deep-fetch newcomers)
  2. MDM            (embed → internal duplicate detection)
  3. PowerObject    (external uniqueness + gpt-4o synthesis, concurrent & filtered)

Phase 1 details (after Partition 2 fixes):
  * Property lookup uses TWO indexes — by site_property_id (primary)
    and by url (fallback when source changed slug). This handles
    cross-domain slug renames without creating duplicates.
  * DELISTED revival: when a previously-DELISTED property reappears
    on the source site, it is automatically promoted back to ACTIVE.
    This means our matching pipeline never has to deal with "zombie"
    properties that are alive on the source but dead in our DB.
  * Coverage check: if a scraper returned suspiciously few results
    (network glitch fingerprint), we abort the DELISTED phase for
    that domain to avoid false-mass-delistings.
  * Re-deep cooldown: existing properties with materially missing
    fields are re-fetched, but rate-limited per property.

Fail-safes:
  * Per-domain scrape failure does not affect other domains.
  * ScraperLog tracks RUNNING → SUCCESS|ERROR for every domain run,
    even if process is killed mid-flight (Partition 8 will add
    "stuck recovery").
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from loguru import logger
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

# --- Scrapers ---
from src.scrapers.gl_real_estate import GLRealEstateScraper
from src.scrapers.greek_exclusive_properties import GreekExclusiveScraper
from src.scrapers.grekodom_development import GrekodomDevelopmentScraper
from src.scrapers.halkidiki_estate import HalkidikiEstateScraper
from src.scrapers.halkidiki_real_estate_hellenic_living import HalkidikiRealEstateScraper
from src.scrapers.real_estate_center_SJ import RealEstateCenterScraper
from src.scrapers.sithonia_rental_sales import SithoniaRentalSalesScraper
from src.scrapers.engel_voelkers import EngelVoelkersScraper
from src.scrapers.sousouras_realestate import SousourasRealEstateScraper

# --- DB & Core ---
from src.core.config import settings
from src.core.scraper_area_constants import HALKIDIKI_REGIONS_WHITELIST
from src.database.db import async_session_maker
from src.database.repository import save_media_records, save_or_update_property
from src.models.domain import (
    ClusterStatus, PriceHistory, Property, PropertyCluster,
    PropertyStatus, utcnow, ScraperLog
)
from src.models.schemas import PropertyTemplate

# --- Services ---
from src.services.embedding_service import EmbeddingService
from src.services.external_unique_finder import (
    ExternalUniqueFinder, GenericHttpAdapter,
)
from src.services.geo_matcher import geo_matcher
from src.services.internal_duplicate_detector import InternalDuplicateDetector
from src.services.media import MediaDownloader
from src.services.power_object_generator import PowerObjectGenerator

# --- Telegram reporting ---
from src.services.cost_tracker import cost_tracker
from src.services.sync_reporter import (
    DomainSyncReport, format_domain_report, format_daily_summary,
)
from src.services.telegram_notifier import telegram_notifier
from sqlalchemy import func, text as sql_text


# =============================================================
# ScraperLog helpers (own session — log writes must not depend on
# the main transaction's success)
# =============================================================
async def _log_start(domain: str) -> str:
    """Insert a 'RUNNING' entry, return its id (string UUID)."""
    async with async_session_maker() as session:
        entry = ScraperLog(source_domain=domain, status="RUNNING")
        session.add(entry)
        await session.flush()
        log_id = str(entry.id)
        await session.commit()
    return log_id


async def _log_finish(
    log_id: str,
    *,
    status: str,
    processed: int = 0,
    new: int = 0,
    duration_seconds: int = 0,
    error_message: Optional[str] = None,
) -> None:
    """Update a ScraperLog entry to its final state."""
    async with async_session_maker() as session:
        entry = (await session.execute(
            select(ScraperLog).where(ScraperLog.id == log_id)
        )).scalar_one()

        entry.status = status
        entry.processed_count = processed
        entry.new_count = new
        entry.duration_seconds = duration_seconds
        entry.error_message = error_message
        await session.commit()


# =============================================================
# Quality metrics — computed from DB after each domain's sync.
# Lightweight (one aggregate query) and gives visibility into
# data degradation that scrape stats alone wouldn't catch.
# =============================================================
async def _collect_quality_metrics(domain: str) -> dict:
    """
    Returns dict with: total, avg_desc_len, pct_with_features, pct_with_price.
    Fault-tolerant: any DB error returns empty dict (we don't want telemetry
    to break the actual sync).
    """
    try:
        async with async_session_maker() as session:
            row = (await session.execute(sql_text("""
                SELECT
                    COUNT(*)::int                                            AS total,
                    AVG(LENGTH(description))::int                            AS avg_len,
                    AVG(
                        CASE WHEN jsonb_array_length(
                            jsonb_path_query_array(extra_features, '$.*')
                        ) >= 5 THEN 1.0 ELSE 0.0 END
                    )::float                                                 AS frac_features,
                    AVG(
                        CASE WHEN price IS NOT NULL AND price > 0
                            THEN 1.0 ELSE 0.0 END
                    )::float                                                 AS frac_price
                FROM properties
                WHERE source_domain = :d AND is_active = TRUE
            """), {"d": domain})).first()

            if not row or not row.total:
                return {}

            return {
                "total":             int(row.total),
                "avg_desc_len":      int(row.avg_len or 0),
                "pct_with_features": int((row.frac_features or 0) * 100),
                "pct_with_price":    int((row.frac_price or 0) * 100),
            }
    except Exception as e:
        logger.warning(f"[reporter] quality metrics query failed for {domain}: {e}")
        return {}


# =============================================================
# Cloudflare / WAF detection.
# Triggered when collect_urls returns empty AND we already had
# >50 properties from this domain — strong signal of WAF block
# rather than legitimate empty listing.
# =============================================================
async def _detect_cloudflare(domain: str, listings_returned: int) -> bool:
    if listings_returned > 0:
        return False
    try:
        async with async_session_maker() as session:
            n = (await session.execute(
                select(func.count(Property.id)).where(
                    Property.source_domain == domain,
                    Property.is_active.is_(True),
                )
            )).scalar() or 0
            return n >= 50
    except Exception:
        return False


# =============================================================
# Dedup statistics — derived from property_clusters created/updated
# during this sync window. Used in the daily Telegram summary.
# =============================================================
async def _collect_dedup_stats(since: datetime) -> dict:
    """
    Returns: pairs_created (approx via clusters), approved, pending.

    Cluster creation is the most reliable proxy for "duplicate pairs
    detected" because the InternalDuplicateDetector only persists
    clusters where it found ≥1 confident match.
    """
    try:
        async with async_session_maker() as session:
            row = (await session.execute(sql_text("""
                SELECT
                  COUNT(*) FILTER (
                    WHERE created_at >= :since OR updated_at >= :since
                  )::int                                       AS touched,
                  COUNT(*) FILTER (
                    WHERE status = 'APPROVED'
                      AND (created_at >= :since OR updated_at >= :since)
                  )::int                                       AS approved,
                  COUNT(*) FILTER (
                    WHERE status = 'PENDING'
                      AND (created_at >= :since OR updated_at >= :since)
                  )::int                                       AS pending
                FROM property_clusters
            """), {"since": since})).first()

            if not row:
                return {}
            return {
                "clusters_touched": int(row.touched or 0),
                "approved":         int(row.approved or 0),
                "pending":          int(row.pending or 0),
            }
    except Exception as e:
        logger.warning(f"[reporter] dedup stats query failed: {e}")
        return {}
    
# =============================================================
# Fetch funnel statistics — per-stage success counts and avg
# latency over the last 24h. Surfaces in the daily Telegram
# summary so operators can see funnel health at a glance.
# =============================================================
async def _collect_funnel_stats(since: datetime) -> dict:
    """
    Returns: {stage_num: {ok, fail, avg_ms}, ...}

    Aggregates fetch_attempts since `since` into per-stage buckets.
    Empty dict on DB error or no data — never raises.
    """
    try:
        async with async_session_maker() as session:
            rows = (await session.execute(sql_text("""
                SELECT
                  stage,
                  COUNT(*) FILTER (WHERE success)         ::int AS ok,
                  COUNT(*) FILTER (WHERE NOT success)     ::int AS fail,
                  AVG(duration_ms) FILTER (WHERE success) ::int AS avg_ms
                FROM fetch_attempts
                WHERE created_at >= :since
                GROUP BY stage
                ORDER BY stage
            """), {"since": since})).all()

            return {
                int(r.stage): {
                    "ok":     int(r.ok or 0),
                    "fail":   int(r.fail or 0),
                    "avg_ms": int(r.avg_ms or 0),
                }
                for r in rows
            }
    except Exception as e:
        logger.warning(f"[reporter] funnel stats query failed: {e}")
        return {}

# =============================================================
# Fetch funnel statistics — per-stage success counts and avg
# latency over the last 24h. Surfaces in the daily Telegram
# summary so operators can see funnel health at a glance.
# =============================================================
async def _collect_funnel_stats(since: datetime) -> dict:
    """
    Returns: {stage_num: {ok, fail, avg_ms}, ...}

    Aggregates fetch_attempts since `since` into per-stage buckets.
    Empty dict on DB error or no data — never raises.
    """
    try:
        async with async_session_maker() as session:
            rows = (await session.execute(sql_text("""
                SELECT
                  stage,
                  COUNT(*) FILTER (WHERE success)         ::int AS ok,
                  COUNT(*) FILTER (WHERE NOT success)     ::int AS fail,
                  AVG(duration_ms) FILTER (WHERE success) ::int AS avg_ms
                FROM fetch_attempts
                WHERE created_at >= :since
                GROUP BY stage
                ORDER BY stage
            """), {"since": since})).all()

            return {
                int(r.stage): {
                    "ok":     int(r.ok or 0),
                    "fail":   int(r.fail or 0),
                    "avg_ms": int(r.avg_ms or 0),
                }
                for r in rows
            }
    except Exception as e:
        logger.warning(f"[reporter] funnel stats query failed: {e}")
        return {}
    
# =============================================================
# PHASE 1: scraping
# =============================================================
async def _run_scrapers(global_stats: Dict[str, int]) -> List[DomainSyncReport]:
    """
    Run all active scrapers per-domain. Returns a list of DomainSyncReport
    objects (one per domain) so the orchestrator can build a daily summary.
    """
    active_scrapers = [
        GLRealEstateScraper(),
        RealEstateCenterScraper(),
        GreekExclusiveScraper(),
        #SousourasRealEstateScraper(),
        SithoniaRentalSalesScraper(),
        HalkidikiEstateScraper(),
        EngelVoelkersScraper(),
        HalkidikiRealEstateScraper(),
        GrekodomDevelopmentScraper(),
    ]

    domain_reports: List[DomainSyncReport] = []

    for scraper in active_scrapers:
        domain = scraper.source_domain
        logger.info(f"🌐 sync start: {domain}")

        start_time = datetime.now(timezone.utc)
        log_id = await _log_start(domain)
        error_msg: Optional[str] = None

        # --- Initialise the per-domain report --------------
        report = DomainSyncReport(domain=domain, started_at=start_time)

        # --- Snapshot DB count before sync (for Cloudflare detection +
        #     Telegram report context). Cheap query.
        try:
            async with async_session_maker() as session:
                report.db_count_before = (await session.execute(
                    select(func.count(Property.id))
                    .where(Property.source_domain == domain)
                )).scalar() or 0
        except Exception:
            report.db_count_before = 0

        # --- collect listing page ----------------------------------
        site_properties: List[PropertyTemplate] = []
        scrape_ok = False
        try:
            site_properties = await scraper.collect_urls(min_price=400000)
            scrape_ok = bool(site_properties)
        except Exception as e:
            logger.error(f"❌ {domain} collect_urls crashed: {e}")
            error_msg = str(e)
            scrape_ok = False

        if not scrape_ok:
            logger.error(f"❌ {domain}: empty/failed listing — skipping domain entirely")
            duration = int((datetime.now(timezone.utc) - start_time).total_seconds())
            await _log_finish(
                log_id,
                status="ERROR",
                duration_seconds=duration,
                error_message=error_msg or "Empty/failed listing",
            )
            # Finalise + send report for failed domain
            report.finished_at = datetime.now(timezone.utc)
            report.error_message = error_msg or "Empty/failed listing"
            report.cloudflare_blocked = await _detect_cloudflare(domain, 0)
            report.cost_snapshot = await cost_tracker.snapshot_and_reset()
            await telegram_notifier.send(
                format_domain_report(report), silent=False
            )
            domain_reports.append(report)
            continue

        # --- whitelist (Halkidiki only) ----------------------------
        valid_properties = [
            p for p in site_properties
            if any(
                region in f"{p.url} {p.location_raw or ''}".lower()
                for region in HALKIDIKI_REGIONS_WHITELIST
            )
        ]
        site_properties = valid_properties
        site_map: Dict[str, PropertyTemplate] = {
            p.site_property_id: p for p in site_properties
        }
        logger.info(f"📊 {domain}: {len(site_map)} listings after whitelist")

        # --- main per-domain transaction ---------------------------
        try:
            async with async_session_maker() as session:
                # Load ALL properties from this domain (including DELISTED/SOLD).
                # We need DELISTED in db_map_id so that revival can work when
                # they reappear on the site.
                db_props = (await session.execute(
                    select(Property).where(Property.source_domain == domain)
                )).scalars().all()

                db_map_id: Dict[str, Property] = {
                    p.site_property_id: p for p in db_props if p.site_property_id
                }
                db_map_url: Dict[str, Property] = {
                    p.url: p for p in db_props if p.url
                }

                logger.info(
                    f"📂 {domain}: {len(db_props)} total rows in DB "
                    f"(active+delisted+sold)"
                )

                # --- FAIL-SAFE for DELISTED ----------------------------
                # If the listing endpoint returned suspiciously few results
                # (network failure fingerprint), don't mass-delist. Leave
                # everyone's status alone for this run.
                coverage_ok = True
                if db_props:
                    coverage = len(site_map) / max(len(db_props), 1)
                    if coverage < settings.DELIST_MIN_COVERAGE_RATIO:
                        logger.warning(
                            f"⚠️ {domain}: coverage {coverage:.0%} "
                            f"(< {settings.DELIST_MIN_COVERAGE_RATIO:.0%}) — "
                            f"aborting DELISTED phase for this domain"
                        )
                        coverage_ok = False

                new_props_to_fetch: List[PropertyTemplate] = []
                delisted_count = 0
                price_changed_count = 0
                revived_count = 0

                # --- DELISTED (guarded) -------------------------------
                # Only mark as DELISTED properties that:
                #   1. We had in our DB with a known site_property_id
                #   2. AND are NOT in the current site_map
                #   3. AND were not already DELISTED (avoid redundant updates
                #      and false `delisted_count` inflation)
                if coverage_ok:
                    for db_id, db_prop in db_map_id.items():
                        if db_id in site_map:
                            continue  # still on site — handled in update loop
                        if db_prop.status == PropertyStatus.DELISTED:
                            continue  # already delisted, no change
                        # Was active/new/price_changed → now gone from site
                        db_prop.status = PropertyStatus.DELISTED
                        db_prop.is_active = False
                        delisted_count += 1

                # --- UPDATES + NEWCOMERS ------------------------------
                for site_id, site_prop in site_map.items():
                    # Lookup priority: by site_property_id first (more reliable),
                    # then by URL (fallback for slug renames).
                    db_prop = db_map_id.get(site_id) or db_map_url.get(site_prop.url)

                    if db_prop:
                        # ---- REVIVAL --------------------------------------
                        # Property is back on the site → promote to ACTIVE
                        # before doing anything else. This must happen first
                        # so price-change detection and re-deep work on the
                        # already-revived entity.
                        if db_prop.status in (PropertyStatus.DELISTED, PropertyStatus.SOLD):
                            old_status = db_prop.status
                            db_prop.status = PropertyStatus.ACTIVE
                            db_prop.is_active = True
                            revived_count += 1
                            logger.info(
                                f"🔁 {site_id}: revived from {old_status.value} → ACTIVE"
                            )

                        # ---- Touch timestamps ------------------------------
                        db_prop.last_checked_at = utcnow()
                        db_prop.last_seen_at = utcnow()

                        # ---- Slug rename detection -------------------------
                        # If found by URL but the site_property_id changed
                        # (slug-style ID rotation), update the stored ID.
                        if db_prop.site_property_id != site_id:
                            logger.info(
                                f"🔀 slug rename: {db_prop.site_property_id} → {site_id}"
                            )
                            db_prop.site_property_id = site_id

                        # ---- Price change detection ------------------------
                        # 1. Recovered NULL price (scraper got it now)
                        if site_prop.price and db_prop.price is None:
                            logger.info(
                                f"💰 {site_id}: empty price restored → {site_prop.price}€"
                            )
                            db_prop.price = site_prop.price

                        # 2. Real price change
                        if (
                            site_prop.price
                            and db_prop.price
                            and site_prop.price != db_prop.price
                        ):
                            logger.info(
                                f"📉 {site_id}: {db_prop.price}€ → {site_prop.price}€"
                            )
                            session.add(PriceHistory(
                                property_id=db_prop.id,
                                old_price=db_prop.price,
                                new_price=site_prop.price,
                            ))
                            db_prop.previous_price = db_prop.price
                            db_prop.price = site_prop.price
                            db_prop.status = PropertyStatus.PRICE_CHANGED
                            price_changed_count += 1

                        # ---- Re-deep fetch (rate-limited) ------------------
                        if _should_redeep(db_prop):
                            await _redeep_existing(session, scraper, db_prop, site_prop)
                    else:
                        new_props_to_fetch.append(site_prop)

                await session.commit()

            logger.success(
                f"✅ {domain} diff: "
                f"new={len(new_props_to_fetch)} "
                f"delisted={delisted_count} "
                f"price_changed={price_changed_count} "
                f"revived={revived_count}"
            )

            global_stats["new"]      += len(new_props_to_fetch)
            global_stats["updated"]  += price_changed_count
            global_stats["delisted"] += delisted_count
            global_stats["revived"]  += revived_count

            # --- Record into per-domain report --------------
            report.listings_found = len(site_map)
            report.new_count      = len(new_props_to_fetch)
            report.delisted_count = delisted_count
            report.price_changed  = price_changed_count
            report.revived_count  = revived_count

            # --- deep parse newcomers (own sessions inside) ----------
            if new_props_to_fetch:
                await _ingest_new_properties(scraper, new_props_to_fetch)
            else:
                logger.info(f"📭 {domain}: nothing new")

            # --- log SUCCESS -----------------------------------------
            duration = int((datetime.now(timezone.utc) - start_time).total_seconds())
            await _log_finish(
                log_id,
                status="SUCCESS",
                processed=len(site_map),
                new=len(new_props_to_fetch),
                duration_seconds=duration,
            )

            # --- Quality metrics + Telegram report -------------------
            quality = await _collect_quality_metrics(domain)
            if quality:
                report.total_after        = quality.get("total")
                report.avg_desc_len       = quality.get("avg_desc_len")
                report.pct_with_features  = quality.get("pct_with_features")
                report.pct_with_price     = quality.get("pct_with_price")

            report.finished_at   = datetime.now(timezone.utc)
            report.cost_snapshot = await cost_tracker.snapshot_and_reset()
            domain_reports.append(report)

            # Routine reports: silent. WARNINGs: audible.
            silent = (report.status == "OK")
            await telegram_notifier.send(
                format_domain_report(report), silent=silent
            )

        except Exception as e:
            logger.exception(f"❌ {domain} sync crashed mid-flight: {e}")
            duration = int((datetime.now(timezone.utc) - start_time).total_seconds())
            await _log_finish(
                log_id,
                status="ERROR",
                duration_seconds=duration,
                error_message=f"sync_crash: {e}",
            )
            # Telegram alert for mid-flight crash
            report.finished_at   = datetime.now(timezone.utc)
            report.error_message = f"sync_crash: {e}"
            report.cost_snapshot = await cost_tracker.snapshot_and_reset()
            domain_reports.append(report)
            await telegram_notifier.send(
                format_domain_report(report), silent=False
            )
            continue

    return domain_reports


# =============================================================
# Re-deep fetch: cooldown logic
# =============================================================
def _should_redeep(db_prop: Property) -> bool:
    """
    Decide whether to re-fetch full details for an existing property.

    Returns True iff:
      1. At least ONE structurally critical field is missing AND
      2. Per-property attempt counter is under the cap AND
      3. Cooldown since last attempt has expired.

    Critical fields are the ones needed for the matching pipeline:
      * size_sqm OR land_size_sqm  — Quality Gate (Partition 3)
      * category                   — Quality Gate
      * description (>= 50 chars)  — Quality Gate
      * extra_features non-empty   — auto-recovery path for properties
                                      ingested before scraper improvements
                                      (e.g. richer extraction added later).
                                      Empty {} is treated as "missing".
      * price                      — fundamental for matching + UI display
                                      (None usually means HTML parse error)

    Non-critical fields (bedrooms, bathrooms, year_built, levels, lat/lng)
    are nice-to-have. We do NOT trigger re-deep for them — the source might
    genuinely not expose those fields, and retrying 5 times costs HTTP
    requests and eventually hits rate limits.

    The extra_features check is what auto-heals "old ingest, new parser"
    drift. When you improve a scraper and previously-ingested rows have
    extra_features={}, daily_sync will quietly re-deep them on the next
    pass without any manual DELETE.
    """
    has_size = db_prop.size_sqm is not None or db_prop.land_size_sqm is not None
    has_category = bool(db_prop.category)
    has_description = bool(db_prop.description) and len(db_prop.description) >= 50
    has_extra = bool(db_prop.extra_features)
    # Price is normally always present. Missing price means scraper hit some
    # defensive sanity-check (e.g. malformed HTML produced an out-of-range value
    # that _to_int_euro rejected). Trigger re-deep so the property gets a real
    # price next time the cooldown allows.
    has_price = db_prop.price is not None

    critical_missing = not (
        has_size and has_category and has_description and has_extra and has_price
    )
    if not critical_missing:
        return False

    if db_prop.details_fetch_attempts >= settings.DETAILS_FETCH_MAX_ATTEMPTS:
        return False

    if db_prop.last_details_fetch_at is not None:
        cooldown = timedelta(days=settings.DETAILS_FETCH_COOLDOWN_DAYS)
        if datetime.now(timezone.utc) - db_prop.last_details_fetch_at < cooldown:
            return False

    return True


async def _redeep_existing(
    session: AsyncSession,
    scraper,
    db_prop: Property,
    site_prop: PropertyTemplate,
) -> None:
    """
    Fill in genuinely missing fields on an existing property.
    Bumps attempt counter regardless of success — this is intentional;
    we don't want infinite retries on broken pages.
    """
    db_prop.details_fetch_attempts += 1
    db_prop.last_details_fetch_at = utcnow()

    try:
        details = await scraper.fetch_details(site_prop.url)
    except Exception as e:
        logger.warning(f"[re-deep] {db_prop.site_property_id}: {e}")
        return

    if not details:
        return

    # Helper: only assign if currently missing AND new value is sane
    def _assign_if_missing_int(field: str) -> None:
        if getattr(db_prop, field) is None:
            v = details.get(field)
            if isinstance(v, int):
                setattr(db_prop, field, v)

    def _assign_if_missing_float(field: str) -> None:
        if getattr(db_prop, field) is None:
            v = details.get(field)
            if isinstance(v, (int, float)):
                setattr(db_prop, field, float(v))

    def _assign_if_missing_str(field: str) -> None:
        if not getattr(db_prop, field):
            v = details.get(field)
            if v:
                setattr(db_prop, field, str(v))

    # --- Critical fields (re-deep was triggered for these) -----
    _assign_if_missing_int("price")
    _assign_if_missing_int("year_built")
    _assign_if_missing_float("land_size_sqm")
    _assign_if_missing_float("size_sqm")
    _assign_if_missing_str("category")

    # --- Description — keep the LONGER version -------------------
    new_desc = details.get("description")
    if new_desc and len(new_desc) > len(db_prop.description or ""):
        db_prop.description = new_desc

    # --- Non-critical bonus fields -------------------------------
    _assign_if_missing_int("bedrooms")
    _assign_if_missing_int("bathrooms")
    _assign_if_missing_str("levels")
    _assign_if_missing_float("latitude")
    _assign_if_missing_float("longitude")

    # --- extra_features merge ------------------------------------
    if details.get("extra_features"):
        current = db_prop.extra_features or {}
        current.update(details["extra_features"])
        db_prop.extra_features = current


# =============================================================
# Newcomer ingestion (each in its own session for isolation)
# =============================================================
async def _ingest_new_properties(
    scraper,
    new_props: List[PropertyTemplate],
) -> None:
    """
    Deep-parse and persist newly-discovered listings.

    Each property gets its own session/transaction — if one ingest
    crashes, others continue. We don't share the diff transaction
    here because:
      1. fetch_details + image download are slow (per-property)
      2. We want partial progress to survive on container kill
    """
    logger.info(f"🚀 deep-parse {len(new_props)} newcomers")
    media_downloader = MediaDownloader()

    for idx, prop_data in enumerate(new_props, 1):
        try:
            logger.info(f"➕ [{idx}/{len(new_props)}] {prop_data.site_property_id}")
            details = await scraper.fetch_details(prop_data.url)
            if not details:
                continue

            base_data = prop_data.model_dump()
            base_data.update(details)

            async with async_session_maker() as session:
                geo = await geo_matcher.find_best_match(
                    session=session,
                    lat=base_data.get("latitude"),
                    lng=base_data.get("longitude"),
                    area_name=base_data.get("area"),
                )
                base_data.update({
                    "location_id":       geo["location_id"],
                    "calc_prefecture":   geo["prefecture"],
                    "calc_municipality": geo["municipality"],
                    "calc_area":         geo["exact_district"],
                })

                prop_validated = PropertyTemplate(**base_data)
                prop_uuid, image_urls = await save_or_update_property(
                    session, prop_validated
                )

            # Image download outside the DB transaction (slow + external)
            if image_urls and prop_uuid:
                downloaded = await media_downloader.download_images(
                    domain=scraper.source_domain,
                    property_id=prop_validated.site_property_id,
                    image_urls=image_urls,
                )
                if downloaded:
                    async with async_session_maker() as session:
                        await save_media_records(session, prop_uuid, downloaded)

            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"❌ ingest {prop_data.site_property_id}: {e}")
            continue


# =============================================================
# PHASE 2: MDM (embed + internal dedup)
# =============================================================
async def _run_mdm_pipeline() -> None:
    embedder = EmbeddingService()
    detector = InternalDuplicateDetector()

    # Phase 1 shadow: engine v2 runs BEFORE the old engine on a SEPARATE
    # session so failures here cannot break the existing pipeline.
    # Gated by settings.USE_NEW_DUPLICATE_ENGINE (default False, opt-in
    # via .env). New engine writes only engine_v2_predictions + cache.
    if settings.USE_NEW_DUPLICATE_ENGINE:
        try:
            from experiments.new_engine_v2.src.engine import HybridEngine
            engine_v2 = HybridEngine.build_default()
            async with async_session_maker() as v2_session:
                shadow_report = await engine_v2.run_full_dedup(v2_session)
                await v2_session.commit()
            logger.info(
                "[MDM] engine v2 shadow report: "
                "scored={s} cached={c} cost=${cost:.4f} "
                "new_clusters={n} attached={a} bridges={b} "
                "approved_disagreements={ad} errors={e} elapsed={t}ms",
                s=shadow_report.pairs_scored,
                c=shadow_report.pairs_cached,
                cost=shadow_report.cost_usd,
                n=shadow_report.new_clusters_proposed,
                a=shadow_report.attached_clusters_count,
                b=shadow_report.bridge_blocks,
                ad=shadow_report.approved_disagreements,
                e=shadow_report.errors_count,
                t=shadow_report.latency_ms,
            )
        except Exception:
            # Shadow mode MUST NOT block production pipeline.
            # logger.exception captures full traceback for diagnosis.
            logger.exception("[MDM] engine v2 shadow run FAILED")

    async with async_session_maker() as session:
        logger.info("[MDM] step 1: refresh embeddings")
        await embedder.refresh_property_embeddings(session)

        logger.info("[MDM] step 2: internal duplicate detection")
        await detector.run(session)


# =============================================================
# PHASE 3: external uniqueness + PowerObject (concurrent, filtered)
# =============================================================
async def _select_clusters_needing_power(session: AsyncSession) -> List:
    """
    Filter clusters down to a small, meaningful subset:
      * status = APPROVED
      * AND one of:
          - never generated, OR
          - cluster touched after generation, OR
          - external re-check overdue
    """
    stale_cutoff = datetime.now(timezone.utc) - timedelta(
        hours=settings.EXTERNAL_RECHECK_HOURS
    )

    q = select(PropertyCluster.id).where(
        PropertyCluster.status == ClusterStatus.APPROVED,
        or_(
            PropertyCluster.power_generated_at.is_(None),
            PropertyCluster.updated_at > PropertyCluster.power_generated_at,
            PropertyCluster.last_external_check_at.is_(None),
            PropertyCluster.last_external_check_at < stale_cutoff,
        ),
    )
    return [row[0] for row in (await session.execute(q)).fetchall()]


async def _process_cluster(
    sem: asyncio.Semaphore,
    cluster_id,
    finder: ExternalUniqueFinder,
    generator: PowerObjectGenerator,
) -> None:
    async with sem:
        try:
            async with async_session_maker() as session:
                is_unique = await finder.check(session, cluster_id)
            if is_unique:
                async with async_session_maker() as session:
                    await generator.generate_for_cluster(session, cluster_id)
        except Exception as e:
            logger.error(f"[MDM/concurrent] cluster {cluster_id}: {e}")


async def _run_power_generation() -> None:
    if not settings.EXTERNAL_API_BASE_URL:
        logger.warning(
            "[MDM] EXTERNAL_API_BASE_URL not set — "
            "external uniqueness + PowerObject are SKIPPED this cycle"
        )
        return

    adapter   = GenericHttpAdapter(settings.EXTERNAL_API_BASE_URL, settings.EXTERNAL_API_KEY)
    embedder  = EmbeddingService()
    finder    = ExternalUniqueFinder(adapter, embedder)
    generator = PowerObjectGenerator()

    async with async_session_maker() as session:
        cluster_ids = await _select_clusters_needing_power(session)

    if not cluster_ids:
        logger.info("[MDM] no clusters need PowerObject regeneration")
        return

    logger.info(
        f"[MDM] feeding {len(cluster_ids)} clusters into the pipeline "
        f"(concurrency={settings.POWER_PIPELINE_CONCURRENCY})"
    )

    sem = asyncio.Semaphore(settings.POWER_PIPELINE_CONCURRENCY)
    tasks = [_process_cluster(sem, cid, finder, generator) for cid in cluster_ids]
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.success("[MDM] PowerObject pipeline finished")


# =============================================================
# ORCHESTRATOR
# =============================================================
async def daily_sync() -> None:
    logger.info("=== DAILY SYNC START ===")
    sync_start_time = datetime.now(timezone.utc)
    global_stats: Dict[str, int] = {
        "new": 0, "updated": 0, "delisted": 0, "revived": 0
    }

    # Reset daily cost counter at the start of each cycle
    await cost_tracker.reset_daily()

    domain_reports = await _run_scrapers(global_stats)
    await _run_mdm_pipeline()
    await _run_power_generation()

    # --- End-of-cycle Telegram summary ---------------------------
    try:
        daily_costs  = await cost_tracker.daily_snapshot()
        dedup_stats  = await _collect_dedup_stats(since=sync_start_time)
        funnel_stats = await _collect_funnel_stats(since=sync_start_time)
        summary = format_daily_summary(
            domain_reports,
            daily_costs,
            dedup_stats=dedup_stats,
            funnel_stats=funnel_stats,
        )
        # Daily summary is silent unless any domain failed
        any_failure = any(
            d.status in ("CRITICAL", "CLOUDFLARE", "WARNING")
            for d in domain_reports
        )
        await telegram_notifier.send(summary, silent=not any_failure)
    except Exception as e:
        logger.warning(f"[reporter] daily summary send failed: {e}")

    logger.success(f"=== DAILY SYNC END === stats={global_stats}")


if __name__ == "__main__":
    asyncio.run(daily_sync())