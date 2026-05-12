"""
~/hodu/src/scripts/refresh_scraper_data.py

Generic one-off back-fill: re-fetch detail pages for all existing
properties of a given source domain, then patch in what the first
ingestion missed (NLP-derived extras, missing photos, etc.).

WHEN TO USE
-----------
After upgrading a scraper (e.g. inheriting EnrichmentMixin, fixing a
photo selector, adding og:description fallback), the existing DB rows
for that source were captured BEFORE the fix. They still have stale
data. This script re-fetches each existing property with the FIXED
scraper code and patches in the missing pieces.

For grekodom specifically, the first ingestion ALSO had a photo-
extraction bug (only matched /photos/ URLs, missing /pictureshd/
for Land properties). Re-running this script after the fix
back-fills the missing Media rows. The same logic works for any
other source where photos were lost — generalised once, used everywhere.

WHY NOT JUST RE-RUN daily_sync
------------------------------
daily_sync has _should_redeep() which gates re-fetch by cooldown +
attempt counter, AND requires at least one critical field to be
missing. Properties that look "complete" but lack NLP-derived
extras (e.g. sea_view, balcony, terrace) will NOT trigger natural
re-deep. We force-refresh ALL rows for the domain once.

USAGE
-----
    # Dry-run on the first 5 properties (recommended first step)
    docker compose exec scraper python -m src.scripts.refresh_scraper_data \\
        --domain engelvoelkers.com --dry-run --limit 5

    # Live run on the same 5 to verify DB writes work
    docker compose exec scraper python -m src.scripts.refresh_scraper_data \\
        --domain engelvoelkers.com --limit 5

    # Full domain refresh
    docker compose exec scraper python -m src.scripts.refresh_scraper_data \\
        --domain engelvoelkers.com

Available --domain values are listed in DOMAIN_REGISTRY below.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.db import async_session_maker
from src.models.domain import Media, Property, utcnow
from src.scrapers.base import BaseScraper
from src.services.media import MediaDownloader


# ─────────────────────────────────────────────────────────────────────────
# Registry: source_domain → scraper class path
# ─────────────────────────────────────────────────────────────────────────
# Each scraper is registered as "module:ClassName". The script dynamically
# imports the module and instantiates the class. When you add a new scraper
# or upgrade an existing one, register it here so the refresh script can
# back-fill its existing DB rows.
DOMAIN_REGISTRY: dict[str, str] = {
    "grekodom.com":
        "src.scrapers.grekodom_development:GrekodomDevelopmentScraper",
    "engelvoelkers.com":
        "src.scrapers.engel_voelkers:EngelVoelkersScraper",
    "halkidikirealestate.com":
        "src.scrapers.halkidiki_real_estate_hellenic_living:"
        "HalkidikiRealEstateScraper",
    "halkidikiestate.com":
        "src.scrapers.halkidiki_estate:HalkidikiEstateScraper",
    "sithoniarental-sales.gr":
        "src.scrapers.sithonia_rental_sales:SithoniaRentalSalesScraper",
    "realestatecenter.gr":
        "src.scrapers.real_estate_center_SJ:RealEstateCenterScraper",
    "glrealestate.gr":
        "src.scrapers.gl_real_estate:GLRealEstateScraper",
}


def _load_scraper(domain: str) -> BaseScraper:
    """Resolve a source_domain to its scraper instance via the registry."""
    if domain not in DOMAIN_REGISTRY:
        valid = ", ".join(sorted(DOMAIN_REGISTRY.keys()))
        raise SystemExit(
            f"Unknown --domain {domain!r}. Valid: {valid}"
        )
    module_path, _, class_name = DOMAIN_REGISTRY[domain].partition(":")
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls()


# ─────────────────────────────────────────────────────────────────────────
# Per-property patch logic
# ─────────────────────────────────────────────────────────────────────────

async def _patch_one(
    session: AsyncSession,
    scraper: BaseScraper,
    downloader: MediaDownloader,
    db_prop: Property,
    dry_run: bool,
) -> dict:
    """Refresh one property in place. Never raises — returns stats dict."""
    stats: dict = {
        "id": db_prop.site_property_id,
        "ok": False,
        "fields_patched": [],
        "extras_added": [],
        "new_images": 0,
        "had_zero_media": False,
        "rescued": False,
        "error": None,
    }

    # ─── Re-fetch detail page with the FIXED scraper ────────────────────
    try:
        details = await scraper.fetch_details(db_prop.url)
    except Exception as exc:
        stats["error"] = f"fetch_details raised: {exc!r}"
        return stats

    if not details:
        stats["error"] = "fetch_details returned empty"
        return stats

    # ─── Column patches: only assign if currently missing ───────────────
    def _patch_if_missing(field: str, caster=None) -> None:
        if getattr(db_prop, field) is not None:
            return
        value = details.get(field)
        if value is None:
            return
        if caster is not None:
            try:
                value = caster(value)
            except (TypeError, ValueError):
                return
        if not dry_run:
            setattr(db_prop, field, value)
        stats["fields_patched"].append(field)

    _patch_if_missing("price", int)
    _patch_if_missing("size_sqm", float)
    _patch_if_missing("land_size_sqm", float)
    _patch_if_missing("bedrooms", int)
    _patch_if_missing("bathrooms", int)
    _patch_if_missing("year_built", int)
    _patch_if_missing("area")
    _patch_if_missing("subarea")
    _patch_if_missing("category")
    _patch_if_missing("levels", str)
    _patch_if_missing("latitude", float)
    _patch_if_missing("longitude", float)
    _patch_if_missing("location_raw", str)

    # ─── Description: keep the longer one ───────────────────────────────
    new_desc = (details.get("description") or "").strip()
    old_desc = (db_prop.description or "").strip()
    if new_desc and len(new_desc) > len(old_desc):
        if not dry_run:
            db_prop.description = new_desc
        stats["fields_patched"].append("description")

    # ─── extra_features merge (existing keys preserved on overlap) ──────
    # JSONB column must be REASSIGNED (not mutated in place) so SQLAlchemy
    # detects the change and flushes it.
    new_extra = details.get("extra_features") or {}
    if new_extra:
        existing_extra = dict(db_prop.extra_features or {})
        added_keys = [k for k in new_extra if k not in existing_extra]
        if added_keys:
            merged = {**existing_extra, **new_extra}
            if not dry_run:
                db_prop.extra_features = merged
            stats["extras_added"] = added_keys

    # ─── Media: download URLs not yet in DB, append Media rows ──────────
    media_q = select(Media).where(Media.property_id == db_prop.id)
    existing_media = (await session.execute(media_q)).scalars().all()
    existing_urls: set = {m.image_url for m in existing_media}
    stats["had_zero_media"] = (len(existing_urls) == 0)

    scraped_urls = details.get("images") or []
    new_urls = [u for u in scraped_urls if u not in existing_urls]
    stats["new_images"] = len(new_urls)

    if new_urls and not dry_run:
        downloaded = await downloader.download_images(
            db_prop.source_domain, db_prop.site_property_id, new_urls
        )
        if downloaded:
            starting_count = len(existing_media)
            for idx, d in enumerate(downloaded):
                session.add(Media(
                    property_id=db_prop.id,
                    image_url=d["url"],
                    local_file_path=d.get("local_path"),
                    is_main_photo=(starting_count == 0 and idx == 0),
                ))
            phashes = list(db_prop.image_phashes or [])
            phashes.extend((d.get("phash") or "") for d in downloaded)
            db_prop.image_phashes = phashes
            if stats["had_zero_media"]:
                stats["rescued"] = True

    if not dry_run:
        db_prop.last_checked_at = utcnow()

    stats["ok"] = True
    return stats


# ─────────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────────

async def main(
    domain: str,
    dry_run: bool,
    limit: int | None,
    batch_commit: int,
) -> None:
    scraper = _load_scraper(domain)
    downloader = MediaDownloader()

    overall: dict = {
        "total":                  0,
        "ok":                     0,
        "errors":                 0,
        "new_images_total":       0,
        "extras_added_total":     0,
        "had_zero_media":         0,
        "rescued_from_zero":      0,
        "columns_patched_total":  0,
    }

    async with async_session_maker() as session:
        q = select(Property).where(Property.source_domain == domain)
        if limit:
            q = q.limit(limit)
        properties = (await session.execute(q)).scalars().all()

        logger.info(
            f"[refresh] domain={domain} count={len(properties)} "
            f"mode={'DRY-RUN' if dry_run else 'LIVE'} "
            f"batch_commit={batch_commit}"
        )

        for i, prop in enumerate(properties, 1):
            overall["total"] += 1
            stats = await _patch_one(
                session, scraper, downloader, prop, dry_run
            )

            if not stats["ok"]:
                overall["errors"] += 1
                logger.warning(
                    f"[{i}/{len(properties)}] {stats['id']}: {stats['error']}"
                )
            else:
                overall["ok"]                    += 1
                overall["new_images_total"]      += stats["new_images"]
                overall["extras_added_total"]    += len(stats["extras_added"])
                overall["columns_patched_total"] += len(stats["fields_patched"])
                if stats["had_zero_media"]:
                    overall["had_zero_media"] += 1
                if stats["rescued"]:
                    overall["rescued_from_zero"] += 1

                if stats["new_images"] or stats["extras_added"] or i % 25 == 0:
                    bits = []
                    if stats["new_images"]:
                        bits.append(f"+{stats['new_images']}img")
                    if stats["extras_added"]:
                        bits.append(
                            f"+{len(stats['extras_added'])}extras "
                            f"({','.join(stats['extras_added'][:4])})"
                        )
                    if stats["fields_patched"]:
                        bits.append(
                            f"cols=[{','.join(stats['fields_patched'][:4])}]"
                        )
                    rescued_tag = " 🆘 rescued" if stats["rescued"] else ""
                    logger.info(
                        f"[{i}/{len(properties)}] {stats['id']}: "
                        f"{' '.join(bits) or 'no-change'}{rescued_tag}"
                    )

            if not dry_run and (i % batch_commit == 0):
                await session.commit()

            await asyncio.sleep(0.4)

        if not dry_run:
            await session.commit()

    logger.info("=" * 60)
    logger.info(f"FINAL SUMMARY for {domain}")
    for k, v in overall.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Generic one-off back-fill: re-fetch detail pages for an "
            "upgraded scraper, patch missing columns + extras + media."
        ),
    )
    parser.add_argument(
        "--domain", required=True,
        help=(
            "source_domain to refresh (e.g. grekodom.com, engelvoelkers.com). "
            "Must be registered in DOMAIN_REGISTRY."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't write to DB; just report what would change.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process only the first N properties (staged testing).",
    )
    parser.add_argument(
        "--batch-commit", type=int, default=10,
        help="Commit the session every N properties (default 10).",
    )
    args = parser.parse_args()

    asyncio.run(main(
        args.domain, args.dry_run, args.limit, args.batch_commit,
    ))