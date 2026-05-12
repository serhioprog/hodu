"""
~/hodu/scripts/refresh_grekodom_media.py

One-off back-fill: re-fetch detail pages for every grekodom.com property
already in the DB and patch in what the first ingestion missed.

BACKGROUND
----------
The first grekodom Level 3b ingestion (May 2026) ran with two bugs in
src/scrapers/grekodom_development.py:

  1. PHOTO EXTRACTION — _extract_images only matched URLs containing
     /userfiles/realtyobjects/photos/. But Land properties (~40% of the
     648 ingested) host their images under /pictureshd/. Those properties
     got ZERO media rows.

  2. NO NLP FALLBACK — the scraper didn't run self.extractor over the
     description text, so amenity features (sea_view, swimming_pool,
     parking, balcony, garden, fireplace, ...) mentioned in prose were
     never captured in extra_features.

Both bugs are now fixed (photo dual-path + EnrichmentMixin integration).
But the existing rows in the DB were captured BEFORE the fix.

WHY a SEPARATE SCRIPT (not just re-run daily_sync)
--------------------------------------------------
daily_sync has _should_redeep() that filters which existing properties to
re-fetch, gated by cooldown + attempt counter. Many grekodom rows have
non-empty extra_features (agent info, region, etc.) and would NOT trigger
re-deep based on the auto-healing predicate. We need to force-refresh
ALL grekodom rows once, unconditionally.

Also, _redeep_existing() only patches Property columns + extra_features —
it does NOT touch the `media` table. The photo back-fill specifically
requires direct media handling.

WHAT THIS SCRIPT DOES (per property)
------------------------------------
  1. Re-run scraper.fetch_details(url) with the FIXED code
  2. Patch top-level columns ONLY when currently NULL (never overwrites)
  3. Keep the LONGER description (parity with _redeep_existing logic)
  4. MERGE extra_features (existing keys preserved, new ones added)
  5. Find image URLs not yet in `media`; download via MediaDownloader;
     APPEND Media rows + extend image_phashes (preserves existing order)
  6. Update last_checked_at on every successful pass

WHAT THIS SCRIPT DOES NOT DO
----------------------------
  * Does not bump details_fetch_attempts (this is a forced fix, not a
    "normal retry" — keeping counter intact for daily_sync's logic)
  * Does not delete or replace existing Media rows
  * Does not re-run collect_urls (seed data in DB is already correct)
  * Does not bypass _PROTECTED_FIELDS — uses direct ORM writes, no
    repository.save_or_update_property roundtrip

USAGE
-----
  # Dry-run on 5 properties first — confirm what would change:
  docker compose exec scraper python scripts/refresh_grekodom_media.py \\
      --dry-run --limit 5

  # Full live run:
  docker compose exec scraper python scripts/refresh_grekodom_media.py

  # With a bigger limit for staged rollout:
  docker compose exec scraper python scripts/refresh_grekodom_media.py \\
      --limit 50

Approximate runtime for full 648: ~10–15 min (politeness sleep + media DLs).
"""
from __future__ import annotations

import argparse
import asyncio

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.db import async_session_maker
from src.models.domain import Media, Property, utcnow
from src.scrapers.grekodom_development import GrekodomDevelopmentScraper
from src.services.media import MediaDownloader

SOURCE_DOMAIN = "grekodom.com"


# ────────────────────────────────────────────────────────────────────────
# Per-property patch logic
# ────────────────────────────────────────────────────────────────────────

async def _patch_one(
    session: AsyncSession,
    scraper: GrekodomDevelopmentScraper,
    downloader: MediaDownloader,
    db_prop: Property,
    dry_run: bool,
) -> dict:
    """Refresh one property in place. Returns a stats dict (never raises)."""
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
    # Mirrors _redeep_existing's "never overwrite" rule. Caller-supplied
    # caster lets us defend against weird value types coming back from
    # fetch_details (e.g. strings where ints expected).
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

    # ─── Description: keep the longer one (parity with _redeep_existing) ─
    new_desc = (details.get("description") or "").strip()
    old_desc = (db_prop.description or "").strip()
    if new_desc and len(new_desc) > len(old_desc):
        if not dry_run:
            db_prop.description = new_desc
        stats["fields_patched"].append("description")

    # ─── extra_features merge (existing keys preserved on overlap) ──────
    # This is where the big win for "old ingest, new parser" lives:
    # the FIXED scraper's NLP fallback adds sea_view/swimming_pool/etc.
    # The JSONB column must be reassigned (not mutated in place) so
    # SQLAlchemy detects the change.
    new_extra = details.get("extra_features") or {}
    if new_extra:
        existing_extra = dict(db_prop.extra_features or {})
        added_keys = [k for k in new_extra if k not in existing_extra]
        if added_keys:
            merged = {**existing_extra, **new_extra}
            if not dry_run:
                db_prop.extra_features = merged
            stats["extras_added"] = added_keys

    # ─── Media: download images not yet in DB, append Media rows ────────
    media_q = select(Media).where(Media.property_id == db_prop.id)
    existing_media = (await session.execute(media_q)).scalars().all()
    existing_urls: set = {m.image_url for m in existing_media}
    stats["had_zero_media"] = (len(existing_urls) == 0)

    scraped_urls = details.get("images") or []
    new_urls = [u for u in scraped_urls if u not in existing_urls]
    stats["new_images"] = len(new_urls)

    if new_urls and not dry_run:
        downloaded = await downloader.download_images(
            SOURCE_DOMAIN, db_prop.site_property_id, new_urls
        )
        if downloaded:
            # If property previously had ZERO media, the first new image
            # becomes the primary. Otherwise the existing primary stays.
            starting_count = len(existing_media)
            for idx, d in enumerate(downloaded):
                session.add(Media(
                    property_id=db_prop.id,
                    image_url=d["url"],
                    local_file_path=d.get("local_path"),
                    is_main_photo=(starting_count == 0 and idx == 0),
                ))
            # Extend image_phashes preserving order (existing first, new appended)
            phashes = list(db_prop.image_phashes or [])
            phashes.extend((d.get("phash") or "") for d in downloaded)
            db_prop.image_phashes = phashes
            if stats["had_zero_media"]:
                stats["rescued"] = True

    if not dry_run:
        # Mark we've touched this row today. We intentionally do NOT
        # change details_fetch_attempts — this script is a manual fix,
        # not a normal daily_sync retry attempt.
        db_prop.last_checked_at = utcnow()

    stats["ok"] = True
    return stats


# ────────────────────────────────────────────────────────────────────────
# Driver
# ────────────────────────────────────────────────────────────────────────

async def main(dry_run: bool, limit: int | None, batch_commit: int) -> None:
    scraper = GrekodomDevelopmentScraper()
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
        q = select(Property).where(Property.source_domain == SOURCE_DOMAIN)
        if limit:
            q = q.limit(limit)
        properties = (await session.execute(q)).scalars().all()

        logger.info(
            f"[refresh-grekodom] {len(properties)} properties — mode="
            f"{'DRY-RUN' if dry_run else 'LIVE'} batch_commit={batch_commit}"
        )

        for i, prop in enumerate(properties, 1):
            overall["total"] += 1
            stats = await _patch_one(session, scraper, downloader, prop, dry_run)

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

                # Log interesting rows + every 25th tick
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

            # Batch commit
            if not dry_run and (i % batch_commit == 0):
                await session.commit()

            # Politeness delay between detail-page fetches
            await asyncio.sleep(0.4)

        if not dry_run:
            await session.commit()

    logger.info("=" * 60)
    logger.info("FINAL SUMMARY")
    for k, v in overall.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 60)


# ────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "One-off back-fill: re-fetch grekodom details + media with the "
            "fixed scraper. Patches missing columns, merges new NLP "
            "extra_features, downloads missing photos."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't write to DB; just report what would change.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process only the first N properties (for staged testing).",
    )
    parser.add_argument(
        "--batch-commit", type=int, default=10,
        help="Commit the DB session every N properties (default 10).",
    )
    args = parser.parse_args()

    asyncio.run(main(args.dry_run, args.limit, args.batch_commit))