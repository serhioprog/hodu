"""
Repository layer: upserts for Property and Media.

Changes vs old version:
  * save_or_update_property now keys by (source_domain, site_property_id),
    not url → slug changes on source sites no longer create duplicates.
  * last_seen_at / last_checked_at are updated on every touch — essential
    for DELISTED staleness logic downstream.
  * previous_price defaults to the first observed price on creation, so
    PriceHistory can be computed consistently from day 1.
  * image_phashes are persisted aligned with the Media order.
"""
from typing import Any
from uuid import UUID

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.domain import Media, PriceHistory, Property, PropertyStatus, utcnow
from src.models.schemas import PropertyTemplate


# Fields the scraper must NEVER overwrite on an existing record.
# Note: last_seen_at is NOT here — it must update on every sighting.
_PROTECTED_FIELDS: frozenset[str] = frozenset({
    "status",            # managed by daily_sync / admin
    "is_active",         # managed by daily_sync
    "previous_price",    # managed explicitly by price-change logic
    "cluster_id",        # managed by InternalDuplicateDetector
    "embedding",         # managed by EmbeddingService
    "content_hash",      # managed by EmbeddingService
    "image_phashes",     # managed by save_media_records
    "details_fetch_attempts",
    "last_details_fetch_at",
})


async def save_or_update_property(
    session: AsyncSession,
    data: PropertyTemplate,
) -> tuple[UUID, list[str]]:
    """
    Upsert a Property by composite key (source_domain, site_property_id).

    Returns:
        (property_uuid, image_urls) — image_urls is the raw list from the
        scraper, to be passed to MediaDownloader.
    """
    try:
        payload: dict[str, Any] = data.model_dump()
        image_urls: list[str] = payload.pop("images", []) or []

        # --- Composite key lookup (slug-resistant) -----------------
        q = select(Property).where(
            Property.source_domain == data.source_domain,
            Property.site_property_id == data.site_property_id,
        )
        existing = (await session.execute(q)).scalar_one_or_none()

        now = utcnow()

        if existing is not None:
            # --- UPDATE path ---------------------------------------
            # URL might have changed — update it (it has a unique constraint,
            # so we must be careful; but our composite key already identifies
            # the row, so writing a new url here is correct).
            for key, value in payload.items():
                if key in _PROTECTED_FIELDS:
                    continue
                if value is None:
                    continue
                if hasattr(existing, key):
                    setattr(existing, key, value)

            existing.last_seen_at = now
            existing.last_checked_at = now
            property_uuid = existing.id

        else:
            # --- INSERT path ---------------------------------------
            # Only set fields that actually exist on the ORM model.
            valid_attrs = {
                c.key for c in Property.__mapper__.column_attrs
            }
            init_kwargs = {
                k: v for k, v in payload.items()
                if v is not None and k in valid_attrs
            }
            init_kwargs["status"] = PropertyStatus.NEW
            init_kwargs["is_active"] = True
            init_kwargs["last_seen_at"] = now
            init_kwargs["last_checked_at"] = now

            # Initial price history anchor — so first price change
            # can compute a meaningful delta.
            if init_kwargs.get("price") is not None:
                init_kwargs["previous_price"] = init_kwargs["price"]

            new_prop = Property(**init_kwargs)
            session.add(new_prop)
            await session.flush()
            property_uuid = new_prop.id

        await session.commit()
        return property_uuid, image_urls

    except Exception as e:
        await session.rollback()
        logger.error(f"[repo] save_or_update_property failed: {e}")
        raise


async def record_price_change(
    session: AsyncSession,
    prop: Property,
    new_price: float | None,
) -> bool:
    """Atomically record a price observation for a property.

    Behaviour:
      * new_price is None        → no-op, returns False
      * prop.price is None       → restore (fill-in, no PriceHistory,
                                    no status flip — this isn't a
                                    "change", it's data recovery).
                                    Returns False.
      * prop.price == new_price  → no-op, returns False
      * prop.price != new_price  → write PriceHistory, set
                                    previous_price = current price,
                                    update price, flip status to
                                    PRICE_CHANGED. Returns True.

    Caller must still flush/commit — this function only stages session
    writes (session.add for PriceHistory, attribute mutations for prop).

    Bug #25: consolidates price-change logic that previously lived
    inline in daily_sync._run_scrapers. Repository is now the single
    source of truth for PriceHistory writes, previous_price tracking,
    and the PRICE_CHANGED status flip. Any caller (daily_sync,
    admin override, future re-deep paths) goes through this one
    function — no risk of forgetting to write history or flip status.
    """
    if new_price is None:
        return False

    site_id = prop.site_property_id

    if prop.price is None:
        # Scraper recovered a price that was previously absent — restore
        # without writing PriceHistory (this isn't a "change", it's a
        # fill-in). previous_price gets the same value so the next real
        # change has a non-NULL anchor for delta computation.
        logger.info(f"💰 {site_id}: empty price restored → {new_price}€")
        prop.price = new_price
        if prop.previous_price is None:
            prop.previous_price = new_price
        return False

    if prop.price == new_price:
        return False  # no-op

    # Real price change — write history, update fields, flip status.
    logger.info(f"📉 {site_id}: {prop.price}€ → {new_price}€")
    session.add(PriceHistory(
        property_id=prop.id,
        old_price=prop.price,
        new_price=new_price,
    ))
    prop.previous_price = prop.price
    prop.price = new_price
    prop.status = PropertyStatus.PRICE_CHANGED
    return True


async def save_media_records(
    session: AsyncSession,
    property_uuid: UUID,
    media_data: list[dict],
) -> None:
    """
    Persist Media rows and synchronise Property.image_phashes to be aligned
    by index with the provided media_data list.

    media_data item shape: {"url", "local_path", "is_main", "phash"}
    """
    try:
        phashes_ordered: list[str] = []

        for img in media_data:
            # Upsert-ish: skip if this (property, url) pair already exists.
            q = select(Media).where(
                Media.property_id == property_uuid,
                Media.image_url == img["url"],
            )
            existing = (await session.execute(q)).scalar_one_or_none()

            if existing is None:
                session.add(Media(
                    property_id=property_uuid,
                    image_url=img["url"],
                    local_file_path=img.get("local_path"),
                    is_main_photo=bool(img.get("is_main", False)),
                ))
            else:
                # Only refresh local_file_path if we just downloaded a new one.
                if img.get("local_path"):
                    existing.local_file_path = img["local_path"]

            phashes_ordered.append(img.get("phash") or "")

        # Align aggregate pHash array on the parent Property.
        prop = (await session.execute(
            select(Property).where(Property.id == property_uuid)
        )).scalar_one_or_none()

        if prop is not None:
            prop.image_phashes = phashes_ordered

        await session.commit()

    except Exception as e:
        await session.rollback()
        logger.error(f"[repo] save_media_records failed: {e}")