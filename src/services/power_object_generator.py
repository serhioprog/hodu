"""
Synthesises a single master record (PowerProperty) from an APPROVED cluster.

Only runs for clusters that are:
    * status = APPROVED
    * last_external_is_unique = True
This condition is enforced inside generate_for_cluster — the caller in
daily_sync also pre-filters, so we avoid wasting gpt-4o quota.
"""
import json
from collections import Counter
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from loguru import logger
from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer

from src.core.config import settings
from src.models.ai_schemas import PowerPropertySynthesis
from src.models.domain import (
    ClusterStatus, Media, PowerProperty, Property, PropertyCluster,
)
from src.services.phash_service import PHashService


class PowerObjectGenerator:

    def __init__(self) -> None:
        self._client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        self._model = settings.OPENAI_CHAT_MODEL

    # ----- Aggregation helpers ------------------------------------
    @staticmethod
    def _mode_or_avg_float(values: list[float]) -> Optional[float]:
        if not values:
            return None
        counter = Counter(values)
        mode_val, mode_count = counter.most_common(1)[0]
        if mode_count > 1:
            return mode_val
        return sum(values) / len(values)

    @staticmethod
    def _mode_int(values: list[Optional[int]]) -> Optional[int]:
        vals = [v for v in values if v is not None]
        if not vals:
            return None
        return Counter(vals).most_common(1)[0][0]

    def _aggregate(self, members: list[Property]) -> dict:
        prices = [p.price         for p in members if p.price]
        sizes  = [p.size_sqm      for p in members if p.size_sqm]
        lands  = [p.land_size_sqm for p in members if p.land_size_sqm]

        return {
            "price":             min(prices) if prices else None,  # business rule: lowest
            "size_sqm":          self._mode_or_avg_float(sizes),
            "land_size_sqm":     self._mode_or_avg_float(lands),
            "bedrooms":          self._mode_int([p.bedrooms   for p in members]),
            "bathrooms":         self._mode_int([p.bathrooms  for p in members]),
            "year_built":        self._mode_int([p.year_built for p in members]),
            "category":          next((p.category          for p in members if p.category),          None),
            "calc_prefecture":   next((p.calc_prefecture   for p in members if p.calc_prefecture),   None),
            "calc_municipality": next((p.calc_municipality for p in members if p.calc_municipality), None),
            "calc_area":         next((p.calc_area         for p in members if p.calc_area),         None),
            "latitude":          next((p.latitude          for p in members if p.latitude),          None),
            "longitude":         next((p.longitude         for p in members if p.longitude),         None),
        }

    # ----- Gallery deduplication by pHash -------------------------
    async def _dedup_gallery(
        self, session: AsyncSession, members: list[Property]
    ) -> tuple[list[str], list[str]]:
        unique_urls:     list[str] = []
        unique_paths:    list[str] = []
        accepted_hashes: list[str] = []

        for prop in members:
            res = await session.execute(
                select(Media)
                .where(Media.property_id == prop.id)
                .order_by(Media.created_at)
            )
            medias = list(res.scalars().all())
            phashes = prop.image_phashes or []

            for idx, m in enumerate(medias):
                h = phashes[idx] if idx < len(phashes) else ""

                if not h:
                    # No pHash (PIL couldn't decode) — keep conservatively,
                    # but remember an empty marker so we don't loop-match it.
                    unique_urls.append(m.image_url)
                    unique_paths.append(m.local_file_path or "")
                    continue

                is_dup = any(
                    acc and PHashService.hamming(h, acc) <= settings.PHASH_HAMMING_THRESHOLD
                    for acc in accepted_hashes
                )
                if not is_dup:
                    unique_urls.append(m.image_url)
                    unique_paths.append(m.local_file_path or "")
                    accepted_hashes.append(h)

        return unique_urls, unique_paths

    # ----- AI synthesis ------------------------------------------
    async def _synthesize(
        self, members: list[Property], aggregated: dict
    ) -> PowerPropertySynthesis:
        descriptions = [p.description for p in members if p.description]
        all_features: dict[str, bool] = {}
        for p in members:
            for k, v in (p.extra_features or {}).items():
                if v is True:
                    all_features[k] = True

        system_prompt = (
            "You are a real estate data editor. Produce a single, factual, "
            "non-promotional description of a property, merging the provided "
            "sources into one consistent text. Rules: do not invent facts; "
            "do not use marketing clichés (stunning, dream, luxurious); do not "
            "mention price, agent names, or source websites. Language: English. "
            "Length: 2-4 paragraphs. If sources disagree, prefer the most specific fact."
        )

        user_payload = {
            "aggregated_facts": {k: v for k, v in aggregated.items() if v is not None},
            "candidate_features": sorted(all_features.keys()),
            "source_descriptions": descriptions[:10],
        }

        try:
            resp = await self._client.beta.chat.completions.parse(
                model=self._model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    # Use JSON, not str(dict) — safe escaping, no Python repr artefacts.
                    {"role": "user",   "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                response_format=PowerPropertySynthesis,
                temperature=0.2,
            )
            parsed = resp.choices[0].message.parsed
            if parsed is None:
                raise RuntimeError("OpenAI returned no parsed content")
            return parsed

        except Exception as e:
            logger.error(f"[PowerGen] OpenAI synthesis failed: {e}")
            fallback_desc = (
                "\n\n".join(descriptions[:3])
                if descriptions else
                "Description unavailable."
            )
            return PowerPropertySynthesis(
                description=fallback_desc,
                features=all_features,
            )

    # ----- Main entry ---------------------------------------------
    async def generate_for_cluster(
        self, session: AsyncSession, cluster_id: UUID
    ) -> PowerProperty | None:
        cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == cluster_id)
        )).scalar_one_or_none()

        if cluster is None:
            logger.warning(f"[PowerGen] cluster {cluster_id} not found")
            return None
        if cluster.status != ClusterStatus.APPROVED:
            logger.debug(f"[PowerGen] cluster {cluster_id} not APPROVED, skip")
            return None
        if cluster.last_external_is_unique is not True:
            logger.debug(f"[PowerGen] cluster {cluster_id} not externally unique, skip")
            return None

        # Load active members — defer embedding (we don't need it here)
        res = await session.execute(
            select(Property)
            .options(defer(Property.embedding))
            .where(
                Property.cluster_id == cluster_id,
                Property.is_active.is_(True),
            )
        )
        members = list(res.scalars().all())
        if not members:
            logger.warning(f"[PowerGen] cluster {cluster_id} has no active members")
            return None

        aggregated = self._aggregate(members)
        urls, paths = await self._dedup_gallery(session, members)
        synthesis = await self._synthesize(members, aggregated)

        existing = (await session.execute(
            select(PowerProperty).where(PowerProperty.cluster_id == cluster_id)
        )).scalar_one_or_none()

        now = datetime.now(timezone.utc)

        # Keep only truthy features — PowerProperty.features is "what the
        # property has", not a mixed-truth map.
        features = {k: v for k, v in synthesis.features.items() if v}

        if existing is None:
            # Build kwargs explicitly to avoid keyword collisions with aggregated.
            po = PowerProperty(
                cluster_id=cluster_id,
                description=synthesis.description,
                features=features,
                image_urls=urls,
                image_local_paths=paths,
                source_property_ids=[p.id for p in members],
                source_domains=sorted({p.source_domain for p in members}),
                generated_at=now,
                regenerated_at=now,
            )
            for k, v in aggregated.items():
                setattr(po, k, v)
            session.add(po)
        else:
            existing.description = synthesis.description
            existing.features = features
            existing.image_urls = urls
            existing.image_local_paths = paths
            existing.source_property_ids = [p.id for p in members]
            existing.source_domains = sorted({p.source_domain for p in members})
            for k, v in aggregated.items():
                setattr(existing, k, v)
            existing.regenerated_at = now
            po = existing

        cluster.power_generated_at = now
        await session.commit()
        logger.success(
            f"[PowerGen] cluster {cluster_id} -> PowerProperty ok (members={len(members)})"
        )
        return po