import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol
from uuid import UUID

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from src.core.config import settings
from src.models.ai_schemas import ExternalPropertyDTO, CanonicalFacts
from src.models.domain import (
    PropertyCluster, Property, ExternalPropertyCache
)
from src.services.canonicalizer import PropertyCanonicalizer
from src.services.embedding_service import EmbeddingService


# =============================================================
# Adapter protocol — любой внешний провайдер реализует это.
# =============================================================
class ExternalPropertyAdapter(Protocol):
    source_name: str

    async def search(
        self,
        category: Optional[str],
        municipality: Optional[str],
        size_sqm: Optional[float],
        size_tolerance: float = 0.15,
    ) -> list[ExternalPropertyDTO]:
        ...


# =============================================================
# Generic HTTP adapter — заглушка/пример. Подставь свой маппинг.
# =============================================================
class GenericHttpAdapter:
    source_name = "external_generic"

    def __init__(self, base_url: str, api_key: str | None = None):
        self._base = base_url.rstrip("/")
        self._key = api_key

    async def search(self, category, municipality, size_sqm, size_tolerance=0.15):
        if size_sqm is None:
            return []
        params = {
            "category": category or "",
            "municipality": municipality or "",
            "min_size": int(size_sqm * (1 - size_tolerance)),
            "max_size": int(size_sqm * (1 + size_tolerance)),
        }
        headers = {"Authorization": f"Bearer {self._key}"} if self._key else {}

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                r = await client.get(f"{self._base}/search", params=params, headers=headers)
                r.raise_for_status()
                payload = r.json()
        except Exception as e:
            logger.error(f"[ExtAdapter] {e}")
            return []

        # Маппинг под твой провайдер — подгонишь по факту.
        items = payload.get("items", [])
        dtos: list[ExternalPropertyDTO] = []
        for it in items:
            dtos.append(ExternalPropertyDTO(
                external_id=str(it["id"]),
                external_source=self.source_name,
                category=it.get("category"),
                calc_municipality=it.get("municipality"),
                calc_area=it.get("area"),
                size_sqm=it.get("size_sqm"),
                bedrooms=it.get("bedrooms"),
                bathrooms=it.get("bathrooms"),
                description=it.get("description"),
                features={k: True for k in it.get("features", [])},
                raw=it,
            ))
        return dtos


# =============================================================
# Finder
# =============================================================
class ExternalUniqueFinder:
    UNIQUE_SIM_THRESHOLD = 0.95   # если найдено что-то >= 0.95 → не уникально

    def __init__(
        self,
        adapter: ExternalPropertyAdapter,
        embedding_service: EmbeddingService,
    ):
        self._adapter = adapter
        self._emb = embedding_service

    # -----------------------------------------------------------
    async def _pick_representative(
        self, session: AsyncSession, cluster_id: UUID
    ) -> Property | None:
        """Берем property с максимально заполненной canonical-картинкой (эвристика)."""
        res = await session.execute(
            select(Property)
            .where(Property.cluster_id == cluster_id, Property.is_active.is_(True))
        )
        members = list(res.scalars().all())
        if not members:
            return None
        # выбираем того, у кого больше всего заполненных ключевых полей
        def score(p: Property) -> int:
            return sum(x is not None for x in (
                p.size_sqm, p.bedrooms, p.bathrooms, p.year_built,
                p.calc_municipality, p.category, p.land_size_sqm,
            )) + (1 if p.embedding is not None else 0)
        members.sort(key=score, reverse=True)
        return members[0]

    # -----------------------------------------------------------
    async def _cache_externals(
        self, session: AsyncSession, dtos: list[ExternalPropertyDTO]
    ) -> list[ExternalPropertyCache]:
        """Upsert в ExternalPropertyCache, возвращаем свежие ORM-объекты."""
        if not dtos:
            return []

        expires = datetime.now(timezone.utc) + timedelta(hours=settings.EXTERNAL_CACHE_TTL_HOURS)
        cached_objs: list[ExternalPropertyCache] = []

        for dto in dtos:
            # canonical_text
            facts = CanonicalFacts(
                category=dto.category,
                calc_municipality=dto.calc_municipality,
                calc_area=dto.calc_area,
                size_sqm=dto.size_sqm,
                bedrooms=dto.bedrooms,
                bathrooms=dto.bathrooms,
                features=dto.features,
            )
            text_, h = PropertyCanonicalizer.canonicalize(facts)

            # пробуем найти существующую
            res = await session.execute(
                select(ExternalPropertyCache).where(
                    ExternalPropertyCache.external_source == dto.external_source,
                    ExternalPropertyCache.external_id == dto.external_id,
                )
            )
            obj = res.scalar_one_or_none()
            if obj is None:
                obj = ExternalPropertyCache(
                    external_source=dto.external_source,
                    external_id=dto.external_id,
                    canonical_text=text_,
                    content_hash=h,
                    raw_payload=dto.raw,
                    expires_at=expires,
                )
                session.add(obj)
            else:
                if obj.content_hash != h:
                    obj.canonical_text = text_
                    obj.content_hash = h
                    obj.embedding = None  # форс-ревекторизация
                obj.raw_payload = dto.raw
                obj.expires_at = expires
            cached_objs.append(obj)

        await session.commit()
        return cached_objs

    # -----------------------------------------------------------
    async def check(self, session: AsyncSession, cluster_id: UUID) -> bool:
        """Основной метод. True = уникальный (нет аналогов во внешней БД)."""
        rep = await self._pick_representative(session, cluster_id)
        if rep is None or rep.embedding is None:
            logger.warning(f"[ExtFinder] cluster {cluster_id} без репрезентанта, пропускаем")
            return False

        logger.info(f"[ExtFinder] cluster={cluster_id} rep={rep.id}")

        # 1. Запрос к внешней API
        dtos = await self._adapter.search(
            category=rep.category,
            municipality=rep.calc_municipality,
            size_sqm=rep.size_sqm,
            size_tolerance=0.15,
        )
        logger.info(f"[ExtFinder] external candidates: {len(dtos)}")

        # 2. Кешируем
        await self._cache_externals(session, dtos)

        # 3. Векторизуем всё, что ещё без embedding
        await self._emb.refresh_external_cache_embeddings(session)

        # 4. Vector search против кэша, ограниченный только что загруженными external_ids
        if not dtos:
            is_unique = True
        else:
            external_ids = [d.external_id for d in dtos]
            sources = list({d.external_source for d in dtos})
            q = text("""
                SELECT external_id,
                       1 - (embedding <=> CAST(:emb AS vector)) AS similarity
                FROM external_property_cache
                WHERE external_source = ANY(CAST(:sources AS text[]))
                  AND external_id = ANY(CAST(:ids AS text[]))
                  AND embedding IS NOT NULL
                ORDER BY embedding <=> CAST(:emb AS vector)
                LIMIT 5
            """)
            res = await session.execute(q, {
                "emb": list(rep.embedding),
                "sources": sources,
                "ids": external_ids,
            })
            rows = res.fetchall()
            top_sim = rows[0].similarity if rows else 0.0
            is_unique = top_sim < self.UNIQUE_SIM_THRESHOLD
            logger.info(f"[ExtFinder] top_sim={top_sim:.3f} is_unique={is_unique}")

        # 5. Аудит на cluster
        cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == cluster_id)
        )).scalar_one()
        cluster.last_external_is_unique = is_unique
        cluster.last_external_check_at = datetime.now(timezone.utc)
        await session.commit()

        return is_unique