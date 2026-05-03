"""
Turns Property & ExternalPropertyCache rows into 1536-dim embeddings
(OpenAI text-embedding-3-small) and persists them back.

Performance notes:
  * We scan ALL active properties in memory to compare canonical hashes.
    To keep RAM bounded we DEFER the `embedding` column — that vector is
    ~6 KB per row and we only need to *replace* it, not read it.
  * Batches of 100 rows per OpenAI call with exponential back-off.
  * Commits per batch — if a batch explodes mid-way, already-embedded
    rows stay persisted.
"""
import asyncio
from typing import Sequence

from loguru import logger
from openai import APIError, AsyncOpenAI, RateLimitError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer

from src.core.config import settings
from src.models.domain import ExternalPropertyCache, Property
from src.services.canonicalizer import PropertyCanonicalizer
from src.services.cost_tracker import cost_tracker


class EmbeddingService:
    BATCH_SIZE = 100
    MAX_RETRIES = 4

    def __init__(self) -> None:
        self._client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        self._model = settings.OPENAI_EMBEDDING_MODEL

    # ----- OpenAI call with back-off ------------------------------
    async def _embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        delay = 2.0
        last_exc: Exception | None = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = await self._client.embeddings.create(
                    model=self._model,
                    input=list(texts),
                )
                # Record cost for the batch. OpenAI returns total
                # input tokens in usage; we attribute output=0 since
                # embeddings have no completion tokens.
                if resp.usage:
                    await cost_tracker.record_embedding(
                        in_tokens=resp.usage.total_tokens,
                        model=self._model,
                    )
                return [item.embedding for item in resp.data]
            except (RateLimitError, APIError) as e:
                last_exc = e
                logger.warning(
                    f"[Embedding] {type(e).__name__}, "
                    f"retry {attempt}/{self.MAX_RETRIES} in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
                delay *= 2
        raise RuntimeError(
            f"Embedding batch failed after {self.MAX_RETRIES} retries"
        ) from last_exc

    # ----- Properties ---------------------------------------------
    async def refresh_property_embeddings(self, session: AsyncSession) -> int:
        """
        For every active Property compute canonical text + SHA256,
        compare to stored content_hash, and re-embed where needed.
        """
        # Defer the heavy vector column — we never read it, just write.
        q = (
            select(Property)
            .options(defer(Property.embedding))
            .where(Property.is_active.is_(True))
        )
        rows = (await session.execute(q)).scalars().all()

        to_update: list[tuple[Property, str, str]] = []  # (prop, text, hash)
        for prop in rows:
            facts = PropertyCanonicalizer.from_property(prop)
            text, h = PropertyCanonicalizer.canonicalize(facts)
            if prop.content_hash != h or prop.content_hash is None:
                to_update.append((prop, text, h))

        if not to_update:
            logger.info("[Embedding] nothing to refresh")
            return 0

        logger.info(f"[Embedding] to refresh: {len(to_update)} properties")

        updated = 0
        for i in range(0, len(to_update), self.BATCH_SIZE):
            batch = to_update[i : i + self.BATCH_SIZE]
            try:
                vectors = await self._embed_batch([t for _, t, _ in batch])
            except Exception as e:
                logger.error(f"[Embedding] batch {i}..{i+len(batch)} failed: {e}")
                continue

            for (prop, _, h), vec in zip(batch, vectors):
                prop.embedding = vec
                prop.content_hash = h
                updated += 1

            await session.commit()
            logger.info(f"[Embedding] committed batch; running total: {updated}")

        logger.success(f"[Embedding] done: {updated} rows")
        return updated

    # ----- ExternalPropertyCache ----------------------------------
    async def refresh_external_cache_embeddings(
        self, session: AsyncSession
    ) -> int:
        q = (
            select(ExternalPropertyCache)
            .options(defer(ExternalPropertyCache.embedding))
            .where(ExternalPropertyCache.embedding.is_(None))
        )
        rows = (await session.execute(q)).scalars().all()
        if not rows:
            return 0

        updated = 0
        for i in range(0, len(rows), self.BATCH_SIZE):
            batch = rows[i : i + self.BATCH_SIZE]
            try:
                vectors = await self._embed_batch([r.canonical_text for r in batch])
            except Exception as e:
                logger.error(f"[Embedding/ext] batch failed: {e}")
                continue
            for r, vec in zip(batch, vectors):
                r.embedding = vec
                updated += 1
            await session.commit()

        logger.success(f"[Embedding/ext] done: {updated} rows")
        return updated