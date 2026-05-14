"""
Engine v2 pair-result cache - RESEARCH.md §12.5.7 + HYBRID_DESIGN.md §4.2.

Per-pair cache stored in engine_pair_cache table. Read-first on every
score; write-back on cache miss. Cache hit avoids the T0-T3 cascade
(saving Tier 3 LLM cost in particular).

Cache key: canonical "lower_uuid:greater_uuid" string. UUID string
comparison is lexicographic = canonical (UUIDs are 36 fixed chars).

Invalidation triggers (any of):
  1. engine_version mismatch (cache row from older engine)
  2. content_hash mismatch on either side (property updated)
  3. expires_at past (optional TTL)

Explicit invalidation:
  - invalidate_pair_cache(a, b): admin re-evaluation triggers
  - invalidate_property_cache(p): property content_hash changes
                                  (cache rows for ALL pairs containing p)

Usage:
    cached = await get_cached_verdict(session, a, b, hash_a, hash_b)
    if cached is not None:
        return cached
    verdict = await engine.score_pair(features)
    await set_cached_verdict(session, a, b, hash_a, hash_b, verdict)
    return verdict
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from loguru import logger
from sqlalchemy import delete, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.engine_v2.config import thresholds as T
from src.models.domain import EnginePairCache, utcnow

from .dedup_report import EngineVerdict


def _make_pair_key(prop_a_id: UUID, prop_b_id: UUID) -> str:
    """Canonical "lower:greater" pair key.

    UUID string format is fixed-length; lexicographic sort over the
    string form gives a stable canonical ordering.
    """
    a, b = sorted([str(prop_a_id), str(prop_b_id)])
    return f"{a}:{b}"


def _canonicalize_hashes(
    prop_a_id: UUID,
    prop_b_id: UUID,
    a_content_hash: str,
    b_content_hash: str,
) -> tuple[str, str]:
    """Pair (hash_a, hash_b) with the canonical (lower, higher) UUID order.

    Caller passes hashes in the order matching their UUID args; cache
    stores them in the canonical pair_key order. This keeps the
    cache module the single authority for ordering — callers don't
    need to pre-sort.
    """
    if str(prop_a_id) <= str(prop_b_id):
        return a_content_hash, b_content_hash
    return b_content_hash, a_content_hash


async def get_cached_verdict(
    session: AsyncSession,
    prop_a_id: UUID,
    prop_b_id: UUID,
    a_content_hash: str,
    b_content_hash: str,
) -> EngineVerdict | None:
    """Cache lookup. Returns EngineVerdict on hit, None on miss/stale.

    A "miss" includes:
      - row absent
      - engine_version mismatch
      - content_hash mismatch on either side (property changed since cache)
      - expires_at in the past (TTL expired)
    """
    pk = _make_pair_key(prop_a_id, prop_b_id)
    canon_a_hash, canon_b_hash = _canonicalize_hashes(
        prop_a_id, prop_b_id, a_content_hash, b_content_hash,
    )

    row = (await session.execute(
        select(EnginePairCache).where(EnginePairCache.pair_key == pk)
    )).scalar_one_or_none()

    if row is None:
        logger.debug("[cache] miss (no row) pair_key={pk}", pk=pk[:17])
        return None

    if row.engine_version != T.ENGINE_VERSION:
        logger.debug(
            "[cache] miss (engine_version stale {old} != {new}) pair_key={pk}",
            old=row.engine_version, new=T.ENGINE_VERSION, pk=pk[:17],
        )
        return None

    if row.a_content_hash != canon_a_hash or row.b_content_hash != canon_b_hash:
        logger.debug("[cache] miss (content_hash stale) pair_key={pk}", pk=pk[:17])
        return None

    if row.expires_at is not None and row.expires_at <= utcnow():
        logger.debug("[cache] miss (expired) pair_key={pk}", pk=pk[:17])
        return None

    logger.debug("[cache] hit pair_key={pk} tier={t}", pk=pk[:17], t=row.tier_emitted)

    # Refinement 1: surface NULL confidence as data-quality warning.
    # Cache schema allows NULL but every write should populate it
    # (EngineVerdict.confidence is non-nullable float). NULL here
    # indicates a write-side bug — log and fall back to 0.0.
    if row.confidence is None:
        logger.warning(
            "[cache] cached row has NULL confidence pair_key={pk} "
            "verdict={v} - should not happen; check write-side",
            pk=pk[:17], v=row.verdict,
        )
        confidence = 0.0
    else:
        confidence = float(row.confidence)

    return EngineVerdict(
        verdict=row.verdict,                  # type: ignore[arg-type]
        confidence=confidence,
        reasoning=row.reasoning or "",
        tier_emitted=int(row.tier_emitted),
        cost_usd=float(row.cost_usd),         # Decimal -> float at boundary
        latency_ms=0.0,                       # cache hit has no scoring latency
    )


async def set_cached_verdict(
    session: AsyncSession,
    prop_a_id: UUID,
    prop_b_id: UUID,
    a_content_hash: str,
    b_content_hash: str,
    verdict: EngineVerdict,
    expires_at: datetime | None = None,
) -> None:
    """Cache write. Idempotent via ON CONFLICT (pair_key) DO UPDATE.

    Stores T.ENGINE_VERSION at write time. Concurrent writers safely
    overwrite each other (last write wins; values are deterministic
    per pair given fixed engine_version + content_hashes).
    """
    pk = _make_pair_key(prop_a_id, prop_b_id)
    canon_a_hash, canon_b_hash = _canonicalize_hashes(
        prop_a_id, prop_b_id, a_content_hash, b_content_hash,
    )
    now = utcnow()

    stmt = pg_insert(EnginePairCache).values(
        pair_key=pk,
        engine_version=T.ENGINE_VERSION,
        a_content_hash=canon_a_hash,
        b_content_hash=canon_b_hash,
        verdict=verdict.verdict,
        confidence=verdict.confidence,
        reasoning=verdict.reasoning,
        tier_emitted=verdict.tier_emitted,
        cost_usd=verdict.cost_usd,
        scored_at=now,
        expires_at=expires_at,
    ).on_conflict_do_update(
        index_elements=["pair_key"],
        set_={
            "engine_version":  T.ENGINE_VERSION,
            "a_content_hash":  canon_a_hash,
            "b_content_hash":  canon_b_hash,
            "verdict":         verdict.verdict,
            "confidence":      verdict.confidence,
            "reasoning":       verdict.reasoning,
            "tier_emitted":    verdict.tier_emitted,
            "cost_usd":        verdict.cost_usd,
            "scored_at":       now,
            "expires_at":      expires_at,
        },
    )
    await session.execute(stmt)
    logger.debug(
        "[cache] write pair_key={pk} verdict={v} tier={t}",
        pk=pk[:17], v=verdict.verdict, t=verdict.tier_emitted,
    )


async def invalidate_pair_cache(
    session: AsyncSession,
    prop_a_id: UUID,
    prop_b_id: UUID,
) -> int:
    """Delete cache row for one pair. Returns number of rows deleted (0 or 1).

    Use for: admin re-evaluation triggers, manual override flows.
    """
    pk = _make_pair_key(prop_a_id, prop_b_id)
    result = await session.execute(
        delete(EnginePairCache).where(EnginePairCache.pair_key == pk)
    )
    deleted = result.rowcount or 0
    logger.debug(
        "[cache] invalidate_pair pair_key={pk} deleted={d}", pk=pk[:17], d=deleted,
    )
    return deleted


async def invalidate_property_cache(
    session: AsyncSession,
    prop_id: UUID,
) -> int:
    """Delete all cache rows containing prop_id. Returns row count.

    Use when property content_hash changes (re-scrape with updated
    description/price/etc.). Pair cache rows for this property's
    pairs are now content-stale; clear them to force re-scoring.

    pair_key format is "<lower>:<higher>" - prop_id is at the start
    or end of string. UUID-string LIKE patterns are unambiguous since
    UUIDs are fixed-length 36-char strings with no ":" inside.
    """
    pid_str = str(prop_id)
    pattern_lower = f"{pid_str}:%"   # prop_id is lower side
    pattern_higher = f"%:{pid_str}"  # prop_id is higher side

    result = await session.execute(
        delete(EnginePairCache).where(
            or_(
                EnginePairCache.pair_key.like(pattern_lower),
                EnginePairCache.pair_key.like(pattern_higher),
            )
        )
    )
    deleted = result.rowcount or 0
    logger.debug(
        "[cache] invalidate_property prop={p} deleted={d}",
        p=pid_str[:8], d=deleted,
    )
    return deleted
