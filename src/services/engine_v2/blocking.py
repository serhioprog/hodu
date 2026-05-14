"""
Engine v2 blocking pipeline — RESEARCH §6.1 MVP.

Single function `get_candidate_pairs` returns canonical-ordered
(a_id < b_id) UUID pairs that pass cheap SQL-friendly filters.
Per-pair Tier 0/1 logic (canonical category, year_diff, feedback,
approved-cluster collision) is handled downstream by score_pair —
those rules need Python logic that doesn't translate cleanly to SQL.

Sophisticated ANN top-K blocking is deferred to Pass 5.5+ when the
property pool grows beyond ~10K and cross-municipality candidates
become a meaningful fraction.
"""
from __future__ import annotations

from uuid import UUID

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from src.models.domain import Property


async def get_candidate_pairs(session: AsyncSession) -> list[tuple[UUID, UUID]]:
    """Generate candidate pair pool via SQL blocking filters.

    Filters (Tier 0/1 SQL-friendly subset):
      1. is_active=true on both sides (skip listings that are gone)
      2. source_domain mismatch (cross-source — same-source pairs
         can never be duplicates per spec assumption)
      3. calc_municipality equal AND NOT NULL on both sides
         (proximity proxy; cross-municipality pairs ~always different)

    Canonical ordering p1.id < p2.id ensures each pair appears once.
    ORDER BY (p1.id, p2.id) provides deterministic output across runs.

    Per RESEARCH.md §6.1 MVP. See module docstring for what's NOT
    in this MVP (per-pair logic stays in score_pair).
    """
    p1 = aliased(Property)
    p2 = aliased(Property)
    stmt = (
        select(p1.id, p2.id)
        .join(p2, p1.id < p2.id)
        .where(
            p1.is_active == True,                                       # noqa: E712
            p2.is_active == True,                                       # noqa: E712
            p1.source_domain != p2.source_domain,
            p1.calc_municipality.is_not(None),
            p2.calc_municipality.is_not(None),
            p1.calc_municipality == p2.calc_municipality,
        )
        .order_by(p1.id, p2.id)
    )
    rows = (await session.execute(stmt)).all()
    pairs: list[tuple[UUID, UUID]] = [(a_id, b_id) for a_id, b_id in rows]
    logger.info(
        "[blocking] generated {n} candidate pairs "
        "(cross-source, same-municipality)",
        n=len(pairs),
    )
    return pairs
