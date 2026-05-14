"""
Internal duplicate detector — multi-level deduplication funnel.

Pipeline (Партия 5):
  1. Quality Gate    — filter unfit pairs at SQL level (Level 0)
  2. Hard Pre-filter — feedback blacklist, price/size/desc filters (Level 1)
  3. Embedding       — pgvector cosine similarity via HNSW (Level 2)
  4. Smart pHash     — image hash matching, ignoring stock photos (Level 3)
  5. Vision Tie-Break — GPT-4o Vision for gray-zone pairs (Level 4)
  6. DSU components  — union approved-edge endpoints
  7. Persist         — write clusters with metrics

Vision tie-breaker activates when:
  * settings.VISION_TIEBREAKER_ENABLED is True
  * pair has verdict='merge_pending' after Levels 2+3
  * we haven't exceeded VISION_MAX_PAIRS_PER_RUN this run

For each Vision-decided pair:
  * is_same=True  + confidence ≥ threshold → 'merge_approved' (joins DSU)
  * is_same=False + confidence ≥ threshold → 'reject_vision'  (writes feedback)
  * confidence below threshold              → stays 'merge_pending'
"""
from __future__ import annotations

import collections
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

from loguru import logger
from sqlalchemy import text, select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.models.domain import (
    ClusterStatus, Property, PropertyCluster, PropertyStatus,
    utcnow,
)
from src.services.phash_service import PHashService
from src.services.vision_tiebreaker import VisionTiebreaker


# =============================================================
# SQL: candidate pair selection (Levels 0+1+2 fused)
# Identical to Партия 3 — no changes here.
# =============================================================
_PAIR_SQL = text("""
WITH eligible AS (
    SELECT
        id, embedding, image_phashes, source_domain, cluster_id,
        price, size_sqm, description, content_hash
    FROM properties
    WHERE
        embedding IS NOT NULL
        AND content_hash IS NOT NULL
        AND status IN ('ACTIVE', 'NEW', 'PRICE_CHANGED', 'DELISTED')
        AND calc_municipality IS NOT NULL
        AND category IS NOT NULL
        AND (bedrooms IS NOT NULL OR land_size_sqm IS NOT NULL)
        AND LENGTH(COALESCE(description, '')) >= 50
)
SELECT
    p1.id            AS a_id,
    p2.id            AS b_id,
    p1.image_phashes AS a_phashes,
    p2.image_phashes AS b_phashes,
    1 - (p1.embedding <=> p2.embedding) AS similarity
FROM eligible p1
CROSS JOIN LATERAL (
    SELECT id, embedding, image_phashes, source_domain, cluster_id,
           price, size_sqm, content_hash
    FROM eligible p2_inner
    WHERE
        p2_inner.id > p1.id
        AND p2_inner.source_domain != p1.source_domain
        AND 1 - (p1.embedding <=> p2_inner.embedding) > :sim_reject
        AND (
            p1.price IS NULL OR p2_inner.price IS NULL
            OR ABS(p1.price - p2_inner.price)::FLOAT
               / GREATEST(p1.price, p2_inner.price) < 0.30
        )
        AND (
            p1.size_sqm IS NULL OR p2_inner.size_sqm IS NULL
            OR ABS(p1.size_sqm - p2_inner.size_sqm)
               / GREATEST(p1.size_sqm, p2_inner.size_sqm) < 0.15
        )
        AND NOT EXISTS (
            SELECT 1 FROM ai_duplicate_feedbacks f
            WHERE (f.prop_a_id = p1.id AND f.prop_b_id = p2_inner.id)
               OR (f.prop_a_id = p2_inner.id AND f.prop_b_id = p1.id)
        )
        -- Skip pairs already in the SAME cluster (whether APPROVED or
        -- still PENDING). For APPROVED clusters this prevents re-asking
        -- Vision about pairs we already paid to decide. For PENDING
        -- clusters it prevents waste while admin is still reviewing.
        --
        -- We don't filter pairs that are in DIFFERENT clusters — those
        -- represent a genuine "should these two clusters merge?" question
        -- worth re-evaluating as new properties arrive.
        AND NOT (
            p1.cluster_id IS NOT NULL
            AND p2_inner.cluster_id IS NOT NULL
            AND p1.cluster_id = p2_inner.cluster_id
        )
    ORDER BY p1.embedding <=> p2_inner.embedding
    LIMIT :per_p1_limit
) p2
ORDER BY similarity DESC
""")


_STOCK_PHASH_SQL = text("""
SELECT phash
FROM (
    SELECT
        unnest(image_phashes)        AS phash,
        COUNT(DISTINCT id)           AS prop_count
    FROM properties
    WHERE
        image_phashes IS NOT NULL
        AND array_length(image_phashes, 1) > 0
    GROUP BY 1
) hash_counts
WHERE prop_count > :min_count
""")


# =============================================================
# DSU
# =============================================================
class _DSU:
    __slots__ = ("_parent",)

    def __init__(self) -> None:
        self._parent: Dict[str, str] = {}

    def add(self, x: str) -> None:
        if x not in self._parent:
            self._parent[x] = x

    def find(self, x: str) -> str:
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[x] != root:
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[ra] = rb

    def components(self) -> Dict[str, List[str]]:
        comps: Dict[str, List[str]] = collections.defaultdict(list)
        for node in self._parent:
            comps[self.find(node)].append(node)
        return comps


# =============================================================
# Edge metadata
# =============================================================
@dataclass
class _EdgeMeta:
    """One classified pair with verdict, similarity, pHash, and Vision trace."""
    a_id: str
    b_id: str
    similarity: float
    phash_matches: int
    verdict: str  # 'merge_approved' | 'merge_pending' | 'reject_vision'
    vision_trace: Optional[str] = None  # human-readable for logs/debugging


# =============================================================
# Detector
# =============================================================
class InternalDuplicateDetector:
    """Multi-level deduplication funnel with optional Vision tie-breaker."""

    def __init__(self) -> None:
        # Vision client is lazy: instantiated only if enabled at run time.
        self._vision: Optional[VisionTiebreaker] = None

    async def run(self, session: AsyncSession) -> Dict[str, int]:
        stats: Dict[str, int] = {
            "approved_merged":     0,
            "approved_singleton_skipped":  0,
            "pending":             0,
            "locked_preserved":    0,
            "orphans_removed":     0,
            "sold_cleaned":    0,
            "vision_resolved":     0,   # Vision verdicts honored (approve+reject)
            "vision_skipped":      0,   # low-confidence or call failed
            "vision_feedback_added": 0, # rejects written to AIDuplicateFeedback
        }

        stats["sold_cleaned"] = await self._release_sold_clusters(session)

        stock_phashes = await self._build_stock_phashes(session)
        logger.info(
            f"[Matcher] stock pHash filter: {len(stock_phashes)} hashes "
            f"appear on >{settings.PHASH_STOCK_MIN_PROPS} different listings"
        )

        # --- collect candidate pairs --------------------------------------
        logger.info("[Matcher] collecting candidate pairs from pgvector…")
        rows = (await session.execute(
            _PAIR_SQL,
            {
                "sim_reject":   settings.SIM_REJECT,
                "per_p1_limit": settings.MAX_PAIRS_PER_PROPERTY,
            },
        )).fetchall()
        logger.info(f"[Matcher] found pairs > {settings.SIM_REJECT}: {len(rows)}")

        # --- Levels 2+3: pre-Vision classification ------------------------
        edges = self._classify_pairs(rows, stock_phashes)

        pending_count = sum(1 for e in edges if e.verdict == "merge_pending")
        logger.info(
            f"[Matcher] pre-Vision: "
            f"approved={sum(1 for e in edges if e.verdict == 'merge_approved')} "
            f"pending={pending_count}"
        )

        # --- Level 4: Vision tie-breaker ----------------------------------
        if settings.VISION_TIEBREAKER_ENABLED and pending_count > 0:
            await self._apply_vision_tiebreaker(session, edges, stats)

        # --- DSU ----------------------------------------------------------
        dsu = _DSU()
        edges_by_pair: Dict[Tuple[str, str], _EdgeMeta] = {}

        for edge in edges:
            # reject_vision edges DO NOT participate in clustering at all —
            # they're filed in AIDuplicateFeedback and that's it.
            if edge.verdict == "reject_vision":
                continue
            dsu.add(edge.a_id)
            dsu.add(edge.b_id)
            if edge.verdict == "merge_approved":
                dsu.union(edge.a_id, edge.b_id)
            edges_by_pair[(edge.a_id, edge.b_id)] = edge

        components = dsu.components()
        logger.info(f"[Matcher] components after union-find: {len(components)}")

        # --- persist clusters ---------------------------------------------
        for member_ids in components.values():
            comp_stats = await self._persist_component(
                session, member_ids, edges_by_pair
            )
            for k, v in comp_stats.items():
                if k in stats:
                    stats[k] += v

        # CRITICAL: flush ORM state before raw-SQL orphan delete.
        # Otherwise, raw DELETE may see in-memory clusters as "orphan"
        # because their member assignments haven't been written yet.
        await session.flush()

        stats["orphans_removed"] = await self._delete_orphan_clusters(session)

        await session.commit()
        logger.success(f"[Matcher] done: {stats}")
        return stats

    # =============================================================
    # Level 4: Vision tie-breaker
    # =============================================================
    async def _apply_vision_tiebreaker(
        self,
        session: AsyncSession,
        edges: List[_EdgeMeta],
        stats: Dict[str, int],
    ) -> None:
        """
        For up to N pending pairs, ask GPT-4o Vision to decide.

        Mutates `edges` in place: changes verdict for confidently-resolved
        pairs. Low-confidence pairs are left as 'merge_pending'.

        For 'reject_vision' verdicts, also writes a row to
        ai_duplicate_feedbacks so the next matcher run won't re-propose them.
        """
        if self._vision is None:
            self._vision = VisionTiebreaker()

        # Pick up to MAX_PAIRS_PER_RUN, sorted by similarity DESC
        # (high-similarity ambiguous pairs are most likely actual dups —
        # spend Vision budget on them first)
        pending_edges = [e for e in edges if e.verdict == "merge_pending"]
        # Bug #58: deterministic order when similarities tie. Previously
        # ties produced non-deterministic Vision budget allocation (which
        # 50 of the tied pairs got the budget depended on iteration order).
        # Stable tiebreaker: prop_a_id alphabetical, then prop_b_id.
        pending_edges.sort(key=lambda e: (-e.similarity, str(e.a_id), str(e.b_id)))
        budget = pending_edges[: settings.VISION_MAX_PAIRS_PER_RUN]

        logger.info(
            f"[Matcher/Vision] processing {len(budget)} pairs "
            f"(out of {len(pending_edges)} pending)"
        )

        for edge in budget:
            verdict = await self._vision.decide_pair(session, edge.a_id, edge.b_id)
            if verdict is None:
                stats["vision_skipped"] += 1
                continue

            if verdict.confidence < settings.VISION_CONFIDENCE_THRESHOLD:
                stats["vision_skipped"] += 1
                edge.vision_trace = (
                    f"low_conf={verdict.confidence:.2f}: {verdict.reason}"
                )
                continue

            # Authoritative Vision verdict
            stats["vision_resolved"] += 1
            edge.vision_trace = (
                f"is_same={verdict.is_same} conf={verdict.confidence:.2f}: "
                f"{verdict.reason}"
            )

            if verdict.is_same:
                edge.verdict = "merge_approved"
            else:
                edge.verdict = "reject_vision"
                # Write feedback so we never re-propose this pair
                await self._record_vision_reject(session, edge)
                stats["vision_feedback_added"] += 1

    async def _record_vision_reject(
        self,
        session: AsyncSession,
        edge: _EdgeMeta,
    ) -> None:
        """Write a Vision-rejected pair to AIDuplicateFeedback (idempotent)."""
        # Get content_hashes (required by the table's NOT NULL constraint)
        rows = (await session.execute(
            select(Property.id, Property.content_hash)
            .where(Property.id.in_([edge.a_id, edge.b_id]))
        )).all()
        hashes = {str(pid): h for pid, h in rows}

        ha = hashes.get(edge.a_id)
        hb = hashes.get(edge.b_id)
        if not ha or not hb:
            logger.warning(
                f"[Vision] cannot record feedback for {edge.a_id[:8]}<>{edge.b_id[:8]}: "
                f"missing content_hash"
            )
            return

        # Normalize order
        a, b, ha_n, hb_n = (
            (edge.a_id, edge.b_id, ha, hb)
            if edge.a_id < edge.b_id
            else (edge.b_id, edge.a_id, hb, ha)
        )

        await session.execute(
            text("""
                INSERT INTO ai_duplicate_feedbacks
                  (id, prop_a_id, prop_b_id, hash_a, hash_b)
                VALUES (gen_random_uuid(), :a, :b, :ha, :hb)
                ON CONFLICT (prop_a_id, prop_b_id) DO NOTHING
            """),
            {"a": a, "b": b, "ha": ha_n, "hb": hb_n},
        )

    # =============================================================
    # Level 3: stock pHash
    # =============================================================
    async def _build_stock_phashes(self, session: AsyncSession) -> Set[str]:
        rows = (await session.execute(
            _STOCK_PHASH_SQL,
            {"min_count": settings.PHASH_STOCK_MIN_PROPS},
        )).fetchall()
        return {r[0] for r in rows if r[0]}

    # =============================================================
    # Levels 2+3: pre-Vision classification
    # =============================================================
    def _classify_pairs(
        self,
        rows: Iterable,
        stock_phashes: Set[str],
    ) -> List[_EdgeMeta]:
        edges: List[_EdgeMeta] = []

        for row in rows:
            a_id        = str(row.a_id)
            b_id        = str(row.b_id)
            similarity  = float(row.similarity)
            a_phashes   = list(row.a_phashes or [])
            b_phashes   = list(row.b_phashes or [])

            phash_matches = PHashService.count_matching(
                a_phashes, b_phashes, common_to_ignore=stock_phashes
            )

            if similarity > settings.SIM_AUTO_MERGE:
                verdict = "merge_approved"
            elif phash_matches >= settings.PHASH_MIN_MATCHES:
                verdict = "merge_approved"
                logger.info(
                    f"[Matcher] pHash bypass {a_id[:8]}<>{b_id[:8]} "
                    f"({phash_matches} matches, sim={similarity:.3f})"
                )
            else:
                verdict = "merge_pending"

            edges.append(_EdgeMeta(
                a_id=a_id, b_id=b_id,
                similarity=similarity,
                phash_matches=phash_matches,
                verdict=verdict,
            ))

        return edges

    # =============================================================
    # Cluster persistence — unchanged from Партия 3
    # =============================================================
    async def _persist_component(
        self,
        session: AsyncSession,
        member_ids: List[str],
        edges_by_pair: Dict[Tuple[str, str], _EdgeMeta],
    ) -> Dict[str, int]:
        out = {
            "approved_merged":    0,
            "approved_singleton_skipped": 0,
            "pending":            0,
            "locked_preserved":   0,
        }

        if len(member_ids) == 1:
            # Spec §3.3: "1 = singleton, typically not stored as cluster".
            # RESEARCH.md §12.5.9 (engine v2) explicitly forbids singleton
            # cluster rows. Disabled per architect decision 2026-05-07
            # (PROD_CLEANUP_TASKS.md §3 fix).
            # Property remains with cluster_id=NULL (its natural singleton
            # representation).
            out["approved_singleton_skipped"] = 1
            return out

        props = (await session.execute(
            select(Property).where(Property.id.in_(member_ids))
        )).scalars().all()
        if len(props) < 2:
            return out

        component_set = set(member_ids)
        edges_in_comp = [
            e for (a, b), e in edges_by_pair.items()
            if a in component_set and b in component_set
        ]
        max_sim = max((e.similarity for e in edges_in_comp), default=None)
        max_phash = max((e.phash_matches for e in edges_in_comp), default=0)
        any_pending = any(e.verdict == "merge_pending" for e in edges_in_comp)

        existing_cluster_ids = {p.cluster_id for p in props if p.cluster_id}
        locked_cluster: Optional[PropertyCluster] = None
        if existing_cluster_ids:
            locked = (await session.execute(
                select(PropertyCluster).where(
                    PropertyCluster.id.in_(existing_cluster_ids),
                    PropertyCluster.verdict_locked.is_(True),
                )
            )).scalars().first()
            if locked:
                locked_cluster = locked

        if locked_cluster is not None:
            for p in props:
                p.cluster_id = locked_cluster.id
            # Bug #66 fix: recompute member_count from the authoritative source
            # (Property.cluster_id), not from len(props). The connected component
            # being promoted may contain only a SUBSET of the locked cluster's
            # true membership; using len(props) would undercount and leave
            # member_count desynced from reality until the next cluster-write.
            await session.flush()
            locked_cluster.member_count = (await session.execute(
                select(func.count(Property.id))
                .where(Property.cluster_id == locked_cluster.id)
            )).scalar_one()
            locked_cluster.ai_score = max_sim
            locked_cluster.phash_matches = max_phash
            locked_cluster.updated_at = utcnow()
            out["locked_preserved"] = 1
            return out

        new_status = ClusterStatus.PENDING if any_pending else ClusterStatus.APPROVED

        target_cluster: Optional[PropertyCluster] = None
        if existing_cluster_ids:
            existing = (await session.execute(
                select(PropertyCluster).where(
                    PropertyCluster.id.in_(existing_cluster_ids),
                    PropertyCluster.verdict_locked.is_(False),
                )
            )).scalars().first()
            if existing:
                target_cluster = existing

        if target_cluster is None:
            target_cluster = PropertyCluster(
                status=new_status,
                member_count=len(props),       # corrected after flush below (Bug #66)
                ai_score=max_sim,
                phash_matches=max_phash,
            )
            session.add(target_cluster)
            await session.flush()
        else:
            target_cluster.status        = new_status
            # member_count is recomputed after the cluster_id assignment loop
            # below (Bug #66) — see comment there for rationale.
            target_cluster.ai_score      = max_sim
            target_cluster.phash_matches = max_phash
            target_cluster.updated_at    = utcnow()

        for p in props:
            p.cluster_id = target_cluster.id

        # Bug #66 fix: recompute member_count from the authoritative source
        # (Property.cluster_id) after cluster_id reassignments are flushed.
        # When target_cluster pre-existed, len(props) only captures the current
        # connected component and misses pre-existing members not in this run's
        # edges. For a freshly created cluster this resolves to the same value
        # as the initial len(props), so the recompute is harmless there.
        await session.flush()
        target_cluster.member_count = (await session.execute(
            select(func.count(Property.id))
            .where(Property.cluster_id == target_cluster.id)
        )).scalar_one()

        if new_status == ClusterStatus.APPROVED:
            out["approved_merged"] = 1
        else:
            out["pending"] = 1

        return out

    async def _release_sold_clusters(self, session: AsyncSession) -> int:
        """Detach SOLD properties from their clusters. Bug #46: this was
        misnamed _release_delisted_clusters but actually filters on SOLD
        status only (DELISTED clusters are released elsewhere via the
        revival flow in daily_sync). Renamed to match what it does."""
        result = await session.execute(
            update(Property)
            .where(
                Property.cluster_id.is_not(None),
                Property.status == PropertyStatus.SOLD,
            )
            .values(cluster_id=None)
        )
        count = result.rowcount or 0
        if count:
            logger.info(f"[Matcher] released cluster_id from {count} SOLD properties")
        return count

    async def _delete_orphan_clusters(self, session: AsyncSession) -> int:
        result = await session.execute(text("""
            DELETE FROM property_clusters c
            WHERE c.verdict_locked = false
              AND NOT EXISTS (
                  SELECT 1 FROM properties p WHERE p.cluster_id = c.id
              )
            RETURNING c.id
        """))
        count = len(result.fetchall())
        if count:
            logger.info(f"[Matcher] removed {count} orphan clusters")
        return count