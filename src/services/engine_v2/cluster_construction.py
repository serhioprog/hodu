"""
DSU cluster construction - RESEARCH.md §12.5.9.

Bridges scoring (pair verdicts) to production output (property_clusters
writes). Engine never directly emits clusters - this module assembles
EngineVerdicts into clusters using a disjoint-set-union over the
property pool.

Algorithm:
  1. Pre-seed APPROVED + verdict_locked=True clusters as forced unions
     (admin authority, spec §11)
  2. Apply DUPLICATE pair verdicts as DSU unions; UNCERTAIN/DIFFERENT no-op
  3. Multi-cluster-bridge detection: when a pair would join two distinct
     pre-existing approved clusters, BLOCK the union and emit a
     BridgeBlockEvent (writer.py converts to mismerge_flag)
  4. Materialize components -> list[ProposedCluster]; singletons skipped
  5. Cluster ID reuse: component containing approved cluster's members
     reuses that cluster_id (attachment); else fresh UUID (new cluster)

This module DOES NOT write to DB. It returns ClusterBuildResult; writer.py
(Phase F) writes property_clusters and emits mismerge_flags.

Cache is NOT touched here - pairs are already scored before reaching DSU.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable
from uuid import UUID, uuid4

from loguru import logger
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.domain import ClusterStatus, Property, PropertyCluster

from .dedup_report import VerdictLiteral


# =============================================================
# DSU HELPER (path compression + union by rank)
# =============================================================

class DSU:
    """Disjoint-set union with path compression + union by rank.

    Items must be hashable (UUIDs in our case). add() is idempotent
    and lazy — items can join the universe over time. union() returns
    True if a merge actually happened, False if x and y were already
    in the same set.

    components() snapshots the current state as {root_id: [member_ids]}.
    Python dict insertion order (3.7+) gives deterministic iteration,
    which propagates to ProposedCluster ordering downstream.
    """

    def __init__(self, items: Iterable[UUID] = ()) -> None:
        self.parent: dict[UUID, UUID] = {}
        self.rank: dict[UUID, int] = {}
        for x in items:
            self.add(x)

    def add(self, x: UUID) -> None:
        """Add x to the universe. Idempotent — no-op if already present."""
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: UUID) -> UUID:
        """Return root of x's set, with path compression.

        Auto-adds x if not yet in universe (defensive — callers may
        union previously-unseen properties).
        """
        if x not in self.parent:
            self.add(x)
            return x
        # Iterative path traversal to find root (avoids recursion-depth risk
        # if the tree degenerates before union-by-rank kicks in).
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        # Second pass: point every node directly at root (path compression).
        cur = x
        while self.parent[cur] != root:
            nxt = self.parent[cur]
            self.parent[cur] = root
            cur = nxt
        return root

    def union(self, x: UUID, y: UUID) -> bool:
        """Union sets containing x and y. Returns True if a merge happened.

        Union by rank: shallower tree attaches under deeper. Returns
        False if x and y were already in the same component (no-op).
        """
        rx = self.find(x)
        ry = self.find(y)
        if rx == ry:
            return False
        if self.rank[rx] < self.rank[ry]:
            self.parent[rx] = ry
        elif self.rank[rx] > self.rank[ry]:
            self.parent[ry] = rx
        else:
            self.parent[ry] = rx
            self.rank[rx] += 1
        return True

    def components(self) -> dict[UUID, list[UUID]]:
        """Materialize {root_id: [members]} for all items in the universe.

        Iteration order: insertion order of items (Python dict default).
        Members within a component appear in the order they were added.
        """
        out: dict[UUID, list[UUID]] = {}
        for x in self.parent:
            root = self.find(x)
            out.setdefault(root, []).append(x)
        return out


# =============================================================
# DATACLASSES — public input + output types
# =============================================================

@dataclass(frozen=True)
class ScoredPair:
    """One pair-level scoring result for DSU input.

    Slimmer than EngineVerdict — DSU only needs the verdict + confidence
    + the two property IDs. Caller wraps EngineVerdict + (a_id, b_id).

    NOTE: prop_a_id, prop_b_id NOT canonically ordered. Caller passes
    whatever order is convenient; the builder canonicalises internally.
    """
    prop_a_id: UUID
    prop_b_id: UUID
    verdict: VerdictLiteral
    confidence: float


@dataclass(frozen=True)
class ProposedCluster:
    """One cluster proposed by DSU construction.

    is_attachment=True when cluster_id matches an existing APPROVED
    cluster (writer.py preserves ai_score per §12.5.13).
    is_attachment=False when cluster_id is a fresh UUID for a new
    PENDING cluster.

    ai_score semantics:
      - new cluster (is_attachment=False): float = mean of pair
        confidences across DUPLICATE pairs in the component
      - attachment (is_attachment=True): None — writer preserves the
        existing approved cluster's ai_score per spec §12.5.13.
        Type-system enforced; writer guards on `if ai_score is not None`.
    """
    cluster_id: UUID
    member_ids: list[UUID]
    ai_score: float | None
    is_attachment: bool


@dataclass(frozen=True)
class BridgeBlockEvent:
    """New-property multi-cluster-bridge block, per RESEARCH.md §12.5.9.

    A new property had DUPLICATE verdicts to members of multiple
    approved clusters. Engine attaches it to the winner; emits this
    event so writer.py writes one mismerge_flag (flag_type=
    'multi_cluster_bridge') row per loser cluster.

    For approved-to-approved DUPLICATE verdicts (admin separated two
    clusters but engine sees them as one), see ApprovedDisagreement.
    """
    new_property_id: UUID
    winner_cluster_id: UUID
    winner_mean_conf: float
    losers: list[tuple[UUID, float]]   # (cluster_id, mean_conf)


@dataclass(frozen=True)
class ApprovedDisagreement:
    """Pair where both properties are already in distinct APPROVED clusters
    AND the engine has emitted a DUPLICATE verdict for them.

    Spec §11: engine cannot auto-dissolve approved clusters. Engine
    emits mismerge_flag (flag_type='engine_t0_disagrees') so admin
    sees that the engine disagrees with the existing separation.
    Distinct from BridgeBlockEvent (which is about a NEW property
    bridging approved clusters).
    """
    cluster_a_id: UUID
    cluster_b_id: UUID
    prop_a_id: UUID
    prop_b_id: UUID
    confidence: float


@dataclass
class ClusterBuildResult:
    """Output of DSUClusterBuilder.build().

    Mutable so callers can sort, filter, or extend (e.g. tests may
    add diagnostic notes). Members are themselves frozen
    (ProposedCluster, BridgeBlockEvent, ApprovedDisagreement) so the
    result is structurally safe even when the outer container is mutated.
    """
    new_clusters: list[ProposedCluster]
    bridge_blocks: list[BridgeBlockEvent]
    approved_disagreements: list[ApprovedDisagreement]


# Internal-only: approved cluster snapshot loaded from DB
@dataclass(frozen=True)
class _ApprovedCluster:
    """Snapshot of one APPROVED (or verdict_locked=True) cluster + its members.

    Loaded once at start of DSUClusterBuilder.build() and used for:
      - DSU pre-seeding (§12.5.9 step 1)
      - Bridge detection (member -> cluster lookup via
        DSUClusterBuilder._member_to_cluster)
      - Bridge winner tiebreaker (created_at)
    """
    cluster_id: UUID
    member_ids: frozenset[UUID]
    created_at: datetime


# =============================================================
# DSUClusterBuilder — main entry point
# =============================================================

class DSUClusterBuilder:
    """Build clusters from scored pair verdicts, per RESEARCH.md §12.5.9.

    Single-use object — construct with run's scored pairs, then call
    build(). build() loads approved clusters from DB (read-only),
    builds DSU, applies verdicts with bridge detection, materializes
    components.

    DOES NOT write to DB. ClusterBuildResult passes to writer.py.

    Usage:
        scored_pairs = [
            ScoredPair(a, b, "duplicate", 0.92),
            ScoredPair(c, d, "different", 1.0),
        ]
        builder = DSUClusterBuilder(scored_pairs)
        result = await builder.build(session)
    """

    def __init__(self, scored_pairs: list[ScoredPair]) -> None:
        self._pairs = scored_pairs
        # Populated during build():
        self._approved: dict[UUID, _ApprovedCluster] = {}
        self._member_to_cluster: dict[UUID, UUID] = {}
        # Canonical pair_conf for _materialize ai_score:
        # key is sorted (lower_uuid, higher_uuid) tuple.
        self._pair_conf: dict[tuple[UUID, UUID], float] = {}

    async def build(self, session: AsyncSession) -> ClusterBuildResult:
        """Run the full DSU pipeline.

        Steps:
          1. _load_approved_clusters — DB query (read-only)
          2. _seed_dsu — forced unions for approved members
          3. _apply_verdicts — DUPLICATE unions + bridge detection
          4. _materialize — components -> ProposedClusters

        Returns ClusterBuildResult; writer.py (Phase F) handles all DB
        writes (property_clusters + mismerge_flags). This method does
        NOT write to DB.
        """
        await self._load_approved_clusters(session)
        dsu = self._seed_dsu()
        bridges, disagreements = self._apply_verdicts(dsu)
        new_clusters = self._materialize(dsu)

        logger.info(
            "[cluster_build] FINAL: {n_new} clusters proposed "
            "({n_attach} attachments, {n_fresh} new), "
            "{n_bridges} bridges blocked, "
            "{n_dis} approved disagreements",
            n_new=len(new_clusters),
            n_attach=sum(1 for c in new_clusters if c.is_attachment),
            n_fresh=sum(1 for c in new_clusters if not c.is_attachment),
            n_bridges=len(bridges),
            n_dis=len(disagreements),
        )

        return ClusterBuildResult(
            new_clusters=new_clusters,
            bridge_blocks=bridges,
            approved_disagreements=disagreements,
        )

    # ----------------------------------------------------------------
    # E-4: load approved cluster snapshots
    # ----------------------------------------------------------------

    async def _load_approved_clusters(
        self, session: AsyncSession,
    ) -> None:
        """Populate self._approved and self._member_to_cluster.

        Loads:
          - PropertyCluster rows where status=APPROVED OR verdict_locked=True
          - For each cluster: properties.id WHERE properties.cluster_id = cluster.id

        Per spec §11: APPROVED AND verdict_locked clusters both sacred.
        Engine never breaks these via DUPLICATE unions.

        Mutates state:
          self._approved        : cluster_id -> _ApprovedCluster snapshot
          self._member_to_cluster : property_id -> cluster_id (reverse map)

        Returns None.
        """
        # 1. Load APPROVED OR verdict_locked clusters
        cluster_stmt = select(PropertyCluster).where(
            or_(
                PropertyCluster.status == ClusterStatus.APPROVED,
                PropertyCluster.verdict_locked == True,                # noqa: E712
            )
        )
        cluster_rows = (await session.execute(cluster_stmt)).scalars().all()

        if not cluster_rows:
            logger.info("[cluster_build] no APPROVED/verdict_locked clusters loaded")
            return

        # 2. Bulk-load all members across all approved clusters (single query)
        cluster_ids = [c.id for c in cluster_rows]
        members_stmt = select(Property.id, Property.cluster_id).where(
            Property.cluster_id.in_(cluster_ids)
        )
        member_rows = (await session.execute(members_stmt)).all()

        # 3. Group members by cluster_id
        members_by_cluster: dict[UUID, list[UUID]] = {}
        for prop_id, cluster_id in member_rows:
            members_by_cluster.setdefault(cluster_id, []).append(prop_id)

        # 4. Build _ApprovedCluster snapshots + reverse map
        for cluster in cluster_rows:
            members = members_by_cluster.get(cluster.id, [])
            if not members:
                # APPROVED cluster with zero members — skip (defensive;
                # post-PROD-cleanup state shouldn't have any).
                logger.warning(
                    "[cluster_build] APPROVED cluster {cid} has 0 members — skipping",
                    cid=str(cluster.id)[:8],
                )
                continue

            snapshot = _ApprovedCluster(
                cluster_id=cluster.id,
                member_ids=frozenset(members),
                created_at=cluster.created_at,
            )
            self._approved[cluster.id] = snapshot
            for m in members:
                self._member_to_cluster[m] = cluster.id

        logger.info(
            "[cluster_build] loaded {n_clusters} approved clusters with "
            "{n_members} total members",
            n_clusters=len(self._approved),
            n_members=len(self._member_to_cluster),
        )

    # ----------------------------------------------------------------
    # E-5: pre-seed DSU with forced unions for approved members
    # ----------------------------------------------------------------

    def _seed_dsu(self) -> DSU:
        """Build DSU pre-seeded with forced unions for approved members.

        For each approved cluster: add all its members to DSU + union
        them under one root. This enforces spec §11 (admin authority
        sacred — engine never breaks approved clusters via subsequent
        DUPLICATE unions).

        Anchor-based union strategy: pick first member as anchor; union
        each subsequent member to it. O(N) unions per N-member cluster
        (vs O(N²) for pairwise). DSU's union-by-rank keeps tree balanced
        regardless of order.

        Returns: DSU instance with all approved members pre-seeded.
        """
        dsu = DSU()

        if not self._approved:
            logger.debug("[cluster_build] DSU seeded with 0 approved clusters")
            return dsu

        n_unions = 0
        for cluster_id, snapshot in self._approved.items():
            members = list(snapshot.member_ids)
            for m in members:
                dsu.add(m)
            if len(members) > 1:
                anchor = members[0]
                for m in members[1:]:
                    if dsu.union(anchor, m):
                        n_unions += 1

        logger.info(
            "[cluster_build] DSU seeded with {n_clusters} approved clusters "
            "({n_members} members, {n_unions} unions performed)",
            n_clusters=len(self._approved),
            n_members=len(self._member_to_cluster),
            n_unions=n_unions,
        )
        return dsu

    # ----------------------------------------------------------------
    # E-6 helpers
    # ----------------------------------------------------------------

    def _record_pair_conf(self, pair: ScoredPair) -> None:
        """Record this pair's confidence under canonical (lower, higher) UUID key.

        Used by _materialize to compute ai_score = mean(pair confidences)
        across all DUPLICATE pairs in a new cluster's component.
        """
        if pair.prop_a_id < pair.prop_b_id:
            key = (pair.prop_a_id, pair.prop_b_id)
        else:
            key = (pair.prop_b_id, pair.prop_a_id)
        self._pair_conf[key] = pair.confidence

    def _find_approved_in_component(
        self, dsu: DSU, prop_id: UUID,
    ) -> UUID | None:
        """Find approved cluster_id IF any approved member is in prop_id's
        DSU component.

        Used by Phase 3 transitive-bridge detection (FIX C).

        Invariant maintained by Phases 1-2: each DSU component contains
        at most ONE approved cluster's members. So returning first
        approved match is sufficient — no need to scan further.

        Optimisation: iterates over self._member_to_cluster (approved
        members only), not all DSU members. Faster when approved/total
        ratio is low (typical production case).
        """
        target_root = dsu.find(prop_id)
        for approved_member, cluster_id in self._member_to_cluster.items():
            if dsu.find(approved_member) == target_root:
                return cluster_id
        return None

    # ----------------------------------------------------------------
    # E-6: apply DUPLICATE pair verdicts with bridge detection
    # ----------------------------------------------------------------

    def _apply_verdicts(
        self, dsu: DSU,
    ) -> tuple[list[BridgeBlockEvent], list[ApprovedDisagreement]]:
        """Apply DUPLICATE pair verdicts to DSU; emit bridge / disagreement events.

        Three-phase internal algorithm:

          Phase 1 — Build intent map (no DSU mutation):
              Classify each DUPLICATE pair into Cases 1-4.

          Phase 2 — Resolve attachments + bridges:
              Single-cluster intent -> union to anchor.
              Multi-cluster intent -> bridge winner + emit BridgeBlockEvent.

          Phase 3 — Apply unclustered-unclustered DUPLICATE pairs WITH
                    transitive-bridge detection (FIX C):
              For each (P_a, P_b) pair where both unclustered at scan
              time: check if union would transitively merge two
              approved-containing components. If yes -> emit
              ApprovedDisagreement, do NOT union. Else -> union safely.

        Anchor selection (FIX B): min(member_ids) for determinism
        across runs, not next(iter(...)) which depends on frozenset
        hash order.

        Returns:
            (bridge_blocks, approved_disagreements)
        """
        intent: dict[UUID, dict[UUID, list[float]]] = {}
        approved_disagreements: list[ApprovedDisagreement] = []
        unclustered_unclustered_pairs: list[ScoredPair] = []

        # --- PHASE 1: classify each DUPLICATE pair -----------------
        for pair in self._pairs:
            if pair.verdict != "duplicate":
                continue

            a_cluster = self._member_to_cluster.get(pair.prop_a_id)
            b_cluster = self._member_to_cluster.get(pair.prop_b_id)

            # Case 1: both in same approved cluster — already pre-seeded
            if a_cluster is not None and b_cluster is not None and a_cluster == b_cluster:
                continue

            # Case 2: both in DIFFERENT approved clusters — direct disagreement
            if a_cluster is not None and b_cluster is not None and a_cluster != b_cluster:
                approved_disagreements.append(ApprovedDisagreement(
                    cluster_a_id=a_cluster,
                    cluster_b_id=b_cluster,
                    prop_a_id=pair.prop_a_id,
                    prop_b_id=pair.prop_b_id,
                    confidence=pair.confidence,
                ))
                continue

            # Case 3: one in approved, other unclustered — record intent
            if a_cluster is not None and b_cluster is None:
                intent.setdefault(pair.prop_b_id, {}).setdefault(
                    a_cluster, []
                ).append(pair.confidence)
                self._record_pair_conf(pair)
                continue
            if a_cluster is None and b_cluster is not None:
                intent.setdefault(pair.prop_a_id, {}).setdefault(
                    b_cluster, []
                ).append(pair.confidence)
                self._record_pair_conf(pair)
                continue

            # Case 4: both unclustered — defer to Phase 3
            unclustered_unclustered_pairs.append(pair)

        # --- PHASE 2: resolve intents (single attach OR bridge) ---
        bridge_blocks: list[BridgeBlockEvent] = []

        for prop_id, cluster_confs in intent.items():
            if len(cluster_confs) == 1:
                # Single-cluster attachment
                target_cluster_id = next(iter(cluster_confs.keys()))
                target_snapshot = self._approved[target_cluster_id]
                anchor = min(target_snapshot.member_ids)              # FIX B
                dsu.union(prop_id, anchor)
                continue

            # Multi-cluster bridge
            winner_cluster_id, winner_mean = _pick_bridge_winner(
                cluster_confs, self._approved,
            )
            winner_snapshot = self._approved[winner_cluster_id]
            winner_anchor = min(winner_snapshot.member_ids)           # FIX B
            dsu.union(prop_id, winner_anchor)

            losers: list[tuple[UUID, float]] = [
                (cid, sum(confs) / len(confs))
                for cid, confs in cluster_confs.items()
                if cid != winner_cluster_id
            ]
            bridge_blocks.append(BridgeBlockEvent(
                new_property_id=prop_id,
                winner_cluster_id=winner_cluster_id,
                winner_mean_conf=winner_mean,
                losers=losers,
            ))
            logger.warning(
                "[cluster_build] bridge blocked: prop={p} winner={w} "
                "(conf={wc:.3f}) losers={n_losers}",
                p=str(prop_id)[:8],
                w=str(winner_cluster_id)[:8],
                wc=winner_mean,
                n_losers=len(losers),
            )

        # --- PHASE 3: unclustered-unclustered DUPLICATE pairs with
        #              transitive-bridge detection (FIX C) ----------
        for pair in unclustered_unclustered_pairs:
            self._record_pair_conf(pair)

            a_approved = self._find_approved_in_component(dsu, pair.prop_a_id)
            b_approved = self._find_approved_in_component(dsu, pair.prop_b_id)

            if (
                a_approved is not None
                and b_approved is not None
                and a_approved != b_approved
            ):
                # Transitive bridge — would merge two approved components
                approved_disagreements.append(ApprovedDisagreement(
                    cluster_a_id=a_approved,
                    cluster_b_id=b_approved,
                    prop_a_id=pair.prop_a_id,
                    prop_b_id=pair.prop_b_id,
                    confidence=pair.confidence,
                ))
                logger.warning(
                    "[cluster_build] transitive bridge blocked: pair={a} <-> {b}, "
                    "would merge approved {ca} with {cb}",
                    a=str(pair.prop_a_id)[:8],
                    b=str(pair.prop_b_id)[:8],
                    ca=str(a_approved)[:8],
                    cb=str(b_approved)[:8],
                )
                continue

            # Safe to union (no transitive bridge)
            dsu.union(pair.prop_a_id, pair.prop_b_id)

        logger.info(
            "[cluster_build] applied verdicts: {n_dup} DUPLICATE pairs, "
            "{n_bridges} bridges blocked, {n_disagreements} approved-disagreements",
            n_dup=sum(1 for p in self._pairs if p.verdict == "duplicate"),
            n_bridges=len(bridge_blocks),
            n_disagreements=len(approved_disagreements),
        )

        return bridge_blocks, approved_disagreements

    # ----------------------------------------------------------------
    # E-7: materialize DSU components into ProposedClusters
    # ----------------------------------------------------------------

    def _materialize(self, dsu: DSU) -> list[ProposedCluster]:
        """Walk DSU components, build list[ProposedCluster].

        Per RESEARCH.md §12.5.9 steps 4-5:
          - Singleton components (len == 1): SKIP — no row written
            (spec §6.1: a property with no duplicates is not a "cluster of one")
          - Multi-member component containing approved cluster's members:
              ATTACHMENT — reuse approved cluster_id, ai_score=None
              (writer preserves existing approved ai_score per §12.5.13)
          - Multi-member component, no approved members:
              NEW CLUSTER — fresh UUID, ai_score=mean(pair_conf)

        Invariant (maintained by Phases 1-3): each multi-member
        component contains AT MOST ONE approved cluster's members.
        Detection of approved-ness uses self._member_to_cluster lookup;
        first match suffices.

        Returns: list[ProposedCluster].
        """
        components = dsu.components()
        proposals: list[ProposedCluster] = []

        for root, members in components.items():
            if len(members) == 1:
                continue                                       # singleton — skip

            # Detect approved-cluster membership (first match suffices per invariant)
            approved_cluster_id: UUID | None = None
            for m in members:
                cid = self._member_to_cluster.get(m)
                if cid is not None:
                    approved_cluster_id = cid
                    break

            if approved_cluster_id is not None:
                # ATTACHMENT — reuse approved cluster_id, preserve ai_score
                proposals.append(ProposedCluster(
                    cluster_id=approved_cluster_id,
                    member_ids=list(members),
                    ai_score=None,
                    is_attachment=True,
                ))
                continue

            # NEW CLUSTER — compute ai_score = mean(pair_conf in component)
            pair_confs: list[float] = []
            for i in range(len(members)):
                for j in range(i + 1, len(members)):
                    a, b = members[i], members[j]
                    key = (min(a, b), max(a, b))
                    if key in self._pair_conf:
                        pair_confs.append(self._pair_conf[key])

            if pair_confs:
                ai_score = sum(pair_confs) / len(pair_confs)
            else:
                # Defensive: every multi-member component should have ≥1 DUPLICATE
                # pair contributing to its formation. If we hit this branch,
                # something upstream is wrong.
                logger.warning(
                    "[cluster_build] component has no recorded pair_conf, "
                    "ai_score=0.0 (members={n})",
                    n=len(members),
                )
                ai_score = 0.0

            proposals.append(ProposedCluster(
                cluster_id=uuid4(),
                member_ids=list(members),
                ai_score=ai_score,
                is_attachment=False,
            ))

        return proposals


# =============================================================
# Module-level helper: bridge-winner selection
# =============================================================

def _pick_bridge_winner(
    cluster_confs: dict[UUID, list[float]],
    approved: dict[UUID, _ApprovedCluster],
) -> tuple[UUID, float]:
    """Pick winner cluster per RESEARCH.md §12.5.9.

    Winner = cluster with highest mean(confidences) across the bridging
    pairs. Tiebreaker (equal mean within EPSILON): cluster with earliest
    created_at. Both criteria deterministic.

    EPSILON guard (FIX A) avoids float-equality flakiness: two means
    that differ in the 16th decimal place from float arithmetic should
    tie-break on created_at, not pick by spurious order.

    Returns: (winner_cluster_id, winner_mean_conf).
    """
    EPSILON = 1e-9
    means = {cid: sum(cs) / len(cs) for cid, cs in cluster_confs.items()}
    max_mean = max(means.values())
    tied = [cid for cid, m in means.items() if abs(m - max_mean) < EPSILON]
    if len(tied) == 1:
        return tied[0], max_mean
    winner = min(tied, key=lambda cid: approved[cid].created_at)
    return winner, max_mean
