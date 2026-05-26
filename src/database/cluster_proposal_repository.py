"""
Repository for cluster_member_proposals (V1-AdminAuthority, Sprint 8).

Backend API for admin actions on proposals created by Engine 1's
_persist_component when new candidate members emerge for locked+APPROVED
clusters. See migration 016 and ClusterMemberProposal docstring.

Functions:
    save_proposal              — UPSERT helper for engine code
    list_pending_for_cluster   — admin queue view (per-cluster)
    list_pending_for_property  — inverse lookup
    get_by_id                  — fetch single
    approve_proposal           — admin APPROVE: add property to cluster
    reject_proposal            — admin REJECT: blacklist pairs in feedbacks
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from uuid import UUID

from loguru import logger
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.domain import (
    ClusterMemberProposal,
    ProposalStatus,
    Property,
    PropertyCluster,
    utcnow,
)


# =============================================================
# Engine-side helper: save (UPSERT) a proposal
# =============================================================

async def save_proposal(
    session: AsyncSession,
    cluster_id: UUID,
    property_id: UUID,
    ai_score: Optional[float],
    phash_matches: int,
    evidence_pairs: List[Dict[str, Any]],
) -> bool:
    """Insert proposal idempotently.

    The unique partial index `cmp_unique_pending` ensures only ONE PENDING
    proposal per (cluster, property) pair. ON CONFLICT DO NOTHING preserves
    the existing row's `proposed_at` and any other fields.

    Returns True if a new row was inserted, False if a PENDING row already
    existed (and was preserved).
    """
    result = await session.execute(
        text("""
            INSERT INTO cluster_member_proposals
              (cluster_id, property_id, ai_score, phash_matches, evidence_pairs)
            VALUES
              (:cluster_id, :property_id, :ai_score, :phash_matches,
               :evidence::jsonb)
            ON CONFLICT (cluster_id, property_id) WHERE status = 'PENDING'
              DO NOTHING
            RETURNING id
        """),
        {
            "cluster_id":    str(cluster_id),
            "property_id":   str(property_id),
            "ai_score":      ai_score,
            "phash_matches": phash_matches,
            "evidence":      json.dumps(evidence_pairs),
        },
    )
    row = result.first()
    return row is not None


# =============================================================
# Admin query helpers
# =============================================================

async def list_pending_for_cluster(
    session: AsyncSession,
    cluster_id: UUID,
) -> List[ClusterMemberProposal]:
    """All PENDING proposals targeting a specific cluster.

    Ordered by proposed_at DESC (newest first) for admin queue display.
    """
    result = await session.execute(
        select(ClusterMemberProposal)
        .where(
            ClusterMemberProposal.cluster_id == cluster_id,
            ClusterMemberProposal.status == ProposalStatus.PENDING,
        )
        .order_by(ClusterMemberProposal.proposed_at.desc())
    )
    return list(result.scalars().all())


async def list_pending_for_property(
    session: AsyncSession,
    property_id: UUID,
) -> List[ClusterMemberProposal]:
    """All PENDING proposals referencing a specific property.

    A property can have multiple PENDING proposals (different clusters
    want it). Per EC1, APPROVED clusters take precedence in the engine;
    admin can see all options here and decide.
    """
    result = await session.execute(
        select(ClusterMemberProposal)
        .where(
            ClusterMemberProposal.property_id == property_id,
            ClusterMemberProposal.status == ProposalStatus.PENDING,
        )
        .order_by(ClusterMemberProposal.proposed_at.desc())
    )
    return list(result.scalars().all())


async def get_by_id(
    session: AsyncSession,
    proposal_id: UUID,
) -> Optional[ClusterMemberProposal]:
    """Fetch a single proposal by ID."""
    result = await session.execute(
        select(ClusterMemberProposal)
        .where(ClusterMemberProposal.id == proposal_id)
    )
    return result.scalar_one_or_none()


# =============================================================
# Admin actions: APPROVE / REJECT
# =============================================================

async def approve_proposal(
    session: AsyncSession,
    proposal_id: UUID,
    agent_id: UUID,
    reason: Optional[str] = None,
) -> ClusterMemberProposal:
    """Admin APPROVE: add property to cluster, mark proposal APPROVED.

    Raises:
        ValueError — proposal not found, or already decided (not PENDING).
    """
    proposal = await get_by_id(session, proposal_id)
    if proposal is None:
        raise ValueError(f"Proposal {proposal_id} not found")
    if proposal.status != ProposalStatus.PENDING:
        raise ValueError(
            f"Proposal {proposal_id} already {proposal.status.value}, "
            f"cannot approve"
        )

    # 1. Assign property to cluster
    await session.execute(
        update(Property)
        .where(Property.id == proposal.property_id)
        .values(cluster_id=proposal.cluster_id)
    )

    # 2. Recompute cluster.member_count from authoritative source
    new_count = (await session.execute(
        select(func.count(Property.id))
        .where(Property.cluster_id == proposal.cluster_id)
    )).scalar_one()
    await session.execute(
        update(PropertyCluster)
        .where(PropertyCluster.id == proposal.cluster_id)
        .values(member_count=new_count, updated_at=utcnow())
    )

    # 3. Mark proposal APPROVED
    proposal.status          = ProposalStatus.APPROVED
    proposal.decided_at      = utcnow()
    proposal.decided_by      = agent_id
    proposal.decision_reason = reason

    logger.info(
        f"[ProposalRepo] APPROVED proposal {str(proposal_id)[:8]}: "
        f"property {str(proposal.property_id)[:8]} -> cluster "
        f"{str(proposal.cluster_id)[:8]} (member_count={new_count})"
    )
    return proposal


async def reject_proposal(
    session: AsyncSession,
    proposal_id: UUID,
    agent_id: UUID,
    reason: Optional[str] = None,
) -> ClusterMemberProposal:
    """Admin REJECT: blacklist all evidence pairs, mark proposal REJECTED.

    Cluster + property are otherwise unchanged. The blacklist entries in
    ai_duplicate_feedbacks ensure the engine won't re-propose these pairs
    in future runs.

    Raises:
        ValueError — proposal not found, or already decided (not PENDING).
    """
    proposal = await get_by_id(session, proposal_id)
    if proposal is None:
        raise ValueError(f"Proposal {proposal_id} not found")
    if proposal.status != ProposalStatus.PENDING:
        raise ValueError(
            f"Proposal {proposal_id} already {proposal.status.value}, "
            f"cannot reject"
        )

    # Blacklist each evidence pair via ai_duplicate_feedbacks.
    # We need content_hash from both properties (table NOT NULL constraint).
    # Use single SQL with JOIN to fetch hashes and INSERT atomically.
    evidence = proposal.evidence_pairs or []
    nm_id = str(proposal.property_id)

    blacklist_count = 0
    for pair_info in evidence:
        member_id = pair_info.get("member_id")
        if not member_id:
            continue

        # Canonical ordering: a < b
        a, b = (nm_id, member_id) if nm_id < member_id else (member_id, nm_id)

        await session.execute(
            text("""
                INSERT INTO ai_duplicate_feedbacks
                  (id, prop_a_id, prop_b_id, hash_a, hash_b, feedback_source)
                SELECT
                    gen_random_uuid(),
                    :a::uuid,
                    :b::uuid,
                    pa.content_hash,
                    pb.content_hash,
                    'proposal_reject'
                FROM properties pa, properties pb
                WHERE pa.id = :a::uuid
                  AND pb.id = :b::uuid
                  AND pa.content_hash IS NOT NULL
                  AND pb.content_hash IS NOT NULL
                ON CONFLICT (prop_a_id, prop_b_id) DO NOTHING
            """),
            {"a": a, "b": b},
        )
        blacklist_count += 1

    # Mark proposal REJECTED
    proposal.status          = ProposalStatus.REJECTED
    proposal.decided_at      = utcnow()
    proposal.decided_by      = agent_id
    proposal.decision_reason = reason

    logger.info(
        f"[ProposalRepo] REJECTED proposal {str(proposal_id)[:8]}: "
        f"blacklisted {blacklist_count} pairs from property "
        f"{nm_id[:8]} to cluster {str(proposal.cluster_id)[:8]} members"
    )
    return proposal
