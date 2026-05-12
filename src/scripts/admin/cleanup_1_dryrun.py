"""
PROD CLEANUP §1 — 14-member Pefkohori mismerged cluster — DRY RUN.

Architect-approved decision (2026-05-07): FULL DISSOLVE cluster
c8fe6cdd (option (i) per Step 7 review).

Reasoning:
  - Spec §2.3 absolute violation (24-year construction span:
    2002-2026, far beyond 5-year limit)
  - Doc's "valid sub-pair" recommendations all violate spec §3.1
    (all 3 candidates same-source: 2475cb93 + 11e81e5e + 7aedddd0
    are all greekexclusive)
  - Auto-cluster + batch-lock pattern (NULL notes, 1.7-day gap to
    lock) != deliberate manual merge — admin batch-approved with
    oversight evidence (3 rejected pairs INSIDE the cluster left
    untouched at lock time)
  - 3743b417 / 6be3de8b precedents NOT applicable: those had
    populated notes (manual merge) and small member counts; this
    is auto-cluster + 14 members + 24-year span + admin oversight
  - Spec §11 forbids ENGINE auto-dissolve; architect manual
    cleanup via PROD_CLEANUP_TASKS.md scope is permitted

Cluster c8fe6cdd-3c14-4fe9-814c-91b6423d00c7:
  14 Villa members in Pefkohori, Kassandra
  status=APPROVED, verdict_locked=true (1097f747)
  notes: NULL
  3 feedback rows already exist within cluster (the §1+§5 overlap
  surfaced during §6+§7 verification on 2026-05-07): 7aedddd0
  paired with 84ac72f6 / eb3b9ac0 / eec9f9ae

Predicted impact:
  - 91 feedback inserts attempted (C(14,2))
  - 3 already exist -> ON CONFLICT DO NOTHING skips -> 88 new rows
  - 14 properties detached
  - 1 cluster deleted
  - APPROVED count 20 -> 19
  - Feedback count 349 -> 437
  - §5 contradictory pairs (currently 3 in c8fe6cdd) -> 0

Mirrors src/main.py:_dissolve_cluster_with_feedback (line 545).

Run via:
    docker compose exec -T scraper python -m src.scripts.admin.cleanup_1_dryrun
"""
from __future__ import annotations

import asyncio

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from src.database.db import async_session_maker
from src.models.domain import (
    AIDuplicateFeedback,
    ClusterStatus,
    Property,
    PropertyCluster,
)

# -----------------------------------------------------------------
# Architect-approved decision (2026-05-07)
# -----------------------------------------------------------------
DISSOLVE_CLUSTER = "c8fe6cdd-3c14-4fe9-814c-91b6423d00c7"

DRY_RUN: bool = True


# -----------------------------------------------------------------
# Helper — mirrors src/main.py:_dissolve_cluster_with_feedback
# -----------------------------------------------------------------
async def full_dissolve(session: AsyncSession, cluster_id: str) -> dict:
    """Mirrors src/main.py:_dissolve_cluster_with_feedback (line 545)."""
    bulk_insert_sql = text("""
        INSERT INTO ai_duplicate_feedbacks (id, prop_a_id, prop_b_id, hash_a, hash_b)
        SELECT
            gen_random_uuid()           AS id,
            LEAST(p1.id, p2.id)         AS prop_a_id,
            GREATEST(p1.id, p2.id)      AS prop_b_id,
            p1.content_hash             AS hash_a,
            p2.content_hash             AS hash_b
        FROM properties p1
        JOIN properties p2
          ON p2.cluster_id = p1.cluster_id
         AND p2.id > p1.id
        WHERE p1.cluster_id = :cid
          AND p1.content_hash IS NOT NULL
          AND p2.content_hash IS NOT NULL
        ON CONFLICT (prop_a_id, prop_b_id) DO NOTHING
        RETURNING id
    """)
    result = await session.execute(bulk_insert_sql, {"cid": cluster_id})
    feedback_count = len(result.fetchall())

    detach_result = await session.execute(
        update(Property)
        .where(Property.cluster_id == cluster_id)
        .values(cluster_id=None)
    )
    detached = detach_result.rowcount or 0

    await session.execute(
        delete(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )
    return {"feedback_inserted": feedback_count, "members_detached": detached}


# -----------------------------------------------------------------
# Helper — count current §5 contradictory pairs (APPROVED clusters
#   that contain pairs also in ai_duplicate_feedbacks)
# -----------------------------------------------------------------
async def count_s5_contradictions(session: AsyncSession) -> int:
    result = await session.execute(text("""
        SELECT COUNT(*) FROM ai_duplicate_feedbacks f
        JOIN properties pa ON pa.id = f.prop_a_id
        JOIN properties pb ON pb.id = f.prop_b_id
        WHERE pa.cluster_id IS NOT NULL
          AND pb.cluster_id IS NOT NULL
          AND pa.cluster_id = pb.cluster_id
    """))
    return result.scalar() or 0


# -----------------------------------------------------------------
# Main flow
# -----------------------------------------------------------------
async def main():
    print(f"=== PROD CLEANUP §1 — {'DRY RUN' if DRY_RUN else 'REAL RUN'} ===\n")

    async with async_session_maker() as session:
        # ---- Pre-state ----
        pre_feedback = await session.scalar(
            select(func.count()).select_from(AIDuplicateFeedback)
        )
        pre_approved = await session.scalar(
            select(func.count()).select_from(PropertyCluster)
            .where(PropertyCluster.status == ClusterStatus.APPROVED)
        )
        pre_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == DISSOLVE_CLUSTER)
        )).scalars().first()
        pre_members = (await session.execute(
            select(Property.id).where(Property.cluster_id == DISSOLVE_CLUSTER)
        )).scalars().all()
        pre_s5 = await count_s5_contradictions(session)

        print(f"PRE-STATE: ai_duplicate_feedbacks rows = {pre_feedback}")
        print(f"PRE-STATE: APPROVED clusters = {pre_approved}")
        print(f"PRE-STATE: cluster c8fe6cdd exists = {pre_cluster is not None}")
        if pre_cluster:
            print(f"           verdict_locked = {pre_cluster.verdict_locked}")
            print(f"           member_count   = {pre_cluster.member_count}")
        print(f"PRE-STATE: cluster members (n={len(pre_members)})")
        print(f"PRE-STATE: §5 contradictory pairs (APPROVED ∩ feedback) = {pre_s5}\n")

        # ---- OPERATION: FULL DISSOLVE ----
        print(f"--- OP: FULL DISSOLVE cluster {DISSOLVE_CLUSTER[:8]} ---")
        print(f"  Members count: {len(pre_members)}")
        c_n_2 = len(pre_members) * (len(pre_members) - 1) // 2
        print(f"  Will attempt feedback insert for C(14,2) = {c_n_2} pairs")
        print(f"  Of those, 3 already exist (§1+§5 overlap) -> ON CONFLICT DO NOTHING skips")
        print(f"  Expected new inserts: {c_n_2 - 3} = 88")
        result = await full_dissolve(session, DISSOLVE_CLUSTER)
        print(f"  -> feedback_inserted (new only): {result['feedback_inserted']}")
        print(f"  -> members_detached:             {result['members_detached']}")
        print(f"  -> cluster row:                  DELETED\n")

        # ---- Post-state (in-transaction) ----
        post_feedback = await session.scalar(
            select(func.count()).select_from(AIDuplicateFeedback)
        )
        post_approved = await session.scalar(
            select(func.count()).select_from(PropertyCluster)
            .where(PropertyCluster.status == ClusterStatus.APPROVED)
        )
        post_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == DISSOLVE_CLUSTER)
        )).scalars().first()
        post_members_clustered = (await session.execute(
            select(Property.id).where(Property.cluster_id == DISSOLVE_CLUSTER)
        )).scalars().all()
        post_s5 = await count_s5_contradictions(session)

        print(f"POST-STATE (in-transaction):")
        print(f"  ai_duplicate_feedbacks rows = {post_feedback}  "
              f"(delta {post_feedback - pre_feedback:+d})")
        print(f"  APPROVED clusters = {post_approved}  (delta {post_approved - pre_approved:+d})")
        print(f"  cluster c8fe6cdd exists = {post_cluster is not None}")
        print(f"  members still pointing at cluster = {len(post_members_clustered)}")
        print(f"  §5 contradictory pairs = {post_s5}  (delta {post_s5 - pre_s5:+d})\n")

        if DRY_RUN:
            await session.rollback()
            print("[DRY RUN] Transaction ROLLED BACK. No DB modifications committed.")
        else:
            await session.commit()
            print("[REAL RUN] Transaction COMMITTED.")

    print("\n=== SUMMARY ===")
    print(f"  feedback_rows_inserted: {result['feedback_inserted']}")
    print(f"  clusters_dissolved:     1")
    print(f"  properties_detached:    {result['members_detached']}")


if __name__ == "__main__":
    asyncio.run(main())
