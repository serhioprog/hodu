"""
PROD CLEANUP §2 — Apartment+Villa cross-label cluster — REAL RUN.

Architect-approved decision (2026-05-07): DISSOLVE cluster 6be3de8b
(option (b) per Step 6 review).

Reasoning:
  - Spec §2.3 absolute: Villa vs Apartment never the same
  - Spec §2.4: prefer UNCERTAIN over wrong DUPLICATE
  - Evidence severity: 3× size disparity (200m² vs 600m²),
    3× bedrooms (4 vs 12), single villa vs multi-unit apartment
    building — fundamentally different property types
  - 3743b417 (§7) preservation stands as border case (similar sizes);
    6be3de8b is qualitatively more incompatible
  - Spec §11 forbids ENGINE auto-dissolve; architect manual cleanup
    via PROD_CLEANUP_TASKS.md scope is permitted
  - Equal admin authority != equal evidence quality

Cluster verdict_locked=true, manual-merge by admin 1097f747-... on
2026-05-04 23:08. Members:
  - 924cbe59 (gl): Villa, 200m², 4BR, €1.1M, single-family villa
  - 8746accd (rec): Apartment building, 600m², 12BR, €1.3M, 3-floor
                    multi-unit rental property

No feedback row exists for this pair (verified Q4 in investigation)
— clean dissolve writes 1 new row.

Mirrors src/main.py:_dissolve_cluster_with_feedback (line 545).

Audit trail: this script is the committed sibling of
src/scripts/admin/cleanup_2_dryrun.py — same logic verbatim, only
DRY_RUN flag and docstring differ. Dry-run output was reviewed by
architect on 2026-05-07 (deltas: +1 feedback / -1 APPROVED /
2 properties detached) before this real-run was authorized.

Run via:
    docker compose exec -T scraper python -m src.scripts.admin.cleanup_2
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
DISSOLVE_CLUSTER = "6be3de8b-e19e-448e-b735-9335a5da52f8"

DRY_RUN: bool = False


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
# Main flow
# -----------------------------------------------------------------
async def main():
    print(f"=== PROD CLEANUP §2 — {'DRY RUN' if DRY_RUN else 'REAL RUN'} ===\n")

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

        print(f"PRE-STATE: ai_duplicate_feedbacks rows = {pre_feedback}")
        print(f"PRE-STATE: APPROVED clusters = {pre_approved}")
        print(f"PRE-STATE: cluster 6be3de8b exists = {pre_cluster is not None}")
        if pre_cluster:
            print(f"           verdict_locked = {pre_cluster.verdict_locked}")
            print(f"           member_count   = {pre_cluster.member_count}")
        print(f"PRE-STATE: cluster members = {[str(m)[:8] for m in pre_members]} (n={len(pre_members)})\n")

        # ---- OPERATION: FULL DISSOLVE ----
        print(f"--- OP: FULL DISSOLVE cluster {DISSOLVE_CLUSTER[:8]} ---")
        print(f"  Members: {[str(m)[:8] for m in pre_members]}")
        print(f"  Will write feedback for C(2,2) = 1 pair")
        result = await full_dissolve(session, DISSOLVE_CLUSTER)
        print(f"  -> feedback_inserted: {result['feedback_inserted']}")
        print(f"  -> members_detached:  {result['members_detached']}")
        print(f"  -> cluster row:       DELETED\n")

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

        print(f"POST-STATE (in-transaction):")
        print(f"  ai_duplicate_feedbacks rows = {post_feedback}  "
              f"(delta {post_feedback - pre_feedback:+d})")
        print(f"  APPROVED clusters = {post_approved}  (delta {post_approved - pre_approved:+d})")
        print(f"  cluster 6be3de8b exists = {post_cluster is not None}")
        print(f"  members still pointing at cluster = {len(post_members_clustered)}\n")

        if DRY_RUN:
            await session.rollback()
            print("[DRY RUN] Transaction ROLLED BACK. No DB modifications committed.")
        else:
            await session.commit()
            print("[REAL RUN] Transaction COMMITTED.")

    print("\n=== SUMMARY ===")
    print(f"  feedback_rows_inserted:     {result['feedback_inserted']}")
    print(f"  clusters_dissolved:         1")
    print(f"  properties_detached:        {result['members_detached']}")


if __name__ == "__main__":
    asyncio.run(main())
