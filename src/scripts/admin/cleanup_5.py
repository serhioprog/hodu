"""
PROD CLEANUP §5 — Contradictory APPROVED ∩ feedback pairs — REAL RUN.

Architect-approved decisions (2026-05-07) per pair:

  Pair 1 — 4ce33bb3 ↔ 80d17037 in cluster 1dc3b136:
    SPLIT — surgically remove 4ce33bb3 (size/BR/price outlier vs 3
    coherent boutique members). Feedback row stays valid; remove
    operation will additionally write feedback for 4ce33bb3 ↔ each
    remaining member (3 pairs total, 1 of which is the §5 row →
    ON CONFLICT DO UPDATE refresh).

  Pair 2 — 0e004fc3 ↔ b0f1df71 in cluster ad393e98:
    DELETE stale feedback row (cluster spec-compliant: 3 Pefkohori
    villas, year_diff=4, sizes 220-250m². Feedback was created
    3 minutes BEFORE cluster — admin override of own rejection).

  Pair 3 — 5f28b379 ↔ 85712784 in cluster becb1b01:
    FULL DISSOLVE 2-member cluster (admin rejected 30 seconds after
    auto-cluster creation — deliberate signal). Feedback row stays;
    full_dissolve will attempt 1 feedback insert which ON CONFLICT
    DO NOTHING skips.

Mirrors src/main.py helpers:
  _dissolve_cluster_with_feedback (line 545)        -> full dissolve path
  admin_cluster_remove_member endpoint (line 1046)  -> split/surgical removal

Audit trail: this script is the committed sibling of
src/scripts/admin/cleanup_5_dryrun.py — same logic verbatim, only
DRY_RUN flag and docstring differ. Dry-run output was reviewed by
architect on 2026-05-07 (deltas: +1 feedback / -1 APPROVED /
3 properties detached) before this real-run was authorized.

Run via:
    docker compose exec -T scraper python -m src.scripts.admin.cleanup_5
"""
from __future__ import annotations

import asyncio
from typing import List
from uuid import UUID

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
# Architect-approved decisions (2026-05-07)
# -----------------------------------------------------------------

# Pair 1 — SPLIT
SPLIT_CLUSTER = "1dc3b136-83a4-4df9-a813-d075ac5019db"
SPLIT_REMOVE_PROP = "4ce33bb3-83c9-4a0c-9191-d4d038c612a9"  # outlier

# Pair 2 — DELETE feedback only
DELETE_FB_PROP_A = "0e004fc3-b6cc-4d0f-8298-28deb8f825c1"
DELETE_FB_PROP_B = "b0f1df71-a19c-448e-b923-a0867c1c8ddb"
PRESERVE_CLUSTER_AD = "ad393e98-2cf8-42b5-921e-7bc6478034c6"  # untouched

# Pair 3 — FULL DISSOLVE
DISSOLVE_CLUSTER = "becb1b01-3517-436d-982d-79a2209b6866"

DRY_RUN: bool = False


# -----------------------------------------------------------------
# Helpers — mirror src/main.py exactly (same as cleanup_67_bundle)
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


async def remove_member(
    session: AsyncSession, cluster_id: str, property_id: str
) -> dict:
    """Mirrors src/main.py:admin_cluster_remove_member endpoint (line 1046)."""
    prop = (await session.execute(
        select(Property)
        .where(Property.id == property_id, Property.cluster_id == cluster_id)
    )).scalars().first()
    if not prop:
        return {"error": f"prop {property_id[:8]} not in cluster {cluster_id[:8]}"}

    remaining = (await session.execute(
        select(Property)
        .where(Property.cluster_id == cluster_id, Property.id != property_id)
    )).scalars().all()

    feedback_written = 0
    for r_prop in remaining:
        if not prop.content_hash or not r_prop.content_hash:
            continue
        a, b = (prop, r_prop) if prop.id < r_prop.id else (r_prop, prop)
        stmt = pg_insert(AIDuplicateFeedback).values(
            prop_a_id=a.id, prop_b_id=b.id,
            hash_a=a.content_hash, hash_b=b.content_hash,
        ).on_conflict_do_update(
            index_elements=["prop_a_id", "prop_b_id"],
            set_=dict(hash_a=a.content_hash, hash_b=b.content_hash, updated_at=func.now()),
        )
        await session.execute(stmt)
        feedback_written += 1

    prop.cluster_id = None

    cluster = (await session.execute(
        select(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )).scalars().first()
    cluster_deleted = False
    if cluster:
        cluster.member_count -= 1
        if cluster.member_count < 2:
            await session.execute(
                delete(PropertyCluster).where(PropertyCluster.id == cluster_id)
            )
            cluster_deleted = True
    return {
        "feedback_written": feedback_written,
        "remaining_count": len(remaining),
        "cluster_deleted_after": cluster_deleted,
    }


async def delete_feedback_pair(
    session: AsyncSession, prop_a: str, prop_b: str,
) -> dict:
    """Delete feedback row(s) for a pair, regardless of stored ordering.
    Per spec §3.4 retroactive cleanup: the feedback row should have
    been deleted when the cluster was approved. We delete it now."""
    a_uuid, b_uuid = UUID(prop_a), UUID(prop_b)
    result = await session.execute(
        delete(AIDuplicateFeedback).where(
            or_(
                and_(AIDuplicateFeedback.prop_a_id == a_uuid,
                     AIDuplicateFeedback.prop_b_id == b_uuid),
                and_(AIDuplicateFeedback.prop_a_id == b_uuid,
                     AIDuplicateFeedback.prop_b_id == a_uuid),
            )
        )
    )
    return {"feedback_deleted": result.rowcount or 0}


# -----------------------------------------------------------------
# Main flow
# -----------------------------------------------------------------
async def main():
    print(f"=== PROD CLEANUP §5 — {'DRY RUN' if DRY_RUN else 'REAL RUN'} ===\n")

    summary = {
        "feedback_rows_inserted_or_refreshed": 0,
        "feedback_rows_deleted": 0,
        "clusters_dissolved": 0,
        "clusters_split_modified": 0,
        "properties_detached": 0,
    }

    async with async_session_maker() as session:
        # ---- Pre-state ----
        pre_feedback = await session.scalar(
            select(func.count()).select_from(AIDuplicateFeedback)
        )
        pre_approved = await session.scalar(
            select(func.count()).select_from(PropertyCluster)
            .where(PropertyCluster.status == ClusterStatus.APPROVED)
        )
        pre_split_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == SPLIT_CLUSTER)
        )).scalars().first()
        pre_preserve_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == PRESERVE_CLUSTER_AD)
        )).scalars().first()
        pre_dissolve_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == DISSOLVE_CLUSTER)
        )).scalars().first()

        print(f"PRE-STATE: ai_duplicate_feedbacks rows = {pre_feedback}")
        print(f"PRE-STATE: APPROVED clusters = {pre_approved}")
        print(f"PRE-STATE: cluster 1dc3b136 (split target) member_count = "
              f"{pre_split_cluster.member_count if pre_split_cluster else 'MISSING'}")
        print(f"PRE-STATE: cluster ad393e98 (preserve)    member_count = "
              f"{pre_preserve_cluster.member_count if pre_preserve_cluster else 'MISSING'}")
        print(f"PRE-STATE: cluster becb1b01 (dissolve)    exists = "
              f"{pre_dissolve_cluster is not None}\n")

        # ---- OPERATION 1: SPLIT cluster 1dc3b136 ----
        print(f"--- OP 1: SPLIT cluster {SPLIT_CLUSTER[:8]} (remove {SPLIT_REMOVE_PROP[:8]}) ---")
        members_before = (await session.execute(
            select(Property.id).where(Property.cluster_id == SPLIT_CLUSTER)
        )).scalars().all()
        print(f"  Members before: {[str(m)[:8] for m in members_before]} (n={len(members_before)})")
        r1 = await remove_member(session, SPLIT_CLUSTER, SPLIT_REMOVE_PROP)
        print(f"  -> feedback_written: {r1['feedback_written']} statements "
              f"(vs {r1['remaining_count']} remaining)")
        print(f"  -> property {SPLIT_REMOVE_PROP[:8]} detached")
        print(f"  -> cluster collapsed (member_count<2): {r1['cluster_deleted_after']}\n")
        summary["feedback_rows_inserted_or_refreshed"] += r1["feedback_written"]
        summary["properties_detached"] += 1
        summary["clusters_split_modified"] += 1

        # ---- OPERATION 2: DELETE feedback (0e004fc3, b0f1df71) ----
        print(f"--- OP 2: DELETE feedback for ({DELETE_FB_PROP_A[:8]}, {DELETE_FB_PROP_B[:8]}) ---")
        r2 = await delete_feedback_pair(session, DELETE_FB_PROP_A, DELETE_FB_PROP_B)
        print(f"  -> feedback_deleted: {r2['feedback_deleted']}")
        print(f"  -> cluster ad393e98: untouched (verified post-state below)\n")
        summary["feedback_rows_deleted"] += r2["feedback_deleted"]

        # ---- OPERATION 3: FULL DISSOLVE cluster becb1b01 ----
        print(f"--- OP 3: FULL DISSOLVE cluster {DISSOLVE_CLUSTER[:8]} ---")
        members = (await session.execute(
            select(Property.id).where(Property.cluster_id == DISSOLVE_CLUSTER)
        )).scalars().all()
        print(f"  Members: {[str(m)[:8] for m in members]} (n={len(members)})")
        r3 = await full_dissolve(session, DISSOLVE_CLUSTER)
        print(f"  -> feedback_inserted (new only, ON CONFLICT skips existing): {r3['feedback_inserted']}")
        print(f"  -> members_detached: {r3['members_detached']}")
        print(f"  -> cluster row: DELETED\n")
        summary["feedback_rows_inserted_or_refreshed"] += r3["feedback_inserted"]
        summary["clusters_dissolved"] += 1
        summary["properties_detached"] += r3["members_detached"]

        # ---- Post-state (in-transaction) ----
        post_feedback = await session.scalar(
            select(func.count()).select_from(AIDuplicateFeedback)
        )
        post_approved = await session.scalar(
            select(func.count()).select_from(PropertyCluster)
            .where(PropertyCluster.status == ClusterStatus.APPROVED)
        )
        post_split_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == SPLIT_CLUSTER)
        )).scalars().first()
        post_preserve_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == PRESERVE_CLUSTER_AD)
        )).scalars().first()
        post_dissolve_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == DISSOLVE_CLUSTER)
        )).scalars().first()

        print(f"POST-STATE (in-transaction):")
        print(f"  ai_duplicate_feedbacks rows = {post_feedback}  "
              f"(delta {post_feedback - pre_feedback:+d})")
        print(f"  APPROVED clusters = {post_approved}  (delta {post_approved - pre_approved:+d})")
        print(f"  cluster 1dc3b136 member_count = "
              f"{post_split_cluster.member_count if post_split_cluster else 'DELETED'}")
        print(f"  cluster ad393e98 member_count = "
              f"{post_preserve_cluster.member_count if post_preserve_cluster else 'MISSING'} "
              f"(should be unchanged)")
        print(f"  cluster becb1b01 exists = {post_dissolve_cluster is not None}\n")

        if DRY_RUN:
            await session.rollback()
            print("[DRY RUN] Transaction ROLLED BACK. No DB modifications committed.")
        else:
            await session.commit()
            print("[REAL RUN] Transaction COMMITTED.")

    print("\n=== SUMMARY ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    asyncio.run(main())
