"""
PROD CLEANUP §6 + §7 BUNDLE — REAL RUN cleanup for cluster mismerges.

Architect-approved decisions (2026-05-07):
  - 28cc6eb3 -> FULL DISSOLVE (no spec-compliant pair)
  - 3743b417 -> PRESERVE (verdict_locked, spec §11 sacred)
  - 3862a14a -> SPLIT (keep [adf72a7c, 75fd5495], dissolve 3 rec members)
  - 79658b5e -> FULL DISSOLVE (multi-unit dev != singles)
  - 7c1787ec -> FULL DISSOLVE (3 distinct properties)

Mirrors src/main.py helpers:
  _dissolve_cluster_with_feedback (line 545)        -> full dissolve path
  admin_cluster_remove_member endpoint (line 1046)  -> split/surgical removal

REAL RUN MODE: session.commit() at end. Changes ARE persisted.

Audit trail: this script is the committed sibling of
src/scripts/admin/cleanup_67_bundle_dryrun.py — same logic verbatim,
only DRY_RUN flag and docstring differ. Dry-run output was reviewed
by architect on 2026-05-07 before this real-run was authorized.

Run via:
    docker compose exec -T scraper python -m src.scripts.admin.cleanup_67_bundle
"""
from __future__ import annotations

import asyncio
from typing import List
from uuid import UUID

from sqlalchemy import delete, func, select, update
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
DISSOLVE_CLUSTERS: List[str] = [
    "28cc6eb3-556b-43db-84b2-ec0a926b9302",
    "79658b5e-7fb3-4bbe-b393-8ab22d538979",
    "7c1787ec-12b5-4c8d-83ab-2135a465327d",
]

SPLIT_CLUSTER: str = "3862a14a-290f-4e1d-8571-fae30ef9f1dc"
SPLIT_KEEP_IDS: List[str] = [
    "adf72a7c-21f4-4dfc-8a04-62c0d1d8d03b",
    "75fd5495-e9f1-43e8-b214-54caa6db5ba4",
]

PRESERVE_CLUSTER: str = "3743b417-e684-43a9-bd8c-792acc233cb4"  # verdict_locked

DRY_RUN: bool = False


# -----------------------------------------------------------------
# Helpers — mirror src/main.py exactly
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


# -----------------------------------------------------------------
# Main flow
# -----------------------------------------------------------------
async def main():
    print(f"=== PROD CLEANUP §6 + §7 BUNDLE — {'DRY RUN' if DRY_RUN else 'REAL RUN'} ===\n")

    summary = {
        "feedback_rows_inserted": 0,
        "clusters_fully_dissolved": 0,
        "clusters_split_modified": 0,
        "properties_detached": 0,
    }

    async with async_session_maker() as session:
        pre_feedback = await session.scalar(
            select(func.count()).select_from(AIDuplicateFeedback)
        )
        pre_approved = await session.scalar(
            select(func.count()).select_from(PropertyCluster)
            .where(PropertyCluster.status == ClusterStatus.APPROVED)
        )
        print(f"PRE-STATE: ai_duplicate_feedbacks rows = {pre_feedback}")
        print(f"PRE-STATE: APPROVED clusters = {pre_approved}\n")

        # -- FULL DISSOLVES
        for cid in DISSOLVE_CLUSTERS:
            print(f"--- FULL DISSOLVE: cluster {cid[:8]} ---")
            members = (await session.execute(
                select(Property.id).where(Property.cluster_id == cid)
            )).scalars().all()
            print(f"  Members: {[str(m)[:8] for m in members]}  (n={len(members)})")
            print(f"  Will write feedback for C({len(members)},2) = "
                  f"{len(members) * (len(members) - 1) // 2} pairs (modulo content_hash gaps)")
            result = await full_dissolve(session, cid)
            print(f"  -> feedback_inserted: {result['feedback_inserted']}")
            print(f"  -> members_detached:  {result['members_detached']}")
            print(f"  -> cluster row:       DELETED\n")
            summary["feedback_rows_inserted"] += result["feedback_inserted"]
            summary["clusters_fully_dissolved"] += 1
            summary["properties_detached"] += result["members_detached"]

        # -- SPLIT (3862a14a)
        print(f"--- SPLIT: cluster {SPLIT_CLUSTER[:8]} ---")
        all_members = (await session.execute(
            select(Property.id).where(Property.cluster_id == SPLIT_CLUSTER)
        )).scalars().all()
        keep_set = {UUID(x) for x in SPLIT_KEEP_IDS}
        to_remove = [m for m in all_members if m not in keep_set]
        print(f"  All members:  {[str(m)[:8] for m in all_members]}  (n={len(all_members)})")
        print(f"  Keep:         {[x[:8] for x in SPLIT_KEEP_IDS]}")
        print(f"  Remove:       {[str(m)[:8] for m in to_remove]}  (n={len(to_remove)})")

        for prop_id in to_remove:
            r = await remove_member(session, SPLIT_CLUSTER, str(prop_id))
            print(f"  Removing {str(prop_id)[:8]}:")
            print(f"    feedback_written: {r['feedback_written']} (vs {r['remaining_count']} remaining)")
            print(f"    cluster collapsed mid-removal: {r['cluster_deleted_after']}")
            summary["feedback_rows_inserted"] += r["feedback_written"]
            summary["properties_detached"] += 1

        final_cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == SPLIT_CLUSTER)
        )).scalars().first()
        if final_cluster:
            status_str = (
                final_cluster.status.value
                if hasattr(final_cluster.status, "value")
                else str(final_cluster.status)
            )
            print(f"  Final cluster state: member_count={final_cluster.member_count}, "
                  f"status={status_str}\n")
            summary["clusters_split_modified"] += 1
        else:
            print(f"  Cluster collapsed entirely (members < 2)\n")

        # -- PRESERVE (3743b417) — verify untouched
        preserved = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == PRESERVE_CLUSTER)
        )).scalars().first()
        print(f"--- PRESERVE: cluster {PRESERVE_CLUSTER[:8]} ---")
        if preserved:
            print(f"  Status: untouched (member_count={preserved.member_count}, "
                  f"verdict_locked={preserved.verdict_locked})\n")
        else:
            print("  WARNING: preserved cluster missing!\n")

        post_feedback = await session.scalar(
            select(func.count()).select_from(AIDuplicateFeedback)
        )
        post_approved = await session.scalar(
            select(func.count()).select_from(PropertyCluster)
            .where(PropertyCluster.status == ClusterStatus.APPROVED)
        )
        print(f"POST-STATE (in-transaction): ai_duplicate_feedbacks rows = {post_feedback}  "
              f"(delta +{post_feedback - pre_feedback})")
        print(f"POST-STATE (in-transaction): APPROVED clusters = {post_approved}  "
              f"(delta {post_approved - pre_approved})")

        if DRY_RUN:
            await session.rollback()
            print("\n[DRY RUN] Transaction ROLLED BACK. No DB modifications committed.")
        else:
            await session.commit()
            print("\n[REAL RUN] Transaction COMMITTED.")

    print("\n=== SUMMARY ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    asyncio.run(main())
