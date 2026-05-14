"""
Engine v2 writer - RESEARCH.md §12.5.13 schema-compat.

Writes ClusterBuildResult to DB. Three concerns:

  1. New PENDING clusters: INSERT property_clusters row + UPDATE
     properties.cluster_id for each member.

  2. Attachment to existing approved cluster: UPDATE properties.cluster_id
     ONLY for the NEW members. Cluster row's ai_score is PRESERVED;
     notes is APPENDED with engine_v2 attach line; member_count is
     bumped explicitly (no DB trigger does this).

  3. Mismerge flags: INSERT mismerge_flags rows for:
     - bridge_blocks (flag_type='multi_cluster_bridge')
     - approved_disagreements (flag_type='engine_t0_disagrees')
     Idempotent via ON CONFLICT DO NOTHING.

Engine writes ONLY to:
  property_clusters: id, status, member_count, ai_score, phash_matches,
                     notes, created_at, updated_at
  properties: cluster_id
  mismerge_flags: id, cluster_id, pair_a_id, pair_b_id, flag_type,
                  flag_reason, detected_at

Engine NEVER touches:
  property_clusters: verdict_locked, verdict_locked_by, verdict_locked_at,
                     last_external_is_unique, last_external_check_at,
                     power_generated_at
  mismerge_flags: admin_action, admin_action_at, admin_action_by

Caller (engine.py / run_full_dedup) manages transaction lifecycle.
Writer adds to session, never commits.
"""
from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from loguru import logger
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.domain import (
    ClusterStatus, EngineV2Prediction, MismergeFlag, Property, PropertyCluster, utcnow,
)

from .cache import _make_pair_key
# Imports private helper from cache.py — accepted tradeoff vs duplicating.
# If usage spreads beyond writer + cache, promote to shared pair_keys.py.
from .cluster_construction import (
    ApprovedDisagreement,
    BridgeBlockEvent,
    ClusterBuildResult,
    ProposedCluster,
)
from .dedup_report import EngineVerdict


# =============================================================
# WriterReport — aggregate output of write_cluster_build_result
# =============================================================

@dataclass
class WriterReport:
    """Counts of what was actually written to DB."""
    new_clusters_created: int = 0       # fresh PENDING property_clusters rows inserted
    attachments_updated: int = 0        # existing approved clusters that gained members
    properties_attached: int = 0        # individual properties whose cluster_id was set
    mismerge_flags_emitted: int = 0     # total mismerge_flags rows inserted
    bridge_flags: int = 0               # subset: 'multi_cluster_bridge' rows
    disagreement_flags: int = 0         # subset: 'engine_t0_disagrees' rows


# =============================================================
# Public top-level API
# =============================================================

async def write_cluster_build_result(
    session: AsyncSession,
    result: ClusterBuildResult,
) -> WriterReport:
    """Write all components of a ClusterBuildResult to DB.

    Per RESEARCH.md §12.5.13 schema-compat. Caller manages transaction
    (no commit inside this function).

    Order of operations:
      1. Loop new_clusters:
         is_attachment=False -> _create_pending_cluster
         is_attachment=True  -> _update_attachment
      2. Loop bridge_blocks -> _emit_bridge_flag
      3. Loop approved_disagreements -> _emit_disagreement_flag

    Returns: WriterReport summarising the write operation.
    """
    report = WriterReport()

    # 1. Process new clusters
    # Sprint 7 Phase B — engine v2 MVP: skip attachment branch. Engine 2
    # builds fresh PENDING clusters each run; attachment to existing engine
    # 2 approved clusters is Sprint 8 work (requires querying engine_version
    # ='2' approved clusters + junction-aware member diff).
    for cluster in result.new_clusters:
        if cluster.is_attachment:
            logger.debug(
                "[writer] skip attachment for cluster {cid} (engine v2 MVP, "
                "Sprint 8 will add attachment logic)",
                cid=str(cluster.cluster_id)[:8],
            )
            continue
        await _create_pending_cluster(session, cluster)
        report.new_clusters_created += 1
        report.properties_attached += len(cluster.member_ids)

    # 2. Process bridge_blocks
    for bridge in result.bridge_blocks:
        n = await _emit_bridge_flag(session, bridge)
        report.bridge_flags += n
        report.mismerge_flags_emitted += n

    # 3. Process approved_disagreements
    for disagreement in result.approved_disagreements:
        n = await _emit_disagreement_flag(session, disagreement)
        report.disagreement_flags += n
        report.mismerge_flags_emitted += n

    logger.info(
        "[writer] wrote {n_new} new + {n_attach} attachments "
        "({n_props} properties), {n_flags} mismerge flags emitted",
        n_new=report.new_clusters_created,
        n_attach=report.attachments_updated,
        n_props=report.properties_attached,
        n_flags=report.mismerge_flags_emitted,
    )
    return report


# =============================================================
# Internal helpers — F-3, F-4, F-5, F-6 replace these stubs
# =============================================================

async def _create_pending_cluster(
    session: AsyncSession,
    cluster: ProposedCluster,
) -> None:
    """Insert one new PENDING cluster row + set cluster_id on all members.

    Field-setting per RESEARCH.md §12.5.13:
      id           = cluster.cluster_id (fresh UUID from _materialize)
      status       = PENDING
      member_count = len(cluster.member_ids)  (stored, not auto-computed)
      ai_score     = cluster.ai_score (must be float for new clusters)
      phash_matches = NULL (engine doesn't compute pHash overlap)
      notes        = "engine_v2: created at {iso_timestamp}"
      created_at, updated_at = utcnow() (default via ORM)

    Then UPDATE properties SET cluster_id = cluster.cluster_id
    WHERE id IN cluster.member_ids.

    Caller guarantees cluster.is_attachment == False.
    """
    assert not cluster.is_attachment, "use _update_attachment for attachments"
    assert cluster.ai_score is not None, "new clusters require ai_score"

    timestamp = utcnow()
    notes_text = f"engine_v2: created at {timestamp.isoformat()}"

    # 1. INSERT property_clusters row with engine_version='2'
    # Sprint 7 Phase B — engine v2 owns its own cluster lifecycle parallel
    # to engine 1. Different engine_version isolates the two engines.
    new_cluster = PropertyCluster(
        id=cluster.cluster_id,
        status=ClusterStatus.PENDING,
        member_count=len(cluster.member_ids),
        ai_score=cluster.ai_score,
        phash_matches=None,
        notes=notes_text,
        engine_version='2',
        # created_at, updated_at default via ORM utcnow
    )
    session.add(new_cluster)

    # 2. INSERT cluster_v2_members rows (junction).
    # Engine v1 uses Property.cluster_id FK exclusively. Engine v2 uses
    # this junction so a property can be in BOTH a v1 cluster AND a v2
    # cluster at the same time — the whole point of parallel operation.
    if cluster.member_ids:
        await session.execute(
            text("""
                INSERT INTO cluster_v2_members (cluster_id, property_id)
                SELECT :cluster_id, unnest(CAST(:member_ids AS UUID[]))
                ON CONFLICT (cluster_id, property_id) DO NOTHING
            """),
            {
                "cluster_id": cluster.cluster_id,
                "member_ids": [str(mid) for mid in cluster.member_ids],
            },
        )

    logger.debug(
        "[writer] new cluster {cid} (PENDING engine_v2, {n} members, ai_score={s:.3f})",
        cid=str(cluster.cluster_id)[:8],
        n=len(cluster.member_ids),
        s=cluster.ai_score,
    )


async def _update_attachment(
    session: AsyncSession,
    cluster: ProposedCluster,
) -> int:
    """Attach new members to existing approved cluster + append notes.

    Identifies new members by querying current cluster_id values for
    cluster.member_ids; updates only those != cluster.cluster_id.

    Per RESEARCH.md §12.5.13:
      - ai_score PRESERVED (engine never overwrites approved confidence)
      - notes APPENDED with engine_v2 attach line
      - updated_at touched
      - member_count incremented (stored value, not auto-computed —
        verified via pre-check; main.py manual-merge pattern matches)
      - verdict_locked NOT touched (admin authority)

    Returns: count of properties whose cluster_id was updated
             (0 if all members already attached, common during re-runs).

    Caller guarantees cluster.is_attachment == True.
    """
    assert cluster.is_attachment, "use _create_pending_cluster for new clusters"
    assert cluster.ai_score is None, "attachments preserve existing ai_score"

    # 1. Query current cluster_id for members
    current_query = await session.execute(
        select(Property.id, Property.cluster_id)
        .where(Property.id.in_(cluster.member_ids))
    )
    current_state = dict(current_query.all())

    # 2. Identify newly-attached members (those NOT already pointing to target)
    newly_attached = [
        prop_id for prop_id in cluster.member_ids
        if current_state.get(prop_id) != cluster.cluster_id
    ]

    if not newly_attached:
        # No-op (all members already attached) — common during re-runs
        logger.debug(
            "[writer] attachment no-op: cluster {cid} all members already attached",
            cid=str(cluster.cluster_id)[:8],
        )
        return 0

    timestamp = utcnow()

    # 3. UPDATE properties.cluster_id for new members only
    await session.execute(
        update(Property)
        .where(Property.id.in_(newly_attached))
        .values(cluster_id=cluster.cluster_id)
    )

    # 4. Append notes line + bump updated_at + increment member_count
    n = len(newly_attached)
    plural = "ies" if n > 1 else "y"
    note_line = (
        f"engine_v2: attached {n} propert{plural} "
        f"at {timestamp.isoformat()}"
    )
    await session.execute(
        update(PropertyCluster)
        .where(PropertyCluster.id == cluster.cluster_id)
        .values(
            notes=func.coalesce(PropertyCluster.notes, '') + '\n' + note_line,
            updated_at=timestamp,
            member_count=PropertyCluster.member_count + n,
            # ai_score NOT updated (preserved per §12.5.13)
            # verdict_locked NOT touched (admin authority)
        )
    )

    logger.debug(
        "[writer] attached {n} properties to cluster {cid}",
        n=n,
        cid=str(cluster.cluster_id)[:8],
    )

    return n


async def _emit_bridge_flag(
    session: AsyncSession,
    bridge: BridgeBlockEvent,
) -> int:
    """Insert mismerge_flag rows for a multi_cluster_bridge event.

    One row PER LOSER cluster:
      flag_type = 'multi_cluster_bridge'
      cluster_id = loser cluster (admin's worklist keyed by cluster)
      pair_a_id, pair_b_id = (new_property_id, loser anchor member),
                              canonically ordered (min, max)
      flag_reason = "winner=ABCDEFGH (conf=0.950); this loser conf=0.850"

    Anchor selection (Decision 2 — Option B): query loser cluster's
    members, pick min(member.id) for deterministic reproducibility.

    Idempotency (Decision 3): per-loser INSERT with ON CONFLICT
    (cluster_id, pair_a_id, pair_b_id, flag_type) DO NOTHING. Returns
    RETURNING id for accurate counting.

    Returns: count of NEW rows inserted (post ON CONFLICT skipping).
    """
    rows_inserted = 0
    timestamp = utcnow()

    for loser_cluster_id, loser_mean_conf in bridge.losers:
        # 1. Query loser cluster's anchor (deterministic min(id))
        anchor_query = await session.execute(
            select(Property.id)
            .where(Property.cluster_id == loser_cluster_id)
            .order_by(Property.id)
            .limit(1)
        )
        anchor_member = anchor_query.scalar_one_or_none()

        if anchor_member is None:
            logger.warning(
                "[writer] bridge loser cluster {cid} has no members — "
                "skipping flag emission",
                cid=str(loser_cluster_id)[:8],
            )
            continue

        # 2. Canonical pair_a, pair_b ordering
        pair_a, pair_b = sorted([bridge.new_property_id, anchor_member])

        # 3. Build flag_reason
        flag_reason = (
            f"winner={str(bridge.winner_cluster_id)[:8]} "
            f"(conf={bridge.winner_mean_conf:.3f}); "
            f"this loser conf={loser_mean_conf:.3f}"
        )

        # 4. INSERT with ON CONFLICT DO NOTHING
        stmt = pg_insert(MismergeFlag).values(
            cluster_id=loser_cluster_id,
            pair_a_id=pair_a,
            pair_b_id=pair_b,
            flag_type='multi_cluster_bridge',
            flag_reason=flag_reason,
            detected_at=timestamp,
        ).on_conflict_do_nothing(
            index_elements=['cluster_id', 'pair_a_id', 'pair_b_id', 'flag_type']
        ).returning(MismergeFlag.id)

        result = await session.execute(stmt)
        if result.scalar_one_or_none() is not None:
            rows_inserted += 1
            logger.debug(
                "[writer] mismerge_flag inserted: "
                "loser={lc} new_prop={p} reason={r}",
                lc=str(loser_cluster_id)[:8],
                p=str(bridge.new_property_id)[:8],
                r=flag_reason,
            )
        else:
            logger.debug(
                "[writer] mismerge_flag skipped (idempotent): "
                "loser={lc} new_prop={p}",
                lc=str(loser_cluster_id)[:8],
                p=str(bridge.new_property_id)[:8],
            )

    return rows_inserted


async def write_engine_v2_prediction(
    session: AsyncSession,
    prop_a_id: UUID,
    prop_b_id: UUID,
    verdict: EngineVerdict,
) -> None:
    """INSERT one row into engine_v2_predictions (Phase 1-2 shadow output).

    Per RESEARCH.md §12.5.7 + §12.5.10. pair_key derived canonically
    from (prop_a_id, prop_b_id) via cache._make_pair_key (single source
    of truth, no risk of drift between cache and prediction tables).

    scored_at defaults to NOW() in DB; UNIQUE (pair_key, scored_at)
    permits multiple history rows per pair across scrape runs (drift
    analysis support — RESEARCH.md §12.5.7 rationale).

    Caller manages transaction (no commit inside, per writer.py
    convention). Concurrent inserts at exact same TZ instant are
    extremely rare; if they collide the second raises IntegrityError —
    intended behaviour ("dedupe within same scrape run").
    """
    pk = _make_pair_key(prop_a_id, prop_b_id)
    stmt = pg_insert(EngineV2Prediction).values(
        pair_key=pk,
        a_id=prop_a_id,
        b_id=prop_b_id,
        verdict=verdict.verdict,
        confidence=verdict.confidence,
        reasoning=verdict.reasoning or None,
        tier_emitted=verdict.tier_emitted,
        cost_usd=verdict.cost_usd,
        # scored_at defaults via DB DEFAULT NOW()
    )
    await session.execute(stmt)
    logger.debug(
        "[writer] engine_v2_prediction inserted: pair_key={pk} "
        "verdict={v} tier={t}",
        pk=pk[:17], v=verdict.verdict, t=verdict.tier_emitted,
    )


async def _emit_disagreement_flag(
    session: AsyncSession,
    disagreement: ApprovedDisagreement,
) -> int:
    """Insert mismerge_flag row for approved-cluster disagreement.

    Engine emits DUPLICATE verdict for a pair where both properties
    are already in DIFFERENT approved clusters. Engine cannot dissolve
    (spec §11). Flags it for admin review.

    Field-setting:
      flag_type   = 'engine_t0_disagrees'
      cluster_id  = min(cluster_a_id, cluster_b_id) for determinism
                    (worklist surfaces under one cluster; admin sees
                    both mentioned in flag_reason)
      pair_a_id   = min(prop_a_id, prop_b_id)  (canonical)
      pair_b_id   = max(prop_a_id, prop_b_id)
      flag_reason = "Engine sees DUPLICATE between approved clusters
                     ABCDEFGH and IJKLMNOP; conf=0.920"

    Idempotency: ON CONFLICT (cluster_id, pair_a_id, pair_b_id, flag_type)
    DO NOTHING. Caller invocations with reversed UUID order map to the
    same canonical flag (no duplicate).

    Returns: count of NEW rows inserted (0 or 1).
    """
    timestamp = utcnow()

    # 1. Canonical cluster_id selection (min for determinism)
    flag_cluster_id = min(disagreement.cluster_a_id, disagreement.cluster_b_id)
    other_cluster_id = max(disagreement.cluster_a_id, disagreement.cluster_b_id)

    # 2. Canonical pair ordering
    pair_a, pair_b = sorted([disagreement.prop_a_id, disagreement.prop_b_id])

    # 3. Build flag_reason
    flag_reason = (
        f"Engine sees DUPLICATE between approved clusters "
        f"{str(flag_cluster_id)[:8]} and {str(other_cluster_id)[:8]}; "
        f"conf={disagreement.confidence:.3f}"
    )

    # 4. INSERT with ON CONFLICT DO NOTHING
    stmt = pg_insert(MismergeFlag).values(
        cluster_id=flag_cluster_id,
        pair_a_id=pair_a,
        pair_b_id=pair_b,
        flag_type='engine_t0_disagrees',
        flag_reason=flag_reason,
        detected_at=timestamp,
    ).on_conflict_do_nothing(
        index_elements=['cluster_id', 'pair_a_id', 'pair_b_id', 'flag_type']
    ).returning(MismergeFlag.id)

    result = await session.execute(stmt)
    if result.scalar_one_or_none() is not None:
        logger.debug(
            "[writer] disagreement flag inserted: "
            "cluster={c} pairs=({a},{b}) reason={r}",
            c=str(flag_cluster_id)[:8],
            a=str(pair_a)[:8],
            b=str(pair_b)[:8],
            r=flag_reason,
        )
        return 1
    else:
        logger.debug(
            "[writer] disagreement flag skipped (idempotent): "
            "cluster={c} pairs=({a},{b})",
            c=str(flag_cluster_id)[:8],
            a=str(pair_a)[:8],
            b=str(pair_b)[:8],
        )
        return 0
