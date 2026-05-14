"""
Feedback repository — single source of truth for writing to
ai_duplicate_feedbacks.

All admin-rejection paths (cluster reject, property removal from cluster,
manual feedback override) go through these helpers so feedback rows are
consistently shaped with reason_attributes + reason_text + feedback_source.

Engine v2's T0 stage reads this table (spec §3.4) — we don't touch
engine v2 code, just enrich what it consumes.

UPSERT semantics: the table has UNIQUE(prop_a_id, prop_b_id), so re-
rejection of the same pair updates the existing row instead of inserting
a new one. The WHERE clause in the ON CONFLICT branch protects admin-
supplied rows ('admin_reject' / 'manual_split') from being overwritten
by automated writers ('migration' / 'cluster_dissolve'). Admin writes
always win.
"""
from __future__ import annotations

import json
from itertools import combinations
from typing import Iterable, Sequence

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.domain import Property


# =============================================================
# Valid reason attribute keys (taxonomy)
# =============================================================
# These are the 9 attribute categories admin can flag as reasons
# why two properties are NOT duplicates. Each maps to a comparable
# field on the Property model — Phase C ML training will use the
# delta on each cited attribute as a feature.
VALID_REASON_ATTRIBUTES = frozenset({
    "location",
    "house_size",
    "land_size",
    "type",
    "bedrooms",
    "bathrooms",
    "levels",
    "year_built",
    "price",
})

# Valid feedback_source values for callers. 'migration' is an internal
# marker applied to legacy rows by migration 014 and must NEVER be
# passed in by callers — kept out of this set on purpose.
VALID_FEEDBACK_SOURCES = frozenset({
    "admin_reject",
    "manual_split",
    "cluster_dissolve",
})


def _validate_reason_attributes(attrs: Iterable[str]) -> list[str]:
    """Filter incoming attribute list against the whitelist.
    Logs unknown values but doesn't raise — defensive against frontend
    drift. Returns the cleaned list (deduped, sorted)."""
    cleaned: set[str] = set()
    unknown: list[str] = []
    for a in attrs:
        if a in VALID_REASON_ATTRIBUTES:
            cleaned.add(a)
        else:
            unknown.append(a)
    if unknown:
        logger.warning(
            f"[feedback_repo] dropping unknown reason attributes: {unknown}. "
            f"Valid set: {sorted(VALID_REASON_ATTRIBUTES)}"
        )
    return sorted(cleaned)


# Bound parameter :ra is a JSON string (json.dumps of a Python list);
# CAST(... AS jsonb) hands it to PostgreSQL as JSONB. The WHERE clause
# on the DO UPDATE branch guards admin-supplied rows from being
# overwritten by automated writers.
_UPSERT_SQL = text("""
    INSERT INTO ai_duplicate_feedbacks (
        id, prop_a_id, prop_b_id, hash_a, hash_b,
        reason_attributes, reason_text, feedback_source
    ) VALUES (
        gen_random_uuid(), :a, :b, :ha, :hb,
        CAST(:ra AS jsonb), :rt, :fs
    )
    ON CONFLICT (prop_a_id, prop_b_id) DO UPDATE SET
        reason_attributes = EXCLUDED.reason_attributes,
        reason_text       = EXCLUDED.reason_text,
        feedback_source   = EXCLUDED.feedback_source,
        updated_at        = NOW()
    WHERE ai_duplicate_feedbacks.feedback_source NOT IN ('admin_reject', 'manual_split')
       OR EXCLUDED.feedback_source IN ('admin_reject', 'manual_split');
""")


async def record_feedback_for_pair(
    session: AsyncSession,
    prop_a: Property,
    prop_b: Property,
    *,
    reason_attributes: Sequence[str] = (),
    reason_text: str | None = None,
    feedback_source: str = "admin_reject",
) -> None:
    """Upsert one feedback row for a (prop_a, prop_b) pair.

    Canonical pair ordering (lower UUID first) is applied internally,
    matching engine v2's pair_key convention. The legacy hash_a / hash_b
    NOT NULL columns are populated from each Property's content_hash;
    if either is None (legacy rows missing the hash), an empty string is
    written — the goal is never to fail an admin rejection because of a
    missing hash.

    Args:
        prop_a, prop_b: Property ORM objects (content_hash is read).
        reason_attributes: subset of VALID_REASON_ATTRIBUTES.
            Unknown values are dropped with a warning.
        reason_text: free-form admin note, capped at 1000 chars.
        feedback_source: one of VALID_FEEDBACK_SOURCES. Defaults to
            'admin_reject'. Caller passes 'manual_split' for property-
            removal flows.

    Returns None. Caller commits.
    """
    if feedback_source not in VALID_FEEDBACK_SOURCES:
        raise ValueError(
            f"Invalid feedback_source {feedback_source!r}. "
            f"Must be one of: {sorted(VALID_FEEDBACK_SOURCES)}"
        )

    # Canonical order: lower UUID first (engine v2 pair_key convention).
    if str(prop_a.id) > str(prop_b.id):
        prop_a, prop_b = prop_b, prop_a

    cleaned_attrs = _validate_reason_attributes(reason_attributes)
    cleaned_text = (reason_text or "").strip()[:1000] or None

    await session.execute(_UPSERT_SQL, {
        "a": prop_a.id,
        "b": prop_b.id,
        "ha": prop_a.content_hash or "",
        "hb": prop_b.content_hash or "",
        "ra": json.dumps(cleaned_attrs),
        "rt": cleaned_text,
        "fs": feedback_source,
    })

    logger.info(
        f"[feedback_repo] {feedback_source}: pair "
        f"{str(prop_a.id)[:8]}/{str(prop_b.id)[:8]} "
        f"attrs={cleaned_attrs} text_len={len(cleaned_text or '')}"
    )


async def record_feedback_for_cluster_rejection(
    session: AsyncSession,
    cluster_members: Sequence[Property],
    *,
    reason_attributes: Sequence[str] = (),
    reason_text: str | None = None,
) -> int:
    """Upsert feedback rows for all pairs in a cluster being rejected.

    For a cluster with N members, writes C(N, 2) = N*(N-1)/2 rows —
    one per pair — all sharing the same reason_attributes / reason_text.
    Phase C ML training treats each pair as an independent labeled
    example while preserving the per-pair feature deltas (which differ
    even when reasoning is shared).

    Uses 'admin_reject' as feedback_source — this is the standard
    cluster rejection path.

    Returns count of pairs written. Caller commits.
    """
    if len(cluster_members) < 2:
        return 0
    n = 0
    for a, b in combinations(cluster_members, 2):
        await record_feedback_for_pair(
            session, a, b,
            reason_attributes=reason_attributes,
            reason_text=reason_text,
            feedback_source="admin_reject",
        )
        n += 1
    return n


async def record_feedback_for_property_removal(
    session: AsyncSession,
    removed: Property,
    remaining: Sequence[Property],
    *,
    reason_attributes: Sequence[str] = (),
    reason_text: str | None = None,
) -> int:
    """Upsert feedback rows when admin removes ONE property from a cluster.

    Writes N rows: (removed, each remaining). The remaining properties
    stay clustered together — feedback is only about the removed
    property's relationship to each of them.

    Uses 'manual_split' as feedback_source.

    Returns count of pairs written. Caller commits.
    """
    n = 0
    for other in remaining:
        await record_feedback_for_pair(
            session, removed, other,
            reason_attributes=reason_attributes,
            reason_text=reason_text,
            feedback_source="manual_split",
        )
        n += 1
    return n


async def fetch_dissolved_feedbacks(
    session: AsyncSession,
    *,
    feedback_source: str | None = None,
    reason_attribute: str | None = None,
    domain: str | None = None,
    source_engine_version: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Read feedback rows for the /admin/dissolved page.

    Joins to Property for both sides to surface domains + titles +
    thumbnails. Filters narrow the result set. Default limit=100;
    pagination via offset.

    Returns list of plain dicts (not ORM objects) for direct template
    rendering. Each dict has keys:
      id, created_at, feedback_source, reason_attributes, reason_text,
      prop_a: {id, title, domain, url, thumbnail, price},
      prop_b: {id, title, domain, url, thumbnail, price}.
    """
    where_clauses: list[str] = []
    params: dict = {"limit": limit, "offset": offset}

    if feedback_source:
        where_clauses.append("f.feedback_source = :source")
        params["source"] = feedback_source

    if reason_attribute:
        where_clauses.append("f.reason_attributes ? :attr")
        params["attr"] = reason_attribute

    if domain:
        where_clauses.append("(pa.source_domain = :domain OR pb.source_domain = :domain)")
        params["domain"] = domain
    if source_engine_version:                                # ← NEW
        where_clauses.append("f.source_engine_version = :sev")
        params["sev"] = source_engine_version

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    sql = text(f"""
        SELECT
            f.id::text AS id,
            f.created_at,
            f.feedback_source,
            f.reason_attributes,
            f.reason_text,
            pa.id::text AS a_id,
            pa.source_domain AS a_domain,
            pa.url AS a_url,
            COALESCE(pa.description, pa.site_property_id) AS a_title,
            pa.price AS a_price,
            (SELECT m.local_file_path FROM media m
              WHERE m.property_id = pa.id
              ORDER BY m.is_main_photo DESC, m.created_at ASC LIMIT 1) AS a_thumbnail,
            pb.id::text AS b_id,
            pb.source_domain AS b_domain,
            pb.url AS b_url,
            COALESCE(pb.description, pb.site_property_id) AS b_title,
            pb.price AS b_price,
            (SELECT m.local_file_path FROM media m
              WHERE m.property_id = pb.id
              ORDER BY m.is_main_photo DESC, m.created_at ASC LIMIT 1) AS b_thumbnail
        FROM ai_duplicate_feedbacks f
        JOIN properties pa ON pa.id = f.prop_a_id
        JOIN properties pb ON pb.id = f.prop_b_id
        {where_sql}
        ORDER BY f.created_at DESC
        LIMIT :limit OFFSET :offset
    """)
    rows = (await session.execute(sql, params)).mappings().all()

    result: list[dict] = []
    for r in rows:
        result.append({
            "id": r["id"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "feedback_source": r["feedback_source"],
            "reason_attributes": r["reason_attributes"] or [],
            "reason_text": r["reason_text"],
            "prop_a": {
                "id": r["a_id"],
                "domain": r["a_domain"],
                "url": r["a_url"],
                "title": (r["a_title"] or "")[:80],
                "price": r["a_price"],
                "thumbnail": r["a_thumbnail"],
            },
            "prop_b": {
                "id": r["b_id"],
                "domain": r["b_domain"],
                "url": r["b_url"],
                "title": (r["b_title"] or "")[:80],
                "price": r["b_price"],
                "thumbnail": r["b_thumbnail"],
            },
        })
    return result


async def count_dissolved_feedbacks(
    session: AsyncSession,
    *,
    feedback_source: str | None = None,
    reason_attribute: str | None = None,
    domain: str | None = None,
    source_engine_version: str | None = None,
) -> int:
    """Count rows matching the same filters as fetch_dissolved_feedbacks.
    Used for pagination total."""
    where_clauses: list[str] = []
    params: dict = {}

    if feedback_source:
        where_clauses.append("f.feedback_source = :source")
        params["source"] = feedback_source
    if reason_attribute:
        where_clauses.append("f.reason_attributes ? :attr")
        params["attr"] = reason_attribute
    if domain:
        where_clauses.append("(pa.source_domain = :domain OR pb.source_domain = :domain)")
        params["domain"] = domain
    if source_engine_version:                                # ← NEW
        where_clauses.append("f.source_engine_version = :sev")
        params["sev"] = source_engine_version

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    sql = text(f"""
        SELECT COUNT(*) AS n
        FROM ai_duplicate_feedbacks f
        JOIN properties pa ON pa.id = f.prop_a_id
        JOIN properties pb ON pb.id = f.prop_b_id
        {where_sql}
    """)
    return int((await session.execute(sql, params)).scalar_one())
