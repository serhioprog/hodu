"""
Pair-level feature extraction — single source of truth for engine v2.

All scoring architectures (rule-based, classical ML, LLM-tier, hybrid)
call `extract_features(a, b, cosine_sim)` to get the same `PairFeatures`
struct. Feature semantics are stable across architectures — if you add
a feature, every scorer can use it.

This module is pure: no DB writes, no network. The convenience helper
`fetch_pair_with_features(session, a_id, b_id)` does a single read-only
DB query to materialize a feature struct from raw IDs.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from math import acos, cos, radians, sin
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from .canonical import to_canonical

_EARTH_R_M = 6_371_000.0


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Distance in meters between two GPS points (clamps acos input for safety)."""
    cos_arg = (
        cos(radians(lat1)) * cos(radians(lat2)) *
        cos(radians(lng1) - radians(lng2)) +
        sin(radians(lat1)) * sin(radians(lat2))
    )
    cos_arg = min(1.0, max(-1.0, cos_arg))
    return _EARTH_R_M * acos(cos_arg)


@dataclass
class PairFeatures:
    """All features the engine uses to score a pair.

    Type-hinted dataclass so scorers can rely on field availability and
    IDEs surface NULL handling. Values that need pair-level math are
    floats (None when input data is missing on either side). Identity /
    equality flags are bools (with None when either side is NULL).
    """
    a_id: str
    b_id: str
    a_source: str
    b_source: str
    cross_source: bool
    canonical_category_a: str
    canonical_category_b: str
    same_canonical_category: bool
    cosine_sim: float | None
    gps_distance_m: float | None
    price_a: int | None
    price_b: int | None
    price_ratio: float | None       # max/min, >= 1.0
    price_diff_pct: float | None    # |a-b|/max * 100
    size_a: float | None
    size_b: float | None
    size_diff_pct: float | None
    year_a: int | None
    year_b: int | None
    year_diff: int | None           # absolute, |a-b|
    bedrooms_match: bool | None
    same_municipality: bool | None
    same_calc_area: bool | None
    shared_phash_count: int
    description_a: str | None
    description_b: str | None
    """Raw marketing text for each property. Populated by
    fetch_pair_with_features. Used by LLM-tier scorer for semantic
    reading; rule-based and classical-ML scorers ignore them."""
    pair_in_feedback: bool
    """Spec §3.4: pair previously rejected by admin (in
    ai_duplicate_feedbacks). All architectures must treat this as
    deterministic DIFFERENT — engine never re-proposes a rejected pair
    without substantially new evidence."""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def extract_features(
    a: dict[str, Any],
    b: dict[str, Any],
    cosine_sim: float | None = None,
    *,
    pair_in_feedback: bool = False,
    description_a: str | None = None,
    description_b: str | None = None,
) -> PairFeatures:
    """
    Compute pair features from two property dicts.

    Both dicts must have keys: id, source_domain, category, price,
    size_sqm, bedrooms, year_built, calc_municipality, calc_area,
    latitude, longitude, image_phashes.

    `cosine_sim` is passed in (this module avoids DB access). Use
    `fetch_pair_with_features` to compute it inline.
    """
    a_id, b_id = str(a["id"]), str(b["id"])
    a_src, b_src = a["source_domain"], b["source_domain"]

    canon_a = to_canonical(a.get("category"))
    canon_b = to_canonical(b.get("category"))
    same_canon = (
        canon_a == canon_b
        and not canon_a.startswith("unknown")
        and not canon_b.startswith("unknown")
    )

    # GPS distance
    gps_dist: float | None = None
    la1, ln1 = a.get("latitude"), a.get("longitude")
    la2, ln2 = b.get("latitude"), b.get("longitude")
    if all(v is not None for v in (la1, ln1, la2, ln2)):
        gps_dist = haversine_m(la1, ln1, la2, ln2)

    # Prices
    pa, pb = a.get("price"), b.get("price")
    price_ratio: float | None = None
    price_diff_pct: float | None = None
    if pa and pb and min(pa, pb) > 0:
        price_ratio = max(pa, pb) / min(pa, pb)
        price_diff_pct = abs(pa - pb) / max(pa, pb) * 100

    # Size
    sa, sb = a.get("size_sqm"), b.get("size_sqm")
    size_diff_pct: float | None = None
    if sa and sb and max(sa, sb) > 0:
        size_diff_pct = abs(sa - sb) / max(sa, sb) * 100

    # Year
    ya, yb = a.get("year_built"), b.get("year_built")
    year_diff: int | None = None
    if ya is not None and yb is not None:
        year_diff = abs(int(ya) - int(yb))

    # Bedrooms
    ba, bb = a.get("bedrooms"), b.get("bedrooms")
    bedrooms_match: bool | None = None
    if ba is not None and bb is not None:
        bedrooms_match = (int(ba) == int(bb))

    # Location (None when either side NULL)
    same_muni: bool | None = None
    if a.get("calc_municipality") is not None and b.get("calc_municipality") is not None:
        same_muni = (a["calc_municipality"] == b["calc_municipality"])
    same_area: bool | None = None
    if a.get("calc_area") is not None and b.get("calc_area") is not None:
        same_area = (a["calc_area"] == b["calc_area"])

    # pHash overlap
    pa_hashes = set(a.get("image_phashes") or [])
    pb_hashes = set(b.get("image_phashes") or [])
    shared_phash = len(pa_hashes & pb_hashes)

    return PairFeatures(
        a_id=a_id, b_id=b_id,
        a_source=a_src, b_source=b_src,
        cross_source=(a_src != b_src),
        canonical_category_a=canon_a,
        canonical_category_b=canon_b,
        same_canonical_category=same_canon,
        cosine_sim=cosine_sim,
        gps_distance_m=gps_dist,
        price_a=pa, price_b=pb,
        price_ratio=price_ratio,
        price_diff_pct=price_diff_pct,
        size_a=sa, size_b=sb,
        size_diff_pct=size_diff_pct,
        year_a=ya, year_b=yb,
        year_diff=year_diff,
        bedrooms_match=bedrooms_match,
        same_municipality=same_muni,
        same_calc_area=same_area,
        shared_phash_count=shared_phash,
        description_a=description_a,
        description_b=description_b,
        pair_in_feedback=pair_in_feedback,
    )


# =============================================================
# DB convenience: fetch both properties + cosine in one query
# =============================================================

_PROP_FIELDS_SQL = """
  id::text          AS id,
  source_domain     AS source_domain,
  category          AS category,
  price             AS price,
  size_sqm          AS size_sqm,
  bedrooms          AS bedrooms,
  year_built        AS year_built,
  calc_municipality AS calc_municipality,
  calc_area         AS calc_area,
  latitude          AS latitude,
  longitude         AS longitude,
  image_phashes     AS image_phashes,
  description       AS description
"""


async def fetch_pair_with_features(
    session: AsyncSession,
    a_id: str,
    b_id: str,
) -> PairFeatures | None:
    """
    Single-DB-roundtrip: load both properties + cosine_sim, return PairFeatures.
    Returns None if either property is missing or has no embedding.
    """
    sql = text(f"""
        WITH p AS (
          SELECT {_PROP_FIELDS_SQL},
            embedding AS embedding,
            (id::text = :a_id) AS is_a
          FROM properties
          WHERE id::text IN :ids
        )
        SELECT
          a.id AS a_id, a.source_domain AS a_source_domain, a.category AS a_category,
          a.price AS a_price, a.size_sqm AS a_size_sqm, a.bedrooms AS a_bedrooms,
          a.year_built AS a_year_built, a.calc_municipality AS a_calc_municipality,
          a.calc_area AS a_calc_area, a.latitude AS a_latitude, a.longitude AS a_longitude,
          a.image_phashes AS a_image_phashes,
          a.description AS a_description,
          b.id AS b_id, b.source_domain AS b_source_domain, b.category AS b_category,
          b.price AS b_price, b.size_sqm AS b_size_sqm, b.bedrooms AS b_bedrooms,
          b.year_built AS b_year_built, b.calc_municipality AS b_calc_municipality,
          b.calc_area AS b_calc_area, b.latitude AS b_latitude, b.longitude AS b_longitude,
          b.image_phashes AS b_image_phashes,
          b.description AS b_description,
          1 - (a.embedding <=> b.embedding) AS cosine_sim,
          EXISTS (
            SELECT 1 FROM ai_duplicate_feedbacks f
            WHERE (f.prop_a_id::text = :a_id AND f.prop_b_id::text = :b_id)
               OR (f.prop_a_id::text = :b_id AND f.prop_b_id::text = :a_id)
          ) AS pair_in_feedback
        FROM p a, p b
        WHERE a.is_a = true AND b.is_a = false
          AND a.embedding IS NOT NULL AND b.embedding IS NOT NULL
    """).bindparams(bindparam("ids", expanding=True))

    row = (await session.execute(sql, {"a_id": a_id, "b_id": b_id, "ids": [a_id, b_id]})).mappings().first()
    if row is None:
        return None

    a_dict = {
        "id": row["a_id"], "source_domain": row["a_source_domain"],
        "category": row["a_category"], "price": row["a_price"],
        "size_sqm": row["a_size_sqm"], "bedrooms": row["a_bedrooms"],
        "year_built": row["a_year_built"],
        "calc_municipality": row["a_calc_municipality"],
        "calc_area": row["a_calc_area"],
        "latitude": row["a_latitude"], "longitude": row["a_longitude"],
        "image_phashes": row["a_image_phashes"],
    }
    b_dict = {
        "id": row["b_id"], "source_domain": row["b_source_domain"],
        "category": row["b_category"], "price": row["b_price"],
        "size_sqm": row["b_size_sqm"], "bedrooms": row["b_bedrooms"],
        "year_built": row["b_year_built"],
        "calc_municipality": row["b_calc_municipality"],
        "calc_area": row["b_calc_area"],
        "latitude": row["b_latitude"], "longitude": row["b_longitude"],
        "image_phashes": row["b_image_phashes"],
    }
    cosine = float(row["cosine_sim"]) if row["cosine_sim"] is not None else None
    return extract_features(
        a_dict, b_dict,
        cosine_sim=cosine,
        pair_in_feedback=bool(row["pair_in_feedback"]),
        description_a=row["a_description"],
        description_b=row["b_description"],
    )
