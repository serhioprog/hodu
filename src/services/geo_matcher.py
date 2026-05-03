"""
Geo resolver: (lat, lng) -> nearest SdhtArea + parent municipality/prefecture.

Key contract change vs old version:
  * Returns `location_id` — the FK to sdht_property_areas — which daily_sync
    assigns to Property.location_id. The old version never returned it and
    the FK was perpetually NULL.
  * Returns Python None (not the Russian "Не определено") for unknown fields.
    Callers write these directly into calc_* and into the canonical text —
    a non-null garbage string would poison embeddings and clustering.
  * Accepts an AsyncSession from the caller — no implicit session_maker.

Strategy: simple Euclidean distance in lat/lng space. Accurate enough for
Halkidiki-scale clusters; faster than haversine at our sizes.
"""
from typing import Optional, TypedDict

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class GeoMatch(TypedDict):
    location_id: Optional[int]
    prefecture: Optional[str]
    municipality: Optional[str]
    exact_district: Optional[str]


_EMPTY: GeoMatch = {
    "location_id": None,
    "prefecture": None,
    "municipality": None,
    "exact_district": None,
}

_NEAREST_SQL = text("""
    SELECT
        a.id            AS area_id,
        a.area_en       AS area,
        m.municipality_en AS municipality,
        p.prefecture_en AS prefecture
    FROM sdht_property_areas a
    JOIN sdht_property_municipalities m ON a.municipality_id = m.id
    JOIN sdht_property_prefectures   p ON m.prefecture_id   = p.id
    WHERE NULLIF(a.lat, '') IS NOT NULL
      AND NULLIF(a.lng, '') IS NOT NULL
    ORDER BY
        POWER(CAST(NULLIF(a.lat, '') AS FLOAT) - :lat, 2) +
        POWER(CAST(NULLIF(a.lng, '') AS FLOAT) - :lng, 2) ASC
    LIMIT 1
""")


class GeoMatcher:
    async def find_best_match(
        self,
        session: AsyncSession,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        area_name: Optional[str] = None,
    ) -> GeoMatch:
        """
        Returns GeoMatch with either real values or None.
        Never invents placeholder strings.
        """
        # No coordinates → we can only echo area_name as a hint.
        # We do NOT write a fake municipality/prefecture in this case.
        if lat is None or lng is None:
            return {
                **_EMPTY,
                "exact_district": area_name or None,
            }

        try:
            res = await session.execute(_NEAREST_SQL, {"lat": lat, "lng": lng})
            row = res.fetchone()
            if row is None:
                return {**_EMPTY, "exact_district": area_name or None}

            return {
                "location_id":    row.area_id,
                "prefecture":     row.prefecture,
                "municipality":   row.municipality,
                "exact_district": row.area or area_name,
            }

        except Exception as e:
            logger.error(f"[GeoMatcher] query failed: {e}")
            return {**_EMPTY, "exact_district": area_name or None}


# Module-level singleton — stateless, cheap to share.
geo_matcher = GeoMatcher()