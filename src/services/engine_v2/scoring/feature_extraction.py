"""
Feature extraction for Tier 2 ML scoring.

Per RESEARCH.md §12.5.10. Extracts 13 features from spec §3 columns.
All NULL handling explicit. No reads from forbidden services per §10.

Per architect approval (Day 3 Phase B):
  - 13 features (numeric + categorical)
  - phash_close_count threshold = 10 (re-encoded same-image cases)
  - embedding_l2_distance dropped (correlated with cosine)
  - source_pair_id deferred to Day 4 (handled by source_pair_calibration)
"""
from __future__ import annotations

import math
from typing import TypedDict

import numpy as np
from loguru import logger

from src.models.domain import Property

from ..canonical import to_canonical


class Tier2Features(TypedDict):
    """13 features used by Tier 2 ML model.

    Field order matters — must match training feature ordering.
    Python 3.7+ guarantees TypedDict.__annotations__ insertion order.
    """
    # Numeric continuous (8)
    price_log_ratio: float
    size_sqm_ratio: float
    land_size_ratio: float
    bedrooms_diff: float
    bathrooms_diff: float
    year_built_diff: float
    distance_km: float
    embedding_cosine_sim: float

    # Numeric discrete — image features (2)
    phash_min_hamming: float
    phash_close_count: float

    # Categorical, encoded 0.0/1.0 (3)
    same_calc_municipality: float
    same_calc_area: float
    same_category_canonical: float


# Public name list for stable feature ordering (training + inference must agree).
FEATURE_NAMES: list[str] = list(Tier2Features.__annotations__.keys())

# pHash close-match threshold per Phase B Decision 2 (architect-approved).
PHASH_CLOSE_THRESHOLD: int = 10


def extract_features(prop_a: Property, prop_b: Property) -> Tier2Features:
    """Extract Tier 2 features from two property records.

    NULL handling:
      - Numeric features: math.nan (HistGradientBoostingClassifier
        handles natively via missing-value bin)
      - Categorical features: 0.0 if either side NULL or unknown

    Returns: Tier2Features dict (13 keys, ordered).
    """
    return Tier2Features(
        price_log_ratio=_price_log_ratio(prop_a, prop_b),
        size_sqm_ratio=_size_sqm_ratio(prop_a, prop_b),
        land_size_ratio=_land_size_ratio(prop_a, prop_b),
        bedrooms_diff=_bedrooms_diff(prop_a, prop_b),
        bathrooms_diff=_bathrooms_diff(prop_a, prop_b),
        year_built_diff=_year_built_diff(prop_a, prop_b),
        distance_km=_distance_km(prop_a, prop_b),
        embedding_cosine_sim=_embedding_cosine_sim(prop_a, prop_b),
        phash_min_hamming=_phash_min_hamming(prop_a, prop_b),
        phash_close_count=_phash_close_count(prop_a, prop_b),
        same_calc_municipality=_same_calc_municipality(prop_a, prop_b),
        same_calc_area=_same_calc_area(prop_a, prop_b),
        same_category_canonical=_same_category_canonical(prop_a, prop_b),
    )


# =============================================================
# Per-feature helpers
# =============================================================

def _price_log_ratio(a: Property, b: Property) -> float:
    """log(max(price_a, price_b) / min(price_a, price_b)).

    Spec §3 says price ±10% common between sources. Log ratio
    normalizes scale (a 2x ratio reads as ~0.69, a 10x as ~2.30).
    """
    if a.price is None or b.price is None:
        return math.nan
    if a.price <= 0 or b.price <= 0:
        return math.nan
    hi, lo = max(a.price, b.price), min(a.price, b.price)
    return math.log(hi / max(lo, 1))


def _size_sqm_ratio(a: Property, b: Property) -> float:
    """max(size_a, size_b) / min(size_a, size_b)."""
    if a.size_sqm is None or b.size_sqm is None:
        return math.nan
    if a.size_sqm <= 0 or b.size_sqm <= 0:
        return math.nan
    hi, lo = max(a.size_sqm, b.size_sqm), min(a.size_sqm, b.size_sqm)
    return hi / lo


def _land_size_ratio(a: Property, b: Property) -> float:
    if a.land_size_sqm is None or b.land_size_sqm is None:
        return math.nan
    if a.land_size_sqm <= 0 or b.land_size_sqm <= 0:
        return math.nan
    hi, lo = max(a.land_size_sqm, b.land_size_sqm), min(a.land_size_sqm, b.land_size_sqm)
    return hi / lo


def _bedrooms_diff(a: Property, b: Property) -> float:
    if a.bedrooms is None or b.bedrooms is None:
        return math.nan
    return float(abs(a.bedrooms - b.bedrooms))


def _bathrooms_diff(a: Property, b: Property) -> float:
    if a.bathrooms is None or b.bathrooms is None:
        return math.nan
    return float(abs(a.bathrooms - b.bathrooms))


def _year_built_diff(a: Property, b: Property) -> float:
    """Per spec §2.3 revised: T0 catches diff > 5. T2 sees nuance in 0-5."""
    if a.year_built is None or b.year_built is None:
        return math.nan
    return float(abs(a.year_built - b.year_built))


def _distance_km(a: Property, b: Property) -> float:
    """Haversine distance in km.

    Spec §3.1 notes GPS often inaccurate; treat as a soft signal.
    """
    if (a.latitude is None or a.longitude is None
            or b.latitude is None or b.longitude is None):
        return math.nan
    R = 6371.0  # Earth radius km
    lat1, lon1 = math.radians(a.latitude), math.radians(a.longitude)
    lat2, lon2 = math.radians(b.latitude), math.radians(b.longitude)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = (math.sin(dlat / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
    return float(2 * R * math.asin(math.sqrt(h)))


def _embedding_cosine_sim(a: Property, b: Property) -> float:
    """Cosine similarity of 1536-dim text-embedding-3-small vectors.

    OpenAI embeddings are unit-norm so cosine = dot product. NULL
    embedding is rare; Pass 1 audit confirmed 100% coverage on
    active properties.
    """
    if a.embedding is None or b.embedding is None:
        logger.warning(
            "[t2_features] missing embedding a={a} b={b}",
            a=str(a.id)[:8], b=str(b.id)[:8],
        )
        return math.nan

    va = np.asarray(a.embedding, dtype=np.float32)
    vb = np.asarray(b.embedding, dtype=np.float32)
    norm_a = np.linalg.norm(va)
    norm_b = np.linalg.norm(vb)
    if norm_a == 0 or norm_b == 0:
        return math.nan
    return float(np.dot(va, vb) / (norm_a * norm_b))


def _phash_min_hamming(a: Property, b: Property) -> float:
    """Minimum Hamming distance over all phash pairs.

    Returns 64.0 (max distance for 64-bit hashes) if either side
    has no phashes.
    """
    if not a.image_phashes or not b.image_phashes:
        return 64.0
    min_dist = 64
    for ha in a.image_phashes:
        for hb in b.image_phashes:
            d = _hamming_str(ha, hb)
            if d < min_dist:
                min_dist = d
    return float(min_dist)


def _phash_close_count(a: Property, b: Property) -> float:
    """Count of phash pairs with Hamming distance <= PHASH_CLOSE_THRESHOLD.

    Threshold 10 (Phase B Decision 2) catches re-encoded / resized
    variants of the same source image.
    """
    if not a.image_phashes or not b.image_phashes:
        return 0.0
    count = 0
    for ha in a.image_phashes:
        for hb in b.image_phashes:
            if _hamming_str(ha, hb) <= PHASH_CLOSE_THRESHOLD:
                count += 1
    return float(count)


def _hamming_str(s1: str, s2: str) -> int:
    """Hamming distance between two equal-length hex pHash strings.

    pHashes are stored as 16-char hex strings (64-bit). Returns 64
    (max distance) on length mismatch or parse failure.
    """
    if len(s1) != len(s2):
        return 64
    try:
        n1 = int(s1, 16)
        n2 = int(s2, 16)
        return bin(n1 ^ n2).count("1")
    except ValueError:
        return 64


def _same_calc_municipality(a: Property, b: Property) -> float:
    if not a.calc_municipality or not b.calc_municipality:
        return 0.0
    return 1.0 if a.calc_municipality == b.calc_municipality else 0.0


def _same_calc_area(a: Property, b: Property) -> float:
    if not a.calc_area or not b.calc_area:
        return 0.0
    return 1.0 if a.calc_area == b.calc_area else 0.0


def _same_category_canonical(a: Property, b: Property) -> float:
    """Compare canonical categories per category_synonyms.json mapping.

    Uses to_canonical() helper from src/canonical.py (existing). Returns
    0.0 if either side maps to "unknown" or "unknown_label_*"; 1.0 if
    both canonicals are equal and known; 0.0 otherwise.
    """
    canon_a = to_canonical(a.category)
    canon_b = to_canonical(b.category)
    if canon_a.startswith("unknown") or canon_b.startswith("unknown"):
        return 0.0
    return 1.0 if canon_a == canon_b else 0.0
