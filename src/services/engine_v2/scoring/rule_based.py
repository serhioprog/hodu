"""
Rule-based pair scoring (Phase 1 of Pass 4 bake-off).

Pure deterministic decision tree over PairFeatures. No LLM, no ML.

Stage 1: hard rules from spec (cross-source, canonical, year_diff).
   Output: DIFFERENT (engine short-circuits) or pass-through.
Stage 2: signal-agreement scoring on the surviving pool.
   Output: DUPLICATE / UNCERTAIN / DIFFERENT.

All thresholds imported from config/thresholds.py — single source of truth.

Spec asymmetric loss: prefer UNCERTAIN over wrong DUPLICATE
(spec §2.4). The signal-score thresholds are conservative:
score >= 2 for DUPLICATE, score <= -2 for DIFFERENT, else UNCERTAIN.
"""
from __future__ import annotations

import time

from src.services.engine_v2.config import thresholds as T
from src.services.engine_v2.evaluation import LabeledPair, PairResult
from src.services.engine_v2.features import PairFeatures


def _hard_rule_verdict(f: PairFeatures) -> tuple[str, str] | None:
    """Apply spec §2.3/§3.1/§3.4 hard rules. Returns (verdict, reason) or None."""
    # Spec §3.4 — pair already rejected by admin via feedback table.
    # Engine never re-proposes a rejected pair without substantially new evidence.
    # (Bug fix Pass 4 Phase 1: this rule was missing in initial version,
    # producing 5 trivial FPs in easy_obvious_different.)
    if f.pair_in_feedback:
        return ("different", "hard: pair in ai_duplicate_feedbacks (spec §3.4)")

    # Spec §3.1 — same source = never duplicates (defensive; blocking should filter)
    if not f.cross_source:
        return ("different", f"hard: same source_domain ({f.a_source})")

    # Spec §2.3 — different canonical category, neither unknown
    if (
        f.canonical_category_a != f.canonical_category_b
        and not f.canonical_category_a.startswith("unknown")
        and not f.canonical_category_b.startswith("unknown")
    ):
        return (
            "different",
            f"hard: canonical category mismatch "
            f"({f.canonical_category_a} vs {f.canonical_category_b})",
        )

    # Spec §2.3 (revised 2026-05-06) — year_built diff > 5
    if f.year_diff is not None and f.year_diff > T.YEAR_DIFF_DETERMINISTIC_DIFFERENT:
        return (
            "different",
            f"hard: year_built diff {f.year_diff} > {T.YEAR_DIFF_DETERMINISTIC_DIFFERENT}",
        )

    return None


def _signal_score(f: PairFeatures) -> tuple[float, list[str]]:
    """
    Signal-agreement score. Positive = duplicate-leaning, negative = different.
    Returns (score, list of reason fragments for transparency).
    """
    score = 0.0
    reasons: list[str] = []

    # Cosine similarity — primary text signal
    if f.cosine_sim is not None:
        if f.cosine_sim >= 0.95:
            score += 2.0
            reasons.append(f"cosine={f.cosine_sim:.3f} >= 0.95 (strong dup)")
        elif f.cosine_sim >= T.COSINE_HIGH_DUPLICATE:  # 0.92
            score += 1.0
            reasons.append(f"cosine={f.cosine_sim:.3f} >= {T.COSINE_HIGH_DUPLICATE} (dup)")
        elif f.cosine_sim < T.LLM_PREFILTER_COSINE_LOW_SKIP:  # 0.40
            score -= 2.0
            reasons.append(f"cosine={f.cosine_sim:.3f} < {T.LLM_PREFILTER_COSINE_LOW_SKIP} (strong diff)")
        elif f.cosine_sim < T.COSINE_LOW_DIFFERENT:  # 0.50
            score -= 1.0
            reasons.append(f"cosine={f.cosine_sim:.3f} < {T.COSINE_LOW_DIFFERENT} (diff)")
        # else: in [0.50, 0.92) — uncertain band, no contribution

    # Price ratio
    if f.price_ratio is not None:
        if f.price_ratio <= T.PRICE_RATIO_DUPLICATE_MAX:  # 1.30
            score += 1.0
            reasons.append(f"price_ratio={f.price_ratio:.2f} <= {T.PRICE_RATIO_DUPLICATE_MAX}")
        elif f.price_ratio >= T.PRICE_RATIO_DIFFERENT_MIN:  # 3.0
            score -= 2.0
            reasons.append(f"price_ratio={f.price_ratio:.2f} >= {T.PRICE_RATIO_DIFFERENT_MIN} (strong diff)")

    # "Same building, different units" check — strong DIFFERENT
    # GPS very close + bedroom mismatch + meaningful price diff
    if (
        f.gps_distance_m is not None and f.gps_distance_m <= T.GPS_SAME_BUILDING_M
        and f.bedrooms_match is False
        and f.price_diff_pct is not None and f.price_diff_pct > 20.0
    ):
        score -= 2.0
        reasons.append(
            f"same-building-diff-units: GPS={f.gps_distance_m:.0f}m, "
            f"bedroom mismatch, price diff {f.price_diff_pct:.1f}%"
        )

    # Long GPS distance — different (defensive; blocking already same-muni)
    if f.gps_distance_m is not None and f.gps_distance_m > T.GPS_DIFFERENT_KM * 1000:
        score -= 1.0
        reasons.append(f"GPS {f.gps_distance_m / 1000:.1f}km apart")

    # Size disagreement
    if f.size_diff_pct is not None and f.size_diff_pct > T.SIZE_DIFF_PCT_DIFFERENT:
        score -= 1.0
        reasons.append(f"size diff {f.size_diff_pct:.0f}%")

    # Bedroom match (weak)
    if f.bedrooms_match is True:
        score += 0.5
        reasons.append("bedrooms match")
    elif f.bedrooms_match is False:
        score -= 0.5
        reasons.append("bedrooms differ")

    # Same calc_area (weak duplicate signal — tighter than same muni)
    if f.same_calc_area is True:
        score += 0.5
        reasons.append("same calc_area")

    # Shared pHashes (when available — only when both sides have data)
    if f.shared_phash_count > 0:
        score += 1.0
        reasons.append(f"{f.shared_phash_count} shared pHash(es)")

    # Year_diff in [0, 5] band — already passed hard rule; weak signal
    if f.year_diff is not None and f.year_diff <= 1:
        score += 0.5
        reasons.append(f"year_diff={f.year_diff} (close)")

    return score, reasons


# Score-to-verdict thresholds. ±2 is the asymmetric-loss-aware boundary:
# at least one strong signal OR multiple weaker signals required.
DUPLICATE_THRESHOLD: float = 2.0
DIFFERENT_THRESHOLD: float = -2.0


async def score_pair(pair: LabeledPair, features: PairFeatures) -> PairResult:
    """
    Rule-based scorer. Async signature for evaluate() compatibility,
    but no actual await — pure CPU work.
    """
    t0 = time.perf_counter()

    # Stage 1: hard rules
    hard = _hard_rule_verdict(features)
    if hard is not None:
        verdict, reason = hard
        return PairResult(
            pair_id=pair.id,
            pair_a_id=pair.property_a_id,
            pair_b_id=pair.property_b_id,
            category=pair.category,
            ground_truth=pair.ground_truth,
            predicted=verdict,
            confidence=1.0,  # hard rule = full confidence
            cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
            reasoning=reason,
        )

    # Stage 2: signal-agreement
    score, reasons = _signal_score(features)
    if score >= DUPLICATE_THRESHOLD:
        verdict = "duplicate"
    elif score <= DIFFERENT_THRESHOLD:
        verdict = "different"
    else:
        verdict = "uncertain"

    # Confidence proxy: |score| / max_possible_score (~6)
    confidence = min(1.0, abs(score) / 6.0)

    return PairResult(
        pair_id=pair.id,
        pair_a_id=pair.property_a_id,
        pair_b_id=pair.property_b_id,
        category=pair.category,
        ground_truth=pair.ground_truth,
        predicted=verdict,
        confidence=confidence,
        cost_usd=0.0,
        latency_ms=(time.perf_counter() - t0) * 1000,
        reasoning=f"signal score={score:+.1f}; " + " | ".join(reasons),
    )
