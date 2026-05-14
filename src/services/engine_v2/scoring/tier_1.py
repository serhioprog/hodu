"""
Tier-1 signal-agreement scoring — RESEARCH.md §12.5.

Weighted-signal score over PairFeatures (cosine, price_ratio, GPS,
size, bedrooms, area, pHash, year_diff). Strong agreement (|score| >= 2)
emits a verdict; weak/contradictory escalates to Tier 2.

Logic ported verbatim from src/scoring/rule_based.py:_signal_score +
score-to-verdict thresholds at lines 145-180. Day 1 produces identical
output to rule_based.py on Tier 0+1 (the safety floor).

Adaptive-weights hook: Tier1Scorer.__init__ accepts an optional
signal_weights dict for Pass-6 Day-4 PART B (per-source-pair
multipliers from source_pair_calibration table per RESEARCH.md §12.4).
Day 1 uses defaults; Day 4 will populate weights from the DB.
"""
from __future__ import annotations

import time

from src.services.engine_v2.config import thresholds as T

from ..dedup_report import EngineVerdict
from ..features import PairFeatures


# Score-to-verdict thresholds. ±2 is the asymmetric-loss-aware boundary
# (spec §2.4: prefer UNCERTAIN over wrong DUPLICATE). At least one
# strong signal OR multiple weak signals required to emit.
DUPLICATE_THRESHOLD: float = 2.0
DIFFERENT_THRESHOLD: float = -2.0

# Confidence normalizer: |score| / MAX_SCORE_NORMALIZER, clamped to 1.0.
# Empirical max observed |score| in Pass-4 train is ~6.0 — matches
# rule_based.py confidence proxy (line 183).
MAX_SCORE_NORMALIZER: float = 6.0


# Default signal weights — keys mirror the contributors below. Pass-6
# Day-4 will replace these with per-source-pair multipliers.
DEFAULT_SIGNAL_WEIGHTS: dict[str, float] = {
    "cosine_strong_dup": 2.0,        # cosine >= 0.95
    "cosine_dup": 1.0,               # cosine in [0.92, 0.95)
    "cosine_low_diff": -2.0,         # cosine < 0.40
    "cosine_diff": -1.0,             # cosine in [0.40, 0.50)
    "price_ratio_dup": 1.0,          # ratio <= 1.30
    "price_ratio_diff": -2.0,        # ratio >= 3.0
    "same_building_diff_units": -2.0,
    "gps_far": -1.0,                 # > 50 km apart
    "size_diff": -1.0,               # > 50% size disagreement
    "bedrooms_match": 0.5,
    "bedrooms_differ": -0.5,
    "same_calc_area": 0.5,
    "shared_phash": 1.0,
    "year_close": 0.5,               # year_diff <= 1
}


class Tier1Scorer:
    """Signal-agreement scoring with adaptive-weights hook.

    Day 1: uses DEFAULT_SIGNAL_WEIGHTS verbatim from rule_based.py.
    Day 4: signal_weights dict will be populated from
    source_pair_calibration table per RESEARCH.md §12.4 PART B.
    """

    def __init__(self, signal_weights: dict[str, float] | None = None) -> None:
        self._weights = signal_weights or DEFAULT_SIGNAL_WEIGHTS

    def score(self, features: PairFeatures) -> EngineVerdict:
        """Compute weighted signal score, return EngineVerdict.

        Returns:
            EngineVerdict with tier_emitted=1.
            verdict = "duplicate" if score >= +2.0
                    | "different" if score <= -2.0
                    | "uncertain" otherwise
            confidence = min(1.0, |score| / 6.0)
        """
        t0 = time.perf_counter()
        score, reasons = self._compute_signals(features)

        if score >= DUPLICATE_THRESHOLD:
            verdict = "duplicate"
        elif score <= DIFFERENT_THRESHOLD:
            verdict = "different"
        else:
            verdict = "uncertain"

        confidence = min(1.0, abs(score) / MAX_SCORE_NORMALIZER)
        reasoning = f"signal score={score:+.1f}; " + " | ".join(reasons)

        return EngineVerdict(
            verdict=verdict,
            confidence=confidence,
            reasoning=reasoning,
            tier_emitted=1,
            cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
        )

    def _compute_signals(self, f: PairFeatures) -> tuple[float, list[str]]:
        """Verbatim port of rule_based._signal_score lines 61-140.

        Positive = duplicate-leaning; negative = different-leaning.
        Returns (score, reasons-fragments) for transparency in
        EngineVerdict.reasoning.
        """
        w = self._weights
        score = 0.0
        reasons: list[str] = []

        # Cosine similarity — primary text signal
        if f.cosine_sim is not None:
            if f.cosine_sim >= 0.95:
                score += w["cosine_strong_dup"]
                reasons.append(f"cosine={f.cosine_sim:.3f} >= 0.95 (strong dup)")
            elif f.cosine_sim >= T.COSINE_HIGH_DUPLICATE:                 # 0.92
                score += w["cosine_dup"]
                reasons.append(f"cosine={f.cosine_sim:.3f} >= {T.COSINE_HIGH_DUPLICATE} (dup)")
            elif f.cosine_sim < T.LLM_PREFILTER_COSINE_LOW_SKIP:          # 0.40
                score += w["cosine_low_diff"]
                reasons.append(f"cosine={f.cosine_sim:.3f} < {T.LLM_PREFILTER_COSINE_LOW_SKIP} (strong diff)")
            elif f.cosine_sim < T.COSINE_LOW_DIFFERENT:                   # 0.50
                score += w["cosine_diff"]
                reasons.append(f"cosine={f.cosine_sim:.3f} < {T.COSINE_LOW_DIFFERENT} (diff)")
            # else: in [0.50, 0.92) — uncertain band, no contribution

        # Price ratio
        if f.price_ratio is not None:
            if f.price_ratio <= T.PRICE_RATIO_DUPLICATE_MAX:              # 1.30
                score += w["price_ratio_dup"]
                reasons.append(f"price_ratio={f.price_ratio:.2f} <= {T.PRICE_RATIO_DUPLICATE_MAX}")
            elif f.price_ratio >= T.PRICE_RATIO_DIFFERENT_MIN:            # 3.0
                score += w["price_ratio_diff"]
                reasons.append(f"price_ratio={f.price_ratio:.2f} >= {T.PRICE_RATIO_DIFFERENT_MIN} (strong diff)")

        # "Same building, different units" — strong DIFFERENT
        if (
            f.gps_distance_m is not None and f.gps_distance_m <= T.GPS_SAME_BUILDING_M
            and f.bedrooms_match is False
            and f.price_diff_pct is not None and f.price_diff_pct > 20.0
        ):
            score += w["same_building_diff_units"]
            reasons.append(
                f"same-building-diff-units: GPS={f.gps_distance_m:.0f}m, "
                f"bedroom mismatch, price diff {f.price_diff_pct:.1f}%"
            )

        # Long GPS distance — different (defensive; blocking already same-muni)
        if f.gps_distance_m is not None and f.gps_distance_m > T.GPS_DIFFERENT_KM * 1000:
            score += w["gps_far"]
            reasons.append(f"GPS {f.gps_distance_m / 1000:.1f}km apart")

        # Size disagreement
        if f.size_diff_pct is not None and f.size_diff_pct > T.SIZE_DIFF_PCT_DIFFERENT:
            score += w["size_diff"]
            reasons.append(f"size diff {f.size_diff_pct:.0f}%")

        # Bedroom match (weak)
        if f.bedrooms_match is True:
            score += w["bedrooms_match"]
            reasons.append("bedrooms match")
        elif f.bedrooms_match is False:
            score += w["bedrooms_differ"]
            reasons.append("bedrooms differ")

        # Same calc_area (weak duplicate signal — tighter than same muni)
        if f.same_calc_area is True:
            score += w["same_calc_area"]
            reasons.append("same calc_area")

        # Shared pHashes (when available — only when both sides have data)
        if f.shared_phash_count > 0:
            score += w["shared_phash"]
            reasons.append(f"{f.shared_phash_count} shared pHash(es)")

        # Year_diff in [0, 1] band — already passed hard rule; weak signal
        if f.year_diff is not None and f.year_diff <= 1:
            score += w["year_close"]
            reasons.append(f"year_diff={f.year_diff} (close)")

        return score, reasons
