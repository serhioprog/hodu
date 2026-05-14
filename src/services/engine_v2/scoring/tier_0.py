"""
Tier-0 hard-rule short-circuit — RESEARCH.md §12.5 + spec §6.1.

Deterministic rules from spec sections:
  §3.4 — pair already in ai_duplicate_feedbacks (admin rejected)
  §3.1 — same source_domain (one site lists each property at most once)
  §2.3 — different canonical category (Villa vs Apartment vs Land vs House)
  §2.3 — year_built diff > YEAR_DIFF_DETERMINISTIC_DIFFERENT (revised 2026-05-06)

If ANY rule fires: short-circuit with verdict="different", tier_emitted=0,
confidence=1.0. The remaining tiers (T1, T2, T3) are not invoked for
this pair.

If NO rule fires: returns None — caller cascades to Tier 1.

Spec §11 verdict_locked check is NOT here. That requires a DB lookup
on the pair's cluster_id and lives in HybridEngine.check_pair, not
in the pure feature-based Tier 0.

Logic ported verbatim from src/scoring/rule_based.py:_hard_rule_verdict
to guarantee Day-1 metrics match the rule_based.py baseline (no
behavioral changes; only refactored into a class).
"""
from __future__ import annotations

import time

from src.services.engine_v2.config import thresholds as T

from ..dedup_report import EngineVerdict
from ..features import PairFeatures


class Tier0Filter:
    """Deterministic spec hard-rule filter.

    Stateless. Single public method evaluate(features) -> EngineVerdict | None.
    """

    def evaluate(self, features: PairFeatures) -> EngineVerdict | None:
        """Apply spec §3.4 / §3.1 / §2.3 hard rules in order.

        Returns:
            EngineVerdict(verdict="different", tier_emitted=0, confidence=1.0)
                if any rule fires.
            None if pair passes through to Tier 1.
        """
        t0 = time.perf_counter()

        # Spec §3.4 — pair already rejected by admin via feedback table.
        # Engine never re-proposes a rejected pair without substantially
        # new evidence.
        if features.pair_in_feedback:
            return self._make_different(
                "hard: pair in ai_duplicate_feedbacks (spec §3.4)", t0,
            )

        # Spec §3.1 — same source = never duplicates (defensive; blocking
        # should filter cross-source upstream).
        if not features.cross_source:
            return self._make_different(
                f"hard: same source_domain ({features.a_source})", t0,
            )

        # Spec §2.3 — different canonical category, neither unknown.
        if (
            features.canonical_category_a != features.canonical_category_b
            and not features.canonical_category_a.startswith("unknown")
            and not features.canonical_category_b.startswith("unknown")
        ):
            return self._make_different(
                f"hard: canonical category mismatch "
                f"({features.canonical_category_a} vs {features.canonical_category_b})",
                t0,
            )

        # Spec §2.3 (revised 2026-05-06) — year_built diff > N.
        if (
            features.year_diff is not None
            and features.year_diff > T.YEAR_DIFF_DETERMINISTIC_DIFFERENT
        ):
            return self._make_different(
                f"hard: year_built diff {features.year_diff} > "
                f"{T.YEAR_DIFF_DETERMINISTIC_DIFFERENT}",
                t0,
            )

        # No rule fired — pass through to Tier 1.
        return None

    @staticmethod
    def _make_different(reason: str, started_at: float) -> EngineVerdict:
        """Construct a hard-rule DIFFERENT verdict with timing."""
        return EngineVerdict(
            verdict="different",
            confidence=1.0,
            reasoning=reason,
            tier_emitted=0,
            cost_usd=0.0,
            latency_ms=(time.perf_counter() - started_at) * 1000,
        )
