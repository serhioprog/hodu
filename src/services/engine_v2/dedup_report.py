"""
Engine v2 scoring outputs — dataclasses for verdicts and aggregate reports.

Per RESEARCH.md §12.5.10. Two types live here:

  EngineVerdict — output of a single pair scoring. Frozen/immutable.
                  Distinct from evaluation.Verdict (which is a Literal
                  string alias) — name "EngineVerdict" avoids collision.

  DedupReport — aggregate output of a full HybridEngine.run_full_dedup
                run. Mutable, accumulating over a session.

cost_usd type: float (matches src.services.cost_tracker storage type).
RESEARCH.md §12.5.10 sketched Decimal; implementation diverged after
verifying cost_tracker uses float to avoid coercion overhead at every
integration point.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


VerdictLiteral = Literal["duplicate", "different", "uncertain"]


@dataclass(frozen=True)
class EngineVerdict:
    """Output of HybridEngine.score_pair (and the public API that wraps it).

    Per RESEARCH.md §12.5.10: tier_emitted is the integer tier that
    produced the verdict, plus a -1 sentinel for the spec §11
    verdict_locked short-circuit (admin authority, no scoring).

    Frozen for safety: a Verdict is a value, not a buffer. Caller must
    not mutate. Reasoning is human-readable; structured fields drive
    decisions.
    """
    verdict: VerdictLiteral
    confidence: float                       # [0.0, 1.0]
    reasoning: str
    tier_emitted: int                       # 0 | 1 | 2 | 3 | -1 (verdict_locked sentinel)
    cost_usd: float = 0.0
    latency_ms: float = 0.0


@dataclass
class DedupReport:
    """Aggregate output of HybridEngine.run_full_dedup.

    Per RESEARCH.md §12.5.10. Mutable so the engine can accumulate
    counts during a single full-rescan pass.

    Fields populated incrementally by the run:
      - cluster counts (created/updated/unchanged) — set after §12.5.9
        cluster construction
      - pair counts (scored/cached) — set during scoring loop
      - mismerge_flags_emitted — set during mismerge detection (§12.5.11)
      - cost_usd — accumulated from each EngineVerdict.cost_usd
      - latency_ms — wall time of the full run
      - by_tier — emit-count histogram across T0..T3
      - uncertain_count — pairs surfaced to admin (PENDING cluster status)

    Day 1 status: dataclass declared but NOT populated by Day 1 code
    (run_full_dedup raises NotImplementedError until Day 5).
    """
    clusters_created: int = 0
    clusters_updated: int = 0
    clusters_unchanged: int = 0
    pairs_scored: int = 0
    pairs_cached: int = 0                   # cache hits
    mismerge_flags_emitted: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    by_tier: dict[int, int] = field(default_factory=dict)
    uncertain_count: int = 0

    # ---- Phase 1 shadow-mode metrics (Pass 6 Sprint B) ----
    # In shadow mode run_full_dedup does NOT write property_clusters,
    # so clusters_created/updated/unchanged stay 0. The DSU output is
    # split into new_clusters_proposed (genuine new merges proposed
    # by engine v2) vs attached_clusters_count (existing approved
    # clusters surfaced as DSU components — pass-through, no merge).
    # The split is critical for daily diff reporting: "engine v2
    # proposed N new merges" is a distinct signal from "DSU echoed
    # M existing approved clusters".
    new_clusters_proposed: int = 0          # ProposedCluster.is_attachment=False
    attached_clusters_count: int = 0        # ProposedCluster.is_attachment=True
    bridge_blocks: int = 0                  # len(cluster_result.bridge_blocks)
    approved_disagreements: int = 0         # len(cluster_result.approved_disagreements)
    errors_count: int = 0                   # per-pair exceptions caught during scoring
