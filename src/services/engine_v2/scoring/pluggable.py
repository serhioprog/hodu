"""
Pluggable scoring backend Protocol — RESEARCH.md §12.5.1.

Tiers 2 and 3 of the hybrid engine are pluggable so that backend
swaps (classical ML model, OpenAI vs Ollama vs fine-tuned) require
only an env-var change + service restart, no code change.

Tiers 0 and 1 are NOT pluggable (deterministic core; only thresholds
and signal weights adapt via Pass-6 Day-4 calibration).

Day 1 ships two stub backends that always return UNCERTAIN. They let
the cascade pipeline (T0 -> T1 -> T2 -> T3) wire correctly even before
real backends arrive on Day 3 (ClassicalMLBackend + OpenAIBackend).
"""
from __future__ import annotations

import time
from typing import Protocol

from src.models.domain import Property

from ..dedup_report import EngineVerdict
from ..features import PairFeatures


class ScoringBackend(Protocol):
    """Protocol for Tier-2 and Tier-3 scoring backends.

    Implementations: ClassicalMLBackend (Tier 2, Pass-7 future),
    OpenAIBackend (Tier 3, Pass-6 Day-3), OllamaBackend (Tier 3,
    Pass-8 future), FineTunedOpenAIBackend (Tier 3, Pass-8 future).

    Day 3 Path B widening: score() takes both PairFeatures (for
    backends that prefer pre-computed signals) AND raw Property
    objects (for ML feature extraction or LLM description reading).
    Backends use whichever input fits their needs.

    fit() is optional — only learnable backends (e.g. classical ML)
    implement it. LLM backends ignore it. Protocol declares only the
    universal score() method; Day 3 ClassicalMLBackend will define
    fit() directly without it being a Protocol requirement.
    """

    async def score(
        self,
        features: PairFeatures,
        prop_a: Property,
        prop_b: Property,
    ) -> EngineVerdict: ...


class StubTier2Backend:
    """Day-1 placeholder for the Tier-2 classical-ML backend.

    Always returns UNCERTAIN — this lets the cascade complete cleanly
    (T0 + T1 emit when confident; T2 + T3 stubs propagate UNCERTAIN
    when not). Replaced on Day 3 with Tier2MLBackend.
    """

    async def score(
        self,
        features: PairFeatures,
        prop_a: Property,
        prop_b: Property,
    ) -> EngineVerdict:
        t0 = time.perf_counter()
        return EngineVerdict(
            verdict="uncertain",
            confidence=0.0,
            reasoning="tier-2 stub (Day-1 placeholder; ClassicalMLBackend on Day 3)",
            tier_emitted=2,
            cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
        )


class StubTier3Backend:
    """Day-1 placeholder for the Tier-3 LLM backend.

    Always returns UNCERTAIN. Replaced on Day 3 with OpenAIBackend
    (gpt-4o-mini via OpenAI Function Calling).
    """

    async def score(
        self,
        features: PairFeatures,
        prop_a: Property,
        prop_b: Property,
    ) -> EngineVerdict:
        t0 = time.perf_counter()
        return EngineVerdict(
            verdict="uncertain",
            confidence=0.0,
            reasoning="tier-3 stub (Day-1 placeholder; OpenAIBackend on Day 3)",
            tier_emitted=3,
            cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
        )
