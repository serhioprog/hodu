"""
AI cost tracker.

Module-level singleton that AI services call after each API request.
The orchestrator (daily_sync) takes a snapshot at domain boundaries to
build per-domain cost reports for the Telegram notifier.

Pricing is hardcoded here as a single source of truth. When OpenAI
changes their rates, edit one dict — every report updates.

All prices in USD. Tokens are billed per 1M.

Thread-safety:
  • The tracker uses asyncio.Lock to prevent races when daily_sync
    captures snapshots while AI services are still recording (e.g. an
    in-flight Vision call landing during the snapshot).
  • Counters are simple ints/floats — no nested mutation.

Reset semantics:
  • snapshot_and_reset() returns the accumulated values AND zeroes
    them. Used at the boundary of each sync domain to get a "diff"
    rather than running totals.
  • Total totals (since process start) are kept separately for
    end-of-day summary.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict


# =====================================================================
# PRICING TABLE — update when OpenAI changes rates.
# =====================================================================
# Per-token rates: USD per 1 million tokens
_PRICE_PER_1M_TOKENS: Dict[str, Dict[str, float]] = {
    "gpt-4o-mini":            {"in": 0.15,   "out": 0.60},
    "gpt-4o":                 {"in": 2.50,   "out": 10.00},
    "text-embedding-3-small": {"in": 0.02,   "out": 0.0},
    "text-embedding-3-large": {"in": 0.13,   "out": 0.0},
}

# Per-call rates: USD per single API call (Vision images priced per call)
_PRICE_PER_CALL: Dict[str, float] = {
    # gpt-4o vision with 3x ~720p images ≈ 1100 input tokens + ~150 output
    # Empirical average $0.011/call. Cheaper than per-token modelling
    # when image counts vary little.
    "gpt-4o-vision":  0.011,
}


def _token_cost(model: str, in_tokens: int, out_tokens: int) -> float:
    """Compute USD cost for a chat/embedding call."""
    rates = _PRICE_PER_1M_TOKENS.get(model)
    if not rates:
        return 0.0
    return (in_tokens * rates["in"] + out_tokens * rates["out"]) / 1_000_000


# =====================================================================
# DATACLASSES — bucketed counters for one snapshot window
# =====================================================================
@dataclass
class _LLMBucket:
    calls:        int   = 0
    in_tokens:    int   = 0
    out_tokens:   int   = 0
    cost_usd:     float = 0.0
    failed_calls: int   = 0


@dataclass
class _VisionBucket:
    calls:        int   = 0
    cost_usd:     float = 0.0
    failed_calls: int   = 0  # incl. image-download failures, API errors


@dataclass
class _EmbeddingBucket:
    calls:     int   = 0
    in_tokens: int   = 0
    cost_usd:  float = 0.0


@dataclass
class CostSnapshot:
    """Aggregated AI usage for one window (domain or whole day)."""
    llm:       _LLMBucket       = field(default_factory=_LLMBucket)
    vision:    _VisionBucket    = field(default_factory=_VisionBucket)
    embedding: _EmbeddingBucket = field(default_factory=_EmbeddingBucket)

    @property
    def total_cost_usd(self) -> float:
        return (
            self.llm.cost_usd
            + self.vision.cost_usd
            + self.embedding.cost_usd
        )


# =====================================================================
# SINGLETON TRACKER
# =====================================================================
class _CostTracker:
    def __init__(self) -> None:
        self._current = CostSnapshot()
        self._daily   = CostSnapshot()
        self._lock    = asyncio.Lock()

    async def record_llm(
        self,
        model: str,
        in_tokens: int,
        out_tokens: int,
        success: bool = True,
    ) -> None:
        cost = _token_cost(model, in_tokens, out_tokens) if success else 0.0
        async with self._lock:
            for bucket in (self._current.llm, self._daily.llm):
                bucket.calls     += 1
                bucket.in_tokens  += in_tokens
                bucket.out_tokens += out_tokens
                bucket.cost_usd  += cost
                if not success:
                    bucket.failed_calls += 1

    async def record_vision(
        self,
        success: bool,
        model: str = "gpt-4o-vision",
    ) -> None:
        # Vision: bill only on success (failed image-download = no charge
        # because OpenAI rejects the request before processing).
        cost = _PRICE_PER_CALL.get(model, 0.0) if success else 0.0
        async with self._lock:
            for bucket in (self._current.vision, self._daily.vision):
                bucket.calls    += 1
                bucket.cost_usd += cost
                if not success:
                    bucket.failed_calls += 1

    async def record_embedding(
        self,
        in_tokens: int,
        model: str = "text-embedding-3-small",
    ) -> None:
        cost = _token_cost(model, in_tokens, 0)
        async with self._lock:
            for bucket in (self._current.embedding, self._daily.embedding):
                bucket.calls     += 1
                bucket.in_tokens += in_tokens
                bucket.cost_usd  += cost

    async def snapshot_and_reset(self) -> CostSnapshot:
        """
        Return current accumulated costs and reset the current bucket.
        Daily totals are NOT reset.
        """
        async with self._lock:
            snap = self._current
            self._current = CostSnapshot()
            return snap

    async def daily_snapshot(self) -> CostSnapshot:
        """
        Return the daily cumulative snapshot (since process start, or
        since last reset_daily). Does NOT reset.
        """
        async with self._lock:
            # Return a defensive copy so caller can't mutate internals
            return CostSnapshot(
                llm=_LLMBucket(**self._daily.llm.__dict__),
                vision=_VisionBucket(**self._daily.vision.__dict__),
                embedding=_EmbeddingBucket(**self._daily.embedding.__dict__),
            )

    async def reset_daily(self) -> None:
        async with self._lock:
            self._daily = CostSnapshot()


# Module-level instance. Import this directly:
#     from src.services.cost_tracker import cost_tracker
cost_tracker = _CostTracker()