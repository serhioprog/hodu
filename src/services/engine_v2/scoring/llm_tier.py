"""
LLM-tier pair scorer: gpt-4o-mini Function Calling.

Pipeline (Phase 2 of Pass 4 bake-off):
  1. Apply spec hard rules first (cross-source, canonical, year_diff,
     feedback). Identical to rule_based — short-circuit on hard rule.
  2. Cosine pre-filter (per thresholds.py decision matrix):
       cosine >= 0.95   -> emit DUPLICATE  (skip LLM)
       cosine <  0.40   -> emit DIFFERENT  (skip LLM)
       otherwise        -> call LLM
  3. LLM call: compact prompt + 2 synthetic few-shot examples + tool
     schema with verdict/confidence/reasoning. Temperature 0 for
     determinism.
  4. Cost tracked via src.services.cost_tracker (allowed exception
     per engine-isolation memory).

Per-pair cost computed locally for results JSON; cumulative via tracker.
"""
from __future__ import annotations

import json
import time
from typing import Any

from openai import AsyncOpenAI

from src.services.engine_v2.config import thresholds as T
from src.services.engine_v2.evaluation import LabeledPair, PairResult
from src.services.engine_v2.features import PairFeatures
from src.services.cost_tracker import cost_tracker  # allowed exception (engine-isolation memory)


# gpt-4o-mini pricing as of 2026-05-07: $0.15 / 1M input, $0.60 / 1M output
_INPUT_USD_PER_1M = 0.15
_OUTPUT_USD_PER_1M = 0.60


def _estimate_cost(in_tokens: int, out_tokens: int) -> float:
    return (
        in_tokens / 1_000_000 * _INPUT_USD_PER_1M
        + out_tokens / 1_000_000 * _OUTPUT_USD_PER_1M
    )


# Lazy singleton — first use creates the async client (uses OPENAI_API_KEY env).
_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


# =============================================================
# Prompt + tool schema
# =============================================================

SYSTEM_PROMPT = """You classify whether two real-estate listings refer to the same physical property.

Hard rules — answer "different" when these apply:
- Different canonical categories (Villa vs Apartment, Land vs House, etc.) are never the same.
- Year_built differs by more than 5 years when both are populated.
- Same building, different units (e.g., apartments 4A vs 4B at the same address).
- Land plot vs the same plot with a house built on it — different transactions.

Otherwise weigh:
- Description / photo similarity
- Price agreement (sources may disagree by up to ~30%)
- Size and bedroom agreement
- GPS proximity (same coordinates = strong signal)
- Year_built proximity (within 5 years is acceptable drift between sources)

Always emit via the classify_pair tool. Prefer "uncertain" over a wrong "duplicate"
— false positives are very harmful in this real-estate-MDM domain (spec §2.4)."""


# Synthetic few-shot examples (do NOT correspond to actual test pairs).
FEW_SHOT_USER_DUPLICATE = """Example pair (DUPLICATE):

Property A:
  source: glrealestate.gr   canonical: villa
  price: 650000 EUR   size: 160 sqm   year_built: 2024
  description: Modern villa with pool, sea view, 160 sqm, 4 bedrooms, built 2024.

Property B:
  source: realestatecenter.gr   canonical: villa
  price: 700000 EUR   size: 160 sqm   year_built: 2024
  description: New construction villa, 160 m2, 4 bedrooms, sea view, swimming pool.

Signals:
  cosine_sim: 0.93   gps_distance_m: 0   price_ratio: 1.08
  year_diff: 0   size_diff_pct: 0   bedrooms_match: True
  same_canonical_category: True   same_calc_area: True
  same_calc_municipality: True   cross_source: True"""

FEW_SHOT_TOOL_DUPLICATE = {
    "verdict": "duplicate",
    "confidence": 0.92,
    "reasoning": (
        "Same coordinates, same year_built, same size, same bedrooms, "
        "8% price disagreement within source-tolerance. Descriptions "
        "describe the same modern villa."
    ),
}

FEW_SHOT_USER_DIFFERENT = """Example pair (DIFFERENT — same building, different units):

Property A:
  source: glrealestate.gr   canonical: villa
  price: 1200000 EUR   size: 220 sqm   year_built: 2020
  description: Beachfront villa with private pool, 220 sqm of luxury living.

Property B:
  source: greekexclusiveproperties.com   canonical: villa
  price: 480000 EUR   size: 130 sqm   year_built: 2024
  description: New 3-bedroom villa, 130 m2, walking distance to beach.

Signals:
  cosine_sim: 0.87   gps_distance_m: 0   price_ratio: 2.50
  year_diff: 4   size_diff_pct: 41   bedrooms_match: False
  same_canonical_category: True   same_calc_area: True
  same_calc_municipality: True   cross_source: True"""

FEW_SHOT_TOOL_DIFFERENT = {
    "verdict": "different",
    "confidence": 0.85,
    "reasoning": (
        "Same coordinates indicate same complex, but 41% size disagreement, "
        "2.5x price ratio, bedroom mismatch all point to different units in "
        "the same building (spec §2.3)."
    ),
}


CLASSIFY_TOOL = {
    "type": "function",
    "function": {
        "name": "classify_pair",
        "description": "Emit the verdict, confidence, and brief reasoning.",
        "parameters": {
            "type": "object",
            "properties": {
                "verdict": {
                    "type": "string",
                    "enum": ["duplicate", "different", "uncertain"],
                    "description": "Final classification.",
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "description": "Confidence in the verdict, 0.0 to 1.0.",
                },
                "reasoning": {
                    "type": "string",
                    "description": "Brief reasoning, 1-3 sentences.",
                },
            },
            "required": ["verdict", "confidence", "reasoning"],
        },
    },
}


def _trim(s: str | None, n: int = 300) -> str:
    if not s:
        return "(no description)"
    s = s.strip().replace("\n", " ")
    return s if len(s) <= n else s[:n] + "..."


def _fmt(v: Any) -> str:
    return "-" if v is None else str(v)


def render_pair_prompt(f: PairFeatures) -> str:
    """Compact pair representation for the LLM."""
    return (
        f"Property A:\n"
        f"  source: {f.a_source}   canonical: {f.canonical_category_a}\n"
        f"  price: {_fmt(f.price_a)} EUR   size: {_fmt(f.size_a)} sqm   "
        f"year_built: {_fmt(f.year_a)}\n"
        f"  description: {_trim(f.description_a)}\n"
        f"\n"
        f"Property B:\n"
        f"  source: {f.b_source}   canonical: {f.canonical_category_b}\n"
        f"  price: {_fmt(f.price_b)} EUR   size: {_fmt(f.size_b)} sqm   "
        f"year_built: {_fmt(f.year_b)}\n"
        f"  description: {_trim(f.description_b)}\n"
        f"\n"
        f"Signals:\n"
        f"  cosine_sim: {_fmt(round(f.cosine_sim, 3) if f.cosine_sim is not None else None)}   "
        f"gps_distance_m: {_fmt(round(f.gps_distance_m) if f.gps_distance_m is not None else None)}   "
        f"price_ratio: {_fmt(round(f.price_ratio, 2) if f.price_ratio is not None else None)}\n"
        f"  year_diff: {_fmt(f.year_diff)}   size_diff_pct: {_fmt(round(f.size_diff_pct, 1) if f.size_diff_pct is not None else None)}   "
        f"bedrooms_match: {_fmt(f.bedrooms_match)}\n"
        f"  same_canonical_category: {f.same_canonical_category}   "
        f"same_calc_area: {_fmt(f.same_calc_area)}\n"
        f"  same_calc_municipality: {_fmt(f.same_municipality)}   "
        f"cross_source: {f.cross_source}   shared_phash: {f.shared_phash_count}"
    )


# =============================================================
# Hard-rule prelude (mirrors rule_based, kept local for clarity)
# =============================================================

def _hard_rule_verdict(f: PairFeatures) -> tuple[str, str] | None:
    if f.pair_in_feedback:
        return ("different", "hard: pair in ai_duplicate_feedbacks (spec §3.4)")
    if not f.cross_source:
        return ("different", f"hard: same source_domain ({f.a_source})")
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
    if f.year_diff is not None and f.year_diff > T.YEAR_DIFF_DETERMINISTIC_DIFFERENT:
        return ("different", f"hard: year_built diff {f.year_diff} > 5")
    return None


# =============================================================
# Scorer
# =============================================================

async def score_pair(pair: LabeledPair, features: PairFeatures) -> PairResult:
    t0 = time.perf_counter()

    # Tier 0: hard rules (free)
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
            confidence=1.0,
            cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
            reasoning=reason,
        )

    # Tier 1: cosine pre-filter (skip LLM at the extremes)
    cs = features.cosine_sim
    if cs is not None and cs >= T.LLM_PREFILTER_COSINE_HIGH_SKIP:
        return PairResult(
            pair_id=pair.id, pair_a_id=pair.property_a_id, pair_b_id=pair.property_b_id,
            category=pair.category, ground_truth=pair.ground_truth,
            predicted="duplicate", confidence=0.95, cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
            reasoning=f"prefilter: cosine {cs:.3f} >= {T.LLM_PREFILTER_COSINE_HIGH_SKIP} (skip LLM)",
        )
    if cs is not None and cs < T.LLM_PREFILTER_COSINE_LOW_SKIP:
        return PairResult(
            pair_id=pair.id, pair_a_id=pair.property_a_id, pair_b_id=pair.property_b_id,
            category=pair.category, ground_truth=pair.ground_truth,
            predicted="different", confidence=0.95, cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
            reasoning=f"prefilter: cosine {cs:.3f} < {T.LLM_PREFILTER_COSINE_LOW_SKIP} (skip LLM)",
        )

    # Tier 2: LLM call
    user_prompt = render_pair_prompt(features)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": FEW_SHOT_USER_DUPLICATE},
        {
            "role": "assistant", "content": None,
            "tool_calls": [{
                "id": "call_dup_example",
                "type": "function",
                "function": {
                    "name": "classify_pair",
                    "arguments": json.dumps(FEW_SHOT_TOOL_DUPLICATE),
                },
            }],
        },
        {"role": "tool", "tool_call_id": "call_dup_example", "content": "ok"},
        {"role": "user", "content": FEW_SHOT_USER_DIFFERENT},
        {
            "role": "assistant", "content": None,
            "tool_calls": [{
                "id": "call_diff_example",
                "type": "function",
                "function": {
                    "name": "classify_pair",
                    "arguments": json.dumps(FEW_SHOT_TOOL_DIFFERENT),
                },
            }],
        },
        {"role": "tool", "tool_call_id": "call_diff_example", "content": "ok"},
        {"role": "user", "content": user_prompt},
    ]

    client = _get_client()
    try:
        response = await client.chat.completions.create(
            model=T.LLM_MODEL,
            messages=messages,
            tools=[CLASSIFY_TOOL],
            tool_choice={"type": "function", "function": {"name": "classify_pair"}},
            temperature=T.LLM_TEMPERATURE,
            max_tokens=T.LLM_MAX_OUTPUT_TOKENS,
        )
    except Exception as e:
        return PairResult(
            pair_id=pair.id, pair_a_id=pair.property_a_id, pair_b_id=pair.property_b_id,
            category=pair.category, ground_truth=pair.ground_truth,
            predicted="uncertain", confidence=None, cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
            reasoning=f"LLM error: {type(e).__name__}: {e}",
        )

    # Parse function call
    msg = response.choices[0].message
    if not msg.tool_calls:
        return PairResult(
            pair_id=pair.id, pair_a_id=pair.property_a_id, pair_b_id=pair.property_b_id,
            category=pair.category, ground_truth=pair.ground_truth,
            predicted="uncertain", confidence=None, cost_usd=0.0,
            latency_ms=(time.perf_counter() - t0) * 1000,
            reasoning="LLM did not emit a tool call",
        )
    args = json.loads(msg.tool_calls[0].function.arguments)
    verdict = args.get("verdict", "uncertain")
    confidence = float(args.get("confidence", 0.5))
    reasoning = args.get("reasoning", "")

    # Cost tracking
    usage = response.usage
    in_tokens = int(usage.prompt_tokens) if usage else 0
    out_tokens = int(usage.completion_tokens) if usage else 0
    pair_cost = _estimate_cost(in_tokens, out_tokens)
    await cost_tracker.record_llm(
        model=T.LLM_MODEL, in_tokens=in_tokens, out_tokens=out_tokens, success=True,
    )

    return PairResult(
        pair_id=pair.id,
        pair_a_id=pair.property_a_id,
        pair_b_id=pair.property_b_id,
        category=pair.category,
        ground_truth=pair.ground_truth,
        predicted=verdict if verdict in ("duplicate", "different", "uncertain") else "uncertain",
        confidence=confidence,
        cost_usd=pair_cost,
        latency_ms=(time.perf_counter() - t0) * 1000,
        reasoning=f"LLM: {reasoning} (tokens in={in_tokens} out={out_tokens})",
    )
