"""
Tier 3 LLM scoring backend for engine v2.

Per RESEARCH.md §12.5.10 + Day 3 Phase E architect approval.

Uses OpenAI gpt-4o-mini with JSON mode. Cache-blind by design — outer
cache lives in run_full_dedup (engine_pair_cache) and wraps this
backend. Tracks cost via cost_tracker (spec §4.3 mandatory).

Architecture per pair:
  1. Build prompt (system + user with computed signals)
  2. Call OpenAI with retry logic (3 attempts, exponential backoff)
  3. Parse JSON response defensively (fallback to UNCERTAIN)
  4. Validate verdict schema
  5. Record cost via cost_tracker.record_llm (async)
  6. Return EngineVerdict
"""
from __future__ import annotations

import asyncio
import json
import math
import time

from loguru import logger
from openai import (
    APIConnectionError,
    APITimeoutError,
    AsyncOpenAI,
    BadRequestError,
    RateLimitError,
)

from src.core.config import settings
from src.models.domain import Property
from src.services.cost_tracker import cost_tracker

from ..dedup_report import EngineVerdict
from ..features import PairFeatures
from .feature_extraction import extract_features


# Module constants
MODEL = "gpt-4o-mini"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0  # seconds; exponential backoff base
DESCRIPTION_TRUNCATE_CHARS = 600
MAX_OUTPUT_TOKENS = 300

# gpt-4o-mini pricing (verified 2026-05-08)
COST_INPUT_PER_TOKEN = 0.150 / 1_000_000     # $0.150 per 1M input tokens
COST_OUTPUT_PER_TOKEN = 0.600 / 1_000_000    # $0.600 per 1M output tokens


SYSTEM_PROMPT = """\
You are a real estate duplicate detection assistant for the hodu MDM
system. You decide whether two property listings refer to the same
physical real estate object on the market.

DEFINITIONS
A DUPLICATE means: one and the same physical real estate object listed
for sale on multiple websites at the same time. Same physical object =
same building, same unit, same plot of land.

Same source_domain on both sides = NEVER duplicates (one site lists
each property at most once). The blocking pipeline filters these
upstream; treat any same-source pair you receive as a data error and
emit "different".

DEFINITIVE NON-DUPLICATES — always emit "different":
1. Different categories: Villa vs Apartment vs Land vs House are never
   the same.
2. Construction year diff > 5: a 2024 building and a 2018 building
   cannot be the same physical structure (4-5 yr drift between sources
   is normal and admissible, but >5 means different objects).
3. Same building, different units: e.g. apartments 4A and 4B share GPS
   coordinates and similar descriptions but are different objects.
4. Land plot vs Land+House bundle: a plot of land later sold with a
   built house is a fundamentally different asset; treat as different.

DECISION POLICY
Use the embedding cosine similarity, phash similarity, GPS proximity,
year/price/size, bedrooms, calc_area, and full-text descriptions
together. No single signal is decisive — multiple weak signals combine
into confident verdicts.

Prefer "uncertain" over a wrong "duplicate". False positives are far
more harmful than false negatives — admin reviews uncertain cases.

Confidence guidelines:
- 0.95+: nearly certain (multiple strong signals agree)
- 0.80-0.95: confident (most signals agree)
- 0.65-0.80: leaning but not confident — usually emit "uncertain"
- below 0.65: too weak, emit "uncertain"

OUTPUT FORMAT (strict JSON, no markdown):
{
  "verdict": "duplicate" | "different" | "uncertain",
  "confidence": <float 0.0-1.0>,
  "reasoning": "<1-2 sentence English explanation>",
  "key_signals": ["<short signal identifiers>"]
}

Reasoning must be in English regardless of input language. Property
descriptions are typically Greek + English mixed; you understand both.
"""


# -----------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------

def _truncate_utf8_safe(text: str | None, max_chars: int) -> str:
    """Truncate at char boundary (Python str is UTF-32 internally so
    char-level slicing is safe)."""
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def _fmt_int(v: int | None) -> str:
    """Format int with thousands separator, NULL if None."""
    return "NULL" if v is None else f"{v:,}"


def _fmt_float(v: float | None, fmt: str = ".0f") -> str:
    """Format float with given format spec, NULL if None."""
    return "NULL" if v is None else f"{v:{fmt}}"


def _fmt_num(v: float, fmt: str = ".0f") -> str:
    """Format float, NULL if NaN."""
    if math.isnan(v):
        return "NULL"
    return f"{v:{fmt}}"


def _build_user_prompt(
    prop_a: Property, prop_b: Property, features: dict[str, float],
) -> str:
    """Build per-pair user prompt with property facts + computed signals."""
    desc_a = _truncate_utf8_safe(prop_a.description, DESCRIPTION_TRUNCATE_CHARS)
    desc_b = _truncate_utf8_safe(prop_b.description, DESCRIPTION_TRUNCATE_CHARS)

    return f"""\
Compare these two property listings:

PROPERTY A
- source: {prop_a.source_domain}
- category: {prop_a.category or 'NULL'}
- price: {_fmt_int(prop_a.price)} EUR
- size: {_fmt_float(prop_a.size_sqm)} sqm (land: {_fmt_float(prop_a.land_size_sqm)})
- year_built: {_fmt_int(prop_a.year_built)}
- bedrooms: {_fmt_int(prop_a.bedrooms)}
- bathrooms: {_fmt_int(prop_a.bathrooms)}
- location: {prop_a.calc_area or '?'}, {prop_a.calc_municipality or '?'} municipality
- description (first {DESCRIPTION_TRUNCATE_CHARS} chars): "{desc_a}"

PROPERTY B
- source: {prop_b.source_domain}
- category: {prop_b.category or 'NULL'}
- price: {_fmt_int(prop_b.price)} EUR
- size: {_fmt_float(prop_b.size_sqm)} sqm (land: {_fmt_float(prop_b.land_size_sqm)})
- year_built: {_fmt_int(prop_b.year_built)}
- bedrooms: {_fmt_int(prop_b.bedrooms)}
- bathrooms: {_fmt_int(prop_b.bathrooms)}
- location: {prop_b.calc_area or '?'}, {prop_b.calc_municipality or '?'} municipality
- description (first {DESCRIPTION_TRUNCATE_CHARS} chars): "{desc_b}"

COMPUTED SIGNALS
- text_embedding_cosine: {_fmt_num(features['embedding_cosine_sim'], '.3f')}
- phash_min_hamming: {_fmt_num(features['phash_min_hamming'])} (lower = more similar; 0=identical, 64=unrelated)
- phash_close_count: {_fmt_num(features['phash_close_count'])} (pairs with hamming <= 10)
- same_municipality: {'yes' if features['same_calc_municipality'] else 'no'}
- same_calc_area: {'yes' if features['same_calc_area'] else 'no'}
- year_built_diff: {_fmt_num(features['year_built_diff'])}
- price_log_ratio: {_fmt_num(features['price_log_ratio'], '.3f')}
- size_sqm_ratio: {_fmt_num(features['size_sqm_ratio'], '.3f')}
- bedrooms_diff: {_fmt_num(features['bedrooms_diff'])}
"""


def _parse_response(content: str) -> tuple[str, float, str, list[str]]:
    """Parse + validate JSON response defensively.

    Returns (verdict, confidence, reasoning, key_signals).
    Falls back to UNCERTAIN with low confidence on any parse failure.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        logger.warning("[t3_llm] JSON parse failed: {c}", c=content[:200])
        return ("uncertain", 0.0, "JSON parse failure", [])

    verdict = data.get("verdict", "uncertain")
    if verdict not in ("duplicate", "different", "uncertain"):
        logger.warning("[t3_llm] invalid verdict {v}, falling back", v=verdict)
        verdict = "uncertain"

    try:
        confidence = float(data.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    reasoning = str(data.get("reasoning", ""))[:500]

    key_signals = data.get("key_signals", [])
    if not isinstance(key_signals, list):
        key_signals = []
    key_signals = [str(s)[:80] for s in key_signals[:10]]

    return (verdict, confidence, reasoning, key_signals)


# -----------------------------------------------------------------
# Backend
# -----------------------------------------------------------------

class Tier3LLMBackend:
    """LLM-based scoring backend for Tier 3.

    Uses OpenAI gpt-4o-mini with JSON mode for structured output.
    Cache-blind — outer cache (run_full_dedup) handles cache layer.

    Tier 3 is the final arbiter — if still UNCERTAIN after T3, that
    verdict cascades to admin review per spec §2.4.
    """

    name: str = "tier_3_llm"

    def __init__(self) -> None:
        """Init OpenAI client.

        No DB session needed — cache layer lives in run_full_dedup (outer).
        """
        self._client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        logger.info("[t3_llm] init with model={m}", m=MODEL)

    async def score(
        self,
        features: PairFeatures,
        prop_a: Property,
        prop_b: Property,
    ) -> EngineVerdict:
        """Score one pair via LLM (cache-blind; outer cache wraps)."""
        t_start = time.perf_counter()

        # 1. Build prompt (extract features for signal section)
        feats = extract_features(prop_a, prop_b)
        user_prompt = _build_user_prompt(prop_a, prop_b, feats)

        # 2. Call OpenAI with retry on transient errors
        response = None
        for attempt in range(MAX_RETRIES):
            try:
                response = await self._client.chat.completions.create(
                    model=MODEL,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.0,
                    max_tokens=MAX_OUTPUT_TOKENS,
                )
                break
            except (APITimeoutError, RateLimitError, APIConnectionError) as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(
                        "[t3_llm] API failed after {n} attempts: {e}",
                        n=MAX_RETRIES, e=str(e),
                    )
                    await cost_tracker.record_llm(
                        model=MODEL, in_tokens=0, out_tokens=0,
                        success=False,
                    )
                    return EngineVerdict(
                        verdict="uncertain",
                        confidence=0.0,
                        reasoning=f"T3 LLM API failure: {type(e).__name__}",
                        tier_emitted=3,
                        cost_usd=0.0,
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
                wait = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "[t3_llm] retry {a}/{n} after {w}s: {e}",
                    a=attempt + 1, n=MAX_RETRIES, w=wait,
                    e=type(e).__name__,
                )
                await asyncio.sleep(wait)
            except BadRequestError as e:
                # Don't retry — prompt-level issue
                logger.error("[t3_llm] BadRequest: {e}", e=str(e))
                await cost_tracker.record_llm(
                    model=MODEL, in_tokens=0, out_tokens=0, success=False,
                )
                return EngineVerdict(
                    verdict="uncertain",
                    confidence=0.0,
                    reasoning="T3 LLM prompt error",
                    tier_emitted=3,
                    cost_usd=0.0,
                    latency_ms=(time.perf_counter() - t_start) * 1000,
                )

        # Defensive — should be set by break out of retry loop
        assert response is not None, "response should be set after retry loop"

        # 3. Parse response defensively
        content = response.choices[0].message.content or "{}"
        verdict, confidence, reasoning, key_signals = _parse_response(content)

        # 4. Track cost
        in_tokens = response.usage.prompt_tokens
        out_tokens = response.usage.completion_tokens
        cost_usd = (
            in_tokens * COST_INPUT_PER_TOKEN
            + out_tokens * COST_OUTPUT_PER_TOKEN
        )
        await cost_tracker.record_llm(
            model=MODEL, in_tokens=in_tokens, out_tokens=out_tokens,
            success=True,
        )

        # 5. Build EngineVerdict
        engine_verdict = EngineVerdict(
            verdict=verdict,                            # type: ignore[arg-type]
            confidence=confidence,
            reasoning=f"T3 LLM: {reasoning} [signals: {','.join(key_signals)}]",
            tier_emitted=3,
            cost_usd=cost_usd,
            latency_ms=(time.perf_counter() - t_start) * 1000,
        )

        logger.info(
            "[t3_llm] verdict={v} conf={c:.3f} cost=${cost:.6f} "
            "tokens={i}/{o} latency={lat:.0f}ms",
            v=verdict, c=confidence, cost=cost_usd,
            i=in_tokens, o=out_tokens,
            lat=engine_verdict.latency_ms,
        )

        return engine_verdict
