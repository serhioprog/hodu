"""
~/hodu/src/scrapers/_enrichment_mixin.py

Shared enrichment helpers for the canonical scraper pipeline.

CANONICAL fetch_details PATTERN (every scraper should follow this order):
  1. Structured panel extraction       — site-specific, source of truth
  2. Description → og:description fallback
  3. Coords (multi-tier: data-attr → setView regex → ...)
  4. Images → og:image fallback
  5. NLP fallback (self.extractor.analyze_full_text on description)
  6. LLM fallback (OPTIONAL — only fires if self._llm_extractor is set)
  7. Quality Gate (description ≥ 50 chars)

USAGE:
    from src.scrapers.base import BaseScraper
    from src.scrapers._enrichment_mixin import EnrichmentMixin

    class MyScraper(EnrichmentMixin, BaseScraper):
        # Override defaults per site
        _NLP_TO_STRUCTURAL = {
            "swimming_pool": {"pool", "private_pool"},
            "sea_view":      {"view"},
        }

        async def fetch_details(self, url):
            # ... steps 1-4 (site-specific HTML walks) ...
            self._apply_nlp_fallback(data)                    # step 5
            # await self._apply_llm_fallback(data, MySchema)  # step 6 (optional)
            if not self._passes_quality_gate(data["description"]):
                logger.warning(f"thin description for {url}")
            return data

WHY this exists:
  * Five+ scrapers had copy-pasted NLP fallback code with subtle drift.
  * Single source of truth for: dedup logic, og:* fallbacks, sanity caps,
    quality gate, LLM trigger conditions.
  * New scrapers: inherit mixin + implement site-specific HTML walks.

EXCLUDED FROM REFACTOR:
  greek_exclusive_properties.py uses its own richer inline LLM integration
  (signal-scored description extraction, dedicated LLMExtractor service).
  It is INTENTIONALLY NOT refactored to use this mixin — "что работает,
  не портим". If/when its LLM logic is later consolidated, this mixin is
  the place.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Set

from loguru import logger
from selectolax.lexbor import LexborHTMLParser


class EnrichmentMixin:
    """
    Canonical enrichment helpers. Mix into any scraper subclass:

        class MyScraper(EnrichmentMixin, BaseScraper): ...

    All helpers are safe to call even when a scraper hasn't set
    `self._llm_extractor` (LLM helper silently no-ops in that case).

    Class attributes are overridable per-scraper if site idiosyncrasies
    demand it (e.g. structural slug names that conflict with NLP keys).
    """

    # ─────────────────────────────────────────────────────────────────
    # Class attributes — override in subclasses if needed
    # ─────────────────────────────────────────────────────────────────

    # Semantic dedup: when NLP returns these keys but the structural
    # parser has already put a related slug into extra_features, skip
    # the NLP duplicate. The MAP: NLP canonical → set of structural slugs.
    _NLP_TO_STRUCTURAL: Dict[str, Set[str]] = {
        "swimming_pool":    {"pool", "communal_pool", "private_pool"},
        "sea_view":         {"view", "sea", "view_sea"},
        "parking":          {"garage", "parking_spot", "outdoor_garage"},
        "alarm_system":     {"alarm"},
        "storage_room":     {"storage"},
        "air_conditioning": {"a_c", "ac"},
        "heating":          {"central_heating"},
        "fireplace":        {"fire_place"},
        "balcony":          {"balconies"},
        "terrace":          {"terraces"},
    }

    # Which top-level Property columns NLP может заполнить когда
    # structural pass их не нашёл. Don't include extra_features here —
    # those are merged separately with dedup.
    _NLP_FILLABLE_COLUMNS: tuple = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "category",
        "levels",
    )

    # Sanity cap for price parsing. 200M EUR is well above any plausible
    # Halkidiki real-estate price (typical range €100k - €20M). Anything
    # above this is almost certainly a concatenation bug from HTML quirks
    # (e.g. struck-through old price + new price merged into one digit run).
    _PRICE_SANITY_CAP: int = 200_000_000

    # Minimum description length to pass Quality Gate.
    # 50 chars catches obvious "Just a moment..." / "Loading..." pages.
    _QUALITY_GATE_MIN_CHARS: int = 50

    # LLM fallback thresholds (only used if scraper opts in by setting
    # self._llm_extractor). Subclasses can override per-source.
    _LLM_MIN_DESCRIPTION_CHARS: int = 200
    _LLM_MIN_REGEX_FEATURES: int = 5

    # ─────────────────────────────────────────────────────────────────
    # Step 5: NLP fallback
    # ─────────────────────────────────────────────────────────────────

    def _apply_nlp_fallback(self, data: Dict[str, Any]) -> None:
        """
        Run DataExtractor's regex/NLP pipeline on data["description"].
        Fills ONLY missing primary columns and adds new extra_features.

        Never overrides values that the structural pass already filled.
        Uses _NLP_TO_STRUCTURAL for semantic dedup of extra_features.

        Requires (from BaseScraper):
          - self.extractor: DataExtractor instance
          - self.source_domain: string, for logging

        Side effects on `data`:
          - data[<column>] populated for missing keys in _NLP_FILLABLE_COLUMNS
          - data["extra_features"] merged with NLP-extracted features
        """
        description = data.get("description") or ""
        if not description:
            return

        try:
            smart = self.extractor.analyze_full_text(description)
        except Exception as exc:
            logger.warning(
                f"[{self.source_domain}] NLP fallback failed: {exc!r}"
            )
            return

        # Fill missing primary columns
        for key in self._NLP_FILLABLE_COLUMNS:
            if data.get(key) is None and smart.get(key) is not None:
                data[key] = smart[key]

        # Merge extra_features with semantic dedup
        nlp_features = smart.get("extra_features") or {}
        if not nlp_features:
            return

        extra = data.setdefault("extra_features", {})
        existing = set(extra.keys())

        for k, v in nlp_features.items():
            if k in existing:
                continue
            # Skip if a semantically-equivalent structural slug already exists
            related = self._NLP_TO_STRUCTURAL.get(k, set())
            if related & existing:
                continue
            extra[k] = v

    # ─────────────────────────────────────────────────────────────────
    # Step 6: LLM fallback (opt-in — only fires if self._llm_extractor set)
    # ─────────────────────────────────────────────────────────────────

    async def _apply_llm_fallback(
        self,
        data: Dict[str, Any],
        schema: type,
        min_description_chars: Optional[int] = None,
        min_regex_features: Optional[int] = None,
        context_prefix: str = "",
    ) -> None:
        """
        Optional LLM-based extraction (gpt-4o-mini via LLMExtractor).

        Fires ONLY when ALL conditions hold:
          - self._llm_extractor exists and is enabled (kill-switch in .env)
          - description is rich enough (≥ min_description_chars)
          - regex coverage was thin (< min_regex_features extra_features)

        Merges NEW keys only — regex wins on overlap (deterministic > stochastic).

        Cost: ~$0.0006/call. Use sparingly.

        Args:
            data: the scraper's accumulated result dict (modified in place)
            schema: Pydantic schema class for structured LLM output
            min_description_chars: trigger floor (defaults to class attr)
            min_regex_features: trigger ceiling (defaults to class attr)
            context_prefix: log identifier; defaults to "{domain}/{site_id}"
        """
        # Opt-in: scraper must have initialized self._llm_extractor
        llm = getattr(self, "_llm_extractor", None)
        if llm is None or not getattr(llm, "enabled", False):
            return

        description = data.get("description") or ""
        min_chars = min_description_chars if min_description_chars is not None \
            else self._LLM_MIN_DESCRIPTION_CHARS
        if len(description) < min_chars:
            return

        extra = data.get("extra_features") or {}
        min_features = min_regex_features if min_regex_features is not None \
            else self._LLM_MIN_REGEX_FEATURES
        if len(extra) >= min_features:
            return

        ctx = (
            context_prefix
            or f"{self.source_domain}/{data.get('site_property_id', '?')}"
        )
        logger.info(
            f"[{self.source_domain}] LLM fallback fired ({ctx}): "
            f"regex gave {len(extra)} features"
        )

        try:
            llm_result = await llm.extract(
                description=description, schema=schema, context=ctx
            )
        except Exception as exc:
            logger.warning(
                f"[{self.source_domain}] LLM fallback failed: {exc!r}"
            )
            return

        if llm_result is None:
            return

        # Convert to dict — LLMExtractor returns Pydantic with to_extra_features()
        llm_dict = (
            llm_result.to_extra_features()
            if hasattr(llm_result, "to_extra_features")
            else dict(llm_result)
        )

        # Merge ONLY new keys — regex (already there) wins on overlap
        added = 0
        for k, v in llm_dict.items():
            if k not in extra:
                extra[k] = v
                added += 1

        data["extra_features"] = extra  # in case extra was newly created
        logger.info(
            f"[{self.source_domain}] LLM added {added} new features ({ctx})"
        )

    # ─────────────────────────────────────────────────────────────────
    # Step 2 & 4: og:* meta tag fallbacks
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _og_description_fallback(parser: LexborHTMLParser) -> str:
        """
        Read <meta property="og:description"> content.
        Returns empty string if not present.

        Use as fallback when site-specific description extraction yielded
        nothing. og:description is typically 1-2 sentences — better than
        empty, and usually still passes Quality Gate (50 chars).
        """
        node = parser.css_first('meta[property="og:description"]')
        if not node:
            return ""
        content = node.attributes.get("content") or ""
        return content.strip()

    @staticmethod
    def _og_image_fallback(parser: LexborHTMLParser) -> Optional[str]:
        """
        Read <meta property="og:image"> URL.
        Returns None if not present or empty.

        Use as last-resort cover photo when gallery selectors find nothing.
        Typically the site's chosen "lead" image — good enough for MDM
        clustering and admin preview.
        """
        node = parser.css_first('meta[property="og:image"]')
        if not node:
            return None
        content = (node.attributes.get("content") or "").strip()
        return content or None

    # ─────────────────────────────────────────────────────────────────
    # Price parsing with sanity cap
    # ─────────────────────────────────────────────────────────────────

    @classmethod
    def _to_int_euro_safe(cls, text: str) -> Optional[int]:
        """
        Parse Greek/EU price string into integer euros, with sanity cap.

        Acceptable inputs:
            "420.000€"           → 420000
            "420.000,00€"        → 420000  (cents dropped)
            "1,350,000 €"        → 1350000
            "Price: 420.000€"    → 420000
            "" / None / "POA"    → None

        Sanity cap: rejects values > _PRICE_SANITY_CAP (default 200M EUR).
        This catches HTML concatenation bugs where struck-through old
        prices merge with the new price (e.g. "1.550.000€1.500.000€"
        → naive parse → 15500001500000 → DB DataError).
        """
        if not text:
            return None
        cleaned = re.sub(r"[^\d.,]", "", text)
        if not cleaned:
            return None
        # If both separators present, treat the last one as decimal
        if "." in cleaned and "," in cleaned:
            last = max(cleaned.rfind("."), cleaned.rfind(","))
            cleaned = cleaned[:last]
        cleaned = re.sub(r"[.,]", "", cleaned)
        try:
            value = int(cleaned)
        except ValueError:
            return None
        if value > cls._PRICE_SANITY_CAP:
            return None
        return value

    # ─────────────────────────────────────────────────────────────────
    # HTML cleanup helpers
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _strip_strikethrough(html: str) -> str:
        """
        Remove <s>...</s> blocks (struck-through old prices) from raw HTML.

        Some brokers render discounted prices as:
            <li>Price: <s>1.550.000€</s> 1.500.000€</li>

        Naive `.text()` returns "Price: 1.550.000€ 1.500.000€", and
        downstream parsers concatenate the digit runs → 15500001500000
        → either DB DataError or absurd price. Strip <s> first.

        Returns the HTML with <s> blocks removed (case-insensitive, DOTALL).
        """
        return re.sub(
            r"<s\b[^>]*>.*?</s>",
            "",
            html,
            flags=re.DOTALL | re.IGNORECASE,
        )

    # ─────────────────────────────────────────────────────────────────
    # Step 7: Quality Gate
    # ─────────────────────────────────────────────────────────────────

    @classmethod
    def _passes_quality_gate(
        cls,
        description: Optional[str],
        min_chars: Optional[int] = None,
    ) -> bool:
        """
        Return True if description is sufficient for further processing.

        Default threshold: _QUALITY_GATE_MIN_CHARS class attr (50 chars).
        Override per-call with min_chars argument.

        Caller decides what to do on False: log warning, drop property,
        or rely on framework's DETAILS_FETCH_MAX_ATTEMPTS for retry.
        """
        threshold = (
            min_chars if min_chars is not None else cls._QUALITY_GATE_MIN_CHARS
        )
        if not description:
            return False
        return len(description.strip()) >= threshold