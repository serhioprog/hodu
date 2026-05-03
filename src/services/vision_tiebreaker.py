"""
GPT-4o Vision tie-breaker for ambiguous property duplicate pairs.

When the InternalDuplicateDetector encounters a pair with text similarity
in the gray zone (0.92-0.985) AND insufficient pHash matches, it cannot
confidently merge or reject. This service uses Vision to look at the
actual property photos and decide.

Cost model:
  * Per call: ~3 photos × 250 input tokens + 500 prompt + 100 output ≈ 2000 tokens
  * At $5/1M input + $15/1M output ≈ $0.011 per call
  * Capped per run via settings.VISION_MAX_PAIRS_PER_RUN

Confidence handling:
  * verdict.confidence ≥ VISION_CONFIDENCE_THRESHOLD → authoritative
  * verdict.confidence below threshold → stay in PENDING (admin decides)

Image delivery (NEW):
  * PRIMARY mode — encode local files as base64 data URLs and send inline
    in the API request. No external HTTP fetch by OpenAI.
    Works from any host (localhost, behind NAT, etc.) — OpenAI never
    talks to our infrastructure. Eliminates the
    "Timeout while downloading <source CDN url>" failures we saw before.
  * FALLBACK mode — if local file is missing (older record, disk cleanup),
    we fall back to the original CDN URL on media.image_url.

Failure modes:
  * OpenAI 5xx / timeout      → return None, edge stays unchanged (admin reviews)
  * Property has no photos    → return None (skip Vision for this pair)
  * Schema validation fails   → return None
  * Rate limit                → return None, log warning

This service NEVER raises exceptions to the caller — it always returns
None on failure and lets the caller decide the fallback path.
"""
from __future__ import annotations

import asyncio
import base64
import os
from typing import List, Optional

from loguru import logger
from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.models.ai_schemas import VisionVerdict
from src.models.domain import Media, Property
from src.services.cost_tracker import cost_tracker


# How many photos to show GPT-4o per property.
# More photos → more context but more cost.
# 3 is a sweet spot: enough to see the building from multiple angles
# without ballooning the bill on serial calls.
PHOTOS_PER_PROPERTY: int = 3


# Mapping file-extension → MIME type. JPEG is the default (most listings
# come as .jpg). Anything not recognised falls back to image/jpeg too —
# OpenAI accepts that consistently for arbitrary photo content.
_EXT_TO_MIME = {
    "jpg":  "image/jpeg",
    "jpeg": "image/jpeg",
    "png":  "image/png",
    "webp": "image/webp",
    "gif":  "image/gif",
    "bmp":  "image/bmp",
}


async def _load_image_as_data_url(local_path: str) -> Optional[str]:
    """
    Read a local image file and return a data: URL with base64 contents,
    suitable for inline submission to OpenAI Vision.

    Returns None if the file is missing or unreadable. The caller is
    expected to fall back to a public URL in that case.

    Implementation note: we use asyncio.to_thread for both stat and
    read — disk I/O on container volume can block tens of milliseconds
    on Windows hosts (where docker-desktop wraps WSL filesystem) and we
    don't want to stall the event loop during a multi-pair Vision run.
    """
    if not local_path:
        return None

    def _read_and_encode() -> Optional[str]:
        if not os.path.exists(local_path):
            return None
        try:
            with open(local_path, "rb") as f:
                raw = f.read()
        except OSError as e:
            logger.warning(f"[Vision] cannot read local image {local_path}: {e}")
            return None

        if not raw:
            return None

        # Pick MIME type from extension; default to JPEG.
        ext = local_path.rsplit(".", 1)[-1].lower() if "." in local_path else "jpg"
        mime = _EXT_TO_MIME.get(ext, "image/jpeg")

        b64 = base64.b64encode(raw).decode("ascii")
        return f"data:{mime};base64,{b64}"

    return await asyncio.to_thread(_read_and_encode)


async def _build_image_url(media: Media) -> Optional[str]:
    """
    Pick the URL to send to GPT-4o Vision for this Media row.

    Priority (changed in 2026-05):
      1. local_file_path → read from disk, encode base64, inline in request.
         This eliminates the OpenAI-side "Timeout while downloading"
         failures we observed when their network hit our source CDNs
         (e.g. glre.estateplus.gr) too fast.
      2. media.image_url — original CDN URL from the source site, used as
         fallback for old records or when the local file went missing
         (disk cleanup, container restart with ephemeral mount, etc.).

    Returns None only if BOTH paths fail.
    """
    if media.local_file_path:
        data_url = await _load_image_as_data_url(media.local_file_path)
        if data_url:
            return data_url
        logger.debug(
            f"[Vision] local file unreadable, falling back to URL: "
            f"{media.local_file_path}"
        )
    return media.image_url or None


_SYSTEM_PROMPT = """You are a strict real-estate visual deduplication assistant.

Your job: decide whether two property listings show the SAME physical property
(same building, same plot, same architecture) OR DIFFERENT properties.

You are NOT deciding whether the listings have similar style or are in similar
locations — you are deciding whether they depict the EXACT SAME real-world property.

Visual cues that suggest SAME:
  - Identical building shape, roof, windows, balconies
  - Same plot layout, same trees, same neighboring structures visible
  - Same interior rooms (same wall colors, same fixtures, same flooring)
  - Same view from windows / terraces

Visual cues that suggest DIFFERENT:
  - Different building shape or floor count
  - Different roof material or color
  - Different surrounding landscape (different mountains, different shoreline)
  - Different interior layouts or finishes
  - Different orientation (one faces sea, the other faces street)

Edge cases:
  - Two adjacent villas in the same complex with identical design → DIFFERENT
    (they are separate properties even if visually identical)
  - Same building photographed in different seasons → SAME
  - One listing has interior shots, the other has exterior — judge by what's
    visible; if you cannot tell, lower your confidence

If photos are stock/marketing imagery (sunset over generic beach, drone over
unidentified land), set confidence < 0.7 — you cannot reliably decide.

Respond ONLY with valid JSON matching the requested schema. No prose."""


_USER_PROMPT_TEMPLATE = """Property A photos: {n_a} images attached (first batch).
Property B photos: {n_b} images attached (second batch).

A's text fields:
  category: {a_cat}
  municipality: {a_muni}
  size: {a_size} m²
  bedrooms: {a_bed}

B's text fields:
  category: {b_cat}
  municipality: {b_muni}
  size: {b_size} m²
  bedrooms: {b_bed}

Decide: is_same / confidence / reason."""


class VisionTiebreaker:
    """
    Calls GPT-4o Vision to decide if two ambiguous properties are the same.

    Stateless aside from the OpenAI client. Safe to instantiate once and
    reuse across many pairs.
    """

    def __init__(self) -> None:
        self._client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

    async def decide_pair(
        self,
        session: AsyncSession,
        prop_a_id: str,
        prop_b_id: str,
    ) -> Optional[VisionVerdict]:
        """
        Send a single pair to Vision and parse the verdict.

        Returns None on any failure — caller treats None as "Vision didn't
        decide, leave the pair in PENDING".
        """
        # --- Load both properties + first N photos each --------------
        prop_a, photos_a = await self._load_property_with_photos(session, prop_a_id)
        prop_b, photos_b = await self._load_property_with_photos(session, prop_b_id)

        if prop_a is None or prop_b is None:
            logger.warning(f"[Vision] property not found: {prop_a_id} or {prop_b_id}")
            return None

        # Build URLs/data-URLs concurrently — base64 encoding is the
        # slowest step and there's no reason to serialise it.
        url_a_results = await asyncio.gather(*[_build_image_url(m) for m in photos_a])
        url_b_results = await asyncio.gather(*[_build_image_url(m) for m in photos_b])
        url_a = [u for u in url_a_results if u]
        url_b = [u for u in url_b_results if u]

        if not url_a or not url_b:
            logger.info(
                f"[Vision] skip {prop_a_id[:8]}<>{prop_b_id[:8]}: "
                f"no image URLs (a={len(url_a)} b={len(url_b)})"
            )
            return None

        # --- Build user message with both image batches --------------
        user_text = _USER_PROMPT_TEMPLATE.format(
            n_a=len(url_a),  n_b=len(url_b),
            a_cat=prop_a.category or "?",
            a_muni=prop_a.calc_municipality or "?",
            a_size=int(prop_a.size_sqm) if prop_a.size_sqm else "?",
            a_bed=prop_a.bedrooms or "?",
            b_cat=prop_b.category or "?",
            b_muni=prop_b.calc_municipality or "?",
            b_size=int(prop_b.size_sqm) if prop_b.size_sqm else "?",
            b_bed=prop_b.bedrooms or "?",
        )

        content_parts: List[dict] = [{"type": "text", "text": user_text}]
        for u in url_a:
            content_parts.append({"type": "image_url", "image_url": {"url": u, "detail": "low"}})
        for u in url_b:
            content_parts.append({"type": "image_url", "image_url": {"url": u, "detail": "low"}})

        # --- Call OpenAI ---------------------------------------------
        try:
            resp = await self._client.beta.chat.completions.parse(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user",   "content": content_parts},
                ],
                response_format=VisionVerdict,
                max_tokens=300,
                temperature=0.0,  # deterministic
            )
        except Exception as e:
            logger.warning(
                f"[Vision] OpenAI call failed for "
                f"{prop_a_id[:8]}<>{prop_b_id[:8]}: {e}"
            )
            # Record failed call (cost = 0 since OpenAI rejects before
            # processing — Timeout/4xx/etc).
            await cost_tracker.record_vision(success=False)
            return None

        verdict = resp.choices[0].message.parsed
        if verdict is None:
            logger.warning(f"[Vision] no parsed content for {prop_a_id[:8]}<>{prop_b_id[:8]}")
            # Successful API call (we got a response) but model didn't
            # produce valid output. Still bill the call — OpenAI charged us.
            await cost_tracker.record_vision(success=True)
            return None

        # Successful Vision call → record cost
        await cost_tracker.record_vision(success=True)

        logger.info(
            f"[Vision] {prop_a_id[:8]}<>{prop_b_id[:8]}: "
            f"is_same={verdict.is_same} conf={verdict.confidence:.2f} "
            f"— {verdict.reason}"
        )
        return verdict

    async def _load_property_with_photos(
        self,
        session: AsyncSession,
        prop_id: str,
    ) -> tuple[Optional[Property], List[Media]]:
        """Load Property + first PHOTOS_PER_PROPERTY Media rows by created_at."""
        prop = (await session.execute(
            select(Property).where(Property.id == prop_id)
        )).scalar_one_or_none()
        if prop is None:
            return None, []

        photos = (await session.execute(
            select(Media)
            .where(Media.property_id == prop_id)
            .order_by(Media.created_at.asc())
            .limit(PHOTOS_PER_PROPERTY)
        )).scalars().all()
        return prop, list(photos)