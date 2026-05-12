"""
~/hodu/src/scrapers/halkidiki_real_estate.py

Hellenic Living / Halkidiki Real Estate (halkidikirealestate.com) — a
boutique broker in Kassandra. Small inventory (~50 properties at
min_price=400k).

Platform: Classic ASP server-rendered HTML, no anti-bot, no JS rendering
required.

Special considerations
----------------------
1. *No GPS exposed.* The site does NOT publish lat/lng anywhere in the
   detail page (no map widget, no embedded coords). We set
   `latitude = longitude = None`. The geo_matcher cannot resolve these
   properties from coordinates; only the broad `area` text is available
   (West Kassandra / East Kassandra / Sithonia / Rest of Halkidiki).

2. *No subarea on detail page.* Only the top-level "Area" field is shown.
   Subarea (Nea Skioni, Possidi, Kallithea, etc.) is not surfaced. We
   leave `subarea = None`.

3. *`site_property_id` keeps the `HL` prefix.* The site uses property
   codes like "HL1342" prominently. We preserve that prefix.

4. *"First Line" banner.* Some properties display a position badge
   ("First Line" / "Second Line" / "Third Line") indicating beachfront
   proximity. Captured to `extra_features['position_label']`.

5. *Photos at /photos/{file}.jpg* (full quality). We use the top-gallery
   set (the cover photo plus all gallery items). The banner/hero image
   (`photos/2_*.jpg`) is decorative and is intentionally skipped.

6. *Mobile/desktop duplication.* The features panel is rendered twice
   (desktop + mobile). We parse only the `.desktop-est-det` container
   to avoid double-counting.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

from loguru import logger
from selectolax import parser
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.models.schemas import PropertyTemplate
from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_BASE_URL = "https://www.halkidikirealestate.com"
_SOURCE_DOMAIN = "halkidikirealestate.com"

# Listing pagination: offset-based, 15 cards per page (confirmed).
_PAGE_SIZE = 15
_MAX_PAGES = 40                     # safety cap (~600 listings worst case)
_INTER_PAGE_SLEEP_SEC = 2.0

# Category map: detail-page "Property Type" text → canonical name.
_CATEGORY_MAP: Dict[str, str] = {
    "halkidiki villas":      "Villa",
    "halkidiki houses":      "House",
    "halkidiki apartments":  "Apartment",
    "halkidiki maisonettes": "Maisonette",
    "halkidiki land":        "Land",
}

# Detail-panel label routing.
# Keys are lowercase labels; values are either:
#   - "<column>"         → set PropertyTemplate.<column> directly
#   - "_extra:<key>"     → put parsed value into extra_features[<key>]
#   - "_category"        → run through _CATEGORY_MAP
#   - "_area"            → set as `area` text
#   - "_skip"            → ignore (handled elsewhere)
_LABEL_TO_FIELD: Dict[str, str] = {
    # Right panel (bgCreateView)
    "property type":   "_category",
    "area":            "_area",
    "year built":      "year_built",
    "size":            "size_sqm",
    "plot":            "land_size_sqm",
    "floor":           "_extra:floor",
    "storeys":         "_extra:storeys",
    "bedrooms":        "bedrooms",
    "bathrooms":       "bathrooms",
    # Left panel (.desktop-est-det)
    "heating":              "_extra:heating",
    "air conditioning":     "_extra:air_conditioning",
    "furnished":            "_extra:furnished",
    "storage":              "_extra:storage",
    "parking":              "_extra:parking",
    "garden":               "_extra:garden",
    "sea view":             "_extra:sea_view",
    "sea":                  "_extra:distance_from_sea_m",
    "town centre":          "_extra:distance_from_town_m",
    "airport":              "_extra:distance_from_airport_m",
}


_YES_VALUES = {"yes", "true", "✔", "✓"}
_NO_VALUES = {"no", "false", "✗", "—", "-"}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_text(s: Optional[str]) -> str:
    """Collapse whitespace, handle non-breaking spaces, strip."""
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


def _parse_int(text: str) -> Optional[int]:
    """Extract first integer from a string (ignores units)."""
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _parse_float(text: str) -> Optional[float]:
    """Extract first decimal/integer from a string (handles spaces, commas)."""
    if not text:
        return None
    cleaned = text.replace(",", ".").replace(" ", "")
    m = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _interpret_bool(value: str) -> Optional[bool]:
    """Yes/No → True/False, else None."""
    if not value:
        return None
    v = value.strip().lower()
    if v in _YES_VALUES:
        return True
    if v in _NO_VALUES:
        return False
    return None


def _parse_distance_meters(text: str) -> Optional[int]:
    """
    Convert '30m' / '4000m' / '90km' / '1.5km' → integer meters.
    Returns None for unparseable input.
    """
    if not text:
        return None
    t = text.strip().lower().replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*(km|m)?", t)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    unit = m.group(2) or "m"
    if unit == "km":
        val *= 1000
    return int(round(val))


def _parse_price_text(text: str) -> Optional[int]:
    """'€ 800000' / ' € 3500000 ' → 800000 / 3500000. None for 'POA'/empty."""
    if not text:
        return None
    cleaned = text.strip()
    lowered = cleaned.lower()
    if "request" in lowered or "poa" in lowered or "agreement" in lowered:
        return None
    digits = re.sub(r"\D", "", cleaned)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _absolute(path: str) -> str:
    """Make a possibly-relative URL absolute against the site base."""
    if not path:
        return path
    if path.startswith("http"):
        return path
    return f"{_BASE_URL}/{path.lstrip('/')}"


def _direct_child_divs(node: LexborNode) -> List[LexborNode]:
    """Return immediate <div> children of a node (depth=1)."""
    out: List[LexborNode] = []
    child = node.child
    while child is not None:
        if child.tag == "div":
            out.append(child)
        child = child.next
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Scraper
# ─────────────────────────────────────────────────────────────────────────────

class HalkidikiRealEstateScraper(EnrichmentMixin, BaseScraper):

    # Defensive override (same rationale as grekodom + halkidiki_estate +
    # engel_voelkers): if the detail page is silent on category, NLP would
    # fill "Land/Plot" and overwrite the card-derived value on next save.
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )

    """
    halkidikirealestate.com — boutique broker, ~50 residential properties
    at min_price=400_000 EUR. Server-rendered HTML, no anti-bot.

    Inherits canonical enrichment helpers from EnrichmentMixin:
      * _apply_nlp_fallback() — fills missing columns + amenities from description
      * _og_description_fallback() — used when main description paragraphs empty
      * _passes_quality_gate() — description quality check

    Site quirk: NO GPS coordinates exposed in HTML — latitude/longitude
    are intentionally left None for downstream geo_matcher to handle.
    Default _NLP_TO_STRUCTURAL works (no conflicting structural slug names).
    """

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ─── URL builders ────────────────────────────────────────────────────────

    def _construct_listing_url(
        self,
        offset: int,
        min_price: int,
        max_price: int = 0,
    ) -> str:
        """
        category.asp endpoint accepts offset directly:
            /category.asp?offset=N&lang=en&keimeno=halkidiki-properties-for-sale
                         &perioxi=halkidiki&timiApo={min}&timiEos={max}
        max_price=0 means "no upper limit".
        """
        params = (
            f"offset={offset}"
            f"&lang=en"
            f"&keimeno=halkidiki-properties-for-sale"
            f"&perioxi=halkidiki"
            f"&timiApo={min_price}"
            f"&timiEos={max_price}"
        )
        return f"{_BASE_URL}/category.asp?{params}"

    # ─── Phase 1: collect_urls ───────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        """Iterate paginated listings, returning all property seeds."""
        seeds: Dict[str, PropertyTemplate] = {}
        offset = 0
        page_num = 0

        for _ in range(max_pages):
            page_num += 1
            url = self._construct_listing_url(offset=offset, min_price=min_price)
            logger.info(
                f"[{self.source_domain}] Парсинг offset={offset} (page {page_num})..."
            )

            try:
                response = await self.client.get(url)
            except Exception as exc:
                logger.error(
                    f"[{self.source_domain}] fetch failed at offset {offset}: {exc!r}"
                )
                break

            parser = LexborHTMLParser(response.text)
            cards = parser.css(".property-item")

            if not cards:
                logger.info(
                    f"[{self.source_domain}] no cards at offset {offset} — end of pagination"
                )
                break

            page_added = 0
            for card in cards:
                seed = self._parse_card(card)
                if seed is None:
                    continue
                if seed.site_property_id in seeds:
                    continue
                seeds[seed.site_property_id] = seed
                page_added += 1

            logger.info(
                f"[{self.source_domain}] offset {offset}: {len(cards)} cards "
                f"(+{page_added}; total seeds: {len(seeds)})"
            )

            # End-of-results: partial page
            if len(cards) < _PAGE_SIZE:
                logger.info(
                    f"[{self.source_domain}] offset {offset} returned "
                    f"{len(cards)} < {_PAGE_SIZE} — last page"
                )
                break

            offset += _PAGE_SIZE
            await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)

        logger.info(
            f"[{self.source_domain}] Phase 1 завершён: {len(seeds)} URLs "
            f"за {page_num} стр."
        )
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """Parse a single .property-item card from a listing page."""
        # Numeric ID from .favBtn[data-estate]; preserved with "HL" prefix.
        fav_btn = card.css_first(".favBtn[data-estate]")
        if not fav_btn:
            return None
        numeric_id = fav_btn.attributes.get("data-estate", "").strip()
        if not numeric_id or not numeric_id.isdigit():
            return None
        site_id = f"HL{numeric_id}"

        # Detail URL — primary source is .property-item-link[data-property-id]
        link_div = card.css_first(".property-item-link[data-property-id]")
        path = ""
        if link_div:
            path = link_div.attributes.get("data-property-id", "") or ""
        # Fallback to the title anchor
        if not path:
            title_a = card.css_first(".suggestedTitle a[href]")
            if title_a:
                path = title_a.attributes.get("href", "") or ""
        if not path:
            return None
        detail_url = _absolute(path)

        # Title (also useful when title is the main differentiator on the card)
        title_node = card.css_first(".suggestedTitle a")
        title = _normalize_text(title_node.text(strip=False)) if title_node else None

        # Price text — '.timi' contains "€ 800000"
        price_node = card.css_first(".timi")
        price_text = _normalize_text(price_node.text(strip=False)) if price_node else ""
        price = _parse_price_text(price_text)

        # Thumbnail from the card's background-image inline style
        thumb_url: Optional[str] = None
        bg_node = card.css_first(".bg_suggested")
        if bg_node:
            style = bg_node.attributes.get("style", "") or ""
            m = re.search(r"url\(([^)]+)\)", style)
            if m:
                raw = m.group(1).strip("'\" ")
                thumb_url = _absolute(raw)

        # Build extra_features only if there's something useful to stash
        extra: Dict[str, Any] = {}
        if title:
            extra["listing_title"] = title
        if thumb_url:
            extra["thumbnail_url"] = thumb_url

        return PropertyTemplate(
            site_property_id=site_id,
            url=detail_url,
            source_domain=self.source_domain,
            price=price,
            extra_features=extra or None,
        )

    # ─── Phase 2: fetch_details ──────────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """Fetch detail page and extract full property data."""
        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.error(
                f"[{self.source_domain}] detail fetch failed for {url}: {exc!r}"
            )
            return {}

        parser = LexborHTMLParser(response.text)
        result: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # ─── Title (h1) ─────────────────────────────────────────────────────
        h1 = parser.css_first(".indexTitle h1")
        if h1:
            title = _normalize_text(h1.text(strip=False))
            if title:
                extra["listing_title"] = title

        # ─── Price (left of property-code span) ─────────────────────────────
        price_block = parser.css_first(".propertyPrice")
        if price_block:
            # Iterate spans; price is the first span whose text doesn't match
            # the property code (HLxxxx).
            for span in price_block.css("span"):
                cls = span.attributes.get("class", "") or ""
                if "property-code" in cls:
                    continue
                price_text = _normalize_text(span.text(strip=False))
                price = _parse_price_text(price_text)
                if price is not None:
                    result["price"] = price
                break

        # ─── "First Line" / "Second Line" banner ───────────────────────────
        banner_p = parser.css_first(".property-item-banner p")
        if banner_p:
            banner_text = _normalize_text(banner_p.text(strip=False))
            if banner_text:
                extra["position_label"] = banner_text

        # ─── Description (multi-paragraph) ─────────────────────────────────
        # The description lives inside `.col-lg-8 .indexTitle.text-left` as
        # direct <p> children (NOT inside the desktop-est-det panel).
        desc_col = parser.css_first("div.col-lg-8.indexTitle.text-left")
        if desc_col:
            paragraphs: List[str] = []
            for p_el in _direct_child_divs(desc_col):
                # We want only <p> direct children; ignore the panels inside
                pass
            # Actually direct_child_divs filters to <div>, so re-walk for <p>:
            child = desc_col.child
            while child is not None:
                if child.tag == "p":
                    txt = _normalize_text(child.text(strip=False))
                    if txt:
                        paragraphs.append(txt)
                child = child.next
            if paragraphs:
                result["description"] = "\n\n".join(paragraphs)

        # Fallback: if main column had no paragraphs, try og:description meta.
        # Better than empty — boutique pages occasionally use server-side
        # rendering quirks that hide the main copy from selectolax.
        if not result.get("description"):
            og_desc = self._og_description_fallback(parser)
            if og_desc:
                result["description"] = og_desc

        # ─── Right panel (.bgCreateView) — main details ─────────────────────
        right_panel = parser.css_first(".bgCreateView")
        if right_panel:
            for dtp in right_panel.css(".detailsToProperty"):
                self._route_field(dtp, result, extra)

        # ─── Left panel (.desktop-est-det only — skip mobile clone) ─────────
        left_panel = parser.css_first(".desktop-est-det")
        if left_panel:
            for dtp in left_panel.css(".detailsToProperty"):
                self._route_field(dtp, result, extra)

        # ─── Photos (top-gallery only, skip banner/hero) ────────────────────
        images = self._extract_images(parser)
        if images:
            result["images"] = images

        # ─── GPS: explicitly None (site doesn't publish coordinates) ───────
        # We leave latitude/longitude unset so the framework defaults apply.

        # Merge accumulated extras into result FIRST so NLP fallback can see
        # them for dedup purposes (canonical step 5 order).
        if extra:
            result["extra_features"] = extra

        # ─── Step 5: NLP fallback over description ─────────────────────────
        # Fills missing year_built/bedrooms/bathrooms/etc + adds amenity
        # features (sea_view, pool, parking, ...) detected in text.
        # Never overwrites structural data.
        self._apply_nlp_fallback(result)

        # ─── Step 7: Quality Gate (log-only) ───────────────────────────────
        if not self._passes_quality_gate(result.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate "
                f"for {url}"
            )

        return result

    def _route_field(
        self,
        container: LexborNode,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Read a single `.detailsToProperty` block: two columns inside `.row`,
        the first carrying `<span>Label</span>`, the second the value.
        """
        row = container.css_first(".row")
        if not row:
            return

        cells = _direct_child_divs(row)
        if len(cells) < 2:
            return

        label_cell, value_cell = cells[0], cells[1]

        # Label always inside <span>
        label_span = label_cell.css_first("span")
        if not label_span:
            return
        label = _normalize_text(label_span.text(strip=False)).lower()
        if not label:
            return

        value_raw = _normalize_text(value_cell.text(strip=False))
        if not value_raw:
            return

        field = _LABEL_TO_FIELD.get(label)
        if not field or field == "_skip":
            # Unknown label — stash under a slugged key in extra
            slug = re.sub(r"\W+", "_", label).strip("_")
            if slug and slug not in extra:
                extra[slug] = value_raw
            return

        # ── Special handlers ───────────────────────────────────────────────
        if field == "_category":
            canonical = _CATEGORY_MAP.get(value_raw.lower())
            if canonical:
                result["category"] = canonical
            else:
                # Unknown category — preserve raw, log for visibility
                logger.warning(
                    f"[{self.source_domain}] unknown category: {value_raw!r}"
                )
                result["category"] = value_raw
            return

        if field == "_area":
            result["area"] = value_raw
            return

        # ── extra_features ─────────────────────────────────────────────────
        if field.startswith("_extra:"):
            key = field[len("_extra:"):]
            # Distance fields → meters
            if key.startswith("distance_from_"):
                meters = _parse_distance_meters(value_raw)
                if meters is not None:
                    extra[key] = meters
                return
            # Boolean-ish fields
            b = _interpret_bool(value_raw)
            if b is not None:
                extra[key] = b
            else:
                # Keep raw string (e.g. floor="Ground Floor", storeys parsed below)
                # Storeys is numeric but may have weird text — let bool failure
                # fall through to string.
                if key == "storeys":
                    n = _parse_int(value_raw)
                    extra[key] = n if n is not None else value_raw
                else:
                    extra[key] = value_raw
            return

        # ── Numeric direct fields ──────────────────────────────────────────
        if field in ("year_built", "bedrooms", "bathrooms"):
            n = _parse_int(value_raw)
            if n is not None:
                result[field] = n
            return

        if field in ("size_sqm", "land_size_sqm"):
            f = _parse_float(value_raw)
            if f is not None:
                result[field] = f
            return

        # Fallback as plain string
        result[field] = value_raw

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        """
        Photos for halkidikirealestate.com (Hellenic Living).

        URL pattern (server-rendered ASP gallery):
            https://www.halkidikirealestate.com/photos/{N}_{NN}.{ext}
        Example:
            https://www.halkidikirealestate.com/photos/8_49.jpg

        Photos appear as both <a href> (full-size lightbox links) and
        <img src> / <img data-src> (carousel thumbnails). We collect from
        all three attributes, dedupe, and normalise to absolute URLs.

        Tight filename pattern `\\d+_\\d+\\.(jpe?g|png|webp)` excludes
        site assets (logo.png, social icons, flags) which live under
        /assets/ rather than /photos/, but we add a defensive check.

        HISTORY (Sprint 4): an earlier session accidentally pasted
        grekodom's /userfiles/realtyobjects/ regex into this method,
        which matches nothing on Hellenic Living's CDN. Result: ~10
        properties ingested after the regression had ZERO media. Fixed
        here; back-fill via refresh_scraper_data.py.
        """
        images: List[str] = []
        seen: set = set()
        pattern = re.compile(
            r"photos/\d+_\d+\.(jpe?g|png|webp)",
            re.IGNORECASE,
        )
        base = "https://www.halkidikirealestate.com"

        def _add_if_match(raw: str) -> None:
            if not raw:
                return
            raw = raw.strip()
            if not pattern.search(raw):
                return
            # Normalise to absolute URL
            if raw.startswith("//"):
                url = "https:" + raw
            elif raw.startswith("http"):
                url = raw
            elif raw.startswith("/"):
                url = f"{base}{raw}"
            else:
                # Relative: "photos/8_49.jpg" or "./photos/..."
                url = f"{base}/{raw.lstrip('./')}"
            if url not in seen:
                seen.add(url)
                images.append(url)

        # Both anchor lightbox links and inline carousel imgs
        for a in parser.css("a[href]"):
            _add_if_match(a.attributes.get("href") or "")
        for img in parser.css("img"):
            _add_if_match(img.attributes.get("src") or "")
            _add_if_match(img.attributes.get("data-src") or "")

        return images