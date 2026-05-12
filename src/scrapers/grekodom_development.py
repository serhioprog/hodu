"""
~/hodu/src/scrapers/grekodom_development.py

Grekodom Development (grekodom.com) — Halkidiki real estate scraper.

Platform: Server-rendered HTML, no anti-bot. Long-tail aggregator: 733+
properties total in Halkidiki at min_price=400000, split across 49 pages
of 15 cards each.

URL pattern: /RealtyObjects?multiRegion=196&aim=1&pricefrom=400000&page=N

Special considerations
----------------------
1.  *Categories filter.* The site mixes residential, commercial, land, and
    multi-unit *Complex* listings. Complex/Building entries are aggregated
    parents whose sub-units (apartments/duplexes) appear separately in the
    same feed — including them would create heavy duplicates. We whitelist
    Villa, Detached House, Maisonette, Apartment (Flat), Duplex (mapped to
    Maisonette), Land, House and skip the rest.

2.  *Size semantics.* For type=Land the "Sq. Meters" field is land area,
    not building area. We route it to `land_size_sqm` accordingly.

3.  *GPS.* Coordinates are embedded in a JS block at the bottom of every
    detail page (`var mapX = '...'; var mapY = '...';`). Extracted via
    regex; circle radius (5000m typical) goes to extra_features.

4.  *Breadcrumb-driven location.* The detail page header has a clean
    chain (Country → Region → Prefecture → Municipality → Locality). We
    use that for `area` / `subarea` instead of the noisy listing-card text.

5.  *Images.* The full-quality CDN path is `gdcdn.grekodom.com/.../photos/`
    (NOT `icdn.../pictureshd/` which is the cropped card thumbnail).
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.models.schemas import PropertyTemplate
from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_BASE_URL = "https://www.grekodom.com"
_LISTING_PATH = "/RealtyObjects"
_DETAIL_PATH_TPL = "/realtyobject/{id}"
_SOURCE_DOMAIN = "grekodom.com"

# Multi-region ID 196 = "Chalkidiki (Macedonia)" (confirmed in HTML)
_REGION_ID_CHALKIDIKI = 196

# Cards per page (confirmed via paged result count: 733 / 49 pages ≈ 15)
_PAGE_SIZE = 15

# Safety cap on pagination
_MAX_PAGES = 60

# Polite pause between listing-page fetches (longer than other scrapers because
# Grekodom is a big aggregator and we want to avoid being noisy)
_INTER_PAGE_SLEEP_SEC = 2.5

# Image CDN: gdcdn = full-size archive, icdn = cropped thumbnails. We use gdcdn.
_IMAGE_CDN_HOST = "gdcdn.grekodom.com"


# Category filter & canonicalization.
# Site shows these dropdown groups:
#   Housing Properties: Flat, Duplex, Maisonette, Detached house, Villa
#   Commercial:        Hotel, Business, Commercial property, Building, Complex
#   Land:              Land, Island
#
# Map a card's <h4> text (lowercased) → canonical category name (or None to skip).
# Complex/Building are skipped to avoid duplication with their sub-unit listings.
#_CATEGORY_FILTER: Dict[str, Optional[str]] = {
#    # Residential — keep
#    "villa":               "Villa",
#    "detached house":      "Detached House",
#    "maisonette":          "Maisonette",
#    "flat":                "Apartment",
#    "duplex":              "Maisonette",     # treat as maisonette flavour
#    "house":               "House",          # not in dropdown, but defensive
#    # Land — keep
#    "land":                "Land",
#    # Commercial & aggregations — skip
#    "complex":             None,
#    "building":            None,
#    "hotel":               None,
#    "business":            None,
#    "commercial property": None,
#    # Geographic — skip
#    "island":              None,
#}

_CATEGORY_FILTER: Dict[str, str] = {
    "villa":               "Villa",
    "detached house":      "Detached House",
    "maisonette":          "Maisonette",
    "flat":                "Apartment",
    "duplex":              "Duplex",
    "house":               "House",
    "land":                "Land",
    "complex":             "Complex",
    "building":            "Building",
    "hotel":               "Hotel",
    "business":            "Business",
    "commercial property": "Commercial Property",
    "island":              "Island",
}


# Detail-page label routing.
# Keys are normalized labels (lowercase, no trailing ':'), values are either:
#   - "_skip"           → ignore (already obtained elsewhere or noise)
#   - "<column>"        → set PropertyTemplate.<column> directly (numeric or text)
#   - "_extra:<key>"    → put parsed value into extra_features[<key>]
_LABEL_TO_FIELD: Dict[str, str] = {
    # property-main-features
    "object code":           "_skip",
    "for sale, for rent":    "_skip",
    "type":                  "_skip",   # confirmed via card category
    "sq. meters":            "_skip",   # already from card (routed by type there)
    "region":                "_skip",   # use breadcrumb
    "location":              "_skip",   # use breadcrumb
    "land area":             "land_size_sqm",
    # property-features (Details)
    "year of construction":  "year_built",
    "year built":            "year_built",
    "condition":             "_extra:condition",
    "distance from sea":     "_extra:distance_from_sea",
    "distance from airport": "_extra:distance_from_airport",
    "distance from beach":   "_extra:distance_from_sea",   # alias
    "rooms":                 "bedrooms",                   # Greek RE convention
    "bedrooms":              "bedrooms",
    "bathrooms":             "bathrooms",
    "floor":                 "_extra:floor",
    "floors":                "_extra:floors_total",
    "energy class":          "_extra:energy_class",
    "heating":               "_extra:heating",
    "elevator":              "_extra:elevator",
    "swimming pool":         "_extra:pool",
    "communal pool":         "_extra:communal_pool",
    "parking":               "_extra:parking",
    "furniture":             "_extra:furnished",
    "sea view":              "_extra:sea_view",
    "buildable land plot":   "_extra:buildable",
    "new construction":      "_extra:new_construction",
    "under construction":    "_extra:under_construction",
    "old building":          "_extra:old_building",
    "needs renovation":      "_extra:needs_renovation",
    "exclusive":             "_extra:exclusive",
}


# Yes/No interpretation for boolean string values
_YES_VALUES = {"yes", "true", "✔", "✓", "+"}
_NO_VALUES = {"no", "false", "✗", "-", "—"}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_text(s: Optional[str]) -> str:
    """Collapse whitespace, decode &nbsp;, strip."""
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


def _parse_int_from_text(text: str) -> Optional[int]:
    """Extract the first integer-looking sequence from a string."""
    if not text:
        return None
    m = re.search(r"-?\d[\d\s\u00a0,]*", text.replace(".", " "))
    if not m:
        return None
    digits = re.sub(r"\D", "", m.group(0))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _parse_float_from_text(text: str) -> Optional[float]:
    """Extract a decimal number (handles spaces, commas, dots)."""
    if not text:
        return None
    cleaned = text.replace("\xa0", " ").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", cleaned.replace(" ", ""))
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _interpret_bool(value: str) -> Optional[bool]:
    """yes/true → True, no/false → False, else None."""
    if not value:
        return None
    v = value.strip().lower()
    if v in _YES_VALUES:
        return True
    if v in _NO_VALUES:
        return False
    return None


def _strip_price_text(text: str) -> Optional[int]:
    """
    Convert a price string to an integer EUR amount.
    Examples:
      '850 000 €'             → 850000
      '1 200 000 €'           → 1200000
      'Price on request'      → None
      '<del>1 200 000 €</del>  880 000 €' (with old/discount, called with new only)
    """
    if not text:
        return None
    cleaned = text.strip()
    lowered = cleaned.lower()
    if "request" in lowered or "agreement" in lowered or "poa" in lowered:
        return None
    return _parse_int_from_text(cleaned)


def _extract_property_id_from_url(href: str) -> Optional[str]:
    """`/realtyobject/58523-for-sale-complex...` → `'58523'`."""
    if not href:
        return None
    m = re.search(r"/realtyobject/(\d+)(?:[-/?]|$)", href)
    return m.group(1) if m else None


def _construct_image_url(id_: str, filename: str) -> str:
    """Build full-quality CDN URL for an image."""
    return f"https://{_IMAGE_CDN_HOST}/userfiles/realtyobjects/photos/{id_}/{filename}"


def _strip_html_to_text(node: LexborNode) -> str:
    """Extract text from a node, preserving paragraph boundaries from <p> tags."""
    paragraphs: List[str] = []
    for p in node.css("p"):
        t = _normalize_text(p.text(strip=False))
        if t:
            paragraphs.append(t)
    if paragraphs:
        return "\n\n".join(paragraphs)
    # Fallback: raw text if no <p> present
    return _normalize_text(node.text(strip=False))


def _li_label_value(li: LexborNode) -> Tuple[Optional[str], Optional[str]]:
    """
    Split a `<li><i>icon</i> Label: <span>Value</span></li>` into (label, value).
    Works for both formats:
        '<li>Year of construction: <span>2026</span></li>'
        '<li>Object Code <span>58523</span></li>'
    """
    span = li.css_first("span")
    value = _normalize_text(span.text(strip=False)) if span else None
    # Label = full text minus span's text
    full = _normalize_text(li.text(strip=False))
    if value:
        # Remove the value substring once (rightmost occurrence)
        idx = full.rfind(value)
        if idx >= 0:
            full = full[:idx]
    label = full.strip().rstrip(":").strip()
    return label.lower() if label else None, value


# ─────────────────────────────────────────────────────────────────────────────
# Scraper
# ─────────────────────────────────────────────────────────────────────────────

class GrekodomDevelopmentScraper(EnrichmentMixin, BaseScraper):
    """
    grekodom.com — Halkidiki residential, ≥ 400_000 EUR.

    Inherits canonical enrichment helpers from EnrichmentMixin:
      * _apply_nlp_fallback() — fills missing columns + extra_features from description
      * _og_description_fallback() — used when main description extraction is empty
      * _passes_quality_gate() — description quality check

    Default _NLP_TO_STRUCTURAL from mixin works for grekodom (no structural
    slugs conflict with the common NLP keys). If duplicates appear in
    Sprint 5 review, override here.
    """

    # Override mixin default: category comes from the listing CARD (via
    # _CATEGORY_FILTER map → canonical "Land", "Villa", etc.). The detail
    # page does NOT expose a category field, so result["category"] is None
    # when NLP fallback runs. Without this override, NLP's extract_type()
    # would fill "Land" → "Land/Plot" (NLP's canonical name) and the
    # detail-page result would overwrite the cleaner card-derived category
    # during the framework's merge. Exclude category from NLP fill.
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ─── URL builders ────────────────────────────────────────────────────────

    def _construct_listing_url(self, page: int, min_price: int) -> str:
        params = (
            f"multiRegion={_REGION_ID_CHALKIDIKI}"
            f"&aim=1"
            f"&pricefrom={min_price}"
            f"&sortFilter=100"
            f"&page={page}"
        )
        return f"{_BASE_URL}{_LISTING_PATH}?{params}"

    def _construct_detail_url(self, site_id: str) -> str:
        """`/realtyobject/58523` — the slug is decorative; numeric id is enough."""
        return f"{_BASE_URL}{_DETAIL_PATH_TPL.format(id=site_id)}"

    # ─── Phase 1: collect_urls ───────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        """Iterate paginated listings, returning filtered residential seeds."""
        seeds: Dict[str, PropertyTemplate] = {}
        skipped_categories: Dict[str, int] = {}
        last_page_processed = 0

        for page in range(1, max_pages + 1):
            url = self._construct_listing_url(page=page, min_price=min_price)
            logger.info(f"[{self.source_domain}] Парсинг страницы {page}...")

            try:
                response = await self.client.get(url)
            except Exception as exc:
                logger.error(f"[{self.source_domain}] page {page} fetch failed: {exc!r}")
                break

            parser = LexborHTMLParser(response.text)
            cards = parser.css(".listing-item")

            if not cards:
                logger.info(
                    f"[{self.source_domain}] нет карточек на странице {page} — "
                    f"конец пагинации"
                )
                break

            page_added = 0
            page_skipped = 0
            for card in cards:
                seed_or_skip_reason = self._parse_card(card)
                if isinstance(seed_or_skip_reason, str):
                    # Skipped — string is the reason (raw category)
                    skipped_categories[seed_or_skip_reason] = (
                        skipped_categories.get(seed_or_skip_reason, 0) + 1
                    )
                    page_skipped += 1
                    continue
                if seed_or_skip_reason is None:
                    continue
                seed = seed_or_skip_reason
                if seed.site_property_id in seeds:
                    continue
                seeds[seed.site_property_id] = seed
                page_added += 1

            last_page_processed = page
            logger.info(
                f"[{self.source_domain}] страница {page}: {len(cards)} объектов "
                f"(+{page_added}, skipped {page_skipped}; всего seeds: {len(seeds)})"
            )

            # End-of-results: partial page
            if len(cards) < _PAGE_SIZE:
                logger.info(
                    f"[{self.source_domain}] page {page} is partial "
                    f"({len(cards)} < {_PAGE_SIZE}) — last page"
                )
                break

            await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)

        logger.info(
            f"[{self.source_domain}] Phase 1 завершён: {len(seeds)} URLs "
            f"за {last_page_processed} стр."
        )
        if skipped_categories:
            top = sorted(skipped_categories.items(), key=lambda kv: -kv[1])[:6]
            summary = ", ".join(f"{k}:{v}" for k, v in top)
            logger.info(f"[{self.source_domain}] категории отфильтрованы — {summary}")
        return list(seeds.values())

    def _parse_card(self, card: LexborNode):
        """
        Parse a single .listing-item.
        Returns:
            PropertyTemplate     — if accepted
            str                  — raw category name (filtered out)
            None                 — malformed card (skipped silently)
        """
        # Detail link (also holds the ID in the path)
        link = card.css_first("a.listing-img-container[href]")
        href = link.attributes.get("href", "") if link else ""
        if not href:
            return None
        # Make absolute
        detail_url = href if href.startswith("http") else f"{_BASE_URL}{href}"

        site_id = _extract_property_id_from_url(href)
        if not site_id:
            return None

        # Category from <h4 a> in .listing-title
        cat_link = card.css_first(".listing-title h4 a")
        category_raw = _normalize_text(cat_link.text(strip=False)) if cat_link else ""
        category_norm = category_raw.lower()

        if category_norm not in _CATEGORY_FILTER:
            # Unknown — log once-ish via warning, then skip
            logger.warning(
                f"[{self.source_domain}] unknown category {category_raw!r} on id={site_id}"
            )
            return category_raw or "unknown"

        canonical = _CATEGORY_FILTER[category_norm]

        # Location — listing card has just one component (e.g. "Sithonia")
        loc_link = card.css_first(".listing-address")
        location_raw = _normalize_text(loc_link.text(strip=False)) if loc_link else ""
        # Add Chalkidiki/Halkidiki prefix for whitelist matching downstream
        if "halkidiki" not in location_raw.lower() and "chalkidiki" not in location_raw.lower():
            location_raw = f"Chalkidiki, {location_raw}".rstrip(", ")

        # Price — may be "Price on request" or include old/new
        price = self._extract_card_price(card)

        # Size (Sq. Meters) — route to size_sqm or land_size_sqm based on category
        size_value = None
        for li in card.css(".listing-features li"):
            label, value = _li_label_value(li)
            if label == "sq. meters" and value:
                size_value = _parse_float_from_text(value)
                break

        size_sqm = None
        land_size_sqm = None
        if size_value is not None:
            if canonical == "Land":
                land_size_sqm = size_value
            else:
                size_sqm = size_value

        # Rooms → bedrooms (Greek RE convention)
        bedrooms = None
        for li in card.css(".listing-features li"):
            label, value = _li_label_value(li)
            if label == "rooms" and value:
                bedrooms = _parse_int_from_text(value)
                break

        return PropertyTemplate(
            site_property_id=site_id,
            url=detail_url,
            source_domain=self.source_domain,
            category=canonical,
            price=price,
            size_sqm=size_sqm,
            land_size_sqm=land_size_sqm,
            bedrooms=bedrooms,
            location_raw=location_raw,
        )

    def _extract_card_price(self, card: LexborNode) -> Optional[int]:
        """
        Extract the *current* sales price from a card. Handles:
          • '<del>1 200 000 €</del> 880 000 €'   — pick the second amount
          • '850 000 €'                          — single amount
          • 'Price on request'                   — None
        """
        price_node = card.css_first(".listing-price")
        if not price_node:
            return None
        # Get the text but ignore <del> (old price) and <i> (price/m² sub-text)
        # Strategy: clone-ish — collect direct text nodes + ignore certain children
        full = _normalize_text(price_node.text(strip=False))
        if not full or "request" in full.lower():
            return None
        # If there's a strikethrough old price, drop it
        del_node = price_node.css_first("del")
        if del_node:
            old_text = _normalize_text(del_node.text(strip=False))
            if old_text and old_text in full:
                full = full.replace(old_text, "", 1)
        # Drop any italic per-m² hint
        for i_node in price_node.css("i"):
            i_text = _normalize_text(i_node.text(strip=False))
            if i_text and i_text in full:
                full = full.replace(i_text, "", 1)
        return _strip_price_text(full)

    # ─── Phase 2: fetch_details ──────────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """Fetch detail page and extract full property data."""
        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.error(f"[{self.source_domain}] detail fetch failed for {url}: {exc!r}")
            return {}

        html_text = response.text
        parser = LexborHTMLParser(html_text)

        result: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # ─── Title (h1) ─────────────────────────────────────────────────────
        h1 = parser.css_first(".property-title h1")
        if h1:
            title = _normalize_text(h1.text(strip=False))
            if title:
                extra["listing_title"] = title

        # ─── Price ──────────────────────────────────────────────────────────
        price_node = parser.css_first(".property-pricing .property-price")
        if price_node:
            price_text = _normalize_text(price_node.text(strip=False))
            price = _strip_price_text(price_text)
            if price is not None:
                result["price"] = price
            elif "request" in price_text.lower():
                extra["has_price_on_request"] = True

        # ─── Breadcrumb location → area / subarea ───────────────────────────
        # `.property-titlebar .listing-address` items, ordered:
        #   Greece, Macedonia, Chalkidiki, Kassandra, Kallithea
        # We want subarea=municipality (Kassandra), area=locality (Kallithea).
        crumbs = []
        for a in parser.css(".property-titlebar .listing-address"):
            t = _normalize_text(a.text(strip=False))
            if t:
                crumbs.append(t)
        crumbs_lower = [c.lower() for c in crumbs]
        if crumbs:
            # Subarea = municipality (3rd entry usually = "Chalkidiki" prefecture,
            # 4th = municipality)
            try:
                idx_chalk = next(
                    i for i, c in enumerate(crumbs_lower)
                    if c in ("chalkidiki", "halkidiki")
                )
            except StopIteration:
                idx_chalk = -1
            if idx_chalk >= 0 and idx_chalk + 1 < len(crumbs):
                result["subarea"] = crumbs[idx_chalk + 1]      # municipality
            if idx_chalk >= 0 and idx_chalk + 2 < len(crumbs):
                result["area"] = crumbs[idx_chalk + 2]         # locality

            # location_raw = full chain
            result["location_raw"] = ", ".join(crumbs)

        # ─── Main features / details lists ──────────────────────────────────
        for ul_selector in (".property-main-features", ".property-features"):
            for ul in parser.css(ul_selector):
                for li in ul.css("li"):
                    self._route_field(li, result, extra)

        # ─── Description ────────────────────────────────────────────────────
        description = self._extract_description(parser)
        if not description:
            description = self._og_description_fallback(parser)
        if description:
            result["description"] = description

        # ─── Photos ─────────────────────────────────────────────────────────
        images = self._extract_images(parser)
        if images:
            result["images"] = images

        # ─── GPS from JS ────────────────────────────────────────────────────
        lat, lng, radius = self._extract_gps(html_text)
        if lat is not None and lng is not None:
            result["latitude"] = lat
            result["longitude"] = lng
            extra["gps_type"] = "circle"
            extra["gps_radius_m"] = radius or 5000

        # ─── Documents (floorplans, PDFs) ───────────────────────────────────
        docs = self._extract_documents(parser)
        if docs:
            extra["documents"] = docs

        # ─── Panorama URL ──────────────────────────────────────────────────
        pan = self._extract_panorama(parser)
        if pan:
            extra["panorama_url"] = pan

        # ─── YouTube video ─────────────────────────────────────────────────
        video = self._extract_video(parser)
        if video:
            extra["video_url"] = video

        # ─── Agent / contact ───────────────────────────────────────────────
        agent = self._extract_agent(parser)
        if agent:
            extra.update(agent)

        # Merge accumulated extras into result FIRST so NLP can see them
        # for dedup purposes (step 5 of canonical pattern).
        if extra:
            result["extra_features"] = extra

        # ─── Step 5: NLP fallback over description ─────────────────────────
        # Fills missing year_built/bedrooms/bathrooms/etc + adds amenity
        # features (sea_view, pool, parking, ...) detected in text.
        # Never overwrites structural data.
        self._apply_nlp_fallback(result)

        # ─── Step 7: Quality Gate (log only — don't drop) ──────────────────
        if not self._passes_quality_gate(result.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate "
                f"for {url}"
            )

        return result

    def _route_field(
        self,
        li: LexborNode,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """Read a single <li>, look up the label, dispatch to result or extra."""
        label, value = _li_label_value(li)
        if not label or value is None:
            return
        field = _LABEL_TO_FIELD.get(label)
        if not field or field == "_skip":
            # Unknown label — stash in extra under a normalized key for visibility
            if label:
                slug = re.sub(r"\W+", "_", label).strip("_")
                if slug and slug not in extra:
                    extra[slug] = value
            return

        # Decide target: column (numeric) or extra (typed)
        if field.startswith("_extra:"):
            key = field[len("_extra:"):]
            # Try bool first, then int, then keep as string
            b = _interpret_bool(value)
            if b is not None:
                extra[key] = b
            else:
                extra[key] = value
            return

        # Numeric columns
        if field in ("bedrooms", "bathrooms", "year_built"):
            n = _parse_int_from_text(value)
            if n is not None:
                result[field] = n
            return
        if field in ("size_sqm", "land_size_sqm"):
            f = _parse_float_from_text(value)
            if f is not None:
                result[field] = f
            return
        # String / other column — write as-is
        result[field] = value

    def _extract_description(self, parser: LexborHTMLParser) -> Optional[str]:
        """Find the <div> that follows the "Description" h3."""
        target_div: Optional[LexborNode] = None
        for h3 in parser.css("h3.desc-headline"):
            heading = _normalize_text(h3.text(strip=False)).lower()
            if heading == "description":
                # Walk forward to the next element sibling
                nxt = h3.next
                # selectolax: .next returns the next sibling (text or element)
                while nxt is not None and nxt.tag in (None, "-text"):
                    nxt = nxt.next
                if nxt is not None and nxt.tag == "div":
                    target_div = nxt
                break
        if target_div is None:
            return None
        text = _strip_html_to_text(target_div)
        return text or None

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        """
        Photos from any gallery widget — match by URL path pattern, not
        by wrapping CSS class. Grekodom uses TWO galleries:
          * .property-slider with /userfiles/realtyobjects/photos/{id}/{file}.jpg
            (Complex / large Villa properties)
          * Alternative widget with /userfiles/realtyobjects/pictureshd/{id}/{file}.jpeg
            (Land + many House/Villa properties)
        Both paths accepted. Reject thumbnail CDN (icdn with ?preset=).
        """
        images: List[str] = []
        seen: set = set()
        pattern = re.compile(
            r"/userfiles/realtyobjects/(photos|pictureshd)/",
            re.IGNORECASE,
        )
        # Match any <a href> with a relevant photo URL (regardless of wrapper)
        for a in parser.css("a[href]"):
            href = (a.attributes.get("href") or "").strip()
            if not href or not pattern.search(href):
                continue
            # Reject thumbnail CDN (icdn) with resize preset
            if "preset=" in href:
                continue
            if href in seen:
                continue
            seen.add(href)
            images.append(href)
        return images

    def _extract_gps(self, html_text: str) -> Tuple[Optional[float], Optional[float], Optional[int]]:
        """
        Pull lat/lng/radius from the inline JS:
            var mapX = '40.07150772733314';
            var mapY = '23.443941771984104';
            var options = { radius: 5000};
        """
        m_x = re.search(r"var\s+mapX\s*=\s*['\"]([-\d.]+)['\"]", html_text)
        m_y = re.search(r"var\s+mapY\s*=\s*['\"]([-\d.]+)['\"]", html_text)
        m_r = re.search(r"radius:\s*(\d+)", html_text)
        lat = _parse_float_from_text(m_x.group(1)) if m_x else None
        lng = _parse_float_from_text(m_y.group(1)) if m_y else None
        radius = int(m_r.group(1)) if m_r else None
        return lat, lng, radius

    def _extract_documents(self, parser: LexborHTMLParser) -> List[Dict[str, str]]:
        """Floorplan PDFs etc. from the Documents section."""
        docs: List[Dict[str, str]] = []
        # The h3 "Documents" is followed by .additional-amenities containing <a> tags
        for h3 in parser.css("h3.desc-headline"):
            if _normalize_text(h3.text(strip=False)).lower() != "documents":
                continue
            container = h3.next
            while container is not None and container.tag in (None, "-text"):
                container = container.next
            if container is None:
                break
            for a in container.css("a[href]"):
                href = a.attributes.get("href", "") or ""
                if not href or not href.lower().endswith(".pdf"):
                    continue
                label = _normalize_text(a.text(strip=False))
                docs.append({"href": href, "label": label or "document"})
            break
        return docs

    def _extract_panorama(self, parser: LexborHTMLParser) -> Optional[str]:
        """360° panorama iframe URL (Pannellum)."""
        for iframe in parser.css('iframe[src*="pannellum"]'):
            src = iframe.attributes.get("src", "")
            if src:
                # Parse out the panorama URL from the iframe src query string
                m = re.search(r"panorama=([^&]+)", src)
                if m:
                    return m.group(1)
                return src
        return None

    def _extract_video(self, parser: LexborHTMLParser) -> Optional[str]:
        """YouTube embed URL → normalized watch URL."""
        for iframe in parser.css('iframe[src*="youtube.com/embed"]'):
            src = iframe.attributes.get("src", "")
            m = re.search(r"youtube\.com/embed/([^?&/]+)", src)
            if m:
                return f"https://www.youtube.com/watch?v={m.group(1)}"
            if src:
                return src
        return None

    def _extract_agent(self, parser: LexborHTMLParser) -> Dict[str, str]:
        """Agent name, role, phone, email from sidebar widget."""
        agent: Dict[str, str] = {}
        widget = parser.css_first(".agent-widget")
        if not widget:
            return agent
        name_link = widget.css_first(".agent-details h4 a")
        if name_link:
            agent["agent_name"] = _normalize_text(name_link.text(strip=False))
        # Phone & email are inside <span> tags after icons; we use a regex on the
        # whole widget text for resilience.
        widget_text = widget.text(strip=False)
        phone_m = re.search(r"\+?\d[\d\s\-()]{8,}\d", widget_text)
        if phone_m:
            agent["agent_phone"] = _normalize_text(phone_m.group(0))
        email_m = re.search(r"[\w.\-+]+@[\w.\-]+\.[a-zA-Z]{2,}", widget_text)
        if email_m:
            agent["agent_email"] = email_m.group(0)
        return agent