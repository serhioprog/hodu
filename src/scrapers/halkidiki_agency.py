"""
~/hodu/src/scrapers/halkidiki_agency.py

Halkidiki Agency (halkidikiagency.gr) — Spitogatos-based agent website
template. Powered by spitogatos.gr's "agentwebsites/template5" CMS, which
~30 Greek real-estate agencies share. This scraper's selectors target the
template5 markup; if a future template upgrade happens, expect breakage in
the same DOM points (info-table, property-amenities, marker data-attrs).

Scale: ~82 residential listings ≥ €400k in Halkidiki at min_price=400000,
split across 10 pages of 9 cards each. Small site — daily sync completes
in ~3-5 min.

URL pattern (list):
    /en/property/search?listingType=sale&category=residential
        &region=196 (Chalkidiki)
        &priceLow=400000
        &page=N

URL pattern (detail):
    /en/propertyDetails/{numeric_id}

Detail-page anatomy
-------------------
1. Title in `<h1>` inside `.property-content`. Format consistent across
   site: "Villa for sale Sani (Kassandra)" — we parse it for category +
   area + subarea, but treat detail-page specs as more authoritative.

2. Description: a SINGLE `<p>` directly inside `.property-content` (no
   wrapper class on the paragraph itself). On some listings this is the
   only narrative text; on others it's 2-3 sentences. og:description is
   used as fallback.

3. Info table — `.info-table` with `<tr><th>Label</th><td>Value</td></tr>`
   rows. Source of truth for: Price, Rooms (= bedrooms in Greek RE),
   Floor, Construction year, Renovation year, Heating, Energy class,
   Levels, Kitchens, Living rooms, Bathrooms, WC, Status, Type.

4. Amenities list — `.property-amenities li`. Mixed format:
       <li>Storage space</li>                       boolean
       <li>Distance from sea (m): <span>800</span>  key + value
   Boolean items go to extra_features.<slug> = True.
   key:value items split on the FIRST ":" — value is the text-red span.

5. Coordinates — `<div class="marker" data-lat="..." data-lng="..."
   data-type="offset">`. data-type="offset" means Spitogatos has
   intentionally offset coordinates by ~200m for privacy. We store as-is
   and mark `gps_type: 'offset'` in extra_features so downstream
   (clustering, map UI) can apply the right radius/uncertainty.

6. Images — `.swiper-slide img` carousel with mix of `src` (loaded) and
   `data-src` (lazy). All on m1/m2/m3.spitogatos.gr CDN at
   1600x1200 resolution. No need to strip a size suffix — these are
   already full-quality.

Anti-bot
--------
Site embeds `<script src="/69616d7761746368696e67796f75">` — hex-decoded
ASCII = "iamwatchingyou". This is Spitogatos' bot fingerprinting layer.
Empirically curl_cffi handles it (no nonce binding, no JS challenge);
the funnel will auto-escalate to Playwright if Stage 0 starts returning
captchas or 429s on real runs.

Map platform (note for future)
------------------------------
Site uses Leaflet/OpenStreetMap (`#osm-map`, `#popup-map`, leaflet.css).
Currently we read coords from the static `.marker` data-attrs — no JS
execution needed. Same selector should keep working if the map provider
swaps Leaflet for Google Maps in the future, because the agent template
keeps the `.marker[data-lat][data-lng]` div for SEO/structured data.

Title parsing — when used
-------------------------
The `<h1>` follows "{Type} for sale {Area} ({Subarea})". We parse it
ONLY as a fallback for `subarea` if the info-table's Neighborhood row
is missing (rare). The info-table is always authoritative — its
Neighborhood field encodes the same "Area (Subarea)" string anyway.
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


# =============================================================================
# Constants
# =============================================================================

_BASE_URL = "https://www.halkidikiagency.gr"
_LISTING_PATH = "/en/property/search"
_DETAIL_PATH_TPL = "/en/propertyDetails/{id}"
_SOURCE_DOMAIN = "halkidikiagency.gr"

# Region 196 = Chalkidiki (same numeric ID as Grekodom uses — both sites are
# built on Spitogatos' shared region taxonomy)
_REGION_ID_CHALKIDIKI = 196

# Confirmed via "Page 1 of 10 - Listings 1 to 9 from 82" pagination footer:
# 9 cards per page (could be 9-12 depending on viewport; treat as ~10 with
# the partial-page detector for end-of-results).
_PAGE_SIZE = 9

# Safety cap — site has at most ~10 pages at min_price=400000 today, but
# we leave headroom for future inventory growth.
_MAX_PAGES = 30

# Polite pause between listing-page fetches. Site is on a shared CDN with
# Spitogatos infra; 2s is comfortable.
_INTER_PAGE_SLEEP_SEC = 2.0


# =============================================================================
# Title parsing
# =============================================================================
#
# H1 format: "{Type} for sale {Area} ({Subarea})"
#   "Villa for sale Sani (Kassandra)"
#   "Maisonette for sale Pefkochori (Pallini)"
#
# Mapping site type-words → canonical category names used elsewhere in hodu.
# Lowercased; the listing-card <h3> text is normalised before lookup.
_CARD_CATEGORY_MAP: Dict[str, str] = {
    "villa":               "Villa",
    "maisonette":          "Maisonette",
    "apartment":           "Apartment",
    "studio":              "Studio",
    "detached":            "Detached House",
    "detached house":      "Detached House",
    "loft":                "Loft",
    "bungalow":            "Bungalow",
    "building":            "Building",
    "farm":                "Farm",
    "house":               "House",
}

_TITLE_RE = re.compile(
    r"^(?P<type>[A-Za-z][A-Za-z\s-]+?)\s+for\s+sale\s+(?P<area>.+?)\s*\((?P<subarea>[^)]+)\)\s*$",
    re.IGNORECASE,
)


# =============================================================================
# Info-table label routing
# =============================================================================
#
# Keys are <th> labels normalised (lowercase, ":" stripped). Values:
#   "_skip"            → ignore (already obtained elsewhere)
#   "<column>"         → set top-level PropertyTemplate.<column>
#   "_extra:<key>"     → put parsed value into extra_features[<key>]
#
# Site quirk: "Rooms" in Greek RE convention = bedrooms (the count of
# private sleeping rooms). This is consistent with how Grekodom labels
# the same field.
_TABLE_LABEL_TO_FIELD: Dict[str, str] = {
    "price":             "price",
    "price per m²":      "_extra:price_per_sqm",
    "price per m2":      "_extra:price_per_sqm",
    "neighborhood":      "_skip",   # parsed separately into area/subarea below
    "zone":              "_extra:zone",
    "rooms":             "bedrooms",
    "floor":             "_extra:floor",
    "parking spot":      "_extra:parking",
    "construction year": "year_built",
    "renovation year":   "_extra:renovation_year",
    "heating system":    "_extra:heating",
    "energy class":      "_extra:energy_class",
    "levels":            "levels",
    "kitchens":          "_extra:kitchens_count",
    "living rooms":      "_extra:living_rooms_count",
    "bathrooms":         "bathrooms",
    "wc":                "_extra:wc_count",
    "status":            "_extra:status",
    "type":              "_extra:usage_type",   # "Holiday home, Investment, ..." — NOT category
    "extra":             "_extra:extra_attributes",
    "available since":   "_extra:available_since",
}


# =============================================================================
# Amenity list label routing (right column of detail page)
# =============================================================================
#
# Each <li> in .property-amenities is either:
#   "Storage space"                              → bool flag → extra[storage_space] = True
#   "Distance from sea (m): <span>800</span>"    → key + numeric value
#   "Glass Type: <span>Double Glass</span>"      → key + string value
#
# This map routes KNOWN labels to canonical keys in extra_features. Unknown
# labels are slug-normalised and stored verbatim (so we still capture them
# for review without crashing).
_AMENITY_LABEL_TO_FIELD: Dict[str, str] = {
    "distance from sea (m)":           "distance_from_sea",
    "size of balconies":               "balconies_size_sqm",
    "lot size":                        "land_size_sqm",   # → top-level column
    "average monthly shared expenses": "monthly_expenses",
    "frames type":                     "frames_type",
    "glass type":                      "glass_type",
    "floors type":                     "floors_type",
    "orientation":                     "orientation",
    "road type":                       "road_type",
    # The rest are boolean amenities matched as plain text (no colon).
    # These don't need an entry here — the routing function detects them
    # by absence of value and uses _slug(label) as the key.
}


# =============================================================================
# Helpers — pure functions, no scraper state
# =============================================================================

def _normalize_text(s: Optional[str]) -> str:
    """Collapse whitespace, decode &nbsp;, strip."""
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


def _slug(label: str) -> str:
    """
    Convert a free-form HTML label into a stable JSON key.

    Examples:
      "Floors Type"           -> "floors_type"
      "Storage space"         -> "storage_space"
      "Distance from sea (m)" -> "distance_from_sea_m"
      "WC"                    -> "wc"
    """
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_int_simple(text: str) -> Optional[int]:
    """Extract first integer from a string. '3', 'Rooms: 3', '3 ' → 3."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """
    Parse area-in-square-meters style strings.
      "100 m²"  → 100.0
      "450"     → 450.0
      "200 m²"  → 200.0
    """
    if not text:
        return None
    m = re.search(r"\d+(?:[.,]\d+)?", text.replace("\xa0", " "))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def _to_int_euro_en(text: str) -> Optional[int]:
    """
    Parse halkidikiagency.gr price strings (English locale: comma = thousand sep).

    Examples:
      "€ 620,000"               → 620000
      "620,000 (↓ 100,000 €...)" → 620000   (takes FIRST number, which is current price)
      "€ 1,500,000"             → 1500000
      "Price on request"        → None

    Site convention is English thousands ("," as separator, no decimal cents
    on listing pages). Sanity-capped at 200M EUR via inherited mixin constant.
    """
    if not text:
        return None
    cleaned = text.strip()
    lowered = cleaned.lower()
    if "request" in lowered or "poa" in lowered:
        return None
    # Pull the first number-like token (may contain commas). Site does NOT
    # use "." for thousands or ",NN" cents on this template.
    m = re.search(r"[\d,]+", cleaned)
    if not m:
        return None
    digits = m.group(0).replace(",", "")
    if not digits:
        return None
    try:
        value = int(digits)
    except ValueError:
        return None
    # Reuse mixin's sanity cap (200M EUR) for consistency
    if value > EnrichmentMixin._PRICE_SANITY_CAP:
        return None
    return value


def _interpret_yes_no(text: str) -> Optional[bool]:
    """'Yes' / 'No' (+ Greek ναι/όχι) → bool. Anything else → None."""
    if not text:
        return None
    v = text.strip().lower()
    if v in {"yes", "ναι", "y", "true", "1"}:
        return True
    if v in {"no", "όχι", "οχι", "n", "false", "0"}:
        return False
    return None


def _extract_property_id_from_url(href: str) -> Optional[str]:
    """`/en/propertyDetails/18708127` → `'18708127'`."""
    if not href:
        return None
    m = re.search(r"/propertyDetails/(\d+)", href)
    return m.group(1) if m else None


def _extract_bg_image_url(style: str) -> Optional[str]:
    """
    Pull the URL out of an inline `background-image: url('...')` style attr.

    Listing cards have:
        style="background-image: url('https://m3.spitogatos.gr/..._900x675.jpg?v=...');"
    """
    if not style:
        return None
    m = re.search(r"url\(['\"]?([^'\"\)]+)", style)
    return m.group(1).strip() if m else None


# =============================================================================
# Scraper
# =============================================================================

class HalkidikiAgencyScraper(EnrichmentMixin, BaseScraper):
    """
    halkidikiagency.gr — Spitogatos agentwebsite/template5.

    Two-phase pattern (canonical):
      Phase 1: collect_urls(min_price) → list[PropertyTemplate] seeds
      Phase 2: fetch_details(url)       → dict for one property

    Inherits the canonical enrichment helpers (EnrichmentMixin):
      * _apply_nlp_fallback()    fills missing columns + dedup'd extras
      * _og_description_fallback() used when site description is too thin
      * _passes_quality_gate()   log-only quality check at end of fetch
      * _to_int_euro_safe()      not used here (site is English locale,
                                  see _to_int_euro_en above)

    NOT used: LLM fallback. Descriptions here are typically 50-200 chars
    (single short <p>), well below the _LLM_MIN_DESCRIPTION_CHARS threshold,
    so the LLM step would no-op anyway. The structured info-table + amenity
    <li> list cover nearly all signal-rich fields on this template, making
    LLM unnecessary for this source.
    """

    # ── Mixin overrides ──────────────────────────────────────────────────

    # Card-derived category from the <h1>/h3 "Villa for sale ..." string is
    # the source of truth. The detail page has no separate "Category" field
    # (its "Type" row is usage-type: "Holiday home, Investment, ..." — that
    # goes to extra_features). Excluding category from NLP fill prevents
    # NLP regex from overwriting our parsed "Villa"/"Maisonette" with its
    # own canonical names (which differ in capitalisation/wording).
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )

    # Semantic dedup map — when NLP regex returns these keys but the
    # structural pass already filled a related extra_features slug, skip
    # the NLP duplicate. The site uses short labels like "Storage space" /
    # "Swimming pool" / "Fireplace"; the NLP dictionary canonicalises to
    # longer forms — both would otherwise land in extras.
    _NLP_TO_STRUCTURAL = {
        "swimming_pool":    {"swimming_pool", "pool"},
        "storage_room":     {"storage_space", "storage"},
        "balcony":          {"balcony"},
        "fireplace":        {"fireplace"},
        "security_door":    {"secure_door", "security_door"},
        "sea_view":         {"view", "sea_view"},
        "parking":          {"parking", "parking_spot"},
        "air_conditioning": {"air_conditioning", "a_c", "ac"},
        "heating":          {"heating", "central_heating"},
        "alarm_system":     {"alarm", "alarm_system"},
        # Frames/Floors/Glass type fields don't collide with NLP keys
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builders ─────────────────────────────────────────────────────

    def _construct_listing_url(self, page: int, min_price: int) -> str:
        """
        Build a paginated search-results URL.

        Includes all the empty query-string parameters that the site's form
        sends by default — minimises the chance of triggering an anti-bot
        path that expects "real" form submissions.
        """
        params = [
            "listingType=sale",
            "category=residential",
            "propertyTypes%5B%5D=",
            f"region={_REGION_ID_CHALKIDIKI}",
            f"priceLow={min_price}",
            "priceHigh=",
            "livingAreaLow=",
            "livingAreaHigh=",
            "myCode=",
            "roomsLow=nd",
            "roomsHigh=nd",
            "floorNumberLow=nd",
            "floorNumberHigh=nd",
            "constructionYearLow=",
            "constructionYearHigh=",
            "heatingControllers=",
            "heatingMedia=",
        ]
        if page > 1:
            params.append(f"page={page}")
        return f"{_BASE_URL}{_LISTING_PATH}?{'&'.join(params)}"

    def _construct_detail_url(self, site_id: str) -> str:
        return f"{_BASE_URL}{_DETAIL_PATH_TPL.format(id=site_id)}"

    # ── Phase 1: collect_urls ────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        """
        Walk paginated search results, returning one PropertyTemplate seed
        per unique listing. Phase 2 (fetch_details) fills in everything
        else from the detail page.
        """
        seeds: Dict[str, PropertyTemplate] = {}
        last_page_processed = 0

        for page in range(1, max_pages + 1):
            url = self._construct_listing_url(page=page, min_price=min_price)
            logger.info(f"[{self.source_domain}] Парсинг страницы {page}...")

            try:
                response = await self.client.get(url)
            except Exception as exc:
                logger.error(
                    f"[{self.source_domain}] page {page} fetch failed: {exc!r}"
                )
                break

            parser = LexborHTMLParser(response.text)
            cards = parser.css("a.property-item")

            if not cards:
                logger.info(
                    f"[{self.source_domain}] нет карточек на странице {page} — "
                    f"конец пагинации"
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

            last_page_processed = page
            logger.info(
                f"[{self.source_domain}] страница {page}: "
                f"{len(cards)} объектов (+{page_added}, всего: {len(seeds)})"
            )

            # End-of-results: partial page (fewer cards than the typical page size)
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
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """
        Parse a single <a class="property-item"> listing-card.

        Returns a PropertyTemplate seed, or None if the card is malformed
        (no href, no parseable ID). Unknown categories are kept (not
        filtered out) — the site only shows residential within our search
        URL anyway, and any new category-word would otherwise silently
        drop listings.
        """
        href = (card.attributes.get("href") or "").strip()
        if not href:
            return None

        site_id = _extract_property_id_from_url(href)
        if not site_id:
            return None

        # ─── Title → category / area / subarea ──────────────────────────
        h3_node = card.css_first("h3")
        title_raw = _normalize_text(h3_node.text(strip=False)) if h3_node else ""

        category: Optional[str] = None
        area: Optional[str] = None
        subarea: Optional[str] = None
        if title_raw:
            m = _TITLE_RE.match(title_raw)
            if m:
                type_word = m.group("type").strip().lower()
                category = _CARD_CATEGORY_MAP.get(type_word)
                area = m.group("area").strip()
                subarea = m.group("subarea").strip()
            else:
                logger.debug(
                    f"[{self.source_domain}] card title didn't match "
                    f"expected pattern: {title_raw!r}"
                )

        # ─── Price ──────────────────────────────────────────────────────
        price_node = card.css_first(".price")
        price = (
            _to_int_euro_en(price_node.text(strip=False))
            if price_node else None
        )

        # ─── Size (m²) ──────────────────────────────────────────────────
        area_node = card.css_first(".area")
        size_sqm = (
            _to_float_sqm(area_node.text(strip=False))
            if area_node else None
        )

        # ─── Bedrooms / Bathrooms (regex on the <b> blob) ──────────────
        bedrooms: Optional[int] = None
        bathrooms: Optional[int] = None
        b_node = card.css_first("p b")
        if b_node:
            b_text = _normalize_text(b_node.text(strip=False))
            m_bed = re.search(r"(\d+)\s*Bedroom", b_text, re.IGNORECASE)
            m_bath = re.search(r"(\d+)\s*Bathroom", b_text, re.IGNORECASE)
            if m_bed:
                bedrooms = int(m_bed.group(1))
            if m_bath:
                bathrooms = int(m_bath.group(1))

        # ─── Location string (best-effort from title) ───────────────────
        # PropertyTemplate has `location_raw` for the unparsed string;
        # area/subarea are filled from detail page (info-table is authoritative).
        location_parts = [p for p in (area, subarea, "Chalkidiki") if p]
        location_raw = ", ".join(location_parts)

        return PropertyTemplate(
            site_property_id=site_id,
            url=href,
            source_domain=self.source_domain,
            category=category,
            price=price,
            size_sqm=size_sqm,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            location_raw=location_raw,
        )

    # ── Phase 2: fetch_details ───────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Fetch one detail page and assemble the full data dict.

        Canonical 7-step pattern (from EnrichmentMixin docstring):
          1. Structured panel extraction       — info-table + amenities
          2. Description → og:description fallback
          3. Coords (data-attrs on .marker)
          4. Images → og:image fallback
          5. NLP fallback (description-only)
          6. (LLM fallback — skipped, see class docstring)
          7. Quality Gate (log only)
        """
        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.error(
                f"[{self.source_domain}] detail fetch failed for {url}: {exc!r}"
            )
            return {}

        html_text = response.text
        parser = LexborHTMLParser(html_text)

        result: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # ─── Title (h1) ─────────────────────────────────────────────────
        h1 = parser.css_first(".property-content h1")
        if h1:
            title = _normalize_text(h1.text(strip=False))
            if title:
                extra["listing_title"] = title
                # Parse the same "Villa for sale Sani (Kassandra)" pattern.
                # The info-table's Neighborhood row is preferred; this is a
                # fallback that fires only if the table parse misses it.
                m = _TITLE_RE.match(title)
                if m:
                    type_word = m.group("type").strip().lower()
                    cat = _CARD_CATEGORY_MAP.get(type_word)
                    if cat:
                        result["category"] = cat
                    result["area"] = m.group("area").strip()
                    result["subarea"] = m.group("subarea").strip()

        # ─── Header price + size (above the info-table) ────────────────
        # The page has two price displays: the big header (.price-area)
        # and the info-table row. Both should agree; we read the header
        # first as a cheap signal, then let _parse_specs_table overwrite
        # if it finds the table row (more authoritative if the values
        # ever diverge).
        header_price = parser.css_first(".property-price-area .price")
        if header_price:
            p = _to_int_euro_en(header_price.text(strip=False))
            if p is not None:
                result["price"] = p

        header_size = parser.css_first(".property-price-area .area")
        if header_size:
            s = _to_float_sqm(header_size.text(strip=False))
            if s is not None:
                result["size_sqm"] = s

        # ─── Step 1a: Info-table (left column) ─────────────────────────
        self._parse_specs_table(parser, result, extra)

        # ─── Step 1b: Amenity list (right column) ──────────────────────
        self._parse_amenities(parser, result, extra)

        # ─── Step 2: Description ───────────────────────────────────────
        description = self._extract_description(parser)
        if not description:
            description = self._og_description_fallback(parser)
        if description:
            result["description"] = description

        # ─── Step 3: Coordinates ───────────────────────────────────────
        lat, lng, gps_type = self._extract_coordinates(parser)
        if lat is not None and lng is not None:
            result["latitude"] = lat
            result["longitude"] = lng
            # Mark whether the coords are exact or intentionally offset.
            # Spitogatos defaults to "offset" (~200m radius) for privacy.
            # Downstream consumers (clustering, map UI) should treat
            # offset coords as having higher position uncertainty.
            extra["gps_type"] = gps_type or "offset"
            if (gps_type or "offset") == "offset":
                # Template's JS uses a 200m visualisation radius for offset
                # markers — record it so downstream code doesn't have to
                # know template internals.
                extra["gps_radius_m"] = 200

        # ─── Step 4: Images ────────────────────────────────────────────
        images = self._extract_images(parser)
        if not images:
            og = self._og_image_fallback(parser)
            if og:
                images = [og]
        if images:
            result["images"] = images

        # Merge accumulated extras BEFORE NLP fallback so dedup can see them.
        if extra:
            result["extra_features"] = extra

        # ─── Step 5: NLP fallback ──────────────────────────────────────
        self._apply_nlp_fallback(result)

        # ─── Step 7: Quality Gate (log-only) ──────────────────────────
        if not self._passes_quality_gate(result.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate "
                f"for {url}"
            )

        return result

    # ── fetch_details helpers ────────────────────────────────────────────

    def _parse_specs_table(
        self,
        parser: LexborHTMLParser,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Walk the `.info-table` rows. Each <tr> has:
            <th>Label</th><td>Value</td>

        Routing is via _TABLE_LABEL_TO_FIELD. Unknown labels (e.g. site adds
        a new row) get slugged and stored in extras for visibility instead
        of being silently dropped.

        Two special-case labels:
          * "Neighborhood" — value format "Sani (Kassandra)" — we split
            into area + subarea here rather than via the map, because the
            split is non-trivial (depends on the parentheses).
          * "Price" — value may include a discount sub-string like
            "620,000 (↓ 100,000 € on 18/05/2026)". We take the first
            integer (the active price); the discount is exposed in
            extras for analytics.
        """
        table = parser.css_first(".info-table")
        if not table:
            return

        for tr in table.css("tr"):
            th = tr.css_first("th")
            td = tr.css_first("td")
            if not th or not td:
                continue
            label_raw = _normalize_text(th.text(strip=False))
            if not label_raw:
                continue
            label = label_raw.lower().rstrip(":").strip()
            # Normalise the m²/m2 variants in the label so both render to
            # the same routing key
            label_key = label.replace("m²", "m²").replace(" m²", " m²")

            # Value extraction: prefer raw text; the energy-class cell has
            # an inner <div> with the letter, which .text() picks up fine.
            value_raw = _normalize_text(td.text(strip=False))

            # Special case: Neighborhood — split "Sani (Kassandra)" → area + subarea.
            # Info-table is authoritative; this overrides whatever the
            # h1-title parse put in earlier.
            if label_key == "neighborhood":
                m_loc = re.match(r"^(?P<area>.+?)\s*\((?P<subarea>[^)]+)\)\s*$", value_raw)
                if m_loc:
                    result["area"] = m_loc.group("area").strip()
                    result["subarea"] = m_loc.group("subarea").strip()
                else:
                    result["area"] = value_raw
                # location_raw kept as-is (full string + ", Chalkidiki" suffix
                # already set in seed); enrich with current value for clarity.
                result["location_raw"] = f"{value_raw}, Chalkidiki"
                continue

            # Special case: Price — capture the discount info if present,
            # then let the standard route handle the integer extraction.
            if label_key == "price":
                # If the value has "(↓ NNN,NNN € on DD/MM/YYYY)" or
                # the .fa-arrow-down icon's sibling number, record it.
                m_disc = re.search(
                    r"(?:↓|\bdown\b)\s*([\d,]+)\s*€?\s*on\s*([\d/.-]+)",
                    value_raw,
                    re.IGNORECASE,
                )
                if m_disc:
                    try:
                        extra["price_discount_amount"] = int(m_disc.group(1).replace(",", ""))
                        extra["price_discount_date"] = m_disc.group(2)
                    except (ValueError, AttributeError):
                        pass
                # Fall through to normal routing (which parses the first
                # integer as the price).

            field = _TABLE_LABEL_TO_FIELD.get(label_key)
            if field == "_skip":
                continue

            if field is None:
                # Unknown label — preserve for review (slug + raw value).
                # Site might add new rows in template updates.
                slug = _slug(label_raw)
                if slug and slug not in extra:
                    extra[slug] = value_raw
                continue

            self._route_table_value(field, value_raw, result, extra)

    def _route_table_value(
        self,
        field: str,
        value_raw: str,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Type-coerce one info-table value into either a Property column or
        extra_features. _extra: prefix → extras dict.

        Coercion rules per known field:
          * price          → int EUR (strip locale punctuation)
          * bedrooms /
            bathrooms /
            year_built     → int
          * levels         → str  (Property column is String — keeps "Two-level" etc)
          * land_size_sqm  → float
          * Yes/No string  → bool
          * Other          → str (verbatim)
        """
        if not value_raw:
            return

        # _extra: prefix dispatch ────────────────────────────────
        if field.startswith("_extra:"):
            key = field[len("_extra:"):]
            # Try bool first
            b = _interpret_yes_no(value_raw)
            if b is not None:
                extra[key] = b
                return
            # Numeric "count" suffix → coerce to int
            if key.endswith("_count") or key in {"renovation_year"}:
                n = _to_int_simple(value_raw)
                if n is not None:
                    extra[key] = n
                    return
            extra[key] = value_raw
            return

        # Top-level Property columns ─────────────────────────────
        if field == "price":
            v = _to_int_euro_en(value_raw)
            if v is not None:
                result["price"] = v
        elif field in {"bedrooms", "bathrooms", "year_built"}:
            n = _to_int_simple(value_raw)
            if n is not None:
                result[field] = n
        elif field == "levels":
            # PropertyTemplate.levels is a String column; keep verbatim
            result["levels"] = value_raw.strip()
        elif field == "size_sqm" or field == "land_size_sqm":
            f = _to_float_sqm(value_raw)
            if f is not None:
                result[field] = f
        else:
            result[field] = value_raw.strip()

    def _parse_amenities(
        self,
        parser: LexborHTMLParser,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Process .property-amenities <li> items.

        Format A — boolean (no <span>):
            <li>Storage space</li>
          → extra_features.storage_space = True

        Format B — key + value (with <span class="text-red">value</span>):
            <li>Distance from sea (m): <span class="text-red">800</span></li>
            <li>Glass Type: <span class="text-red">Double Glass</span></li>
          → extra_features.distance_from_sea = 800   (numeric coerced)
          → extra_features.glass_type = "Double Glass"

        Special: "Lot size: <span>450 m²</span>" goes into top-level
        land_size_sqm (Property column), not extras. Routed via the
        _AMENITY_LABEL_TO_FIELD map.
        """
        ul = parser.css_first(".property-amenities")
        if not ul:
            return

        for li in ul.css("li"):
            li_text_full = _normalize_text(li.text(strip=False))
            if not li_text_full:
                continue

            span = li.css_first("span")
            if span is None:
                # Format A — boolean amenity (whole li text IS the label)
                label = li_text_full
                slug = _slug(label)
                if slug and slug not in extra:
                    extra[slug] = True
                continue

            # Format B — key + value. Strip the span text to get the label.
            value_text = _normalize_text(span.text(strip=False))
            label_with_colon = li_text_full
            # The full text is "Label: value"; remove the value portion to
            # isolate the label. Rightmost occurrence avoids confusion if
            # the label itself contains the value substring (rare but safe).
            if value_text and value_text in label_with_colon:
                idx = label_with_colon.rfind(value_text)
                if idx >= 0:
                    label_with_colon = label_with_colon[:idx]
            label = label_with_colon.strip().rstrip(":").strip()
            label_key = label.lower()

            # Look up the canonical destination
            target = _AMENITY_LABEL_TO_FIELD.get(label_key)
            if target == "land_size_sqm":
                # Goes to top-level Property.land_size_sqm
                f = _to_float_sqm(value_text)
                if f is not None and result.get("land_size_sqm") is None:
                    result["land_size_sqm"] = f
                continue

            # Otherwise, target is the key to use in extras (or fall back
            # to the slugged label if not in the map).
            key = target or _slug(label)
            if not key:
                continue

            # Coerce value: try numeric (sqm or plain int), else verbatim
            num_sqm_match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:m²|sqm|m2)", value_text, re.I)
            if num_sqm_match:
                try:
                    extra[key] = float(num_sqm_match.group(1).replace(",", "."))
                    continue
                except ValueError:
                    pass

            num_eur_match = re.search(r"(\d[\d,]*)\s*€", value_text)
            if num_eur_match:
                try:
                    extra[key] = int(num_eur_match.group(1).replace(",", ""))
                    continue
                except ValueError:
                    pass

            # Plain integer (e.g. "Distance from sea (m): 800")
            plain_int = _to_int_simple(value_text) if value_text and value_text.strip().isdigit() else None
            if plain_int is not None:
                extra[key] = plain_int
                continue

            # Fallback: store the string verbatim
            extra[key] = value_text

    def _extract_description(
        self,
        parser: LexborHTMLParser,
    ) -> Optional[str]:
        """
        Pull the body description.

        Layout: a single <p> directly inside .property-content, AFTER the
        h1 and the price-area table. We collect ALL <p> children to be
        safe — some listings have 2-3 paragraphs.

        If the content is too thin (<50 chars) the caller falls back to
        og:description and the Quality Gate logs a warning.
        """
        content_node = parser.css_first(".property-content")
        if not content_node:
            return None

        paragraphs: List[str] = []
        for p in content_node.css("p"):
            txt = _normalize_text(p.text(strip=False))
            if not txt:
                continue
            # Skip the contact-CTA paragraph if it sneaks in (defensive —
            # currently it's in .property-bottom outside .property-content,
            # but template tweaks could move it).
            if "Request Additional Info" in txt:
                continue
            paragraphs.append(txt)

        if not paragraphs:
            return None
        return "\n\n".join(paragraphs)

    def _extract_coordinates(
        self,
        parser: LexborHTMLParser,
    ) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        """
        Read the .marker div's data-attrs.

            <div class="marker" data-lat="40.094227" data-lng="23.320971"
                                data-type="offset">

        Returns (lat, lng, gps_type) — gps_type is "offset" or "exact" as
        the site marked it. Returns (None, None, None) if the marker is
        missing or has unparseable attrs.
        """
        marker = parser.css_first(".marker[data-lat]")
        if not marker:
            return None, None, None

        lat_raw = marker.attributes.get("data-lat", "") or ""
        lng_raw = marker.attributes.get("data-lng", "") or ""
        gps_type = (marker.attributes.get("data-type") or "").strip().lower() or None

        try:
            lat = float(lat_raw)
            lng = float(lng_raw)
        except (ValueError, TypeError):
            return None, None, gps_type

        # Sanity check — Halkidiki bounding box (matches Greek Exclusive's regex)
        # Lat 39-41, Lng 22-24. If marker data is clearly out of region,
        # treat as unset (template bug, stale data, etc.)
        if not (39.0 <= lat <= 41.5 and 22.0 <= lng <= 25.0):
            logger.debug(
                f"[{self.source_domain}] marker coords {lat},{lng} "
                f"outside Halkidiki bbox — ignoring"
            )
            return None, None, gps_type

        # Normalise gps_type — only "offset" and "exact" are documented;
        # default to "offset" for any other value (privacy-safe default).
        if gps_type not in {"offset", "exact"}:
            gps_type = "offset"

        return lat, lng, gps_type

    def _extract_images(
        self,
        parser: LexborHTMLParser,
    ) -> List[str]:
        """
        Collect images from the Swiper gallery.

        Each slide is one of:
            <img src="https://m3.spitogatos.gr/335349770_1600x1200.jpg?v=...">
            <img data-src="https://m1.spitogatos.gr/..._1600x1200.jpg?v=...">

        The Swiper duplicates the first/last slide for looping — we
        de-duplicate by URL. All photos are on m1/m2/m3.spitogatos.gr at
        1600x1200 (full-quality template default); no size-suffix
        stripping needed.

        Filter rules:
          * Must be a spitogatos.gr CDN URL (rejects template chrome /
            logos / branding from d2dlxvmcs24r4u.cloudfront.net).
          * Skip SVG (no SVG in galleries on this template, but defensive).
          * Skip the agency-logo image (m2.spitogatos.gr/120212851_*).
        """
        seen: set = set()
        images: List[str] = []
        logo_re = re.compile(r"/120212851_", re.IGNORECASE)

        # Gallery container — we restrict to .swiper-container to avoid
        # picking up the header/footer logo imgs which also point at
        # m2.spitogatos.gr.
        gallery = parser.css_first(".swiper-container")
        nodes = gallery.css(".swiper-slide img") if gallery else parser.css(".swiper-slide img")

        for img in nodes:
            src = (
                img.attributes.get("src")
                or img.attributes.get("data-src")
                or ""
            ).strip()
            if not src:
                continue
            if not re.search(r"\.spitogatos\.gr/", src, re.IGNORECASE):
                continue
            if src.lower().endswith(".svg"):
                continue
            if logo_re.search(src):
                continue
            if src in seen:
                continue
            seen.add(src)
            images.append(src)

        return images
