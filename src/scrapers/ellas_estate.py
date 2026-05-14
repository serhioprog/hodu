"""
Ellas Estate scraper — ellasestate.com.

ellasestate.com runs WordPress + the "RealHomes" theme by Inspiry Themes
(design_ultra skin). Multiple clean data sources are present per property,
in descending order of cleanliness:

  1. JSON-LD <script type="application/ld+json"> RealEstateListing block
     — geo.lat/lng, offers.price/priceCurrency, address.streetAddress,
       offers.availability (For Sale flag), additionalProperty[]
       (Bedrooms / Bathrooms / Garages / Area Size / Year Built).
  2. Visible H1 + .rh-ultra-price + .rh-property-id panels — title,
     price, numeric site_property_id (also in body class postid-{N}).
  3. Overview panel — .rh_ultra_prop_card_meta_wrap .rh_ultra_prop_card__meta
     rows (label = .rh-ultra-meta-label, value = .figure). Includes
     bedrooms, bathrooms, year built, area/land size, distance to
     sea/airport, heating system, parking count + parking Yes/No, etc.
  4. Features taxonomy — .rh_property__features ul li a[href*="/property-feature/"]
     ~24 boolean amenities per property (Air Conditioning, Pool, ...).
  5. Description — .rh-content-wrapper .rh_content p, full body text
     (JSON-LD description is truncated to ~500 chars with '...').
  6. Gallery — .rh-ultra-property-slider a.rh-ultra-property-thumb,
     HD JPG/WEBP, 46 images in the sample. Slick adds clone duplicates
     for infinite-loop — de-duped by URL.
  7. og:* meta tags — fallbacks for description and cover image.

CANONICAL fetch_details PATTERN is followed (see _enrichment_mixin docs):
  1. Structured panel extraction  — JSON-LD + Overview + Features + title
  2. Description → og:description fallback
  3. Coords — JSON-LD geo (Leaflet inits via JS so setView regex MISSES on
     this site)
  4. Images → og:image fallback
  5. NLP fallback (self.extractor.analyze_full_text)
  6. LLM fallback — SKIPPED (structured + description give rich coverage)
  7. Quality Gate (≥ 50 chars)

Listing-page strategy
=====================

URL pattern (English UI is default):
  Page 1: /search-properties/?location[]=halkidiki&min-price=400000&max-price=5500000
  Page N: /search-properties/page/N/?location[]=halkidiki&min-price=400000&max-price=5500000

At min_price=400000 the filter yields ~128 properties / 7 pages of 20.
Cards anchored by .add-to-compare-span[data-property-id], which carries
ALL needed seed fields as data-* attributes:
    data-property-id, data-property-url, data-property-title,
    data-property-image
This makes the card seed extraction near-trivial and immune to layout
churn. Sidebar widgets on detail pages (Featured Properties) use the
SAME element type — defensive filter: any .add-to-compare-span whose
ancestor chain contains `.rh-sidebar` is skipped. (Listing pages do
not render that sidebar.)

The card additionally shows "Added: DD/MM/YYYY" → we capture this as
site_last_updated, the only ellasestate-specific source for it (detail
page has no published-date meta tag or visible stamp).

Key differences from prior scrapers
====================================

1) JSON-LD source of truth
   Unlike Estate+/Bootstrap sites (sithonia, GL), this site embeds a
   clean RealEstateListing JSON-LD with geo + price + currency +
   labeled metrics. We parse JSON-LD first, then top up from Overview
   panel for fields it omits (land_size_sqm, distance_from_sea, etc.).

2) Coordinates from JSON-LD, not setView()
   Leaflet map is initialised via separate JS that injects lat/lng
   from a JSON config — NOT inline in HTML. Static HTML scan finds
   no setView() call. JSON-LD geo.latitude/longitude carry precise
   coords (sub-meter precision).

3) Year Built sanity filter (observed admin bug)
   Sample property 94298 shows "Year Built: 20" (admin typed 20
   instead of 2020). Both Overview panel and JSON-LD reproduce this.
   We apply 100 < y < 2100 filter; out-of-range → silently None.
   NLP fallback may recover from description text (e.g. "built in
   2020").

4) Two "Parking" rows in Overview panel
   Property 94298 has BOTH `<meta>Parking</meta><figure>2</figure>`
   (count of parking spots) AND `<meta class="...parking">Parking
   </meta><figure>Yes</figure>` (boolean). We disambiguate by value:
     numeric value → extra_features.parking_count
     yes/no value  → extra_features.parking
   NLP semantic dedup map prevents NLP from re-adding `parking: True`
   when the structural pass already wrote `parking_count`.

5) No LLM fallback
   Same reasoning as sithonia + GL: rich Overview + Features +
   description body already give 25-30 extra_features per property.
   LLM cost not justified for the marginal additional fields.

6) location_raw from H1 title
   Title format: "For Sale – Villa 280 sq.m. in Kallithea, Halkidiki".
   Trailing "{Locality}, Halkidiki" extracted via regex. Always appends
   " Halkidiki" to satisfy the global HALKIDIKI_REGIONS_WHITELIST when
   the title is malformed. Falls back to JSON-LD address.streetAddress
   if title regex misses (rare — Greek-script characters).

7) Category from property-type taxonomy
   The .rh-ultra-type anchor on the title block exposes the canonical
   property type (Villa, Apartment, Maisonette, Studio, Detached House,
   etc.). URL slug at /property-type/<slug>/ disambiguates the rare
   case where text differs from slug.
"""
from __future__ import annotations

import asyncio
import html
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.models.schemas import PropertyTemplate


# =============================================================
# Mappings — pure data, no logic
# =============================================================

# Overview-panel labels (after .strip().lower()) routed to first-class
# Property columns. Labels NOT in this map go into extra_features.
# 'parking' / 'year built' / 'area size' / 'land size' get special handling
# (sanity caps + numeric/bool disambiguation).
_LABEL_TO_PROPERTY_COLUMN: Dict[str, str] = {
    "bedrooms":   "bedrooms",
    "bathrooms":  "bathrooms",
    "year built": "year_built",
    "area size":  "size_sqm",
    "land size":  "land_size_sqm",
}

# JSON-LD `additionalProperty[].name` → same Property column mapping.
# 'Garages' appears in JSON-LD only (not labelled in Overview — site
# labels it "Parking" instead, hence the two-Parking-rows quirk).
_JSONLD_PROPERTY_NAME_TO_COLUMN: Dict[str, str] = {
    "bedrooms":   "bedrooms",
    "bathrooms":  "bathrooms",
    "year built": "year_built",
    "area size":  "size_sqm",
    "land size":  "land_size_sqm",
}

# Overview labels we always DROP — already captured upstream as
# site_property_id (from URL slug + body class postid-{N}).
_OVERVIEW_DROP_LABELS: set = {
    "property id",
}

# Yes/No normalisation (English-only — site UI is English by default).
_YES_VALUES = {"yes", "y", "true", "1"}
_NO_VALUES  = {"no",  "n", "false", "0"}

# Halkidiki bounding box — sanity check for GPS coords from JSON-LD.
# Anything outside is treated as malformed (defensive — shouldn't happen
# given JSON-LD is admin-curated).
_HALKIDIKI_LAT_RANGE = (39.0, 41.0)
_HALKIDIKI_LNG_RANGE = (22.0, 24.0)

# Year sanity bounds. "20" (admin typo for 2020) → rejected. Same bounds
# as sithonia's _write_column.
_YEAR_MIN = 1900
_YEAR_MAX = 2100

# Pagination safety cap. Actual is ~7 pages at min_price=400000; 15
# protects against pagination loops or HTML breaks.
_PAGE_SAFETY_CAP = 15

# Property-type URL slug → canonical category text. Site renders the
# anchor text in English, but the slug is more stable across the site
# (and survives WPML language-switch quirks if/when the site enables
# multilingual).
_TYPE_SLUG_TO_CATEGORY: Dict[str, str] = {
    "villa":               "Villa",
    "apartment":           "Apartment",
    "apartment-studio":    "Studio",
    "studio":              "Studio",
    "maisonette":          "Maisonette",
    "duplex":              "Duplex",
    "detached-house":      "Detached House",
    "house":               "House",
    "land":                "Land",
    "plot":                "Land",
    "parcel":              "Land",
    "building":            "Building",
    "hotel":               "Hotel",
    "office":              "Office",
    "shop":                "Shop",
    "storage":             "Storage",
    "business":            "Business",
    "commercial-property": "Commercial",
    "complex":             "Complex",
    "island":              "Island",
    "other":               "Other",
}


# =============================================================
# Helpers — pure functions, no scraper state
# =============================================================

def _slug(label: str) -> str:
    """
    Convert a free-form English HTML label into a stable JSON key.

      "Distance from sea"               -> "distance_from_sea"
      "Number of rooms"                 -> "number_of_rooms"
      "Number of floors in the building"-> "number_of_floors_in_the_building"
      "Heating System"                  -> "heating_system"
    """
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_int_simple(text: Optional[str]) -> Optional[int]:
    """Extract first integer from a string. '3', 'Bedrooms: 3' both -> 3."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: Optional[str]) -> Optional[float]:
    """
    Parse sqm strings tolerantly.

      "280"            -> 280.0
      "280 m²"         -> 280.0
      "Area: 280"      -> 280.0
      "113,7 m²"       -> 113.7
      "10,500 m²"      -> 10500.0   (thousands sep — heuristic: comma+3 = thousands)
    """
    if not text:
        return None
    # Try to grab the first numeric token (may include ',' or '.')
    m = re.search(r"\d[\d.,]*", text)
    if not m:
        return None
    raw = m.group(0)
    # Heuristic: a trailing ",3-digit" group is a thousands separator,
    # not a decimal separator. "10,500" → 10500; "113,7" → 113.7
    if "," in raw and "." not in raw:
        last_comma = raw.rfind(",")
        tail = raw[last_comma + 1:]
        if len(tail) == 3 and tail.isdigit():
            raw = raw.replace(",", "")
        else:
            raw = raw.replace(",", ".")
    else:
        # both . and , (e.g. EU thousands "1.500,5") → '.' is thousands
        if "," in raw and "." in raw:
            raw = raw.replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _to_int_year_safe(text: Optional[str]) -> Optional[int]:
    """
    Extract a build-year integer with sanity bounds. Rejects '20'
    (admin typo for 2020) and 'Under Construction'.
    """
    n = _to_int_simple(text)
    if n is None:
        return None
    if _YEAR_MIN < n < _YEAR_MAX:
        return n
    return None


def _normalise_yes_no(text: Optional[str]) -> Optional[bool]:
    """Normalise 'Yes'/'No' to bool. Returns None for anything else."""
    if not text:
        return None
    s = text.strip().lower()
    if s in _YES_VALUES:
        return True
    if s in _NO_VALUES:
        return False
    return None


def _extract_locality_from_title(title: str) -> Optional[str]:
    """
    Pull the trailing "{Locality}, Halkidiki" out of an English title.

      "For Sale – Villa 280 sq.m. in Kallithea, Halkidiki"
                                       -> "Kallithea"
      "For Sale – Maisonette 165 sq.m. in Elani, Halkidiki"
                                       -> "Elani"
      "For sale — Detached house of 110 sq.m. in Potidaia, Halkidiki"
                                       -> "Potidaia"
      "Villa for sale in Pefkochori"   -> None  (no Halkidiki suffix)
    """
    if not title:
        return None
    # Look for "<locality>, Halkidiki" (also '.Halkidiki' tail)
    m = re.search(r"in\s+([A-Za-z][\w\s\-.']*?),\s*Halkidiki", title, re.IGNORECASE)
    if not m:
        return None
    locality = m.group(1).strip().rstrip(".")
    return locality or None


def _locality_from_url_slug(url: str) -> Optional[str]:
    """
    Extract a locality name from a property URL when the title regex
    fails. RealHomes encodes the locality slug inside the URL slug —
    last segment of /property/<slug>/.

    URL slug pattern: "<status>-<type>-<size>-sq-m-in-<locality>[-halkidiki]"

      /property/for-sale-villa-280-sq-m-in-kallithea-halkidiki/
        → "Kallithea"
      /property/for-sale-detached-house270sq-m-in-ormos-panagias-halkidiki/
        → "Ormos Panagias"
      /property/villa-for-sale-240-sq-m-in-pefkochori/
        → "Pefkochori"
      /property/for-sale-detached-house-190-sq-m-in-agia-paraskeui-loutra/
        → "Agia Paraskeui Loutra"

    Returns the locality as Title-Case English, or None if the slug
    doesn't match the expected "<...>-in-<locality>" tail.
    """
    if not url:
        return None
    # Last non-empty path segment
    slug = url.rstrip("/").rsplit("/", 1)[-1].lower()
    if not slug:
        return None
    # Strip trailing region suffixes (Halkidiki, Chalkidiki, Sithonia)
    slug = re.sub(
        r"-(?:halkidiki|chalkidiki|sithonia)(?:-\d+)?$",
        "",
        slug,
    )
    # "-in-<locality>" tail
    m = re.search(r"-in-([a-z][a-z0-9\-]*)$", slug)
    if not m:
        return None
    locality_slug = m.group(1)
    return locality_slug.replace("-", " ").title()


def _build_location_raw(title: str, url: Optional[str] = None) -> str:
    """
    Build a clean English location_raw for downstream whitelisting.
    Always appends ", Halkidiki" so the global HALKIDIKI_REGIONS_WHITELIST
    matches (case-insensitive substring).

    Strategy:
      1. Title regex   "in {Locality}, Halkidiki" (highest precision)
      2. URL slug      "in-{locality-slug}" (covers titles without comma)
      3. Bare fallback "Halkidiki"

    Returns:
      "Kallithea, Halkidiki"  — when locality extracted
      "Halkidiki"             — fallback (still whitelist-safe)
    """
    locality = _extract_locality_from_title(title)
    if not locality and url:
        locality = _locality_from_url_slug(url)
    if locality:
        return f"{locality}, Halkidiki"
    return "Halkidiki"


def _type_slug_from_anchor(href: str) -> Optional[str]:
    """
    Extract the property-type slug from a /property-type/<slug>/ URL.
    Returns the slug in lowercase, no trailing slash.
    """
    if not href:
        return None
    m = re.search(r"/property-type/([^/?#]+)", href)
    return m.group(1).lower() if m else None


# Ancestor classes that signal "this card belongs to a side widget,
# NOT the main search-results grid". Used by _in_excluded_widget to
# drop false-positive .add-to-compare-span elements during listing
# pagination.
#
# Confirmed on detail-page HTML (94298 sample) — same widgets appear
# on listing pages:
#   rh-sidebar / sidebar / rh-property-sidebar
#       → Featured Properties widget in <aside>
#   rh_property__similar_properties
#       → "Similar Properties" section at the bottom of detail pages
#   ere-ultra-side-properties / ere-properties-slide
#       → Owl-carousel widget rendered above / below the main grid
#         (the Estate Real Estate plugin's auxiliary widget)
#   rh-ultra-property-slider-wrapper / rh-ultra-property-thumb-container
#       → The PAGE'S OWN main-banner compare button on detail pages —
#         identical pattern to a card. Filtering it out keeps detail-page
#         smoke tests honest; listing pages never render this container.
_EXCLUDED_ANCESTOR_CLASSES = (
    "rh-sidebar",
    "sidebar",
    "rh-property-sidebar",
    "rh_property__similar_properties",
    "ere-ultra-side-properties",
    "ere-properties-slide",
    "rh-ultra-property-slider-wrapper",
    "rh-ultra-property-thumb-container",
)


def _in_excluded_widget(node: Optional[LexborNode]) -> bool:
    """
    Walk up the parent chain. Return True if any ancestor's class list
    intersects _EXCLUDED_ANCESTOR_CLASSES — meaning the card is part
    of an auxiliary widget (sidebar / similar / carousel) rather than
    the main search-results grid we want.

    Cheap (O(depth × #ancestor-classes)) and safe to call on every
    candidate during listing pagination.
    """
    cur = node
    while cur is not None:
        cls = (cur.attributes.get("class") or "") if hasattr(cur, "attributes") else ""
        if cls:
            classes = cls.split()
            if any(c in classes for c in _EXCLUDED_ANCESTOR_CLASSES):
                return True
        cur = getattr(cur, "parent", None)
    return False


def _parse_date_ddmmyyyy(text: str) -> Optional[str]:
    """
    Listing card has "Added: DD/MM/YYYY". Return the date string as-is
    (PropertyTemplate.site_last_updated is Optional[str], no parsing
    needed). Returns None if format mismatches.
    """
    if not text:
        return None
    m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", text)
    if not m:
        return None
    return m.group(1)


# =============================================================
# Scraper
# =============================================================

class EllasEstateScraper(EnrichmentMixin, BaseScraper):
    """
    Ellas Estate (ellasestate.com) — WordPress + RealHomes theme.

    At min_price=400000 with location=halkidiki: ~128 listings across
    7 pages of 20 cards each. Pure For-Sale focus (filter rules out
    most rentals via the price floor; status is captured per listing).
    """

    # Override NLP-fillable columns. We trust JSON-LD + Overview as
    # primary source; NLP only fills in narrow gaps from the description.
    # Category is EXCLUDED — site supplies it cleanly via the property-type
    # anchor. Letting NLP overwrite would silently regress "Detached House"
    # to NLP's coarser "Land/Plot" guess on stripped descriptions.
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )

    # Semantic dedup map — extends EnrichmentMixin defaults with the
    # ellasestate-specific slug names produced by the /property-feature/
    # taxonomy and the Overview panel. Without these additions, NLP
    # re-detects the same amenity under a different canonical key:
    #
    #   structural slug          NLP canonical (would re-add)
    #   ─────────────────────    ────────────────────────────
    #   personal_swimming_pool → swimming_pool
    #   barbecue_area          → bbq
    #   sauna_jacuzzi, jacuzzi → jacuzzi_sauna  (NLP joins word-order
    #                                            agnostically — same concept)
    #   parking_count          → parking          (numeric → boolean dup)
    #   heating_system         → heating          (Overview catch-all
    #                                            stores 'With heating' as
    #                                            string under this slug;
    #                                            blocks NLP heating=True)
    #
    # We copy the parent class's dict explicitly (Python class-attr
    # override is REPLACE, not MERGE). If the mixin's defaults grow
    # later, only the new key needs adding here.
    _NLP_TO_STRUCTURAL = {
        "swimming_pool":    {"pool", "communal_pool", "private_pool",
                             "personal_swimming_pool"},
        "sea_view":         {"view", "sea", "view_sea"},
        "parking":          {"garage", "parking_spot", "outdoor_garage",
                             "parking_count"},
        "alarm_system":     {"alarm"},
        "storage_room":     {"storage"},
        "air_conditioning": {"a_c", "ac"},
        "heating":          {"central_heating", "heating_system"},
        "fireplace":        {"fire_place"},
        "balcony":          {"balconies"},
        "terrace":          {"terraces"},
        # ─── ellasestate-specific (taxonomy slugs)
        "bbq":              {"barbecue_area"},
        "jacuzzi_sauna":    {"sauna_jacuzzi", "jacuzzi"},
    }

    BASE_URL = "https://www.ellasestate.com"

    def __init__(self):
        super().__init__()
        self.source_domain = "ellasestate.com"

    async def fetch_listings(self):
        """Backwards-compatible entry point for the dispatcher."""
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # URL builders
    # ---------------------------------------------------------------

    def _build_list_url(self, page: int, min_price: int) -> str:
        """
        WordPress pagination format:
          Page 1: /search-properties/?location[]=halkidiki&min-price=...
          Page N: /search-properties/page/N/?location[]=halkidiki&...

        `location[]` is the array-bracket form; WP accepts both bare `[]`
        and indexed `[0]`. We use bare brackets for readability — curl_cffi
        URL-encodes them transparently.
        """
        query = (
            f"?location%5B%5D=halkidiki"
            f"&min-price={min_price}"
            f"&max-price=5500000"
        )
        if page <= 1:
            return f"{self.BASE_URL}/search-properties/{query}"
        return f"{self.BASE_URL}/search-properties/page/{page}/{query}"

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs
    # ---------------------------------------------------------------

    async def collect_urls(self, min_price: int = 400000) -> List[PropertyTemplate]:
        """
        Walk paginated listing. Stop when:
          - no .add-to-compare-span found on the page (end of pagination)
          - PAGE_SAFETY_CAP exceeded (defensive)

        Each card yields a seed PropertyTemplate with site_property_id,
        full detail URL, location_raw (derived from title), and
        site_last_updated (from "Added: DD/MM/YYYY" on the card).
        """
        all_properties: List[PropertyTemplate] = []
        seen_ids: set = set()
        page = 1

        while page <= _PAGE_SAFETY_CAP:
            url = self._build_list_url(page, min_price)
            logger.info(f"[{self.source_domain}] Парсинг страницы {page}...")

            try:
                response = await self.client.get(url)
                parser = LexborHTMLParser(response.text)

                # Anchor on .add-to-compare-span — guaranteed unique per card
                # and contains all seed fields in data-* attributes.
                spans = parser.css(".add-to-compare-span[data-property-id]")
                # Defensive: drop side-widget cards (sidebar / similar
                # properties / owl carousel) — see _EXCLUDED_ANCESTOR_CLASSES.
                spans = [s for s in spans if not _in_excluded_widget(s)]

                if not spans:
                    logger.info(
                        f"[{self.source_domain}] нет карточек на странице {page} "
                        f"— конец пагинации"
                    )
                    break

                page_count = 0
                for span in spans:
                    try:
                        prop = self._parse_card(span)
                        if prop and prop.site_property_id not in seen_ids:
                            all_properties.append(prop)
                            seen_ids.add(prop.site_property_id)
                            page_count += 1
                    except Exception as e:
                        logger.error(
                            f"[{self.source_domain}] ошибка парсинга карточки: {e}"
                        )

                logger.info(
                    f"[{self.source_domain}] страница {page}: "
                    f"собрано {page_count} объектов"
                )

                # If the page was rendered but every card was a duplicate,
                # we're probably past the last real page (WP some times
                # re-renders page 1 for out-of-range page numbers).
                if page_count == 0 and page > 1:
                    logger.info(
                        f"[{self.source_domain}] страница {page} — все дубликаты, "
                        f"останавливаюсь"
                    )
                    break

                await asyncio.sleep(2)
                page += 1

            except Exception as e:
                logger.error(
                    f"[{self.source_domain}] критическая ошибка на странице {page}: {e}"
                )
                break

        logger.info(
            f"[{self.source_domain}] Phase 1 завершён: "
            f"{len(all_properties)} URLs за {page - 1} стр."
        )
        return all_properties

    def _parse_card(self, span: LexborNode) -> Optional[PropertyTemplate]:
        """
        Build a seed PropertyTemplate from one .add-to-compare-span.

        Data sources on the card:
          .add-to-compare-span data-*  → id, url, title, cover image
          (walk up to .rh-ultra-property-card-2 wrapper)
          .rh-ultra-status                            → For Sale / For Rent
          a[href*="/property-type/"]                  → category
          .rh_prop_card__price_ultra / .rh-ultra-price → price (string)
          .rh-ultra-year-built                        → year built (visible)
          .rh-ultra-property-card-date / text         → "Added: DD/MM/YYYY"

        Returns None if the essential fields (site_id, url) are missing.
        """
        attrs = span.attributes
        site_id = (attrs.get("data-property-id") or "").strip()
        detail_url = (attrs.get("data-property-url") or "").strip()
        title = html.unescape(attrs.get("data-property-title") or "").strip()
        cover_img = (attrs.get("data-property-image") or "").strip() or None

        if not site_id or not detail_url:
            return None

        # Walk up to the enclosing card wrapper. Fall back to the parent
        # if the card class isn't found — defensive against template
        # changes. We match ANY class starting with "rh-ultra-property-card"
        # to catch bare `rh-ultra-property-card` (listing page + similar
        # properties section) AS WELL AS the suffixed -1 / -2 variants
        # (Owl carousel + grid). Without bare match, walk-up reaches the
        # page root → category lookup picks up filter-sidebar anchors.
        card = span
        walk_succeeded = False
        for _ in range(10):
            cls = (card.attributes.get("class") or "")
            if cls:
                cls_list = cls.split()
                if any(c.startswith("rh-ultra-property-card") for c in cls_list):
                    walk_succeeded = True
                    break
                if any(c in cls_list for c in (
                        "rh-ultra-grid-card",
                        "rh-ultra-list-card")):
                    walk_succeeded = True
                    break
            parent = getattr(card, "parent", None)
            if parent is None:
                break
            card = parent

        # ── Category from /property-type/ anchor (most reliable source)
        # DEFENSIVE: if walk-up failed (template variant we don't know
        # about), `card` is somewhere near the page root and a CSS scan
        # would hit filter-sidebar links → wrong category propagates
        # downstream because _parse_title_block won't overwrite a non-None
        # card seed. Safer to skip the seed category entirely and let
        # fetch_details fill it from JSON-LD / title block.
        if walk_succeeded:
            category = self._extract_category_from_card(card)
        else:
            category = None
            logger.debug(
                f"[{self.source_domain}] card walk-up failed for id={site_id} "
                f"— category will be filled by fetch_details"
            )

        # ── Visible price string (parsed by PropertyTemplate.clean_price)
        price_text = self._extract_price_from_card(card)

        # ── Date added (only available on listing card, not detail page)
        date_added = self._extract_date_from_card(card)

        # ── Location from title + URL fallback — always English on this site
        location_raw = _build_location_raw(title, url=detail_url)

        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=detail_url,
            price=price_text,
            location_raw=location_raw,
            category=category,
            site_last_updated=date_added,
            images=[cover_img] if cover_img else [],
        )

    def _extract_category_from_card(self, card: LexborNode) -> Optional[str]:
        """
        First /property-type/ anchor inside the card. Cards may show
        multiple type tags (e.g. "Maisonette, Villa" for combo listings)
        — we take the FIRST one to keep category atomic.
        """
        for a in card.css('a[href*="/property-type/"]'):
            slug = _type_slug_from_anchor(a.attributes.get("href") or "")
            if not slug:
                continue
            mapped = _TYPE_SLUG_TO_CATEGORY.get(slug)
            if mapped:
                return mapped
            # Unknown slug — keep the visible text as-is
            txt = a.text(strip=True)
            return txt or None
        return None

    @staticmethod
    def _extract_price_from_card(card: LexborNode) -> Optional[str]:
        """
        Price text from the price block. RealHomes renders one of:
          <p class="rh_prop_card__price_ultra">700,000€</p>
          <p class="rh_prop_card__price_ultra">Available upon request</p>
          <span class="rh-ultra-price">700,000€</span>
        We return the raw text; PropertyTemplate.clean_price handles
        parsing or null-out for "Available upon request".
        """
        for sel in ("p.rh_prop_card__price_ultra",
                    "span.rh-ultra-price",
                    ".rh-ultra-price"):
            node = card.css_first(sel)
            if not node:
                continue
            txt = node.text(separator=" ", strip=True)
            if txt:
                return txt
        return None

    @staticmethod
    def _extract_date_from_card(card: LexborNode) -> Optional[str]:
        """
        Listing card shows "Added: DD/MM/YYYY" somewhere in the bottom
        meta row. RealHomes class is .rh-ultra-property-card-date but
        can vary — we fall back to a text scan of the card.
        """
        # Try the dedicated class first
        node = card.css_first(".rh-ultra-property-card-date")
        if node:
            d = _parse_date_ddmmyyyy(node.text(strip=True))
            if d:
                return d

        # Fallback: scan whole card text for "Added: dd/mm/yyyy"
        txt = card.text(separator=" ", strip=True)
        m = re.search(r"Added[:\s]+(\d{1,2}/\d{1,2}/\d{4})", txt)
        if m:
            return m.group(1)
        return None

    # ---------------------------------------------------------------
    # PHASE 2 — fetch full details
    # ---------------------------------------------------------------

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Canonical 7-step fetch_details pattern.
        """
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)
            raw_html = response.text

            data: Dict[str, Any] = {
                "description":       "",
                "price":             None,
                "size_sqm":          None,
                "land_size_sqm":     None,
                "bedrooms":          None,
                "bathrooms":         None,
                "year_built":        None,
                "area":              None,
                "subarea":           None,
                "category":          None,
                "levels":            None,
                "site_last_updated": None,
                "latitude":          None,
                "longitude":         None,
                "images":            [],
                "extra_features":    {},
            }

            # ───────────────────────────────────────────────────────
            # STEP 1a — JSON-LD (RealEstateListing): primary structured
            # source. Geo + price + currency + 5 labeled metrics.
            # ───────────────────────────────────────────────────────
            self._parse_json_ld(raw_html, data)

            # ───────────────────────────────────────────────────────
            # STEP 1b — Title block: status (For Sale/Rent), property
            # type tag (category), H1 title. Title block is rendered
            # twice on the page (top banner + content header) — both
            # carry the same data; we use the first occurrence.
            # ───────────────────────────────────────────────────────
            self._parse_title_block(parser, data)

            # ───────────────────────────────────────────────────────
            # STEP 1c — Overview panel: 12 metric rows. Tops up fields
            # JSON-LD omits (land_size, distance_from_sea, heating, ...).
            # ───────────────────────────────────────────────────────
            self._parse_overview_panel(parser, data)

            # ───────────────────────────────────────────────────────
            # STEP 1d — Features taxonomy: ~24 boolean amenities via
            # /property-feature/<slug>/ anchors. Each becomes an
            # extra_features[<slug>] = True.
            # ───────────────────────────────────────────────────────
            self._parse_features(parser, data)

            # ───────────────────────────────────────────────────────
            # STEP 2 — Description: full <p> body → og:description
            # fallback. JSON-LD's description is truncated with '...'
            # so we re-extract from the body block.
            # ───────────────────────────────────────────────────────
            data["description"] = self._parse_description(parser)

            # ───────────────────────────────────────────────────────
            # STEP 3 — Coords already filled by JSON-LD (STEP 1a).
            # No setView regex on this site (Leaflet inits via JS
            # config that's not in static HTML). Defensive: warn if
            # JSON-LD missed lat/lng.
            # ───────────────────────────────────────────────────────
            if data.get("latitude") is None or data.get("longitude") is None:
                logger.debug(
                    f"[{self.source_domain}] no GPS coords in JSON-LD for {url}"
                )

            # ───────────────────────────────────────────────────────
            # STEP 4 — Images: slick-slider gallery → og:image fallback
            # ───────────────────────────────────────────────────────
            data["images"] = self._parse_images(parser)

            # ───────────────────────────────────────────────────────
            # STEP 5 — NLP fallback over description. Fills missing
            # primary columns (size, bedrooms, year) and amenities
            # not already in extra_features. Semantic dedup against
            # structural slugs via _NLP_TO_STRUCTURAL.
            # ───────────────────────────────────────────────────────
            self._apply_nlp_fallback(data)

            # ───────────────────────────────────────────────────────
            # STEP 6 — LLM fallback: SKIPPED (see module docstring §5)
            # ───────────────────────────────────────────────────────

            # ───────────────────────────────────────────────────────
            # STEP 7 — Quality Gate (log-only; daily_sync owns retry)
            # ───────────────────────────────────────────────────────
            if not self._passes_quality_gate(data.get("description")):
                logger.warning(
                    f"[{self.source_domain}] description below quality gate "
                    f"for {url}"
                )

            # Drop None values so card seed isn't clobbered. Empty
            # list/dict/string are KEPT (signal "parsed but empty").
            return {k: v for k, v in data.items() if v is not None}

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] ошибка fetch_details для {url}: {e}"
            )
            return {}

    # ---------------------------------------------------------------
    # Phase 2 helpers — JSON-LD
    # ---------------------------------------------------------------

    def _parse_json_ld(self, raw_html: str, data: Dict[str, Any]) -> None:
        """
        Find <script type="application/ld+json"> blocks, parse each,
        and apply the RealEstateListing block (if present) to `data`.

        Multiple LD+JSON scripts may exist on the page (e.g. there's
        also a RealEstateAgent block) — we walk all of them and pick
        the first one with @type == "RealEstateListing".

        Fills:
          - latitude / longitude  (with Halkidiki bbox sanity check)
          - price                 (offers.price → int)
          - subarea               (address.streetAddress → trimmed Greek text)
          - additionalProperty[]  → bedrooms/bathrooms/year_built/size_sqm
        """
        # Find all <script type="application/ld+json"> blocks via regex
        # — selectolax struggles with multi-line JSON inside <script>.
        pattern = re.compile(
            r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            re.DOTALL | re.IGNORECASE,
        )
        for match in pattern.finditer(raw_html):
            body = match.group(1).strip()
            if not body:
                continue
            try:
                obj = json.loads(body)
            except json.JSONDecodeError:
                continue

            # Some sites wrap multiple LD nodes in @graph
            candidates = obj if isinstance(obj, list) else [obj]
            if isinstance(obj, dict) and "@graph" in obj:
                candidates = obj["@graph"]

            for node in candidates:
                if not isinstance(node, dict):
                    continue
                if node.get("@type") != "RealEstateListing":
                    continue
                self._apply_json_ld_listing(node, data)
                return  # first match wins

    def _apply_json_ld_listing(
        self,
        node: Dict[str, Any],
        data: Dict[str, Any],
    ) -> None:
        """Apply one RealEstateListing JSON-LD object to `data`."""
        # ── geo
        geo = node.get("geo") or {}
        try:
            lat = float(geo.get("latitude"))
            lng = float(geo.get("longitude"))
            if (_HALKIDIKI_LAT_RANGE[0] <= lat <= _HALKIDIKI_LAT_RANGE[1]
                    and _HALKIDIKI_LNG_RANGE[0] <= lng <= _HALKIDIKI_LNG_RANGE[1]):
                data["latitude"] = lat
                data["longitude"] = lng
        except (TypeError, ValueError):
            pass

        # ── offers.price
        offers = node.get("offers") or {}
        price_raw = offers.get("price")
        if price_raw not in (None, ""):
            price = self._to_int_euro_safe(str(price_raw))
            if price is not None:
                data["price"] = price

        # ── offers.availability — flag For Sale/Rent in extra_features
        avail = (offers.get("availability") or "").lower()
        if avail:
            extra = data.setdefault("extra_features", {})
            if "forsale" in avail.replace("_", "").replace("/", ""):
                extra["listing_status"] = "for_sale"
            elif "forrent" in avail.replace("_", "").replace("/", ""):
                extra["listing_status"] = "for_rent"

        # ── address.streetAddress — usually Greek script, may include
        # municipality + prefecture chain. We store the whole string in
        # extra_features for downstream geo-matching if needed (not used
        # as primary subarea since it's not English).
        addr = node.get("address") or {}
        street = (addr.get("streetAddress") or "").strip()
        if street:
            extra = data.setdefault("extra_features", {})
            extra["address_streetline"] = street

        # ── additionalProperty[] — labeled metrics
        for prop in node.get("additionalProperty") or []:
            if not isinstance(prop, dict):
                continue
            name = (prop.get("name") or "").strip().lower()
            value = prop.get("value")
            if value in (None, ""):
                continue

            column = _JSONLD_PROPERTY_NAME_TO_COLUMN.get(name)
            if column is None:
                # Garages → parking_count (only place garages appears)
                if name == "garages":
                    n = _to_int_simple(str(value))
                    if n is not None:
                        data.setdefault("extra_features", {})["parking_count"] = n
                continue

            self._write_column(column, str(value), data)

    # ---------------------------------------------------------------
    # Phase 2 helpers — Title block (status + type)
    # ---------------------------------------------------------------

    def _parse_title_block(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Read the first .rh-ultra-property-tags.rh-property-title block.

        Provides:
          - extra_features.listing_status  ('for_sale' / 'for_rent')
          - category                       ('Villa', 'Apartment', ...)

        Defensive: if category is already set (e.g. from a card seed
        upstream), we DO NOT overwrite — card-derived category is
        authoritative per design.
        """
        block = parser.css_first(
            ".rh-ultra-property-tags.rh-property-title"
        )
        if not block:
            return

        # Status anchor (URL slug is the canonical signal)
        for a in block.css('a[href*="/property-status/"]'):
            href = a.attributes.get("href") or ""
            m = re.search(r"/property-status/([^/?#]+)", href)
            if not m:
                continue
            slug = m.group(1).lower()
            status = None
            if slug in ("for-sale", "sale"):
                status = "for_sale"
            elif slug in ("for-rent", "rent"):
                status = "for_rent"
            elif "sold" in slug:
                status = "sold"
            if status:
                data.setdefault("extra_features", {})["listing_status"] = status
            break

        # Type anchor → category (only fill if not already set by card)
        if data.get("category") is None:
            for a in block.css('a[href*="/property-type/"]'):
                slug = _type_slug_from_anchor(a.attributes.get("href") or "")
                if not slug:
                    continue
                mapped = _TYPE_SLUG_TO_CATEGORY.get(slug)
                if mapped:
                    data["category"] = mapped
                else:
                    txt = a.text(strip=True)
                    if txt:
                        data["category"] = txt
                break

    # ---------------------------------------------------------------
    # Phase 2 helpers — Overview panel
    # ---------------------------------------------------------------

    def _parse_overview_panel(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Walk .rh_ultra_prop_card_meta_wrap .rh_ultra_prop_card__meta rows.

        Each row has shape:
          <div class="rh_ultra_prop_card__meta [modifier]">
            <div class="rh_ultra_meta_icon_wrapper">
              <span class="rh-ultra-meta-label">{label}</span>
              <div class="rh-ultra-meta-icon-wrapper">
                <span class="rh_ultra_meta_icon">...icon...</span>
                <span class="rh_ultra_meta_box">
                  <span class="figure">{value}</span>
                  [<span class="label">m²</span>]   # area/land only
                </span>
              </div>
            </div>
          </div>

        Some rows nest `.figure` directly under `.rh-ultra-meta-icon-wrapper`
        (no inner `.rh_ultra_meta_box`) — handled by selecting on '.figure'
        from the row root.
        """
        wrap = parser.css_first(".rh_ultra_prop_card_meta_wrap")
        if not wrap:
            return

        for row in wrap.css(".rh_ultra_prop_card__meta"):
            label_node = row.css_first(".rh-ultra-meta-label")
            figure_node = row.css_first(".figure")
            if not label_node or not figure_node:
                continue

            label = label_node.text(separator=" ", strip=True)
            value = figure_node.text(separator=" ", strip=True)
            if not label:
                continue

            self._route_overview_row(label, value, row, data)

    def _route_overview_row(
        self,
        label: str,
        value: str,
        row: LexborNode,
        data: Dict[str, Any],
    ) -> None:
        """
        Route one Overview-panel row into either:
          * dropped (Property ID — already known)
          * Property column (typed via _write_column)
          * extra_features (typed Yes/No/int/string)

        Special case: "Parking" appears TWICE — once as count (numeric),
        once as Yes/No (with .parking modifier on the row). Disambiguated
        by value content here, no class lookup needed.
        """
        label_lower = label.strip().lower()
        slug = _slug(label)
        if not slug:
            return

        # ── Explicit drops
        if label_lower in _OVERVIEW_DROP_LABELS:
            return

        # ── Parking — two rows per property; route by modifier class
        # then by value type. The boolean ("Yes"/"No") row carries the
        # `.parking` modifier on the .rh_ultra_prop_card__meta wrapper;
        # the count row (numeric value, e.g. "2") does NOT have any
        # modifier. We trust the modifier-class signal first because
        # `_normalise_yes_no("1")` would otherwise misclassify a single
        # parking spot as parking=True. Final fallback: value pattern.
        if label_lower == "parking":
            row_classes = (row.attributes.get("class") or "").split()
            is_boolean_row = "parking" in row_classes
            yn = _normalise_yes_no(value)
            n_match = re.fullmatch(r"\d+", (value or "").strip())
            extra = data.setdefault("extra_features", {})

            if is_boolean_row and yn is not None:
                extra["parking"] = yn
            elif n_match:
                # setdefault → JSON-LD's Garages count wins on conflict
                extra.setdefault("parking_count", int(value.strip()))
            elif yn is not None:
                # Defensive: a future page with Yes/No row but no
                # `.parking` modifier still routes correctly.
                extra["parking"] = yn
            return

        # ── First-class Property column
        column = _LABEL_TO_PROPERTY_COLUMN.get(label_lower)
        if column is not None:
            self._write_column(column, value, data)
            return

        # ── extra_features (catch-all)
        if not value:
            return  # ignore empty values for unknown labels

        # Yes/No → bool (Furnished, Sea view, Mountain view, ...)
        yn = _normalise_yes_no(value)
        if yn is not None:
            data.setdefault("extra_features", {})[slug] = yn
            return

        # Bare integer (Number of rooms, Number of floors) → int
        # — only if the value is purely digits + optional unit suffix
        n = _to_int_simple(value)
        if n is not None and re.fullmatch(r"\d+\s*[a-zA-Z²]*", value.strip()):
            # If unit present (e.g. "1000 m", "70 km"), keep as string;
            # if just a number, store as int.
            if re.fullmatch(r"\d+", value.strip()):
                data.setdefault("extra_features", {})[slug] = n
            else:
                data.setdefault("extra_features", {})[slug] = value.strip()
            return

        # Anything else (Heating System: 'With heating', etc.) → string
        data.setdefault("extra_features", {})[slug] = value.strip()

    def _write_column(
        self,
        column: str,
        value: Optional[str],
        data: Dict[str, Any],
    ) -> None:
        """
        Type-coerce a value into the named Property column. Skips empty
        values so structural pass never overwrites with garbage. Does
        NOT overwrite already-set non-None values (JSON-LD writes first;
        Overview panel may try to top up — JSON-LD wins).
        """
        if value is None or str(value).strip() == "":
            return
        if data.get(column) is not None:
            return  # earlier source already wrote this; keep it

        if column == "price":
            v = self._to_int_euro_safe(value)
            if v is not None:
                data["price"] = v
        elif column == "size_sqm":
            v = _to_float_sqm(value)
            if v is not None:
                data["size_sqm"] = v
        elif column == "land_size_sqm":
            v = _to_float_sqm(value)
            if v is not None:
                data["land_size_sqm"] = v
        elif column in {"bedrooms", "bathrooms"}:
            v = _to_int_simple(value)
            if v is not None:
                data[column] = v
        elif column == "year_built":
            v = _to_int_year_safe(value)
            if v is not None:
                data["year_built"] = v
        elif column == "levels":
            data["levels"] = str(value).strip()
        elif column == "category":
            data["category"] = str(value).strip()
        elif column in {"area", "subarea"}:
            data[column] = str(value).strip()

    # ---------------------------------------------------------------
    # Phase 2 helpers — Features taxonomy
    # ---------------------------------------------------------------

    def _parse_features(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Walk .rh_property__features > li > a[href*="/property-feature/"].
        Each anchor becomes extra_features[<slug>] = True.

        The slug comes from the URL path (stable), with anchor text
        as fallback. NLP semantic dedup runs later via _NLP_TO_STRUCTURAL
        so e.g. structural 'air_conditioning' blocks NLP from re-adding
        'ac' / 'a_c'.
        """
        block = parser.css_first(".rh_property__features_wrap")
        if not block:
            return

        extra = data.setdefault("extra_features", {})
        for a in block.css('a[href*="/property-feature/"]'):
            href = a.attributes.get("href") or ""
            m = re.search(r"/property-feature/([^/?#]+)", href)
            if m:
                slug = _slug(m.group(1).replace("-", "_"))
            else:
                slug = _slug(a.text(strip=True))
            if slug and slug not in extra:
                extra[slug] = True

    # ---------------------------------------------------------------
    # Phase 2 helpers — Description
    # ---------------------------------------------------------------

    def _parse_description(self, parser: LexborHTMLParser) -> str:
        """
        Description = <p> children of .rh-content-wrapper .rh_content,
        joined with blank lines. JSON-LD's description is truncated
        with '...' — we always prefer the body.

        Fallback: og:description meta (typically 1-2 sentences, still
        passes Quality Gate).
        """
        block = parser.css_first(".rh-content-wrapper .rh_content")
        if block:
            paragraphs: List[str] = []
            for p in block.css("p"):
                txt = p.text(separator=" ", strip=True)
                if txt and len(txt) >= 20 and txt not in paragraphs:
                    paragraphs.append(txt)
            if paragraphs:
                return "\n\n".join(paragraphs)

        # Fallback to og:description
        return self._og_description_fallback(parser)

    # ---------------------------------------------------------------
    # Phase 2 helpers — Gallery
    # ---------------------------------------------------------------

    def _parse_images(self, parser: LexborHTMLParser) -> List[str]:
        """
        Photos via .rh-ultra-property-slider a.rh-ultra-property-thumb[href].
        Slick adds .slick-cloned duplicates for infinite-loop — we dedup
        by URL to drop them. Returns ordered, unique list.

        Fallback: og:image (cover photo only) if gallery is empty.
        """
        photos: List[str] = []
        seen: set = set()

        for a in parser.css(".rh-ultra-property-slider a.rh-ultra-property-thumb"):
            href = (a.attributes.get("href") or "").strip()
            if not href or href in seen:
                continue
            seen.add(href)
            photos.append(href)

        if photos:
            return photos

        og = self._og_image_fallback(parser)
        return [og] if og else []
