"""
Sithonia Properties scraper — sithoniaproperties.gr.

Bootstrap-based custom template (ts-* class prefix throughout). Images are
hosted on the Spitogatos CDN (m1-m3.spitogatos.gr) but the SITE template is
NOT Spitogatos template5 — completely different DOM than halkidiki_agency.gr
or ergonrealestate.com. No anti-bot: Stage 0 (curl_cffi) returns clean HTML.

Scale: ~15 residential properties ≥€400k on a single page (no pagination
populated for this size). Land category covered as a separate Phase 1 walk.
Total addressable target: 15-30 properties.

URL structure
=============
  Search:  /en/property/search
             ?region=196          (196 = Chalkidiki)
             &listingType=sale
             &category=<X>        (residential | land | commercial)
             &priceLow=400000
  Detail:  /en/propertyDetails/<numeric-id>

Card structure on listing (`.ts-result-link > a.card.ts-item`):
  <a href="/en/propertyDetails/19633145" class="card ts-item ts-card ts-result">
      <div class="card-img" style="background-image: url(.../900x675.jpg)"></div>
      <div class="card-body">
          <div class="ts-item__info-badge">€ 1,150,000</div>
          <figure class="ts-item__info">
              <h4>Detached House for sale</h4>
              <aside><i class="fa fa-map-marker"></i>Sithonia, Vourvourou</aside>
          </figure>
          <div class="ts-description-lists">
              <dl><dt>Area</dt><dd>90 m²</dd></dl>
              <dl><dt>Rooms</dt><dd>3</dd></dl>
              <dl><dt>Bathrooms</dt><dd>2</dd></dl>
          </div>
      </div>
  </a>

Detail page structure
=====================
  <h1>Detached House for sale</h1>                  ← title → category
  <h3>€ 1,150,000</h3> / <span class="badge">…</span> ← absolute price (multiple)
  <dl> 18× dt/dd pairs </dl>                        ← full feature table
  <div class="marker" data-lat="40.18" data-lng="23.79"></div> ← GPS
  <#description>                                     ← 500-1500ch English text
  <img src="https://m[1-3].spitogatos.gr/..._1600x1200.jpg"> × ~20 ← gallery

DL features observed in order:
  ID                  → site_property_id_dl   (numeric, matches URL)
  Area                → size_sqm              (sqm, building footprint)
  Price per m²        → extras.price_per_sqm
  Neighborhood        → area                  (e.g. "Vourvourou (Sithonia)")
  Zone                → extras.zone
  Rooms               → bedrooms
  Floor               → extras.floor
  Parking spot        → extras.parking_spot   (Yes/No → bool)
  Heating System      → extras.heating_system
  Energy class        → extras.energy_class
  Levels              → levels
  Kitchens            → extras.kitchens_count
  Living rooms        → extras.living_rooms_count
  Bathrooms           → bathrooms
  WC                  → extras.wc_count
  Status              → extras.status_text    (e.g. "Painted, Furnished")
  Type                → extras.usage_types    (e.g. "Holiday home, Investment, …")
  Extra               → extras.extra_text     (e.g. "Luxurious home")
  Lot size            → land_size_sqm         (only on Villa/Land typically)
  Construction year   → year_built

H1 title → hodu category mapping:
  "Detached House for sale"    → House
  "Villa for sale"             → House
  "Maisonette for sale"        → Maisonette
  "Apartment for sale"         → Apartment
  "Apartment complex for sale" → Apartment        (treated as multi-unit Apartment)
  "Studio for sale"            → Apartment
  "Plot for sale"              → Land
  "Land for sale"              → Land
  "Office for sale"            → Business
  "Shop for sale"              → Business
  "Hotel for sale"             → Business
  "Building for sale"          → Building

Location format on card aside (`"First, Second"`):
  "Sithonia, Vourvourou"   → peninsula=Sithonia, settlement=Vourvourou
  "Pallini, Paliouri"      → peninsula=Pallini (Kassandra)
  "Toroni, Center"         → village=Toroni, area=Center
  "Sithonia, Paralia Nikitis" → peninsula=Sithonia, settlement=Paralia Nikitis

We extract the FIRST token to route calc_municipality (Sithonia/Kassandra/
Nea Propontida/Aristotelis/Polygyros). Same canonical pattern as
clever_estate.py and greek_exclusive.py. geo_matcher will override if our
hint is wrong (verified with clever_estate session).

Key design notes
================
1) Single-page listing per category, no pagination today. Code still walks
   `?page=N` defensively, stops when no new cards / page returns same IDs.
2) Both `residential` and `land` walked per session — covers main inventory.
3) site_property_id extracted from URL path (not CSS class — unlike
   halkidiki_agency/clever_estate).
4) Price has multiple selectors: card-level `.ts-item__info-badge`,
   detail-level `<h3>` containing €, `.badge.badge-primary`. We prefer
   detail-page absolute over card-level (same value anyway).
5) DL block is ONE element with all 18 dt/dd pairs flat — iterate via
   `dl.css("dt")` and `dl.css("dd")` zipped.
6) Marker GPS is in Spitogatos format (.marker[data-lat][data-lng]) —
   identical to halkidiki_agency.gr coordinate extraction.
7) Description is English (server-rendered) — DataExtractor NLP works
   without any locale-specific changes.
8) Image URLs are full-res 1600x1200 by default on detail page — no need
   for thumbnail-stripping regex.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.models.schemas import PropertyTemplate


# =============================================================================
# Constants
# =============================================================================

_BASE_URL = "https://www.sithoniaproperties.gr"
_SEARCH_PATH = "/en/property/search"
_SOURCE_DOMAIN = "sithoniaproperties.gr"

_REGION_HALKIDIKI = 196

# Phase 1 categories — single Spitogatos search endpoint, looped per category.
# Commercial yields ~0-2 results on this site historically; cover for safety.
_CATEGORIES = ("residential", "land", "commercial")

_MAX_PAGES = 20
_INTER_PAGE_SLEEP_SEC = 1.5
_MIN_PRICE_DEFAULT = 400_000

_HALKIDIKI_LAT_RANGE = (39.0, 41.0)
_HALKIDIKI_LNG_RANGE = (22.0, 24.0)


# h1 title (lowercase substring) → hodu category. Ordered: most-specific first
# so "apartment complex" wins before "apartment", "farm parcel" before "farm".
_TITLE_KEYWORD_TO_CATEGORY: List[Tuple[str, str]] = [
    ("apartment complex",  "Apartment"),
    ("detached house",     "House"),
    ("maisonette",         "Maisonette"),
    ("studio",             "Apartment"),
    ("apartment",          "Apartment"),
    ("villa",              "House"),
    ("hotel",              "Business"),
    ("office",             "Business"),
    ("shop",               "Business"),
    ("warehouse",          "Business"),
    ("commercial",         "Business"),
    ("building",           "Building"),
    # Land variants — Spitogatos sites use "Farm parcel" extensively for
    # agricultural/large lots that aren't building plots.
    ("farm parcel",        "Land"),
    ("plot",               "Land"),
    ("land",               "Land"),
    ("agricultural",       "Land"),
    ("farm",               "Land"),
    ("parcel",             "Land"),
    ("field",              "Land"),
    ("olive grove",        "Land"),
    ("vineyard",           "Land"),
    ("lot",                "Land"),
    ("house",              "House"),
]


# Detail-page <dl> labels (lowercase, stripped of `:`) → routing hint.
# Values prefixed `_EXTRAS` go into extra_features; everything else routes
# to a top-level hodu column.
_DL_LABEL_TO_FIELD: Dict[str, str] = {
    "id":                "_dl_id",
    "area":              "size_sqm",
    "price per m²":      "_extras_price_per_sqm",
    "neighborhood":      "area",
    "zone":              "_extras_zone",
    "rooms":             "bedrooms",
    "floor":             "_extras_floor",
    "parking spot":      "_extras_parking_spot",
    "heating system":    "_extras_heating_system",
    "energy class":      "_extras_energy_class",
    "levels":            "levels",
    "kitchens":          "_extras_kitchens_count",
    "living rooms":      "_extras_living_rooms_count",
    "bathrooms":         "bathrooms",
    "wc":                "_extras_wc_count",
    "status":            "_extras_status_text",
    "type":              "_extras_usage_types",
    "extra":             "_extras_extra_text",
    "lot size":          "land_size_sqm",
    "construction year": "year_built",
    "year of construction": "year_built",
    "year built":        "year_built",
    "fireplace":         "_extras_fireplace",
    "garden":            "_extras_garden",
    "swimming pool":     "_extras_swimming_pool",
    "view":              "_extras_view",
    "balcony":           "_extras_balcony",
    "distance from sea": "_extras_distance_from_sea",
}


# Location first-token (lowercase) → calc_municipality. Spitogatos location
# format is "Peninsula, Settlement" or "Settlement, Sub-area". First token
# is enough to disambiguate for ~95% of listings; geo_matcher overrides
# when ambiguous (verified with clever_estate session).
_LOCATION_TOKEN_TO_MUNICIPALITY: Dict[str, str] = {
    # Sithonia peninsula
    "sithonia":      "Sithonia",
    "toroni":        "Sithonia",
    "nikiti":        "Sithonia",
    "vourvourou":    "Sithonia",
    "elia":          "Sithonia",
    "fteroti":       "Sithonia",
    "schoinia":      "Sithonia",
    "marmaras":      "Sithonia",
    "neos marmaras": "Sithonia",
    "sarti":         "Sithonia",
    "kalamitsi":     "Sithonia",
    "paralia nikitis": "Sithonia",
    "limani karra":  "Sithonia",
    "porto carras":  "Sithonia",
    "porto koufo":   "Sithonia",
    # Kassandra peninsula
    "pallini":       "Kassandra",
    "kassandra":     "Kassandra",
    "pefkochori":    "Kassandra",
    "polychrono":    "Kassandra",
    "hanioti":       "Kassandra",
    "chanioti":      "Kassandra",
    "kallithea":     "Kassandra",
    "afytos":        "Kassandra",
    "sani":          "Kassandra",
    "skioni":        "Kassandra",
    "paliouri":      "Kassandra",
    "fourka":        "Kassandra",
    "kalandra":      "Kassandra",
    "siviri":        "Kassandra",
    "kryopigi":      "Kassandra",
    # Nea Propontida (north-west)
    "moudania":      "Nea Propontida",
    "nea moudania":  "Nea Propontida",
    "kallikrateia":  "Nea Propontida",
    "kallikrat":     "Nea Propontida",
    "flogita":       "Nea Propontida",
    "potidea":       "Nea Propontida",
    "nea potidea":   "Nea Propontida",
    "triglia":       "Nea Propontida",
    "sozopoli":      "Nea Propontida",
    # Aristotelis (north-east)
    "ouranoupoli":   "Aristotelis",
    "ierissos":      "Aristotelis",
    "olympiada":     "Aristotelis",
    "stratoni":      "Aristotelis",
    "nea roda":      "Aristotelis",
    "amouliani":     "Aristotelis",
    # Polygyros (center)
    "polygyros":     "Polygyros",
    "gerakini":      "Polygyros",
    "ormylia":       "Polygyros",
    "psakoudia":     "Polygyros",
    "taxiarchis":    "Polygyros",
    "metamorfosi":   "Polygyros",
}


# =============================================================================
# Helpers
# =============================================================================

_PROPERTY_DETAILS_RX = re.compile(r"/en/propertyDetails/(\d+)")
_HALKIDIKI_COORD_RX = re.compile(
    r"((?:39|40|41)\.\d{4,})[\s,]+((?:22|23|24)\.\d{4,})"
)
_SPITOGATOS_IMG_RX = re.compile(
    r"https?://m[0-9]\.spitogatos\.gr/[0-9a-zA-Z_./?=&-]+\.(?:jpg|jpeg|png|webp)",
    re.IGNORECASE,
)


def _to_int_euro(text: str) -> Optional[int]:
    """
    Parse English/US-style euro price into integer euros.
      "€ 1,150,000"  → 1150000
      "€ 400,000"    → 400000
      "1.150.000 €"  → 1150000
      "Price: € 12,778 / m²"  → 12778
    """
    if not text:
        return None
    cleaned = re.sub(r"[^\d.,]", "", text)
    if not cleaned:
        return None
    if "." in cleaned and "," in cleaned:
        # Last separator wins — typically EU format "1.150.000,00"
        last = max(cleaned.rfind("."), cleaned.rfind(","))
        cleaned = cleaned[:last]
    cleaned = re.sub(r"[.,]", "", cleaned)
    try:
        value = int(cleaned)
    except ValueError:
        return None
    if value > 200_000_000:
        return None
    return value


def _to_int_simple(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """Parse "90 m²" / "1,500.5 m²" → 90.0 / 1500.5."""
    if not text:
        return None
    cleaned = text.replace(",", "")
    m = re.search(r"\d+(?:\.\d+)?", cleaned)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _yesno_to_bool(text: str) -> Optional[bool]:
    """Map "Yes"/"No"/"Ναι"/"Όχι" → True/False, anything else → None."""
    if not text:
        return None
    t = text.strip().lower()
    if t in ("yes", "ναι", "true", "1"):
        return True
    if t in ("no", "όχι", "false", "0", "-"):
        return False
    return None


def _extract_property_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = _PROPERTY_DETAILS_RX.search(url)
    return m.group(1) if m else None


def _title_to_category(title: str) -> Optional[str]:
    """Match h1 text (lowercased) against keyword table; most-specific wins."""
    if not title:
        return None
    t = title.lower()
    for keyword, category in _TITLE_KEYWORD_TO_CATEGORY:
        if keyword in t:
            return category
    return None


def _location_to_municipality(loc_str: str) -> Optional[str]:
    """
    Spitogatos location format "X, Y" — first token is peninsula or
    settlement; lookup in whitelist. Falls back to second token if first
    not recognised.
    """
    if not loc_str:
        return None
    parts = [p.strip().lower() for p in loc_str.split(",")]
    for part in parts:
        if part in _LOCATION_TOKEN_TO_MUNICIPALITY:
            return _LOCATION_TOKEN_TO_MUNICIPALITY[part]
    return None


def _clean_location_text(node: LexborNode) -> str:
    """
    Strip <i> icon tags out of <aside> text. E.g.:
      <aside><i class="fa fa-map-marker"></i>Sithonia, Vourvourou</aside>
    → "Sithonia, Vourvourou"
    """
    if node is None:
        return ""
    # selectolax .text(strip=True) already drops elements but leaves text
    # nodes; we just need to collapse whitespace.
    txt = node.text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", txt).strip()


def _extract_bg_image_url(style: str) -> Optional[str]:
    """Pull URL out of `background-image: url(...)` style attribute."""
    if not style:
        return None
    m = re.search(r"url\(\s*['\"]?([^'\")]+)", style)
    return m.group(1).strip() if m else None


# =============================================================================
# Scraper
# =============================================================================

class SithoniaPropertiesScraper(EnrichmentMixin, BaseScraper):
    """
    sithoniaproperties.gr — small Sithonia-focused agency, ~15-30 listings
    addressable. Bootstrap custom template, English-by-default, Spitogatos
    image CDN. No anti-bot.
    """

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

    async def fetch_listings(self):
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # URL builders
    # ---------------------------------------------------------------

    def _build_search_url(
        self,
        category: str,
        page: int,
        min_price: int,
    ) -> str:
        # Replicate the URL parameter order observed in the form so query
        # signature matches what the site expects.
        qs = (
            f"region={_REGION_HALKIDIKI}"
            f"&listingType=sale"
            f"&category={category}"
            f"&propertyTypes%5B%5D="
            f"&myCode="
            f"&priceLow={min_price}"
            f"&priceHigh="
            f"&livingAreaLow="
            f"&livingAreaHigh="
            f"&roomsLow=nd"
            f"&roomsHigh=nd"
            f"&floorNumberLow=nd"
            f"&floorNumberHigh=nd"
            f"&constructionYearLow="
            f"&constructionYearHigh="
            f"&heatingControllers="
            f"&heatingMedia="
        )
        if page > 1:
            qs += f"&page={page}"
        return f"{_BASE_URL}{_SEARCH_PATH}?{qs}"

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs across all categories
    # ---------------------------------------------------------------

    async def collect_urls(
        self,
        min_price: int = _MIN_PRICE_DEFAULT,
    ) -> List[PropertyTemplate]:
        all_props: List[PropertyTemplate] = []
        seen_property_ids: set[str] = set()

        for category in _CATEGORIES:
            logger.info(
                f"[{self.source_domain}] === Category '{category}' ==="
            )
            page = 1

            while page <= _MAX_PAGES:
                url = self._build_search_url(category, page, min_price)
                logger.info(
                    f"[{self.source_domain}] {category} page {page}: {url[:140]}"
                )

                try:
                    response = await self.client.get(url)
                    parser = LexborHTMLParser(response.text)
                    cards = parser.css(".ts-result-link a.card.ts-item")

                    if not cards:
                        logger.info(
                            f"[{self.source_domain}] {category} page {page}: "
                            f"no cards, stopping"
                        )
                        break

                    page_collected = 0
                    page_new_ids = 0
                    page_dup_ids = 0

                    for card in cards:
                        try:
                            pid, prop = self._parse_card(card)
                            if not pid:
                                continue
                            if pid in seen_property_ids:
                                page_dup_ids += 1
                                continue
                            seen_property_ids.add(pid)
                            page_new_ids += 1
                            if prop is not None:
                                all_props.append(prop)
                                page_collected += 1
                        except Exception as e:
                            logger.error(
                                f"[{self.source_domain}] card parse error: {e}"
                            )

                    logger.info(
                        f"[{self.source_domain}] {category} page {page}: "
                        f"+{page_collected} kept "
                        f"({page_new_ids} new ids, {page_dup_ids} duplicates)"
                    )

                    # Server-loop guard: if every id was duplicate, stop.
                    if page > 1 and page_new_ids == 0 and page_dup_ids > 0:
                        logger.info(
                            f"[{self.source_domain}] {category} page {page} "
                            f"only returned duplicates, stopping"
                        )
                        break

                    await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)
                    page += 1

                except Exception as e:
                    logger.error(
                        f"[{self.source_domain}] {category} page {page} "
                        f"critical: {e}"
                    )
                    break

        logger.info(
            f"[{self.source_domain}] Phase 1 done: {len(all_props)} URLs "
            f"across {len(_CATEGORIES)} categories"
        )
        return all_props

    def _parse_card(
        self,
        card: LexborNode,
    ) -> Tuple[Optional[str], Optional[PropertyTemplate]]:
        """
        Extract a single listing card → (property_id, PropertyTemplate).
        Returns (None, None) when card is malformed.
        """
        # 1. Detail URL + property ID from URL
        href = card.attributes.get("href")
        if not href:
            return (None, None)
        if href.startswith("/"):
            href = _BASE_URL + href
        pid = _extract_property_id_from_url(href)
        if not pid:
            return (None, None)

        # 2. Price from .ts-item__info-badge
        price_int: Optional[int] = None
        badge = card.css_first(".ts-item__info-badge")
        if badge:
            price_int = _to_int_euro(badge.text(strip=True))

        # 3. Title → category (h4 on card)
        title_h4 = card.css_first("h4")
        category: Optional[str] = None
        if title_h4:
            category = _title_to_category(title_h4.text(strip=True))

        # 4. Location from <aside>
        location_raw = ""
        aside = card.css_first("aside")
        if aside:
            location_raw = _clean_location_text(aside)

        prop = PropertyTemplate(
            site_property_id=pid,
            source_domain=self.source_domain,
            url=href,
            price=price_int,
            location_raw=location_raw or None,
            category=category,
        )
        return (pid, prop)

    # ---------------------------------------------------------------
    # PHASE 2 — deep-parse detail page
    # ---------------------------------------------------------------

    async def fetch_details(self, url: str) -> Dict[str, Any]:
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
                "calc_municipality": None,
                "levels":            None,
                "latitude":          None,
                "longitude":         None,
                "images":            [],
                "extra_features":    {},
            }

            # 1. Title → category
            h1 = parser.css_first("h1")
            if h1:
                category = _title_to_category(h1.text(strip=True))
                if category:
                    data["category"] = category

            # 2. Price — multiple selectors, prefer explicit price element
            self._parse_price(parser, data)

            # 3. The 18-row <dl> with all structured features
            self._parse_dl_block(parser, data)

            # 4. GPS — `.marker[data-lat][data-lng]` (Spitogatos pattern)
            self._parse_coordinates(parser, raw_html, data)

            # 5. Description — `#description`
            data["description"] = self._parse_description(parser)

            # 6. Image gallery — Spitogatos CDN img tags
            data["images"] = self._collect_image_urls(parser, raw_html)

            # 7. calc_municipality — route from location (set in Phase 1)
            # Detail page also has Neighborhood in dl which sets area.
            # If we have a meaningful area now, route municipality from it.
            self._route_municipality(data)

            # 7b. Safety net: if category still unknown but size_sqm > 1000
            # the property is almost certainly a Land plot (Spitogatos uses
            # h1 titles like "Couple parcels for sale" that miss our keyword
            # list). Reroute size_sqm → land_size_sqm.
            if (data.get("category") is None
                    and data.get("size_sqm") is not None
                    and data["size_sqm"] >= 1000):
                logger.info(
                    f"[{self.source_domain}] inferred Land category from "
                    f"size_sqm={data['size_sqm']:.0f} (no title match)"
                )
                data["category"] = "Land"
                if data.get("land_size_sqm") is None:
                    data["land_size_sqm"] = data["size_sqm"]
                data["size_sqm"] = None

            # 8. NLP fallback (EnrichmentMixin) — pulls amenities from
            # the English description into extra_features.
            self._apply_nlp_fallback(data)

            # 9. Quality gate log-only
            if not self._passes_quality_gate(data.get("description")):
                logger.warning(
                    f"[{self.source_domain}] description below quality "
                    f"gate for {url}"
                )

            return {k: v for k, v in data.items() if v is not None}

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] fetch_details error for {url}: {e}"
            )
            return {}

    # ---------------------------------------------------------------
    # Phase 2 sub-parsers
    # ---------------------------------------------------------------

    def _parse_price(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Absolute price shows up in several places. Order of preference:
          1. <span class="badge badge-primary">€ 1,150,000</span>
          2. <h3>€ 1,150,000</h3>           (in hero section)
          3. element matching .ts-item__info-badge      (fallback)
          4. any text with "€ X,XXX,XXX"                (last resort)
        """
        for selector in (
            "span.badge.badge-primary",
            "h3",
            ".ts-item__info-badge",
        ):
            for el in parser.css(selector):
                txt = el.text(strip=True)
                if "€" not in txt:
                    continue
                v = _to_int_euro(txt)
                if v is not None and v >= 1000:
                    data["price"] = v
                    return

    def _parse_dl_block(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Detail page has one big <dl> with all property features as flat
        dt/dd pairs. Route each label to the right hodu field.
        """
        for dl in parser.css("dl"):
            dts = dl.css("dt")
            dds = dl.css("dd")
            # Quick filter: real feature dl has ≥5 dt/dd pairs. Skip card-style
            # dls that have 1 pair (Area/Rooms/Bathrooms in listings).
            if len(dts) < 5:
                continue
            for dt, dd in zip(dts, dds):
                label = dt.text(strip=True).rstrip(":").strip().lower()
                value = dd.text(strip=True)
                if not label or not value:
                    continue
                self._route_dl_pair(label, value, data)
            return  # only one main feature dl per page

    def _route_dl_pair(
        self,
        label: str,
        value: str,
        data: Dict[str, Any],
    ) -> None:
        """Dispatch one (label, value) row to the right hodu field."""
        field = _DL_LABEL_TO_FIELD.get(label)
        if field is None:
            # Unknown label — stash raw under extras for diagnosis
            data["extra_features"][f"dl_{label.replace(' ', '_')}"] = value
            return

        # 1) `_dl_id` sanity check (matches URL path)
        if field == "_dl_id":
            data["extra_features"]["site_property_id_dl"] = value
            return

        # 2) Column fields
        if field == "size_sqm":
            v = _to_float_sqm(value)
            if v is not None and data.get("size_sqm") is None:
                # For Land category, area→land_size_sqm reroute
                if data.get("category") == "Land":
                    data["land_size_sqm"] = v
                else:
                    data["size_sqm"] = v
            return
        if field == "land_size_sqm":
            v = _to_float_sqm(value)
            if v is not None:
                data["land_size_sqm"] = v
            return
        if field == "bedrooms":
            n = _to_int_simple(value)
            if n is not None:
                data["bedrooms"] = n
            return
        if field == "bathrooms":
            n = _to_int_simple(value)
            if n is not None:
                data["bathrooms"] = n
            return
        if field == "year_built":
            n = _to_int_simple(value)
            if n is not None and 1900 < n < 2100:
                data["year_built"] = n
            return
        if field == "levels":
            n = _to_int_simple(value)
            if n is not None:
                data["levels"] = str(n)
            return
        if field == "area":
            # "Vourvourou (Sithonia)" — strip parens, keep first token
            clean = re.sub(r"\s*\([^)]*\)\s*", "", value).strip()
            if clean and data.get("area") is None:
                data["area"] = clean
            return

        # 3) `_extras_*` → push into extra_features as typed value
        if field.startswith("_extras_"):
            key = field[len("_extras_"):]
            # Yes/No flags → bool
            if key in ("parking_spot", "fireplace", "garden",
                       "swimming_pool", "view", "balcony"):
                bv = _yesno_to_bool(value)
                if bv is not None:
                    data["extra_features"][key] = bv
                    return
            # Numeric counts
            if key in ("kitchens_count", "living_rooms_count", "wc_count"):
                n = _to_int_simple(value)
                if n is not None:
                    data["extra_features"][key] = n
                    return
            # Pricing or distance numerics
            if key in ("price_per_sqm", "distance_from_sea"):
                v = _to_int_euro(value) or _to_int_simple(value)
                if v is not None:
                    data["extra_features"][key] = v
                    return
            # Everything else → store as-is text
            data["extra_features"][key] = value
            return

    def _parse_coordinates(
        self,
        parser: LexborHTMLParser,
        raw_html: str,
        data: Dict[str, Any],
    ) -> None:
        """
        Order of preference:
          1. `.marker[data-lat][data-lng]`  (Spitogatos pattern)
          2. raw HTML regex over JS variables (Halkidiki bbox)
        Coordinates outside Halkidiki bbox are rejected.
        """
        # 1. Marker element
        marker = parser.css_first(".marker[data-lat]")
        if marker:
            try:
                lat = float(marker.attributes.get("data-lat") or "")
                lng = float(marker.attributes.get("data-lng") or "")
                if (_HALKIDIKI_LAT_RANGE[0] <= lat <= _HALKIDIKI_LAT_RANGE[1]
                        and _HALKIDIKI_LNG_RANGE[0] <= lng
                        <= _HALKIDIKI_LNG_RANGE[1]):
                    data["latitude"] = lat
                    data["longitude"] = lng
                    # data-type=offset = privacy obfuscation
                    if marker.attributes.get("data-type") == "offset":
                        data["extra_features"]["gps_type"] = "offset"
                    return
            except (ValueError, TypeError):
                pass

        # 2. Raw regex fallback
        m = _HALKIDIKI_COORD_RX.search(raw_html)
        if m:
            try:
                lat = float(m.group(1))
                lng = float(m.group(2))
                if (_HALKIDIKI_LAT_RANGE[0] <= lat <= _HALKIDIKI_LAT_RANGE[1]
                        and _HALKIDIKI_LNG_RANGE[0] <= lng
                        <= _HALKIDIKI_LNG_RANGE[1]):
                    data["latitude"] = lat
                    data["longitude"] = lng
            except ValueError:
                pass

    def _parse_description(self, parser: LexborHTMLParser) -> str:
        """English description; main source `#description`."""
        for selector in ("#description", ".entry", ".ts-property__description"):
            el = parser.css_first(selector)
            if el:
                txt = el.text(separator=" ", strip=True)
                if txt and len(txt) >= 50:
                    return txt
        # og:description fallback
        og = parser.css_first('meta[property="og:description"]')
        if og:
            txt = (og.attributes.get("content") or "").strip()
            if txt and len(txt) >= 30:
                return txt
        return ""

    def _collect_image_urls(
        self,
        parser: LexborHTMLParser,
        raw_html: str,
    ) -> List[str]:
        """
        Two-chain extraction:
          1. <img src=...spitogatos.gr...> (~20 imgs typical)
          2. raw regex sweep over HTML for any spitogatos JPG/PNG (catches
             lazy-loaded references embedded in JS arrays).
        Dedup, prefer 1600x1200 over 900x675 thumbnails.
        """
        seen: set[str] = set()
        ordered: List[str] = []

        # 1. img tags
        for img in parser.css("img"):
            src = (
                img.attributes.get("src", "")
                or img.attributes.get("data-src", "")
            )
            if src and "spitogatos.gr" in src and src not in seen:
                seen.add(src)
                ordered.append(src)

        # 2. Raw regex sweep — catches embedded references missed by DOM
        for m in _SPITOGATOS_IMG_RX.finditer(raw_html):
            url = m.group(0)
            if url not in seen:
                seen.add(url)
                ordered.append(url)

        # 3. Prefer full-res (1600x1200) over thumbnails (900x675) by
        # dropping the smaller variant when the larger exists for the same
        # base ID.
        result: List[str] = []
        base_seen: set[str] = set()
        # Sort: prefer 1600x1200, then 900x675, then anything else
        def _priority(u: str) -> int:
            if "1600x1200" in u:
                return 0
            if "900x675" in u:
                return 1
            return 2
        for url in sorted(ordered, key=_priority):
            m = re.search(r"(\d+)_\d+x\d+\.", url)
            base_id = m.group(1) if m else url
            if base_id not in base_seen:
                base_seen.add(base_id)
                result.append(url)

        return result

    def _route_municipality(self, data: Dict[str, Any]) -> None:
        """
        Set calc_municipality from data["area"] (filled from <dl>) — falls
        back to no-op when no area or no whitelist match. geo_matcher will
        do the canonical work; this is just a hint.
        """
        area = data.get("area")
        if not area:
            return
        # Try exact match on area
        key = area.lower()
        muni = _LOCATION_TOKEN_TO_MUNICIPALITY.get(key)
        if muni:
            data["calc_municipality"] = muni
            return
        # Try first comma-separated token of area
        for token in re.split(r"[,(]", area):
            t = token.strip().lower()
            if t in _LOCATION_TOKEN_TO_MUNICIPALITY:
                data["calc_municipality"] = _LOCATION_TOKEN_TO_MUNICIPALITY[t]
                return
