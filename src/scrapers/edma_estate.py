"""
~/hodu/src/scrapers/edma_estate.py

EDMA Estate (edmaestate.gr) — classic PHP/MySQL real estate broker, Halkidiki
focus. One of the cleanest scraper targets in the hodu pipeline.

Scale at min_price=400_000 with filter_nomoi[]=2 (Halkidiki):
  ~86 properties across ~11 pagination pages (8 per page).

URL patterns
------------
List:    /results.php?filter_int_to=1&filter_nomoi[]=2&filter_price_from=N&page=N
Detail:  /property.php?id=NNNN

Tech stack
----------
PHP + jQuery + select2 + Owl Carousel + Leaflet/OpenStreetMap. Server-side
rendered HTML (no SPA, no JS-gating for the public listing). Async AJAX
endpoint exists (`async_fetch.php`) but is not needed — full-page parse is
straightforward.

Anti-bot
--------
None for browsing. reCAPTCHA only on contact forms. **Stage 0 (curl_cffi)
sufficient** for both listing and detail pages.

Strategy
--------
Phase 1 (paginated walk):
  GET results.php?...&page=N with Halkidiki filter (`filter_nomoi[]=2`).
  Each page returns 8 cards via `.advertCard .advert_item`. Terminate when
  pagination's `.next` link is absent or page returns 0 cards.

  CRITICAL: `.advertCard` ALSO contains a `.contact_item` (mid-page CEO
  contact card). MUST use `.advertCard .advert_item` (not just `.advertCard`)
  to skip it. Otherwise scraper picks up a non-property card and crashes
  on missing fields.

Phase 2 (detail page):
  Canonical 7-step pipeline. Highlights:
    Step 1: `.mad-property-list ul li` — each <li> has 2 <span>s:
            <span>Label</span><span>Value</span>
            Authoritative source for price, year, type, sizes, rooms.
    Step 2: `.propertyDesc` — 600+ char description, all amenities listed.
    Step 3: Leaflet `setView([lat, lng], zoom)` JS literal.
    Step 4: Owl Carousel `.owl-item:not(.cloned) img` (skip slider clones).
    Step 5: NLP fallback for missing `levels` etc.
    Step 6: SKIPPED — structural panel + 19 amenities are richer than LLM.
    Step 7: Quality Gate (log-only).

  Additional: `#amentities-tab .mad-list--icon li.propertyMore` — 19
  boolean/key-value amenities per listing.

Mapping decisions
-----------------
- Category: site uses canonical English vocabulary already (Villa, Apartment,
  Maisonette — with proper spelling, NOT "Maisonetta"). Direct map.
- Region: `.advertRegion` (Kassandra/Sithonia/Pallini/etc.) → municipality
  routing via _REGION_TO_MUNICIPALITY. GeoMatcher overrides downstream when
  coords are good.
- Floor: "Ground floor" / "1st" / "2nd" → extras["floor"] (string verbatim).
  NOT the same as `levels` ("consists of 2 levels" — house storey count).
- Price format: Greek convention (`€ 430.000,00` = 430,000 EUR). Period =
  thousand separator, comma = decimal. _to_int_euro_gr handles this.
- Discount markers: NOT observed on this site (clean prices only).

Edge cases handled
------------------
- CEO contact card mid-listings → filtered via .advert_item selector
- Owl Carousel image clones (left/right buffer for infinite scroll) → 
  :not(.cloned) selector
- Relative URLs in listing cards (property.php?id=N) → _resolve_url helper
- Greek number format (1.400.000,00 → 1400000) → _to_int_euro_gr
- Image URL filter: strip logos and SVG icons from gallery
- Halkidiki bbox sanity check (39.0-41.5 lat, 22.0-25.0 lng) before storing
  coords

LLM fallback
------------
NOT used. Structured spec panel + 19 explicit amenities per listing already
satisfy `len(extra_features) < 5` LLM trigger condition with margin to spare.
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

_BASE_URL = "https://www.edmaestate.gr"
_SOURCE_DOMAIN = "edmaestate.gr"

# Greek nomos (prefecture) IDs from the site's location dropdown.
# 1 = Thessaloniki, 2 = Halkidiki, 3 = Rest of Greece, 4 = All counties.
_NOMOS_HALKIDIKI = 2

# Halkidiki geographic bounding box. Coords outside this are wrong — verified
# across multiple scrapers (some listings have mistakenly entered Crete coords,
# etc.). Drop them to prevent poisoning GeoMatcher.
_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)  # (lat_min, lng_min, lat_max, lng_max)

# Phase 1 safety caps. Site has ~86 properties at typical filters → 11 pages.
# 25 pages = 200 properties — leaves ample headroom while preventing runaway.
_PHASE1_MAX_PAGES = 25
_PHASE1_MAX_SEEDS = 1000

# Politeness pause between listing-page fetches.
_PAGE_DELAY_SECONDS = 2


# =============================================================================
# Category mapping — site terms → canonical hodu vocabulary
# =============================================================================
#
# Site exposes its full type taxonomy via filter dropdown:
#   Residential: Studio, Apartment, Villa, Maisonette, House, Building, Loft,
#                Apartment Complex
#   Commercial:  Industrial Site, Shop, Hotel, Office, Small Industrial Space,
#                Building
#   Land:        Agricultural, Plot
#
# Site uses CORRECT "Maisonette" spelling (not "Maisonetta" like Dionisiou).
# Direct 1:1 mapping for residential. Commercial collapsed to canonical
# "Hotel/Commercial" except Hotel itself (own canonical bucket because hotels
# have distinct comparable characteristics in our cluster engine).

_CATEGORY_MAP: Dict[str, str] = {
    # Residential — direct mapping
    "Studio":               "Studio",
    "Apartment":            "Apartment",
    "Villa":                "Villa",
    "Maisonette":           "Maisonette",
    "House":                "House",
    "Building":             "Building",
    "Loft":                 "Loft",
    "Apartment Complex":    "Apartment Complex",
    # Commercial
    "Hotel":                "Hotel",
    "Shop":                 "Hotel/Commercial",
    "Office":               "Hotel/Commercial",
    "Industrial Site":      "Hotel/Commercial",
    "Small Industrial Space": "Hotel/Commercial",
    # Land
    "Plot":                 "Land",
    "Agricultural":         "Land",
}


# =============================================================================
# Region → municipality routing
# =============================================================================
#
# `.advertRegion` text in listing cards uses demos-level names. Map to
# canonical hodu municipalities (the 5 Halkidiki demoi + adjacent Thessaloniki
# satellites). Used as a HINT for extras["municipality"] — daily_sync's
# GeoMatcher overrides this with GPS-based matching when coords are good.

_REGION_TO_MUNICIPALITY: Dict[str, str] = {
    # Kassandra peninsula (the 4 demoi)
    "Kassandra":            "Kassandra",
    "Pallini":              "Kassandra",   # Pallini demos sits in Kassandra peninsula
    # Sithonia peninsula
    "Sithonia":             "Sithonia",
    "Toroni":               "Sithonia",    # Toroni demos within Sithonia
    "Ormulia":              "Polygyros",
    # Aristotelis (north-east Halkidiki, includes Mt Athos coast)
    "Stagiron - Akanthou":  "Aristotelis",
    # Polygyros (central mainland)
    "Polygyros":            "Polygyros",
    "Poluguros":            "Polygyros",   # transliteration variant
    # Nea Propontida (north Halkidiki / Thessaloniki suburbs)
    "Moudania":             "Nea Propontida",
    "Kallikrateia":         "Nea Propontida",
    # Note: Thessaloniki-perimeter regions (Thermi/Kalamaria/etc.) intentionally
    # omitted. They appear in site's "All counties" filter but our Halkidiki
    # filter (filter_nomoi[]=2) excludes them at source.
}


# =============================================================================
# Spec panel field routing
# =============================================================================
#
# `.mad-property-list ul li` rows — each has exactly two <span> children:
# label and value. Map labels to (a) Property columns or (b) extras keys.
#
# Routing prefixes:
#   "_skip"         → drop entirely
#   "<column>"      → top-level PropertyTemplate column
#   "_extra:<key>"  → extras[<key>] (string verbatim, except special keys)

_SPEC_PANEL_FIELDS: Dict[str, str] = {
    "ID":                    "_skip",           # already in site_property_id
    "Available for":         "_skip",           # always "For sale" in our scope
    "Construction date":     "year_built",      # int
    "Type":                  "category",        # str via _CATEGORY_MAP
    "Square meters":         "size_sqm",        # float
    "Square meters of plot": "land_size_sqm",   # float
    "Rooms":                 "bedrooms",        # int
    "Floor":                 "_extra:floor",    # str (Ground floor / 1st / 2nd / ...)
    "Price":                 "price",           # int (€ 430.000,00 → 430000)
    "Price per sq.m.":       "_extra:price_per_sqm",  # int (€4.526,32/sq.m.)
}


# =============================================================================
# Amenity slug canonicalization
# =============================================================================
#
# Site uses idiosyncratic labels for some amenities. Map to canonical hodu
# slugs used across all other scrapers so dedup and cluster matching work.

_AMENITY_SLUG_MAP: Dict[str, str] = {
    "a_c":            "air_conditioning",
    "ac":             "air_conditioning",
    "parking_spot":   "parking",
    "tents":          "awnings",            # site quirk — "Tents" = sun awnings
    "electrical_devices": "electrical_appliances",
    "high_street":    "near_high_street",
}


# =============================================================================
# Pure helper functions (no scraper state)
# =============================================================================

def _normalize_text(s: Optional[str]) -> str:
    """Collapse whitespace, normalise narrow-NBSP and NBSP, strip."""
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


def _slug(label: str) -> str:
    """Free-form label → stable snake_case key."""
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _resolve_url(href: str) -> str:
    """
    Resolve a relative listing-card URL to absolute.

    Site uses bare `property.php?id=N` hrefs (no leading slash) in cards.
    Absolute URLs are needed for save_or_update_property's URL uniqueness
    constraint and for downstream linking.
    """
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return f"{_BASE_URL}{href}"
    return f"{_BASE_URL}/{href}"


def _extract_id_from_url(url: str) -> Optional[str]:
    """Extract `id=NNNN` query parameter — the site's authoritative property ID."""
    if not url:
        return None
    m = re.search(r"\?id=(\d+)", url)
    return m.group(1) if m else None


def _to_int_simple(text: str) -> Optional[int]:
    """First integer in string. '2 Rooms' → 2, '2025' → 2025."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """
    Parse area-in-square-meters strings. Handles English (95.00) and the
    occasional Greek-locale value (95,00). Greek `,` becomes `.` for parsing.

    Examples:
      "95.00"        → 95.0
      "900"          → 900.0
      "150,5 sqm"    → 150.5
    """
    if not text:
        return None
    text = text.replace("\xa0", " ")
    m = re.search(r"\d+(?:[.,]\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def _to_int_euro_gr(text: str) -> Optional[int]:
    """
    Parse Greek-format euro prices into integer euros.

    Greek convention: `.` = thousand separator, `,` = decimal separator.

      "€ 430.000,00"        → 430000   (drop cents)
      "430.000€"            → 430000
      "€4.526,32/sq.m."     → 4526     (drop cents, drop unit suffix)
      "1.500.000"           → 1500000
      "Price upon request"  → None

    Defensive sanity cap inherited from EnrichmentMixin (200M EUR). Rejects
    pathological inputs (e.g. two prices concatenated by a parser quirk).
    """
    if not text:
        return None
    cleaned = re.sub(r"[^\d.,]", "", text)
    if not cleaned:
        return None
    # Strip cents (text after Greek-style decimal comma)
    if "," in cleaned:
        cleaned = cleaned.split(",", 1)[0]
    # Strip thousands dots
    cleaned = cleaned.replace(".", "")
    if not cleaned or not cleaned.isdigit():
        return None
    try:
        value = int(cleaned)
    except ValueError:
        return None
    # Sanity cap — reject implausible (almost certainly malformed input)
    if value > EnrichmentMixin._PRICE_SANITY_CAP:
        return None
    return value


def _bbox_check(lat: float, lng: float) -> bool:
    """True if coords are within Halkidiki bounding box."""
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


# =============================================================================
# Scraper
# =============================================================================

class EdmaEstateScraper(EnrichmentMixin, BaseScraper):
    """
    edmaestate.gr — see module docstring for full strategy notes.
    """

    # ── Mixin overrides ──────────────────────────────────────────────────

    # Category is sourced from the structured spec panel ("Type" row).
    # Excluding it from NLP fill prevents the regex from overwriting our
    # authoritative value with an inferred form (e.g. NLP would say "House"
    # when spec panel says "Apartment Complex" — wrong override).
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",         # description: "consists of 2 levels" — NLP fills this
    )

    # Semantic dedup map — when NLP extracts these canonical names but the
    # spec panel already filled a structurally equivalent extras key, drop
    # the NLP duplicate.
    _NLP_TO_STRUCTURAL = {
        "swimming_pool":     {"swimming_pool", "pool"},
        "sea_view":          {"sea_view"},
        "mountain_view":     {"mountain_view"},
        "parking":           {"parking"},
        "air_conditioning":  {"air_conditioning"},
        "garden":            {"garden"},
        "fireplace":         {"fireplace"},
        "balcony":           {"balcony"},
        "storage_room":      {"storage_room"},
        "heating":           {"heating"},
        "solar_water_heater": {"solar_boiler"},
        "double_glazed":     {"double_glazed_windows"},
    }

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN
        # No LLM extractor: structured panel + 19 explicit amenities per
        # listing provide far more signal than gpt-4o-mini could add.

    # =========================================================================
    # PHASE 1 — collect_urls (paginated walk through results.php)
    # =========================================================================

    async def collect_urls(
        self,
        min_price: int = 400_000,
    ) -> List[PropertyTemplate]:
        """
        Walk the paginated listing endpoint and emit PropertyTemplate seeds.

        Termination conditions (any one is sufficient):
          * Empty `.advertCard .advert_item` set on a page
          * Missing `.next` pagination link
          * Page returns HTTP ≥ 400
          * Hit _PHASE1_MAX_PAGES safety cap
          * Hit _PHASE1_MAX_SEEDS safety cap
        """
        all_seeds: Dict[str, PropertyTemplate] = {}
        page = 1

        while page <= _PHASE1_MAX_PAGES:
            params = {
                "filter_int_to": 1,                  # 1 = Sale, 2 = Rent
                "filter_nomoi[]": _NOMOS_HALKIDIKI,
                "filter_price_from": min_price,
                "filter_price_to": "",
                "filter_bedrooms": "",
                "special[]": "",
                "page": page,
            }
            logger.info(
                f"[{self.source_domain}] Phase 1 page {page} "
                f"(min_price={min_price})"
            )

            try:
                response = await self.client.get(
                    f"{_BASE_URL}/results.php", params=params
                )
            except Exception as exc:
                logger.error(
                    f"[{self.source_domain}] page {page} fetch failed: {exc!r}"
                )
                break

            if response.status_code >= 400:
                logger.warning(
                    f"[{self.source_domain}] page {page} returned "
                    f"{response.status_code}, stopping"
                )
                break

            parser = LexborHTMLParser(response.text)

            # CRITICAL: `.advertCard` ALSO matches the CEO contact card mid-page
            # (which has `.contact_item` instead of `.advert_item` inside).
            # Filter to `.advertCard .advert_item` to exclude it. Otherwise
            # _parse_listing_card crashes on missing fields.
            cards = parser.css(".advertCard .advert_item")
            if not cards:
                logger.info(
                    f"[{self.source_domain}] page {page}: no cards — "
                    f"end of pagination"
                )
                break

            new_on_page = 0
            for card in cards:
                try:
                    seed = self._parse_listing_card(card)
                except Exception as exc:
                    logger.error(
                        f"[{self.source_domain}] card parse error "
                        f"on page {page}: {exc!r}"
                    )
                    continue
                if seed is None:
                    continue
                if seed.site_property_id in all_seeds:
                    continue
                all_seeds[seed.site_property_id] = seed
                new_on_page += 1

            logger.info(
                f"[{self.source_domain}] page {page}: "
                f"{len(cards)} cards, {new_on_page} new seeds "
                f"(total: {len(all_seeds)})"
            )

            if len(all_seeds) >= _PHASE1_MAX_SEEDS:
                logger.warning(
                    f"[{self.source_domain}] hit max seeds cap "
                    f"({_PHASE1_MAX_SEEDS}), stopping"
                )
                break

            # Pagination terminator — explicit "next" arrow absent on last page
            next_link = parser.css_first("ul.mad-pagination li a.next")
            if not next_link:
                logger.info(
                    f"[{self.source_domain}] no .next pagination link "
                    f"on page {page} — last page reached"
                )
                break

            await asyncio.sleep(_PAGE_DELAY_SECONDS)
            page += 1

        logger.info(
            f"[{self.source_domain}] Phase 1 done: "
            f"{len(all_seeds)} unique seeds across {page} page(s)"
        )
        return list(all_seeds.values())

    def _parse_listing_card(
        self,
        card: LexborNode,
    ) -> Optional[PropertyTemplate]:
        """
        Extract one listing card → PropertyTemplate seed.

        Card structure (after .advert_item filter):
            <div class="advert_item">
              <img src=".../properties/{type_code}-{id}{hash}.jpg">
              <div class="advertRegion">{region}</div>            ← location_raw hint
              <div class="advertTitleAbsDiv">
                <a href="property.php?id={id}">
                  <div>{Type}</div>                                ← category hint
                  <div>{rooms} Rooms. | {price}€</div>
                </a>
              </div>
              <div class="advertShowMoreDiv">
                <a href="property.php?id={id}">
                  <div class="advertInfo">
                    <div>{Type}</div>     ← [0]
                    <div>{region}</div>   ← [1]
                    <div>Code {code}</div><!-- [2] e.g. "Code 4-1631" -->
                    <div>{price}€</div>   ← [3] authoritative price text
                    <div>{rooms} Rooms.</div> ← [4]
                    <div>{size}.00 sq.m.</div> ← [5]
                  </div>
                </a>
              </div>
              <button onclick="setFav({id}, ...)">
            </div>

        Returns None for cards missing url or site_property_id (defensive —
        shouldn't happen after .advert_item filter, but tolerate site quirks).
        """
        # ─── URL + site_property_id ───────────────────────────────────
        # Either of the two <a> elements points to property.php?id=N.
        link = card.css_first("a[href^='property.php']")
        if not link:
            return None
        href = link.attributes.get("href")
        if not href:
            return None
        url = _resolve_url(href)
        site_id = _extract_id_from_url(url)
        if not site_id:
            return None

        # ─── Region (used for Halkidiki whitelist + location_raw) ─────
        region_node = card.css_first(".advertRegion")
        region = _normalize_text(region_node.text(strip=False)) if region_node else ""

        # ─── Category hint (from title block) ─────────────────────────
        # First <div> child of .advertTitleAbsDiv > a is the type label.
        category: Optional[str] = None
        cat_node = card.css_first(".advertTitleAbsDiv > a > div:first-child")
        if cat_node:
            cat_raw = _normalize_text(cat_node.text(strip=False))
            category = _CATEGORY_MAP.get(cat_raw)
            # Fall back to verbatim if unknown — Phase 2 spec panel will
            # override anyway via the authoritative "Type" row.
            if category is None and cat_raw:
                category = cat_raw

        # ─── Structured fields from .advertInfo ───────────────────────
        # 6 divs in fixed order: type, region, code, price, rooms, size.
        info_divs = card.css(".advertInfo > div")
        info_texts = [_normalize_text(d.text(strip=False)) for d in info_divs]

        price_hint: Optional[int] = None
        bedrooms_hint: Optional[int] = None
        size_hint: Optional[float] = None
        if len(info_texts) >= 6:
            price_hint = _to_int_euro_gr(info_texts[3])
            bedrooms_hint = _to_int_simple(info_texts[4])
            size_hint = _to_float_sqm(info_texts[5])

        # ─── Main image hint (full gallery collected in Phase 2) ──────
        img_node = card.css_first("img")
        main_image = (img_node.attributes.get("src") if img_node else "") or ""
        images: Optional[List[str]] = None
        if main_image and "logo" not in main_image.lower() and not main_image.endswith(".svg"):
            images = [main_image]

        # ─── location_raw — required for Halkidiki whitelist filter ───
        # daily_sync filters seeds by region keywords in url+location_raw.
        # Always append "Halkidiki" to ensure whitelist match even for
        # cards with empty .advertRegion (defensive — shouldn't happen).
        if region:
            location_raw = f"{region}, Halkidiki"
        else:
            location_raw = "Halkidiki"

        seed = PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=url,
            category=category,
            price=price_hint,
            bedrooms=bedrooms_hint,
            size_sqm=size_hint,
            location_raw=location_raw,
        )
        # Phase 1 hints — Phase 2's detail page parse overrides these where
        # better data exists.
        if region:
            seed.area = region  # type: ignore[attr-defined]
        if images:
            seed.images = images  # type: ignore[attr-defined]
        return seed

    # =========================================================================
    # PHASE 2 — fetch_details (canonical 7-step pipeline)
    # =========================================================================

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """Detail page parse. Returns dict for merge with Phase 1 seed."""
        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.error(
                f"[{self.source_domain}] detail fetch failed for {url}: {exc!r}"
            )
            return {}

        if not response.text:
            return {}

        parser = LexborHTMLParser(response.text)
        data: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # ── Step 1: Structured spec panel (authoritative for top-level cols) ──
        self._parse_spec_panel(parser, data, extra)

        # ── Step 1b: Amenities list (#amentities-tab) ─────────────────
        self._parse_amenities(parser, extra)

        # ── Step 2: Description → og:description fallback ─────────────
        desc_node = parser.css_first(".propertyDesc")
        description = ""
        if desc_node:
            description = _normalize_text(desc_node.text(strip=False))
        if not description:
            description = self._og_description_fallback(parser) or ""
        if description:
            data["description"] = description

        # ── Step 2b: Village extraction from description ──────────────
        # Description starts: "... in {Municipality} {Village} for € {price}..."
        # E.g. "in Kassandra Kriopigi for € 430.000". When this pattern hits,
        # the last word is the most specific area name → useful for GeoMatcher
        # fallback when coords are missing/wrong.
        if description:
            m = re.search(
                r"\bin\s+([A-Z][\w\s\-]+?)\s+for\s+€",
                description,
            )
            if m:
                loc_str = m.group(1).strip()
                parts = loc_str.split()
                if len(parts) >= 2:
                    # First word = municipality, last word = village
                    village = parts[-1]
                    municipality_first = parts[0]
                    data["area"] = village
                    muni = _REGION_TO_MUNICIPALITY.get(municipality_first)
                    if muni:
                        extra["municipality"] = muni

        # ── Step 3: Coords (Leaflet setView regex + bbox sanity) ──────
        lat, lng = self._extract_coordinates(response.text)
        if lat is not None and lng is not None:
            if _bbox_check(lat, lng):
                data["latitude"] = lat
                data["longitude"] = lng
            else:
                logger.debug(
                    f"[{self.source_domain}] coords {lat},{lng} outside "
                    f"Halkidiki bbox — ignoring"
                )

        # ── Step 4: Images (regex on raw HTML — see _extract_images note) ──
        images = self._extract_images(response.text)
        if not images:
            og = self._og_image_fallback(parser)
            if og:
                images = [og]
        if images:
            data["images"] = images

        # Merge extras BEFORE NLP so semantic dedup can inspect them
        if extra:
            data["extra_features"] = extra

        # ── Step 5: NLP fallback (fills levels from description) ──────
        self._apply_nlp_fallback(data)

        # ── Step 6: SKIP LLM ──
        # 19 explicit amenities from structural parse vastly exceed
        # LLM trigger threshold (`extra_features < 5`).

        # ── Step 7: Quality Gate (log-only — daily_sync handles retry) ──
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate "
                f"for {url}"
            )

        return data

    # =========================================================================
    # Phase 2 step helpers
    # =========================================================================

    def _parse_spec_panel(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Walk `.mad-property-list ul li` rows. Each <li> has exactly 2 spans:
            <span>{label}</span><span>{value}</span>
        Route to top-level columns or extras via _SPEC_PANEL_FIELDS.
        """
        for li in parser.css(".mad-property-list ul li"):
            spans = li.css("span")
            if len(spans) < 2:
                continue
            label = _normalize_text(spans[0].text(strip=False))
            value = _normalize_text(spans[1].text(strip=False))
            if not label or not value:
                continue

            target = _SPEC_PANEL_FIELDS.get(label)
            if not target or target == "_skip":
                continue

            if target.startswith("_extra:"):
                extra_key = target[len("_extra:"):]
                if extra_key == "price_per_sqm":
                    # Numeric — parse as euros
                    v = _to_int_euro_gr(value)
                    if v is not None:
                        extra[extra_key] = v
                else:
                    # Verbatim string (e.g. floor = "Ground floor")
                    extra[extra_key] = value
                continue

            # Top-level column routing with appropriate type coercion
            if target == "price":
                v = _to_int_euro_gr(value)
                if v is not None:
                    data["price"] = v
            elif target == "year_built":
                v = _to_int_simple(value)
                if v is not None and 1800 <= v <= 2100:
                    data["year_built"] = v
            elif target == "category":
                # Look up canonical form; fall back to verbatim
                cat = _CATEGORY_MAP.get(value)
                data["category"] = cat if cat else value
            elif target == "size_sqm":
                v = _to_float_sqm(value)
                if v is not None:
                    data["size_sqm"] = v
            elif target == "land_size_sqm":
                v = _to_float_sqm(value)
                if v is not None:
                    data["land_size_sqm"] = v
            elif target == "bedrooms":
                v = _to_int_simple(value)
                if v is not None:
                    data["bedrooms"] = v

    def _parse_amenities(
        self,
        parser: LexborHTMLParser,
        extra: Dict[str, Any],
    ) -> None:
        """
        Walk `#amentities-tab .mad-list--icon li.propertyMore` for amenities.
        (Note: site has typo `amentities` not `amenities` — preserved verbatim.)

        Each <li> has <span>{amenity_text}</span><i>check_circle</i>. Text
        is either:
          * Boolean amenity: "Garden", "A/C", "Furnished"
          * Key:Value: "View: Sea", "Energy class: B+", "Floors: Tiles"
          * Compound key:value: "View: Sea, Mountain" (rare)
        """
        for li in parser.css(".mad-list--icon li.propertyMore"):
            span = li.css_first("span")
            if not span:
                continue
            text = _normalize_text(span.text(strip=False))
            if not text:
                continue
            self._route_amenity(text, extra)

    def _route_amenity(self, text: str, extra: Dict[str, Any]) -> None:
        """
        Parse one amenity line into extras.

        Shape detection:
          * Has colon  → "Label: Value" pair (special handling per label)
          * No colon   → Boolean amenity (slug → True)

        Special handlers (when ":" is present):
          * "View: {dirs}"     → {dir}_view = True for each comma-separated dir
          * "Energy class: X"  → energy_class = "X" (preserve grade form like B+)
          * "Transport: Bus"   → transport_bus = True
          * "Frames: {type}"   → frames = "{type}"
          * "Floors: {type}"   → floor_type = "{type}"
          * Everything else    → {slug(label)} = "{value}"
        """
        if ":" in text:
            label, _, value = text.partition(":")
            key = _slug(label.strip())
            val = value.strip()
            if not val:
                return

            if key == "view":
                # "Sea, Mountain" → sea_view + mountain_view
                for v in val.split(","):
                    v = v.strip().lower()
                    if v:
                        v_slug = _slug(v)
                        if v_slug:
                            extra[f"{v_slug}_view"] = True
            elif key == "energy_class":
                extra["energy_class"] = val
            elif key == "transport":
                v_slug = _slug(val)
                if v_slug:
                    extra[f"transport_{v_slug}"] = True
            elif key == "frames":
                extra["frames"] = val
            elif key == "floors":
                extra["floor_type"] = val
            elif key == "heating_system":
                extra["heating"] = val
            else:
                # Unknown labeled amenity — store verbatim under slugged key
                extra[key] = val
        else:
            # Boolean amenity — presence alone signals truth
            slug = _slug(text)
            if not slug:
                return
            # Apply canonicalization to match other scrapers' vocabulary
            slug = _AMENITY_SLUG_MAP.get(slug, slug)
            extra[slug] = True

    def _extract_coordinates(
        self,
        html_text: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Find Leaflet `setView([lat, lng], zoom)` in inline <script>.

        Page has multiple coord-bearing calls (setView, L.marker, L.circle) —
        all reference the same point. We use setView as it's the most stable
        identifier (Leaflet API contract for map centering).
        """
        m = re.search(
            r"setView\(\[\s*([\d.\-]+)\s*,\s*([\d.\-]+)\s*\]",
            html_text,
        )
        if not m:
            return None, None
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            return None, None

    def _extract_images(self, html_text: str) -> List[str]:
        """
        Collect property images via regex over raw HTML text.

        Why regex instead of CSS selectors
        ----------------------------------
        Site uses Owl Carousel for image galleries. Owl's `loop: true` mode
        wraps each slide in <div class="owl-item">…</div> CLIENT-SIDE via
        JavaScript at carousel init time. It also CREATES `<div class="owl-item
        cloned">` copies of edge slides for seamless infinite scroll.

        BROWSER DOM (what DevTools "Inspect" shows): contains all the
        `.owl-item` / `.owl-item.cloned` wrappers.

        SERVER RESPONSE (what curl_cffi receives, JS not executed): does NOT
        contain `.owl-item` wrappers. The images are present in the raw HTML
        but inside whatever pre-init container the site outputs (likely
        plain <img> children of `#sync1` etc., or wrapped in `.mad-grid-item`).

        Initial implementation used `.owl-item:not(.cloned) img` selector —
        returned 0 matches because server-side `.owl-item` doesn't exist.
        Fallback then picked up og:image (single cover image), explaining
        the smoke-test result of `images: 1`.

        Solution: regex match `/images/properties/…` paths in the raw HTML
        body. Image URL pattern is consistent and easy to identify:
            https://edmaestate.gr/images/properties/{type}-{site_id}{hash}.jpg

        The regex is wrapped in case-insensitive mode because some image
        extensions appear uppercased (e.g. `.JPG`). `agents/` subdirectory
        excluded via negative lookahead to skip agent profile photos.

        Dedupe by URL — multiple Owl carousels (#sync1, #sync2, #sync3) reference
        the same images, and pre-Owl server HTML may also duplicate references.
        """
        if not html_text:
            return []

        url_pattern = re.compile(
            r'https?://(?:www\.)?edmaestate\.gr/images/properties/'
            r'(?!agents/)'                          # exclude agent profile photos
            r'[^\s"\'<>]+\.(?:jpe?g|png|webp)',
            re.IGNORECASE,
        )

        seen: set = set()
        images: List[str] = []
        for src in url_pattern.findall(html_text):
            if src in seen:
                continue
            seen.add(src)
            images.append(src)
        return images
