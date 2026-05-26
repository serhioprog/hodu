"""
~/hodu/src/scrapers/dionisiou_realestate.py

Dionisiou Real Estate (dionisiou-realestate.com) — WordPress 4.7.29 +
Inspiry Real Places theme. A friendly target: NO anti-bot detected,
Stage 0 (curl_cffi) sufficient for both listing and detail pages.

Scale: ~195 properties at min_price=400000 across ~51 Halkidiki villages.
Russian counterpart: dionisiou-realestate.ru (separate scraper, NOT here).

URL patterns
------------
List (single page — see Strategy):
    /for-sale/?property-id=&location=any&type=any&bedrooms=any
        &min-price=400000&max-price=any...

Detail (two interchangeable forms — both work, same content):
    /sale/{slug}-{id}/         ← URL used in listing JSON; we use this
    /property/{slug}-{id}/     ← canonical (rel=canonical) link

Strategy (single-fetch URL collection)
--------------------------------------
The Inspiry "Real Places" theme renders a Google Map on the for-sale page
that needs ALL filtered properties' coordinates upfront — so it embeds
the complete result set in a `var properties = [...]` JS array inside
initializePropertiesMap(). One HTTP GET to /for-sale/?min-price=400000
returns all 195 seeds (title, price, lat, lng, thumb, icon, url) — we
skip pagination crawl entirely.

Each JSON item:
    {"title":"Maisonetta, Hanioti","price":"400,000€",
     "lat":40.001,"lng":23.576,
     "thumb":"...850x570.jpg","url":"https://.../sale/maisonetta-hanioti-329-1/",
     "icon":"...maisonetta-map-icon.png"}

Detail-page anatomy
-------------------
1. H1 at `.single-property-title` with three <span> children:
       <span class="entry-title">Maisonetta, Hanioti</span>
       <span class="entry-city">Hanioti</span>
       <span class="entry-id">ID number: 329-1</span>
   The `entry-id` text after "ID number:" is the AUTHORITATIVE site ID
   (e.g. "329-1"). The URL slug also encodes this but is fragile to
   slug-rename drift.

2. Price: `<span class="single-property-price price">400,000€</span>`
   Discount markers (rare): `.pricestrike` on the original price +
   `.property-action-price` showing the discounted price. When present,
   the strike price is the "old" price and we record both.

3. Meta items: `.property-meta .meta-item` blocks (also present on the
   listing card). Each has:
       <i class="meta-item-icon icon-{key}" title="{label}">
       <div class="meta-inner-wrapper">
         <span class="meta-item-label">{label}</span>
         <div class="meta-item-value">{value}</div>   ← may be absent
       </div>
   When the value div is absent, the field is a boolean amenity
   (presence = True). Known keys (icon class suffix):
     floor, area, construction_year, beds, living_rooms, baths,
     wc_room, parking, private_garden, sea_view, swimming_pool,
     beach_dist, ap_dist, shops_dist, airconditions, fireplace,
     heating_system.

4. Property type: AUTHORITATIVE source is the breadcrumb link
   `<a href=".../property-type/{slug}/">`. The slug maps to:
     apartment | maisonetta | house-villa | land | business
   The detail page's own propertyMarkerInfo.icon is UNRELIABLE
   (verified — a maisonette listing showed single-family-home icon).
   The LISTING JSON icon is reliable as a Phase-1 hint.

5. Description: `.property-content p` (inside `.entry-content`).
   Deeply nested in Visual Composer wrappers (vc_row > wpb_column >
   vc_column-inner > wpb_wrapper > wpb_text_column > wpb_wrapper > p),
   but `.property-content p` flat-selects them. Internal <a> links
   point at village pages (/hanioti/) — `.text()` strips them cleanly.

6. Coordinates: parsed from JS literal
       var propertyMarkerInfo = {"lat":40.001, "lng":23.576, "icon":"..."}
   The listing JSON coords are AUTHORITATIVE; this is a fallback for
   listings that somehow lack the listing-JSON entry. Both sources
   sometimes contain wrong coords (e.g. Kalyves listing showed Crete
   coords) — Halkidiki bbox sanity check applies.

7. Image gallery: `<ul id="image-gallery">` with `<li>` items. Skip the
   slider clones — first `<li class="clone left">` and last
   `<li class="clone right">` are duplicates of the boundary slides
   (lightSlider lib's loop mechanism). For real slides:
       <li class="lslide [active]"><a class="swipebox" href="FULL.jpg">
         <img src="..._850x570.jpg" alt="...">
       </a></li>
   Use the `<a href>` (full size); `<img src>` is a thumbnail variant.

8. Virtual tour (optional): `<a class="video-popup" href="youtube..."/>`.
   Stored in extra_features.virtual_tour_url.

9. WordPress post ID is in body class: `postid-8295`. Stored in extras
   as wp_post_id for cross-reference.

Anti-bot
--------
None. No challenge, no nonce, no JS gating. WordPress/Inspiry theme is
a friendly target. If the site ever adds Cloudflare etc. the funnel
will auto-escalate to Stage 1 via scraper_routing.preferred_stage.

Title parsing — when used
-------------------------
Listing title format: "{Type}, {Area}" or "{Type} with {feature}, {Area}"
or "{Type} in front of the beach, {Area}". We do NOT rely on title
parsing for category (use icon/breadcrumb) or area (use entry-city).
Title is kept verbatim in extras.listing_title for review.
"""
from __future__ import annotations

import asyncio
import json
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

_BASE_URL = "https://dionisiou-realestate.com"
_LISTING_PATH = "/for-sale/"
_SOURCE_DOMAIN = "dionisiou-realestate.com"

# Polite pause between detail-page fetches is handled by BaseScraper's
# funnel-level rate limiter. Phase 1 is a single GET so no inter-page sleep.

# Hard cap on properties returned from Phase 1 — defensive against runaway
# JS array sizes (e.g. if the site stops filtering server-side and dumps
# the entire inventory). At time of writing site has ~195 ≥ €400k.
_PHASE1_MAX_SEEDS = 3000


# =============================================================================
# Category routing
# =============================================================================
#
# Listing JSON exposes one of these icon filenames per property. Map →
# canonical category names. Verified by enumerating property-type taxonomy
# in site footer:
#   Apartment (129)  Maisonetta (164)  House (Villa) (188)
#   Land (160)       Business (49)
#
# Note: dionisiou's term is "Maisonetta" (Greek-flavoured); we canonicalise
# to "Maisonette" to match the rest of hodu's vocabulary (halkidiki_agency,
# grekodom_development, etc. all use "Maisonette").
_ICON_TO_CATEGORY: Dict[str, str] = {
    "house-villa-map-icon.png":         "House (Villa)",
    "maisonetta-map-icon.png":          "Maisonette",
    "apartment-map-icon.png":           "Apartment",
    "business-map-icon.png":            "Business",
    "single-family-home-map-icon.png":  "House",
    # Defensive: any other icon name → unknown (category stays None)
}

# Authoritative source on the DETAIL page: breadcrumb link to taxonomy term.
# Slug appears in URL path `/property-type/{slug}/`.
_BREADCRUMB_SLUG_TO_CATEGORY: Dict[str, str] = {
    "apartment":     "Apartment",
    "maisonetta":    "Maisonette",
    "house-villa":   "House (Villa)",
    "land":          "Land",
    "business":      "Business",
}


# =============================================================================
# Halkidiki village → municipality routing
# =============================================================================
#
# 51 villages observed in the property-city footer taxonomy. Mapping these
# to the three peninsulas + mainland helps downstream clustering and the
# admin UI's region filter. Municipality strings follow Greek admin
# nomenclature (Kassandra, Sithonia, Athos, Polygyros, Thermaikos, Aristotle).
#
# Used to populate the optional `municipality` field on PropertyTemplate
# extras; NOT used for filtering (the search URL already restricts to
# Halkidiki via the site's location dropdown).
_CITY_TO_MUNICIPALITY: Dict[str, str] = {
    # Kassandra peninsula
    "afytos":          "Kassandra",
    "kallithea":       "Kassandra",
    "kriopigi":        "Kassandra",
    "polychrono":      "Kassandra",
    "hanioti":         "Kassandra",
    "pefkohori":       "Kassandra",
    "paliouri":        "Kassandra",
    "agia_paraskevi":  "Kassandra",
    "nea_skioni":      "Kassandra",
    "possidi":         "Kassandra",
    "kalandra":        "Kassandra",
    "fourka":          "Kassandra",
    "kassandria":      "Kassandra",
    "kassandra":       "Kassandra",          # bare peninsula name
    "siviri":          "Kassandra",
    "sani":            "Kassandra",
    "elani":           "Kassandra",
    "loutra":          "Kassandra",
    "mola_kaliva":     "Kassandra",
    "nea_fokea":       "Kassandra",
    # Sithonia peninsula
    "nikiti":          "Sithonia",
    "neos_marmaras":   "Sithonia",
    "sarti":           "Sithonia",
    "toroni":          "Sithonia",
    "sikia":           "Sithonia",
    "vourvourou":      "Sithonia",
    "porto_koufo":     "Sithonia",
    "ormos_panagias":  "Sithonia",
    "metamorfosi":     "Sithonia",
    "psakoudia":       "Sithonia",
    "agios_nicolaos":  "Sithonia",
    # Aristotelis peninsula (DB/geo canonical name; "Athos" is monastic state)
    "ouranoupoli":     "Aristotelis",
    "ierissos":        "Aristotelis",
    "ammouliani":      "Aristotelis",
    "nea_roda":        "Aristotelis",
    "pirgadikia":      "Aristotelis",
    "stratoni":        "Aristotelis",
    "olympiada":       "Aristotelis",
    "athos":           "Aristotelis",
    # Polygyros (mainland Halkidiki)
    "gerakini":        "Polygyros",
    "agios_mamas":     "Polygyros",
    "akti_salonikiou": "Polygyros",
    "kalyves":         "Polygyros",
    "poligiros":       "Polygyros",
    "zografou":        "Polygyros",
    "dionisiou_beach": "Polygyros",
    # Nea Propontida (northern Halkidiki / Thessaloniki suburbs)
    "flogita":         "Nea Propontida",
    "kallikratia":     "Nea Propontida",
    "nea_iraklia":     "Nea Propontida",
    "nea_moudania":    "Nea Propontida",
    "nea_plagia":      "Nea Propontida",
    "nea_potidea":     "Nea Propontida",
    "triglia_paralia": "Nea Propontida",
    "sozopoli":        "Nea Propontida",
}


# =============================================================================
# Meta-item routing
# =============================================================================
#
# Keys are the icon-class suffix (e.g. "icon-beach_dist" → "beach_dist").
# Values:
#   "_skip"        → ignore
#   "<column>"     → set top-level PropertyTemplate.<column>
#   "_extra:<key>" → put parsed value into extra_features[<key>]
#   "_bool:<key>"  → boolean flag (presence = True even if no value div)
#                    NOTE: if a value IS present, we still store True (the
#                    flag's semantic is "amenity exists"); the descriptive
#                    text (e.g. "yes" / "3 units") goes to <key>_detail.
_META_ICON_TO_FIELD: Dict[str, str] = {
    "floor":             "_extra:floor",
    "area":              "size_sqm",
    "construction_year": "year_built",
    "beds":              "bedrooms",
    "living_rooms":      "_extra:living_rooms_count",
    "baths":             "bathrooms",
    "wc_room":           "_bool:wc",
    "parking":           "_bool:parking",
    "private_garden":    "_extra:private_garden_sqm",
    "sea_view":          "_bool:sea_view",
    "swimming_pool":     "_bool:swimming_pool",
    "fireplace":         "_bool:fireplace",
    "heating_system":    "_extra:heating",
    "beach_dist":        "_extra:distance_to_beach_m",
    "ap_dist":           "_extra:distance_to_airport_km",
    "shops_dist":        "_extra:distance_to_shop_m",
    "airconditions":     "_bool:air_conditioning",
    # Defensive: unknown icon keys fall through to _slug() storage
}


# =============================================================================
# Regexes
# =============================================================================

# Anchors for the start of the JS literal — used by the bracket-walking
# extractors below. NOT used to capture the literal itself: regex can't
# do balanced-bracket matching, and the real page omits the trailing `;`
# after the array (JS ASI), which would have caused a `\]\s*;` pattern
# to over-match into the next `var X = ...;` block. Confirmed live:
# regex-based capture failed with "Extra data: char 84456" because the
# non-greedy `.*?` extended past the array's `]` looking for `];`.
_PROPERTIES_ANCHOR_RE = re.compile(r"var\s+properties\s*=\s*")
_PROPERTY_MARKER_ANCHOR_RE = re.compile(r"var\s+propertyMarkerInfo\s*=\s*")

# Site ID from URL slug. Patterns observed:
#   /sale/maisonetta-hanioti-329-1/   → "329-1"
#   /sale/villa-pefkohori-12345/      → "12345"
#   /sale/villa-pefkohori-12345-2/    → "12345-2"
# The trailing /\d+(-\d+)?/?$ captures both. Anchored at end-of-path.
_URL_ID_RE = re.compile(r"/sale/[^/]+?-(\d+(?:-\d+)?)/?(?:\?|$)")

# Detail page entry-id: <span class="entry-id">ID number: 329-1</span>
_ENTRY_ID_RE = re.compile(r"ID\s*number\s*:\s*([\w-]+)", re.IGNORECASE)

# Breadcrumb property-type link slug from href.
_PROPERTY_TYPE_RE = re.compile(r"/property-type/([^/]+)/")

# WP post ID from body class: `class="... postid-8295 ..."`
_WP_POSTID_RE = re.compile(r"\bpostid-(\d+)\b")

# Title typical format: "{Type}, {Area}" or "{Type} <phrase>, {Area}"
# We just need to split off the trailing area.
_TITLE_AREA_RE = re.compile(r",\s*([^,]+)\s*$")


# =============================================================================
# Helpers — pure functions, no scraper state
# =============================================================================

def _normalize_text(s: Optional[str]) -> str:
    """Collapse whitespace, decode &nbsp; and narrow-NBSP, strip."""
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


def _slug(label: str) -> str:
    """Free-form label → stable snake_case key."""
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_int_simple(text: str) -> Optional[int]:
    """First integer in string. '90 km' → 90, '2 ' → 2."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """Parse area-in-square-meters strings. '100 m²' → 100.0, '450' → 450.0."""
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
    Parse dionisiou price strings (English locale: comma = thousand sep).

    Examples:
      "400,000€"       → 400000
      "€ 1,500,000"    → 1500000
      "Price on request" → None

    Sanity-capped at 200M EUR via the inherited mixin constant.
    """
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    lowered = cleaned.lower()
    if "request" in lowered or "poa" in lowered:
        return None
    m = re.search(r"[\d,]+", cleaned)
    if not m:
        return None
    digits = m.group(0).replace(",", "")
    if not digits or not digits.isdigit():
        return None
    try:
        value = int(digits)
    except ValueError:
        return None
    if value > EnrichmentMixin._PRICE_SANITY_CAP:
        return None
    return value


def _extract_balanced_literal(
    text: str,
    anchor_re: re.Pattern,
    open_ch: str,
    close_ch: str,
) -> Optional[str]:
    """
    After an anchor regex match, walk forward through `text` from the
    next `open_ch` and return the substring up to (and including) its
    BALANCED `close_ch`. Respects string literals — brackets inside
    `"..."` (with `\\` escape handling) don't count toward depth.

    Used for both:
      * `var properties = [...]`  (open='[', close=']')
      * `var propertyMarkerInfo = {...}`  (open='{', close='}')

    This is necessary because Python's `re` module can't do balanced-
    bracket matching, and the regex shortcut `\\[.*?\\]\\s*;` over-matches
    when the JS array isn't followed by a semicolon (which is allowed
    in JavaScript via ASI — and which dionisiou-realestate.com actually
    does on its for-sale page).

    Returns the literal text (including the outer brackets) or None if
    the anchor isn't found or the brackets don't balance.
    """
    if not text:
        return None
    m = anchor_re.search(text)
    if not m:
        return None

    n = len(text)
    i = m.end()
    # Skip optional whitespace (the anchor already consumed `=\\s*` but be
    # defensive in case the regex changes).
    while i < n and text[i].isspace():
        i += 1
    if i >= n or text[i] != open_ch:
        return None

    start = i
    depth = 0
    in_str = False
    escape = False

    while i < n:
        ch = text[i]
        if escape:
            # Previous char was a backslash; consume this one literally
            # whatever it is, then resume normal state.
            escape = False
        elif in_str:
            if ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    # Found the matching closer
                    return text[start:i + 1]
        i += 1

    # Walked off the end without balancing — malformed JS
    return None


def _extract_id_from_url(url: str) -> Optional[str]:
    """
    Extract the site_id from a /sale/ URL slug.

    /sale/maisonetta-hanioti-329-1/   → "329-1"
    /sale/villa-pefkohori-12345/      → "12345"

    IDs can be compound (parent-listing-with-sub-units) hence the optional
    ``-\\d+`` tail. Returns None on unparseable URLs.
    """
    if not url:
        return None
    m = _URL_ID_RE.search(url)
    return m.group(1) if m else None


def _extract_icon_filename(icon_url: str) -> str:
    """`.../wp-content/.../maisonetta-map-icon.png` → `maisonetta-map-icon.png`."""
    if not icon_url:
        return ""
    return icon_url.rstrip("/").rsplit("/", 1)[-1]


def _meta_key_from_icon_class(class_attr: str) -> Optional[str]:
    """
    From `meta-item-icon icon-beach_dist` extract `beach_dist`.

    Returns None if no recognisable `icon-{key}` token is present.
    """
    if not class_attr:
        return None
    for token in class_attr.split():
        if token.startswith("icon-") and len(token) > 5:
            return token[5:]
    return None


def _strip_units(text: str) -> str:
    """
    Helper for value cells with units: '100 m²' → '100', '90 km' → '90',
    '300 m' → '300'. Preserves non-numeric tokens that aren't unit suffixes.
    """
    if not text:
        return text
    cleaned = text.replace("\xa0", " ")
    # Drop common metric/imperial units when paired with digits.
    return re.sub(
        r"\s*(?:m²|m2|sq\.?m|sqm|km|m|mi|ft)\b\.?",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()


# =============================================================================
# Scraper
# =============================================================================

class DionisiouRealEstateScraper(EnrichmentMixin, BaseScraper):
    """
    dionisiou-realestate.com — WordPress Inspiry Real Places theme.

    Two-phase canonical pattern:
      Phase 1: collect_urls(min_price) → List[PropertyTemplate] seeds
      Phase 2: fetch_details(url)       → Dict[str, Any] for one property

    Phase 1 is a SINGLE GET (the Inspiry map widget dumps the full filtered
    result-set into a JS array on the first page); no pagination crawl.

    Inherits canonical enrichment helpers (EnrichmentMixin):
      * _apply_nlp_fallback()       fills missing columns + dedup'd extras
      * _og_description_fallback()  fallback when site description thin
      * _og_image_fallback()        fallback when gallery yields nothing
      * _passes_quality_gate()      log-only description quality check

    NOT used: LLM fallback. Descriptions on Inspiry theme are structured
    (typically 200-1000 chars across 3-5 <p> tags) and the meta-item +
    breadcrumb signal-rich; LLM adds noise here. If a future listing has
    a degenerate description we fall back to og:description and the
    quality gate logs a warning for manual review.
    """

    # ── Mixin overrides ──────────────────────────────────────────────────

    # Category is sourced from breadcrumb /property-type/{slug}/ (detail
    # page) or from the listing JSON icon (Phase 1 seed). Excluding it
    # from NLP fill prevents the regex from overwriting our authoritative
    # value with its own canonical form (e.g. NLP says "House" when the
    # breadcrumb says "House (Villa)").
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )

    # Semantic dedup map — when NLP regex emits these keys but our
    # structural parse already filled the equivalent extra slug, skip
    # the NLP duplicate. Keys in this map are NLP-side canonical names;
    # value sets are the structural-side slugs that should suppress them.
    _NLP_TO_STRUCTURAL = {
        "swimming_pool":    {"swimming_pool", "pool"},
        "sea_view":         {"sea_view", "view"},
        "parking":          {"parking"},
        "fireplace":        {"fireplace"},
        "air_conditioning": {"air_conditioning", "airconditions", "a_c", "ac"},
        "heating":          {"heating", "heating_system"},
        "balcony":          {"balcony", "balconies"},
        "storage_room":     {"storage", "storage_space"},
        "garden":           {"private_garden", "private_garden_sqm", "garden"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builders ─────────────────────────────────────────────────────

    def _construct_listing_url(self, min_price: int) -> str:
        """
        Build the for-sale search URL with all default-blank query params
        the site form sends. Including ALL of them (even when empty)
        minimises risk that a future WAF rule classifies our request as
        non-form-origin.
        """
        params = [
            "property-id=",
            "location=any",
            "type=any",
            "bedrooms=any",
            f"min-price={min_price}",
            "max-price=any",
            "min-area=any",
            "max-area=any",
            "private_garden=any",
            "beach-dist=any",
            "ap-dist=any",
            "construction_year=any",
            "min-building_allowance=any",
            "max-building_allowance=any",
        ]
        return f"{_BASE_URL}{_LISTING_PATH}?{'&'.join(params)}"

    # ── Phase 1: collect_urls (single-GET via JS array) ──────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = 1,   # accepted for interface symmetry; unused
    ) -> List[PropertyTemplate]:
        """
        Fetch the for-sale page once, extract `var properties = [...]` JS
        array, and emit one PropertyTemplate seed per item.

        The `max_pages` parameter is accepted but ignored — by design,
        because the Inspiry map widget always renders the full filtered
        set in a single payload. Keeping the parameter preserves the
        BaseScraper.collect_urls contract.
        """
        url = self._construct_listing_url(min_price=min_price)
        logger.info(f"[{self.source_domain}] Phase 1: GET {url}")

        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.error(
                f"[{self.source_domain}] listing fetch failed: {exc!r}"
            )
            return []

        html_text = response.text or ""
        if not html_text:
            logger.error(f"[{self.source_domain}] empty listing response")
            return []

        # ── Pull out the `var properties = [...]` array ─────────────────
        # Use a bracket-balanced walker rather than a `\[.*?\]` regex —
        # the real for-sale page omits the trailing `;` after the array
        # (JS ASI is fine with that), which causes a non-greedy regex
        # to extend past the array's `]` looking for `];` and capture
        # whatever JS comes next. See `_extract_balanced_literal()`.
        raw_json = _extract_balanced_literal(
            html_text, _PROPERTIES_ANCHOR_RE, "[", "]"
        )
        if raw_json is None:
            logger.error(
                f"[{self.source_domain}] no `var properties = [...]` block "
                f"found on listing page — page layout may have changed"
            )
            return []

        try:
            items = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            logger.error(
                f"[{self.source_domain}] properties array JSON parse "
                f"failed: {exc!r} (first 200 chars: {raw_json[:200]!r}, "
                f"last 200 chars: {raw_json[-200:]!r})"
            )
            return []

        if not isinstance(items, list):
            logger.error(
                f"[{self.source_domain}] properties array is not a list "
                f"(got {type(items).__name__}) — aborting Phase 1"
            )
            return []

        if len(items) > _PHASE1_MAX_SEEDS:
            logger.warning(
                f"[{self.source_domain}] properties array has "
                f"{len(items)} items, exceeding cap {_PHASE1_MAX_SEEDS} — "
                f"truncating defensively"
            )
            items = items[:_PHASE1_MAX_SEEDS]

        seeds: Dict[str, PropertyTemplate] = {}
        skipped_no_id = 0
        skipped_no_url = 0
        skipped_dupe = 0

        for item in items:
            seed = self._parse_json_item(item)
            if seed is None:
                if not item.get("url"):
                    skipped_no_url += 1
                else:
                    skipped_no_id += 1
                continue
            if seed.site_property_id in seeds:
                skipped_dupe += 1
                continue
            seeds[seed.site_property_id] = seed

        logger.info(
            f"[{self.source_domain}] Phase 1 завершён: "
            f"{len(seeds)} URLs из {len(items)} JSON-items "
            f"(skipped: {skipped_no_url} no-url, {skipped_no_id} no-id, "
            f"{skipped_dupe} dupes)"
        )
        return list(seeds.values())

    def _parse_json_item(self, item: Dict[str, Any]) -> Optional[PropertyTemplate]:
        """
        One listing-JSON item → PropertyTemplate seed.

        Returns None if the item is missing critical fields (URL or ID).
        Coordinates from the JSON are kept as-is for Phase 1; the
        Halkidiki-bbox sanity check happens in Phase 2 (where we have
        more context if the JSON coord is bad).
        """
        if not isinstance(item, dict):
            return None

        url = (item.get("url") or "").strip()
        if not url:
            return None

        site_id = _extract_id_from_url(url)
        if not site_id:
            return None

        # ─── Title / area (city) ────────────────────────────────────────
        title_raw = _normalize_text(item.get("title") or "")
        area: Optional[str] = None
        if title_raw:
            m = _TITLE_AREA_RE.search(title_raw)
            if m:
                area = m.group(1).strip()

        # ─── Price ──────────────────────────────────────────────────────
        price_raw = item.get("price") or ""
        price = _to_int_euro_en(price_raw)

        # ─── Coordinates (best-effort; sanity-checked in Phase 2) ──────
        lat: Optional[float] = None
        lng: Optional[float] = None
        try:
            if item.get("lat") is not None:
                lat = float(item["lat"])
            if item.get("lng") is not None:
                lng = float(item["lng"])
        except (TypeError, ValueError):
            lat, lng = None, None

        # ─── Category from icon (PRIMARY in Phase 1; breadcrumb wins in Phase 2) ──
        icon_filename = _extract_icon_filename(item.get("icon") or "")
        category = _ICON_TO_CATEGORY.get(icon_filename)
        if icon_filename and not category:
            logger.debug(
                f"[{self.source_domain}] unknown icon {icon_filename!r} "
                f"on {url} — category will be filled from detail page"
            )

        # ─── Thumb image (preview only; Phase 2 collects the full gallery) ──
        thumb = (item.get("thumb") or "").strip() or None
        images = [thumb] if thumb else None

        location_parts = [p for p in (area, "Halkidiki") if p]
        location_raw = ", ".join(location_parts)

        seed = PropertyTemplate(
            site_property_id=site_id,
            url=url,
            source_domain=self.source_domain,
            category=category,
            price=price,
            location_raw=location_raw,
        )
        # PropertyTemplate accepts these as keyword args; assign defensively
        # in case the seed model evolves.
        if area:
            seed.area = area  # type: ignore[attr-defined]
        if lat is not None and lng is not None:
            seed.latitude = lat   # type: ignore[attr-defined]
            seed.longitude = lng  # type: ignore[attr-defined]
        if images:
            seed.images = images  # type: ignore[attr-defined]
        return seed

    # ── Phase 2: fetch_details ───────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Fetch one detail page and assemble the full data dict.

        Canonical 7-step pattern (EnrichmentMixin docstring):
          1. Structured panel extraction (meta-items + breadcrumb)
          2. Description (`.property-content p`) → og:description fallback
          3. Coordinates (propertyMarkerInfo JS literal; bbox-checked)
          4. Images (`#image-gallery` <li>) → og:image fallback
          5. NLP fallback (description-only)
          6. (LLM fallback — skipped; structural pass is already rich)
          7. Quality Gate (log only)
        """
        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.error(
                f"[{self.source_domain}] detail fetch failed for {url}: {exc!r}"
            )
            return {}

        html_text = response.text or ""
        if not html_text:
            return {}

        parser = LexborHTMLParser(html_text)
        result: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # ─── Title block + entry-id + entry-city ───────────────────────
        self._parse_title_block(parser, result, extra)

        # ─── Price (single-property-price; check for discount markup) ──
        self._parse_price(parser, result, extra)

        # ─── Breadcrumb → category (AUTHORITATIVE — overrides icon hint) ──
        self._parse_breadcrumb_category(parser, result)

        # ─── Meta items (Floor, Area, Bedrooms, etc.) ──────────────────
        self._parse_meta_items(parser, result, extra)

        # ─── Description ───────────────────────────────────────────────
        description = self._extract_description(parser)
        if not description:
            description = self._og_description_fallback(parser)
        if description:
            result["description"] = description

        # ─── Coordinates (propertyMarkerInfo JS literal) ───────────────
        lat, lng = self._extract_coordinates(html_text)
        if lat is not None and lng is not None:
            result["latitude"] = lat
            result["longitude"] = lng
            # Dionisiou publishes exact coords (no privacy offset like
            # Spitogatos). Mark explicitly so downstream clustering can
            # use a tighter radius.
            extra["gps_type"] = "exact"

        # ─── Images (full gallery, clones excluded) ────────────────────
        images = self._extract_images(parser)
        if not images:
            og = self._og_image_fallback(parser)
            if og:
                images = [og]
        if images:
            result["images"] = images

        # ─── Virtual tour (optional) ───────────────────────────────────
        video_url = self._extract_video_url(parser)
        if video_url:
            extra["virtual_tour_url"] = video_url

        # ─── WordPress post ID (cross-reference convenience) ───────────
        wp_id = self._extract_wp_post_id(parser)
        if wp_id is not None:
            extra["wp_post_id"] = wp_id

        # ─── City → municipality (best-effort) ─────────────────────────
        # We use the city slugged from the parsed entry-city; if that
        # didn't fire (rare), we'd fall back to parsing the URL slug —
        # not implemented because entry-city is reliably present.
        city = extra.get("entry_city") or result.get("area")
        if city:
            city_slug = _slug(city)
            muni = _CITY_TO_MUNICIPALITY.get(city_slug)
            if muni:
                extra["municipality"] = muni

        # Merge extras BEFORE NLP fallback so dedup can inspect them.
        if extra:
            result["extra_features"] = extra

        # ─── NLP fallback ──────────────────────────────────────────────
        self._apply_nlp_fallback(result)

        # ─── Quality Gate (log-only) ───────────────────────────────────
        if not self._passes_quality_gate(result.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate "
                f"for {url}"
            )

        return result

    # ── fetch_details helpers ────────────────────────────────────────────

    def _parse_title_block(
        self,
        parser: LexborHTMLParser,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Pull the three spans inside <h1 class="single-property-title">:
            entry-title  → full title string (e.g. "Maisonetta, Hanioti")
            entry-city   → city/area name (e.g. "Hanioti")
            entry-id     → "ID number: 329-1" → 329-1

        We do NOT use entry-title to determine category (the breadcrumb
        is more reliable). We DO use entry-city to set `area` and to
        route municipality lookup. The entry-id is the AUTHORITATIVE
        site_property_id — overrides any URL-slug derived id.
        """
        h1 = parser.css_first("h1.single-property-title")
        if not h1:
            return

        title_node = h1.css_first("span.entry-title")
        if title_node:
            title = _normalize_text(title_node.text(strip=False))
            if title:
                extra["listing_title"] = title

        city_node = h1.css_first("span.entry-city")
        if city_node:
            city = _normalize_text(city_node.text(strip=False))
            if city:
                result["area"] = city
                extra["entry_city"] = city
                result["location_raw"] = f"{city}, Halkidiki"

        id_node = h1.css_first("span.entry-id")
        if id_node:
            id_text = _normalize_text(id_node.text(strip=False))
            m = _ENTRY_ID_RE.search(id_text)
            if m:
                # This overrides the URL-slug-derived id if they differ.
                # The site treats the "ID number" displayed to users as
                # canonical, so we do too.
                extra["site_id_authoritative"] = m.group(1)
                # Note: PropertyTemplate.site_property_id is set in Phase 1
                # from the URL; we surface the authoritative id in extras
                # so downstream can flag drift if URL slug ever changes.

    def _parse_price(
        self,
        parser: LexborHTMLParser,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Read `.single-property-price`. Handle the discount-strike pattern:
        when a `.pricestrike` element is present alongside a
        `.property-action-price`, the strike is the OLD price and the
        action-price is the CURRENT/discounted price.
        """
        # Check for discount markup first
        action_price_node = parser.css_first(".property-action-price")
        strike_node = parser.css_first(".pricestrike")

        if action_price_node and strike_node:
            # Discount layout
            current_p = _to_int_euro_en(
                _normalize_text(action_price_node.text(strip=False))
            )
            old_p = _to_int_euro_en(
                _normalize_text(strike_node.text(strip=False))
            )
            if current_p is not None:
                result["price"] = current_p
            if old_p is not None and current_p is not None and old_p > current_p:
                extra["price_original"] = old_p
                extra["price_discount_amount"] = old_p - current_p
            return

        # Standard layout — single price element
        price_node = parser.css_first(".single-property-price")
        if not price_node:
            return
        price = _to_int_euro_en(_normalize_text(price_node.text(strip=False)))
        if price is not None:
            result["price"] = price

    def _parse_breadcrumb_category(
        self,
        parser: LexborHTMLParser,
        result: Dict[str, Any],
    ) -> None:
        """
        AUTHORITATIVE category source: breadcrumb `<a href=".../property-type/{slug}/">`.

        This overrides the Phase-1 icon-based category (icons on detail
        pages have been observed wrong; the breadcrumb's taxonomy term
        is always correct).
        """
        breadcrumb = parser.css_first("ol.breadcrumb")
        if not breadcrumb:
            return

        for a in breadcrumb.css("a[href]"):
            href = (a.attributes.get("href") or "").strip()
            m = _PROPERTY_TYPE_RE.search(href)
            if not m:
                continue
            slug = m.group(1).lower()
            cat = _BREADCRUMB_SLUG_TO_CATEGORY.get(slug)
            if cat:
                result["category"] = cat
                return
            # Unknown taxonomy term — keep the slug verbatim for review
            # rather than silently drop. Title-case is a reasonable display.
            result["category"] = slug.replace("-", " ").title()
            return

    def _parse_meta_items(
        self,
        parser: LexborHTMLParser,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Walk `.property-meta .meta-item` divs. Each item has:
            <i class="meta-item-icon icon-{key}" title="{label}">
            <div class="meta-inner-wrapper">
              <span class="meta-item-label">{label}</span>
              <div class="meta-item-value">{value}</div>  ← optional
            </div>

        When the value div is absent, the item is a boolean amenity:
        presence alone means "this feature exists". When present with
        value "yes" (case-insensitive), same semantic. With a numeric
        value (e.g. airconditions: "3"), we still set the boolean True
        AND record the count in <key>_detail.

        Restricting the query to `.property-meta` (rather than searching
        site-wide) keeps us inside the main listing's meta block and
        excludes the similar-properties carousel which has its own
        `.meta-item` blocks pointing at OTHER listings' data.
        """
        meta_blocks = parser.css(".single-property-wrapper .property-meta")
        if not meta_blocks:
            # Fallback — first .property-meta on the page (defensive
            # against template tweaks that drop .single-property-wrapper).
            meta_blocks = parser.css(".property-meta")
            meta_blocks = meta_blocks[:1] if meta_blocks else []

        if not meta_blocks:
            return

        meta_block = meta_blocks[0]

        for item_node in meta_block.css(".meta-item"):
            icon_node = item_node.css_first(".meta-item-icon")
            if not icon_node:
                continue
            class_attr = icon_node.attributes.get("class") or ""
            key = _meta_key_from_icon_class(class_attr)
            if not key:
                continue

            value_node = item_node.css_first(".meta-item-value")
            label_node = item_node.css_first(".meta-item-label")

            label_raw = _normalize_text(label_node.text(strip=False)) if label_node else ""
            value_raw = _normalize_text(value_node.text(strip=False)) if value_node else ""

            self._route_meta_item(key, label_raw, value_raw, result, extra)

    def _route_meta_item(
        self,
        key: str,
        label: str,
        value_raw: str,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        One meta-item → either Property column or extras slot.

        Routing target from _META_ICON_TO_FIELD:
          "_skip"        → drop
          "<column>"     → top-level (coerce to int/float as appropriate)
          "_extra:<key>" → extras (value-shape-aware coercion)
          "_bool:<key>"  → extras[key]=True; if numeric value present,
                           also set extras[key+'_detail']=int(value)
          None (unknown) → extras[_slug(label)] = value_raw (preserve for
                           review when the site adds new icons)
        """
        target = _META_ICON_TO_FIELD.get(key)

        # Unknown key — slug the label and store verbatim. Better than
        # dropping; lets us spot new template fields in production.
        if target is None:
            if label:
                slug = _slug(label)
                if slug and slug not in extra:
                    extra[slug] = value_raw or True
            return

        if target == "_skip":
            return

        # Boolean amenity — presence is the signal
        if target.startswith("_bool:"):
            extra_key = target[len("_bool:"):]
            extra[extra_key] = True
            # If a numeric detail is given (e.g. airconditions: "3"),
            # capture it as a count for analytics. We keep the boolean
            # too because downstream code keys off it for "has X?" UI.
            if value_raw:
                num = _to_int_simple(value_raw)
                if num is not None and num > 1:
                    extra[f"{extra_key}_count"] = num
            return

        # Extras slot
        if target.startswith("_extra:"):
            extra_key = target[len("_extra:"):]
            self._route_extra_value(extra_key, value_raw, extra)
            return

        # Top-level Property column
        if target == "size_sqm":
            v = _to_float_sqm(value_raw)
            if v is not None:
                result["size_sqm"] = v
        elif target == "year_built":
            v = _to_int_simple(value_raw)
            if v is not None and 1800 <= v <= 2100:
                result["year_built"] = v
        elif target in {"bedrooms", "bathrooms"}:
            v = _to_int_simple(value_raw)
            if v is not None:
                result[target] = v
        else:
            # Unknown top-level — store verbatim
            result[target] = value_raw

    def _route_extra_value(
        self,
        key: str,
        value_raw: str,
        extra: Dict[str, Any],
    ) -> None:
        """
        Shape-aware coercion for extras values.

        Heuristics, in order:
          1. distance_to_*_m / *_sqm / *_km  → int (strip the unit)
          2. living_rooms_count              → int
          3. heating / floor                 → str (verbatim)
          4. fallback                        → str (verbatim) or bool True
        """
        if not value_raw:
            # No value but the meta-item exists — treat as boolean True
            # (e.g. "heating_system" cell present but empty). Defensive.
            extra[key] = True
            return

        # Unit-suffixed numerics
        if key.endswith("_m") or key.endswith("_km") or key.endswith("_sqm"):
            stripped = _strip_units(value_raw)
            v = _to_int_simple(stripped)
            if v is not None:
                extra[key] = v
                return
            # Fall through to verbatim if regex failed (rare; defensive)

        # Plain integer counters
        if key.endswith("_count"):
            v = _to_int_simple(value_raw)
            if v is not None:
                extra[key] = v
                return

        # Default — string verbatim
        extra[key] = value_raw

    def _extract_description(
        self,
        parser: LexborHTMLParser,
    ) -> Optional[str]:
        """
        Pull the body description.

        Inspiry layout: `.entry-content > h4.fancy-title (== "Description") >
        .property-content > vc_row > wpb_column > vc_column-inner >
        wpb_wrapper > wpb_text_column > wpb_wrapper > <p>` (deeply nested
        Visual Composer wrappers).

        `.property-content p` flat-selects ALL paragraphs regardless of
        wrapper depth, which is what we want. We also collect <p> from
        anywhere inside `.entry-content` as a backstop in case the
        Visual Composer wrapper is absent on some legacy listings.

        Skip the "More information about ..." CTA paragraph if it leaks
        in (some templates wrap it in a vc_btn3 div but it can collapse
        into a plain <p> when JS isn't executed).
        """
        # Primary path — strict
        content_node = parser.css_first(".property-content")
        nodes = content_node.css("p") if content_node else []

        # Backstop — looser
        if not nodes:
            entry = parser.css_first(".entry-content")
            if entry:
                nodes = entry.css("p")

        if not nodes:
            return None

        paragraphs: List[str] = []
        for p in nodes:
            txt = _normalize_text(p.text(strip=False))
            if not txt:
                continue
            # CTA filter — common Inspiry button text fragments
            low = txt.lower()
            if (
                low.startswith("more information about")
                or low.startswith("request additional")
                or low.startswith("contact us")
            ):
                continue
            paragraphs.append(txt)

        if not paragraphs:
            return None
        return "\n\n".join(paragraphs)

    def _extract_coordinates(
        self,
        html_text: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Parse `var propertyMarkerInfo = {"lat":..., "lng":..., "icon":"..."}`
        from the detail-page <script>.

        We DON'T trust the `icon` field here (it's been observed wrong on
        listings whose breadcrumb category is correct). But the lat/lng
        pair is reliable enough — bbox-checked against Halkidiki.

        Uses the same bracket-balanced walker as Phase 1 to avoid the
        non-greedy `\\{.*?\\}` regex pitfall (would over-match if the
        marker object was followed by another `{...}` literal nearby).
        """
        raw_obj = _extract_balanced_literal(
            html_text, _PROPERTY_MARKER_ANCHOR_RE, "{", "}"
        )
        if raw_obj is None:
            return None, None

        try:
            obj = json.loads(raw_obj)
        except json.JSONDecodeError:
            # Some listings have unescaped slashes in the icon URL that
            # already trip Python's strict JSON parser even though they
            # are valid JSON. Try a forgiving cleanup pass.
            try:
                cleaned = raw_obj.replace("\\/", "/")
                obj = json.loads(cleaned)
            except json.JSONDecodeError as exc:
                logger.debug(
                    f"[{self.source_domain}] propertyMarkerInfo JSON "
                    f"parse failed: {exc!r}"
                )
                return None, None

        try:
            lat = float(obj.get("lat"))
            lng = float(obj.get("lng"))
        except (TypeError, ValueError):
            return None, None

        # Halkidiki bbox sanity (matches halkidiki_agency's check).
        # Some Dionisiou listings have wrong coords (verified: Kalyves
        # entry → Crete coords; Trikorfo → wrong region). Filter them.
        if not (39.0 <= lat <= 41.5 and 22.0 <= lng <= 25.0):
            logger.debug(
                f"[{self.source_domain}] propertyMarkerInfo coords "
                f"{lat},{lng} outside Halkidiki bbox — ignoring"
            )
            return None, None

        return lat, lng

    def _extract_images(
        self,
        parser: LexborHTMLParser,
    ) -> List[str]:
        """
        Collect full-resolution gallery images from `<ul id="image-gallery">`.

        Each real slide is:
            <li class="lslide [active]">
              <a class="swipebox" href="FULL.jpg">
                <img class="img-responsive" src="..._850x570.jpg">
              </a>
            </li>

        SKIP slider clones:
            <li class="clone left">...</li>     — duplicate of last slide
            <li class="clone right">...</li>    — duplicate of first slide
        These are inserted by lightSlider's `loop:true` and contain the
        same image URLs as real slides; including them would duplicate
        first/last photos in the persisted list.

        Use the `<a href>` (full size, ~1500×1000) not `<img src>`
        (resized 850×570). De-duplicate by URL — defensive against
        templates that show the same photo in primary + thumbnail rails.

        Restricted to `.single-property-slider` so the similar-properties
        carousel below (which has its OWN thumbnails) doesn't bleed in.
        """
        slider = parser.css_first(".single-property-slider")
        if not slider:
            return []

        gallery = slider.css_first("ul#image-gallery") or slider.css_first("ul.lightSlider")
        if not gallery:
            return []

        seen: set = set()
        images: List[str] = []

        for li in gallery.css("li"):
            class_attr = (li.attributes.get("class") or "").lower()
            # Skip slider boundary clones
            if "clone" in class_attr.split():
                continue

            a_node = li.css_first("a.swipebox")
            href = (a_node.attributes.get("href") if a_node else "") or ""
            href = href.strip()

            # Fallback to <img src> if the <a> wasn't found (defensive)
            if not href:
                img_node = li.css_first("img")
                src = (img_node.attributes.get("src") if img_node else "") or ""
                href = src.strip()

            if not href:
                continue
            if href.lower().endswith(".svg"):
                continue
            if href in seen:
                continue
            seen.add(href)
            images.append(href)

        return images

    def _extract_video_url(
        self,
        parser: LexborHTMLParser,
    ) -> Optional[str]:
        """Pull the YouTube virtual-tour URL from `<a class="video-popup">`."""
        a_node = parser.css_first("a.video-popup[href]")
        if not a_node:
            return None
        href = (a_node.attributes.get("href") or "").strip()
        if not href:
            return None
        # Sanity — only accept http(s) URLs
        if not href.lower().startswith(("http://", "https://")):
            return None
        return href

    def _extract_wp_post_id(
        self,
        parser: LexborHTMLParser,
    ) -> Optional[int]:
        """Extract `postid-NNNN` from <body class="...">."""
        body = parser.css_first("body")
        if not body:
            return None
        class_attr = body.attributes.get("class") or ""
        m = _WP_POSTID_RE.search(class_attr)
        if not m:
            return None
        try:
            return int(m.group(1))
        except ValueError:
            return None
