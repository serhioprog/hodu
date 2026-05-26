"""
MProperties scraper — RealStatus/iarts CMS platform (mproperties.gr).

mproperties.gr is built on the same "Real Status" broker program as
halkidikiestate.com (powered by iarts.gr). Same overall server-rendered
PHP architecture with Leaflet maps and predictable HTML classes, but
adapted for the MProperties Real Estate agency in Thessaloniki/Halkidiki.

Site analysis findings
======================

URL structure (path-segment style):

  Base:       /listings
  Filters:    /listings/for/1/areas/{N196-R4015-R4016}/category/{cat}
              /sortby/dateDesc/priceFrom/{min}
  Pagination: append /page/{N}
  Detail:     /property/{id}  (slug optional, server resolves on ID alone)

  for/1                  = Sale (vs for/2 = Rent)
  areas/N196-R4015-R4016 = Halkidiki prefecture (N196) + Kassandra/Sithonia
                           regions (R4015, R4016). MUST be included to
                           filter out properties from other prefectures.
  category/1 = Residential
  category/2 = Commercial
  category/3 = Land
  category/4 = Others

Two-category collection:
  At min_price=400000 in Halkidiki: cat=1 returns 7 properties (residential
  buildings, villas, apartments), cat=3 returns 2 properties (land plots).
  Total expected: 9 properties. We iterate both categories and dedup by URL.

Language switching:
  Append ?language=en to ANY URL to force English. Default is Greek.
  Same mechanism as halkidikiestate. (NB: differs from kwgreece.gr which
  uses cookie-based switching via /change-language POST.)

Listing card structure (`.property-item`):
  - <div class="favorite-add" data-id="7606">    — CLEAN site_property_id
  - <a class="prop-link" href="/property/7606/"> — detail URL
  - <span class="labels"><span class="label">SALE</span>
  - Title/price/location info in sibling structure within .card

Detail page structure:
  - <h1>Building 573 sq.m.</h1>                 — clean type+size H1
  - <span><i la-map-marker/>Chalkidiki, Kallikrateia, Nea Kallikrateia</span>
  - .property-price span                          — main price "472.000 €"
  - .property-badges span                         — "FOR SALE"
  - .property-details > p                         — description
  - .property-inform .information-list li         — info block
      <li><i></i>Label: <span>value</span></li>
  - Distance section: another <ul class="information-list">
      <li><i></i>Sea: 700 meters.</li>           — NO <span>
  - .property-feautures li span                   — features (typo "feautures")
      "Communication: Bus", "Open", "View: Sea"
  - .energy-section .energy.active                — active energy class p tag
      data-id="7" + Greek text "Ε" (E)
  - .gallery .item-img a.card-img[href]           — image gallery
  - <script>var lat = N; var long = N;</script>  — Leaflet GPS

Key architectural decisions
===========================

1) Force ?language=en — stable English labels (default is Greek).

2) Two-category collection: iterate cat=1 (Residential) + cat=3 (Land),
   dedup by site_property_id. Pagination per category.

3) Card ID from .favorite-add[data-id] attribute (same as halkidiki_estate).

4) Type-word from clean H1 ("Building 573 sq.m." → "Building" → Maisonette).
   No marketing copy — type-word is always the first word of H1.

5) Info vs Distance discrimination — BOTH use <ul class="information-list">:
   - INFO  <li> contains <span> with value     — has <span> child
   - DIST  <li> is plain text "Sea: 300 meters." — no <span>

6) Information list <li> format differs from halkidiki_estate (no .inf-item
   wrapper):
   - mproperties:    <li><i></i>Label: <span>value</span></li>
   - halkidiki_est:  <li><div class="inf-item"><span>Label:</span></div><span>Value</span></li>
   We extract label as "full text minus value" approach.

7) Energy class — different element/extraction from halkidiki_estate:
   - .energy-section .energy.active (a <p> tag, not <span>)
   - data-id attribute is primary signal (1-9 → A+...G)
   - Greek letter text fallback if data-id missing

8) GPS = same Leaflet pattern as halkidiki_estate. L.circle for privacy
   (400m radius typical), L.marker for exact. Bbox sanity check.

9) Description location differs: .property-details > p (mproperties) vs
   #property-desc (halkidiki_estate).

10) Tiny site (9 properties) — pagination loops are defensive but
    typically all results fit on page 1 of each category.
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


# =============================================================
# Mappings — pure data, no logic
# =============================================================

# English labels (after ?language=en) → first-class Property columns.
# Lowercase, no trailing colons. NOTE: 'Rooms' on this CMS family means
# bedrooms; both wordings observed across iarts properties.
_LABEL_TO_PROPERTY_COLUMN: Dict[str, str] = {
    # Info block
    "rooms":               "bedrooms",
    "bedrooms":            "bedrooms",
    "bedroom":             "bedrooms",
    "bathroom":            "bathrooms",
    "bathrooms":           "bathrooms",
    "year of manufacture": "year_built",
    "year of construction": "year_built",
    "levels":              "levels",
    # Features block
    "area":                "size_sqm",
    "plot area":           "land_size_sqm",
    "plot":                "land_size_sqm",
    "plot size":           "land_size_sqm",
}

# Labels we explicitly DROP — always-same values, duplicates, or info we
# already track elsewhere.
_DROP_LABELS: set = {
    "code",            # captured upstream from .favorite-add[data-id]
    "available from",  # marketing date, low value for MDM
    "last updated",    # site_last_updated tracked separately
}

# Integer count fields stored as extra_features.<slug>_count
# (checked BEFORE yes/no routing since '1' is in _YES_VALUES).
_COUNT_LABELS: set = {
    "kitchen",
    "living room",
    "wc",
}

# Greek energy class labels → Latin (Greek law since 2010).
_GREEK_ENERGY_CLASS: Dict[str, str] = {
    "α+": "A+",
    "α":  "A",
    "β+": "B+",
    "β":  "B",
    "γ":  "C",
    "δ":  "D",
    "ε":  "E",
    "ζ":  "F",
    "η":  "G",
}

# data-id (1-9) → Latin energy class. Primary signal on mproperties since
# the visible text can be either Greek or Latin depending on language param.
_ENERGY_CLASS_BY_INDEX: Dict[int, str] = {
    1: "A+", 2: "A", 3: "B+", 4: "B", 5: "C", 6: "D", 7: "E", 8: "F", 9: "G",
}

# H1 type-word → hodu category. mproperties H1 is clean (e.g. "Building 573 sq.m.",
# "Land 2.500 sq.m."), so the type-word is always the FIRST WORD or first
# 2-3 words. Sort by length descending when matching.
_TYPE_TO_CATEGORY: Dict[str, str] = {
    # Maisonette (multi-unit residential building)
    "residential building": "Maisonette",
    "apartment building":   "Maisonette",
    "residential complex":  "Maisonette",
    "maisonette":           "Maisonette",
    "duplex":               "Maisonette",
    "building":             "Maisonette",
    "residential":          "Maisonette",
    # Apartment
    "apartment":            "Apartment",
    "flat":                 "Apartment",
    "studio":               "Apartment",
    "loft":                 "Apartment",
    # House
    "detached house":       "House",
    "single family house":  "House",
    "country house":        "House",
    "house":                "House",
    # Villa
    "villa":                "House (Villa)",
    "mansion":              "House (Villa)",
    # Land
    "plot of land":         "Land",
    "agricultural land":    "Land",
    "land area":            "Land",
    "agricultural":         "Land",
    "land":                 "Land",
    "plot":                 "Land",
    "parcel":               "Land",
    "field":                "Land",
    # Business
    "office":               "Business",
    "shop":                 "Business",
    "store":                "Business",
    "warehouse":            "Business",
    "hotel":                "Business",
    "commercial":           "Business",
    "industrial":           "Business",
    "restaurant":           "Business",
}

# Yes/No normalisation. English-only — site is forced to ?language=en.
_YES_VALUES = {"yes", "y", "true", "1"}
_NO_VALUES  = {"no",  "n", "false", "0"}

# Halkidiki bounding box. Coords outside are rejected as malformed.
_HALKIDIKI_LAT_RANGE = (39.0, 41.0)
_HALKIDIKI_LNG_RANGE = (22.0, 24.0)

# Maximum pages per category to walk. Defensive against pagination bugs.
_PAGE_SAFETY_CAP = 30

# Two categories to collect for Halkidiki ≥€400k.
# 1 = Residential (houses, villas, apartments, buildings)
# 3 = Land (plots, parcels, agricultural)
# (cat=2 Commercial and cat=4 Others currently yield 0 results at our filter.)
_TARGET_CATEGORIES = (1, 3)

# URL area segment encoding Halkidiki + its Kassandra/Sithonia subregions.
# N196 = Halkidiki prefecture
# R4015 = Kassandra peninsula
# R4016 = Sithonia peninsula
# (Aristotelis peninsula not explicitly listed in this filter, but site
# uses geo bbox for finer filtering so Athos-region listings still surface
# when properly tagged.)
_HALKIDIKI_AREA_PATH = "N196-R4015-R4016"


# =============================================================
# Helpers — pure functions, no scraper state
# =============================================================

def _slug(label: str) -> str:
    """English-label → stable JSON key."""
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_int_euro(text: str) -> Optional[int]:
    """
    Parse a EU price string into integer euros.

      "472.000 €"     -> 472000
      "1.250.000 €"   -> 1250000
      "400.000,00€"   -> 400000  (cents dropped)
    """
    if not text:
        return None
    cleaned = re.sub(r"[^\d.,]", "", text)
    if not cleaned:
        return None
    if "." in cleaned and "," in cleaned:
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
    """Extract first integer from a string."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """Parse area-in-square-meters strings. '573 sq.m' → 573.0"""
    if not text:
        return None
    m = re.search(r"\d+(?:[.,]\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def _normalise_yes_no(text: str) -> Optional[bool]:
    """'Yes'/'No' → bool; None otherwise."""
    if not text:
        return None
    s = text.strip().lower()
    if s in _YES_VALUES:
        return True
    if s in _NO_VALUES:
        return False
    return None


def _split_label_value(text: str) -> Tuple[str, Optional[str]]:
    """
    "Area: 573 sq.m"  -> ("Area", "573 sq.m")
    "Sea: 700 meters." -> ("Sea", "700 meters")  (trailing period stripped)
    "Bright"           -> ("Bright", None)        (boolean amenity)
    """
    text = text.strip().rstrip(".")
    if ":" not in text:
        return text, None
    label, _, value = text.partition(":")
    value = value.strip()
    return label.strip(), (value if value else None)


def _translate_energy_class(text: str) -> str:
    """
    Translate Greek energy class letter to Latin.
      "Γ"  -> "C"
      "A+" -> "A+"  (already Latin, unchanged)
      "XX" -> "XX"  (unknown, unchanged)
    """
    if not text:
        return text
    t = text.strip()
    if re.match(r"^[A-G][+]?$", t, re.IGNORECASE):
        return t.upper()
    return _GREEK_ENERGY_CLASS.get(t.lower(), t)


def _guess_category_from_h1(title: str) -> Optional[str]:
    """
    mproperties H1 is clean type+size: "Building 573 sq.m.", "Land 2.500 sq.m.",
    "Maisonette 110 sq.m." etc. Type-word is at the START (1-3 words).

    We scan type-words sorted by length descending so multi-word matches
    ("Residential Building") win over single-word ("Building").

    Returns None if no keyword matched.
    """
    if not title:
        return None
    lower = title.strip().lower()
    # Try longest-first match at start of H1
    for keyword in sorted(_TYPE_TO_CATEGORY.keys(), key=len, reverse=True):
        if lower.startswith(keyword + " ") or lower == keyword:
            return _TYPE_TO_CATEGORY[keyword]
    # Fallback: word boundary anywhere in title
    for keyword in sorted(_TYPE_TO_CATEGORY.keys(), key=len, reverse=True):
        if re.search(rf"\b{re.escape(keyword)}\b", lower):
            return _TYPE_TO_CATEGORY[keyword]
    return None


def _build_location_raw(card_location_text: Optional[str]) -> str:
    """
    Build whitelist-safe location_raw. Card text format:
      "Chalkidiki, Kallikrateia, Nea Kallikrateia"

    We force-append " Halkidiki" if neither Chalkidiki nor Halkidiki present.
    """
    if not card_location_text:
        return "Halkidiki"
    text = card_location_text.strip()
    lower = text.lower()
    if "halkidiki" in lower or "chalkidiki" in lower:
        return text
    return f"{text}, Halkidiki"


# =============================================================
# Scraper
# =============================================================

class MPropertiesScraper(EnrichmentMixin, BaseScraper):
    # mproperties detail page reliably provides structured info, so we
    # restrict NLP fallback to numeric fields that may be missing for
    # land listings (no beds/baths in features section).
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )

    """
    MProperties.gr scraper — RealStatus/iarts CMS, MProperties Real Estate
    agency (Thessaloniki, covering Halkidiki).

    At min_price=400000 in Halkidiki (N196-R4015-R4016):
      cat=1 (Residential): ~7 listings (buildings, villas, apartments)
      cat=3 (Land):        ~2 listings (plots, parcels)
      Total: ~9 unique properties.
    """

    BASE_URL = "https://www.mproperties.gr"

    def __init__(self):
        super().__init__()
        self.source_domain = "mproperties.gr"

    async def fetch_listings(self):
        """Backwards-compatible entry point for the dispatcher."""
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # URL builders
    # ---------------------------------------------------------------

    def _build_list_url(self, page: int, category: int, min_price: int) -> str:
        """
        RESTful path-segment URL with mandatory area filter for Halkidiki.
        for/1 = Sale, areas = Halkidiki+Kassandra+Sithonia, sortby=dateDesc.
        """
        return (
            f"{self.BASE_URL}/listings"
            f"/for/1/areas/{_HALKIDIKI_AREA_PATH}"
            f"/category/{category}"
            f"/sortby/dateDesc"
            f"/priceFrom/{min_price}"
            f"/page/{page}"
            f"?language=en"
        )

    def _construct_detail_url(self, site_id: str) -> str:
        """Detail URL with bare ID; server resolves without slug."""
        return f"{self.BASE_URL}/property/{site_id}?language=en"

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs from both categories
    # ---------------------------------------------------------------

    async def collect_urls(self, min_price: int = 400000) -> List[PropertyTemplate]:
        """
        Walk paginated listings for each category in _TARGET_CATEGORIES,
        dedup by site_property_id across categories.

        Per-category stop conditions:
          1. .property-item cards empty on page N
          2. .pagination-list.active reports back a page lower than requested
             (server fallback to page 1 signals overflow)
          3. _PAGE_SAFETY_CAP exceeded
        """
        all_properties: List[PropertyTemplate] = []
        seen_site_ids: set = set()

        for category in _TARGET_CATEGORIES:
            page = 1
            cat_count = 0
            logger.info(
                f"[{self.source_domain}] Phase 1: starting category {category}"
            )

            while page <= _PAGE_SAFETY_CAP:
                url = self._build_list_url(page, category, min_price)
                logger.info(
                    f"[{self.source_domain}] cat={category} page {page}: {url}"
                )

                try:
                    response = await self.client.get(url)
                    parser = LexborHTMLParser(response.text)
                    cards = parser.css(".property-item")

                    if not cards:
                        logger.info(
                            f"[{self.source_domain}] cat={category} page {page}: "
                            f"no cards — end of pagination"
                        )
                        break

                    # Sanity: detect silent fallback to page 1 (overflow)
                    active = parser.css_first(".pagination-list.active")
                    if active and page > 1:
                        active_text = active.text(strip=True)
                        if active_text.isdigit():
                            active_n = int(active_text)
                            if active_n != page:
                                logger.info(
                                    f"[{self.source_domain}] cat={category} "
                                    f"requested page {page}, server returned "
                                    f"page {active_n} — end of pagination"
                                )
                                break

                    page_count = 0
                    for card in cards:
                        try:
                            prop = self._parse_card(card)
                            if prop is None:
                                continue
                            if prop.site_property_id in seen_site_ids:
                                continue  # already collected in earlier category
                            seen_site_ids.add(prop.site_property_id)
                            all_properties.append(prop)
                            page_count += 1
                            cat_count += 1
                        except Exception as e:
                            logger.error(
                                f"[{self.source_domain}] card parse error: {e}"
                            )

                    logger.info(
                        f"[{self.source_domain}] cat={category} page {page}: "
                        f"{page_count} new"
                    )

                    await asyncio.sleep(1.5)
                    page += 1

                except Exception as e:
                    logger.error(
                        f"[{self.source_domain}] cat={category} page {page} "
                        f"critical error: {e}"
                    )
                    break

            logger.info(
                f"[{self.source_domain}] cat={category} done: {cat_count} props"
            )

        logger.info(
            f"[{self.source_domain}] Phase 1 complete: {len(all_properties)} unique URLs"
        )
        return all_properties

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """
        Extract seed PropertyTemplate from one .property-item card.

        Card structure on mproperties is sparser than halkidiki_estate —
        the listing primarily shows image + favorite + prop-link, with
        title/price/location often in a sibling element. We extract what's
        reliably present and rely on fetch_details for the rest.
        """
        # site_property_id from .favorite-add[data-id]
        fa = card.css_first(".favorite-add")
        if not fa:
            return None
        site_id = (fa.attributes.get("data-id") or "").strip()
        if not site_id:
            return None

        full_url = self._construct_detail_url(site_id)

        # Title — try h2 (common in iarts cards) or .property-title
        title = ""
        for selector in ("h2", ".property-title h2", ".card-title", ".prop-title"):
            el = card.css_first(selector)
            if el:
                title = el.text(strip=True)
                if title:
                    break

        # Location — scan for span containing Halkidiki/Chalkidiki
        location_raw = "Halkidiki"
        for span in card.css("span"):
            text = span.text(separator=" ", strip=True)
            lower = text.lower()
            if ("halkidiki" in lower or "chalkidiki" in lower
                    or "χαλκιδική" in lower):
                location_raw = _build_location_raw(text)
                break

        # Price — try .property-price or .listing-price or any span with €
        price_text = None
        for selector in (".property-price span", ".listing-price .fw-bold",
                         ".listing-price span", ".price"):
            el = card.css_first(selector)
            if el:
                t = el.text(strip=True)
                if "€" in t or any(c.isdigit() for c in t):
                    price_text = t
                    break
        if not price_text:
            # Last resort: scan for first span with € sign
            for span in card.css("span"):
                t = span.text(strip=True)
                if "€" in t and any(c.isdigit() for c in t):
                    price_text = t
                    break

        # Beds/baths/size from listing-icons (if shown on card)
        bedrooms = None
        bathrooms = None
        size_sqm = None
        for li in card.css(".listing-icons li, .property-icons li, .icons-list li"):
            text = li.text(separator=" ", strip=True).lower()
            if "bed" in text:
                m = re.search(r"(\d+)", text)
                if m:
                    bedrooms = int(m.group(1))
            elif "bathroom" in text or "bath" in text:
                m = re.search(r"(\d+)", text)
                if m:
                    bathrooms = int(m.group(1))
            elif "sq.m" in text or "sq m" in text or "m²" in text:
                m = re.search(r"(\d+(?:[.,]\d+)?)", text)
                if m:
                    try:
                        size_sqm = float(m.group(1).replace(",", "."))
                    except ValueError:
                        pass

        # Category guess from title (will be refined by H1 in fetch_details)
        category = _guess_category_from_h1(title)

        # For Land category, the card's "sq.m" icon represents LAND area,
        # not building size. Drop size_sqm to avoid double-fill conflict in
        # _ingest_new_properties merge (fetch_details fills land_size_sqm
        # correctly, but won't overwrite the non-None size_sqm seeded here).
        if category == "Land":
            size_sqm = None

        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=full_url,
            price=price_text,
            location_raw=location_raw,
            size_sqm=size_sqm,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            category=category,
        )

    # ---------------------------------------------------------------
    # PHASE 2 — fetch full details
    # ---------------------------------------------------------------

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Fetch detail page and extract:
          - Description (.property-details > p)
          - Header (price, badges, location)
          - Information block (.information-list li with <span>)
          - Distance block (.information-list li without <span>)
          - Features block (.property-feautures li span)
          - Energy class (.energy-section .energy.active)
          - GPS coords from Leaflet JS
          - Photos (.gallery .item-img a.card-img)
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

            # 1. Photos
            data["images"] = self._collect_image_urls(parser)

            # 2. Description
            data["description"] = self._parse_description(parser)

            # 3. Header (price + H1 type)
            self._parse_header(parser, data)

            # 4. Location breakdown from header (Chalkidiki, X, Y)
            self._parse_location_from_header(parser, data)

            # 5. Structured blocks (info + distance + features)
            self._parse_structured_blocks(parser, data)

            # 6. Energy class (separate element)
            self._parse_energy_class(parser, raw_html, data)

            # 7. Coordinates from Leaflet JS
            self._parse_coordinates(raw_html, data)

            # 8. NLP fallback over description (EnrichmentMixin)
            self._apply_nlp_fallback(data)

            # 9. Quality Gate (log-only)
            if not self._passes_quality_gate(data.get("description")):
                logger.warning(
                    f"[{self.source_domain}] description below quality gate "
                    f"for {url}"
                )

            # 10. Drop None values so card seed isn't clobbered.
            return {k: v for k, v in data.items() if v is not None}

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] fetch_details error for {url}: {e}"
            )
            return {}

    # ---------------------------------------------------------------
    # Phase 2 helpers — Photos / Description / Header / Location
    # ---------------------------------------------------------------

    def _collect_image_urls(self, parser: LexborHTMLParser) -> List[str]:
        """
        Photos from .gallery .item-img a.card-img[href].

        mproperties uses display:none on all but first 5 thumbnails — but
        the full list is in the HTML. We extract all unique image URLs.

        Fallback: meta[property="og:image"] for properties without gallery.
        """
        photos: List[str] = []
        for a in parser.css(".gallery .item-img a.card-img"):
            href = a.attributes.get("href", "") or ""
            if href and re.search(r"\.(jpe?g|png|webp)(\?|$)", href, re.IGNORECASE):
                if href not in photos:
                    photos.append(href)

        # Fallback to og:image
        if not photos:
            og = parser.css_first('meta[property="og:image"]')
            if og:
                href = (og.attributes.get("content") or "").strip()
                if href and re.search(r"\.(jpe?g|png|webp)(\?|$)", href, re.IGNORECASE):
                    photos.append(href)

        return photos

    def _parse_description(self, parser: LexborHTMLParser) -> str:
        """
        Description from .property-details > p (the FIRST p inside that block).

        Falls back to og:description meta tag when missing.
        """
        desc_block = parser.css_first(".property-details")
        if desc_block:
            paragraphs = []
            for p in desc_block.css("p"):
                txt = p.text(separator=" ", strip=True)
                if txt and len(txt) >= 30 and txt not in paragraphs:
                    paragraphs.append(txt)
            if paragraphs:
                return "\n\n".join(paragraphs)

        return self._og_description_fallback(parser)

    def _parse_header(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Parse top header: H1 (type+size title) and .property-price span.

        H1 type-word also refines category (overrides card-derived guess
        which was based on potentially incomplete card title).
        """
        # Price
        price_span = parser.css_first(".property-price span")
        if price_span:
            v = _to_int_euro(price_span.text(strip=True))
            if v is not None:
                data["price"] = v

        # H1 → category refinement
        h1 = parser.css_first("h1")
        if h1:
            title = h1.text(strip=True)
            category = _guess_category_from_h1(title)
            if category:
                data["category"] = category
            # Also extract size from H1 if not already filled.
            # "Building 573 sq.m." → 573 → size_sqm
            # "Land 5290 sq.m."   → 5290 → land_size_sqm (Land category)
            m = re.search(r"(\d+(?:[.,]\d+)?)\s*sq\.?\s*m", title, re.I)
            if m:
                try:
                    sqm_val = float(m.group(1).replace(",", "."))
                    if data.get("category") == "Land":
                        if data.get("land_size_sqm") is None:
                            data["land_size_sqm"] = sqm_val
                    else:
                        if data.get("size_sqm") is None:
                            data["size_sqm"] = sqm_val
                except ValueError:
                    pass

    def _parse_location_from_header(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Header has a <span> with map-marker icon and text:
          'Chalkidiki, Kallikrateia, Nea Kallikrateia'

        Split into:
          - DROP first segment (prefecture, always Chalkidiki/Halkidiki)
          - subarea = second segment (municipality/region)
          - area    = last segment (specific city, most useful for clustering)
        """
        title_block = parser.css_first(".property-title")
        if not title_block:
            return
        for span in title_block.css("span"):
            text = span.text(separator=" ", strip=True)
            lower = text.lower()
            if "chalkidiki" in lower or "halkidiki" in lower:
                parts = [p.strip() for p in text.split(",") if p.strip()]
                non_prefecture = [
                    p for p in parts
                    if "chalkidiki" not in p.lower()
                    and "halkidiki" not in p.lower()
                ]
                if len(non_prefecture) >= 1:
                    if data.get("area") is None:
                        data["area"] = non_prefecture[-1]
                if len(non_prefecture) >= 2:
                    if data.get("subarea") is None:
                        data["subarea"] = non_prefecture[-2]
                break

    # ---------------------------------------------------------------
    # Phase 2 helpers — Structured blocks (info / distance / features)
    # ---------------------------------------------------------------

    def _parse_structured_blocks(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Iterate three structured blocks:
          1. .information-list li WITH <span> child → INFO block
          2. .information-list li WITHOUT <span> child → DISTANCE block
          3. .property-feautures li → FEATURES block
        """
        for li in parser.css(".information-list li"):
            if li.css_first("span"):
                self._parse_info_li(li, data)
            else:
                self._parse_distance_li(li, data)

        for li in parser.css(".property-feautures li"):
            self._parse_feature_li(li, data)

    def _parse_info_li(self, li: LexborNode, data: Dict[str, Any]) -> None:
        """
        Info li structure (mproperties):
          <li><i class="..."></i>Label: <span>value</span></li>

        Strategy: extract value from <span>, then derive label as
        "full li text minus value", stripping the trailing colon.
        """
        span = li.css_first("span")
        if not span:
            return  # not an info li
        value = span.text(strip=True)
        if not value:
            return

        full_text = li.text(separator=" ", strip=True)
        # Strip the value from the end to get label
        label_idx = full_text.rfind(value)
        if label_idx <= 0:
            return
        label = full_text[:label_idx].strip().rstrip(":").strip()
        if not label:
            return

        self._route_keyed_field(label, value, data)

    def _parse_distance_li(self, li: LexborNode, data: Dict[str, Any]) -> None:
        """
        Distance li structure:
          <li><i class="la-umbrella-beach"></i>Sea: 700 meters.</li>

        Result: extra_features.distance_from_<slug> = "700 meters"
        """
        text = li.text(separator=" ", strip=True)
        if not text or ":" not in text:
            return
        label, value = _split_label_value(text)
        if value is None:
            return
        slug = _slug(f"distance_from_{label}")
        if slug:
            data["extra_features"][slug] = value
            # Numeric meters → also store as int for distance_to_<slug>_m
            m = re.search(r"(\d+)\s*meter", value, re.I)
            if m:
                meters = int(m.group(1))
                meter_slug = _slug(f"distance_to_{label}_m")
                if meter_slug:
                    data["extra_features"][meter_slug] = meters

    def _parse_feature_li(self, li: LexborNode, data: Dict[str, Any]) -> None:
        """
        Feature li structure:
          <li><i></i><span>Area: 573 sq.m</span></li>      — key:value
          <li><i></i><span>Bright</span></li>               — boolean amenity
          <li><i></i><span>Communication: Bus</span></li>   — key:value (extra)
        """
        span = li.css_first("span")
        if not span:
            return
        text = span.text(strip=True)
        if not text:
            return
        label, value = _split_label_value(text)
        self._route_keyed_field(label, value, data)

    # ---------------------------------------------------------------
    # Phase 2 helpers — Energy class
    # ---------------------------------------------------------------

    def _parse_energy_class(
        self,
        parser: LexborHTMLParser,
        raw_html: str,
        data: Dict[str, Any],
    ) -> None:
        """
        Energy class on mproperties has TWO sources in priority order:

        1) Inline JS variable (runtime-applied, pre-JS HTML has all energy
           letters as inactive). Server renders:
              var energy = '7';
              if(energy !='') {
                  document.querySelector('.energy[data-id="'+energy+'"]')
                    .classList.add("active");
              }
           We read the `var energy = '7';` integer and map via
           _ENERGY_CLASS_BY_INDEX (1-9 → A+...G).

        2) Static `.energy.active` element (fallback, in case server
           already pre-renders the .active class on some templates).
        """
        # Source 1: inline JS variable (primary, since selectolax sees
        # pre-JS HTML where .active class hasn't been applied yet)
        m = re.search(
            r"var\s+energy\s*=\s*['\"]?(\d+)['\"]?",
            raw_html,
        )
        if m:
            try:
                idx = int(m.group(1))
                if idx in _ENERGY_CLASS_BY_INDEX:
                    data["extra_features"]["energy_class"] = _ENERGY_CLASS_BY_INDEX[idx]
                    return
            except ValueError:
                pass

        # Source 2: static .active class (fallback)
        active = parser.css_first(".energy-section .energy.active")
        if active is None:
            active = parser.css_first(".energy.active")
        if active is None:
            return

        data_id = (active.attributes.get("data-id") or "").strip()
        if data_id.isdigit():
            idx = int(data_id)
            if idx in _ENERGY_CLASS_BY_INDEX:
                data["extra_features"]["energy_class"] = _ENERGY_CLASS_BY_INDEX[idx]
                return

        # Fallback: text content (may be Greek or Latin)
        text = active.text(strip=True)
        if text:
            translated = _translate_energy_class(text)
            if translated:
                data["extra_features"]["energy_class"] = translated

    # ---------------------------------------------------------------
    # Phase 2 helpers — Coordinates
    # ---------------------------------------------------------------

    def _parse_coordinates(self, raw_html: str, data: Dict[str, Any]) -> None:
        """
        Parse Leaflet JS for GPS. Same pattern as halkidiki_estate:

          var lat = 40.315219310123354;
          var long = 23.06646125628688;
          var icon = 1;  // 1 = privacy circle, 0 = exact marker
          ...
          L.circle([lat,long], 400, {fillColor: 'blue', radius: 40}).addTo(map);

        Halkidiki bbox sanity check rejects malformed coords.
        """
        m_lat = re.search(r"var\s+lat\s*=\s*([0-9.\-]+)", raw_html)
        m_lng = re.search(r"var\s+long?\s*=\s*([0-9.\-]+)", raw_html)
        if m_lat and m_lng:
            try:
                lat = float(m_lat.group(1))
                lng = float(m_lng.group(1))
                if (_HALKIDIKI_LAT_RANGE[0] <= lat <= _HALKIDIKI_LAT_RANGE[1]
                        and _HALKIDIKI_LNG_RANGE[0] <= lng <= _HALKIDIKI_LNG_RANGE[1]):
                    data["latitude"] = lat
                    data["longitude"] = lng
            except ValueError:
                pass

        # Icon type — circle (privacy) vs marker (exact)
        m_icon = re.search(r"var\s+icon\s*=\s*(\d+)", raw_html)
        if m_icon and data.get("latitude") is not None:
            icon_val = int(m_icon.group(1))
            if icon_val == 1:
                m_circle = re.search(
                    r"L\.circle\(\[\s*[a-z_,\s]+\]\s*,\s*(\d+)",
                    raw_html,
                )
                if m_circle:
                    try:
                        radius = int(m_circle.group(1))
                        data["extra_features"]["gps_type"] = "circle"
                        data["extra_features"]["gps_radius_m"] = radius
                    except ValueError:
                        pass
            elif icon_val == 0:
                data["extra_features"]["gps_type"] = "exact"

    # ---------------------------------------------------------------
    # Generic field routing
    # ---------------------------------------------------------------

    def _route_keyed_field(
        self,
        label: str,
        value: Optional[str],
        data: Dict[str, Any],
    ) -> None:
        """
        Generic routing: label/value → property column or extra_features.
        """
        if not label:
            return

        label_lower = label.strip().lower()
        slug = _slug(label)
        if not slug:
            return

        # Explicit drops
        if label_lower in _DROP_LABELS:
            return

        # SPECIAL: For Land category, "Area" means LAND area (the plot size)
        # not building size. The info block uses a single "Area" label for
        # both; we disambiguate by category set in _parse_header.
        if label_lower == "area" and data.get("category") == "Land":
            self._write_column("land_size_sqm", value, data)
            return

        # Property column?
        column = _LABEL_TO_PROPERTY_COLUMN.get(label_lower)
        if column is not None:
            self._write_column(column, value, data)
            return

        # extra_features
        # Boolean amenity (no colon, no value)
        if value is None:
            data["extra_features"][slug] = True
            return

        # Count fields ("Living Room: 1") — MUST be before yes/no due to
        # '1' being in _YES_VALUES.
        if label_lower in _COUNT_LABELS:
            n = _to_int_simple(value)
            if n is not None:
                data["extra_features"][f"{slug}_count"] = n
                return

        # Yes/No → bool
        yn = _normalise_yes_no(value)
        if yn is not None:
            data["extra_features"][slug] = yn
            return

        # Default: string (heating, status, view, frames, ...)
        data["extra_features"][slug] = value

    def _write_column(
        self,
        column: str,
        value: Optional[str],
        data: Dict[str, Any],
    ) -> None:
        """Type-coerce a value into the named Property column."""
        if value is None or value == "":
            return

        if column == "price":
            v = _to_int_euro(value)
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
            v = _to_int_simple(value)
            if v is not None and 1900 < v < 2100:
                data["year_built"] = v
        elif column == "levels":
            data["levels"] = value.strip()
        elif column == "category":
            data["category"] = value.strip()
        elif column in {"area", "subarea"}:
            if data.get(column) is None:
                data[column] = value.strip()
