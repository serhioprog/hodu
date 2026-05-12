"""
Halkidiki Estate scraper — RealStatus/iarts CMS platform.

Halkidikiestate.com is built on the "Real Status" broker program by iarts.gr —
a Greek real-estate CMS with a clean HTML structure and predictable RESTful
URLs. Server-rendered (no SPA), no anti-bot challenges, 200 OK on Stage 0.

Site analysis findings
======================

URL structure (path-segment style, NOT query params):

  Base:      /listings
  Filters:   /listings/priceFrom/{min}/for/1/category/1
  Pagination: /listings/priceFrom/{min}/for/1/category/1/page/{N}
  Detail:    /property/{id}/{transliterated-slug}

  for/1     = Sale (vs for/2 = Rent)
  category/1 = Residential (vs 2=Commercial, 3=Land, 4=Other)

Language switching:
  Append ?language=en to ANY URL to force English. Confirmed: with no query
  param, content is Greek (ΠΩΛΗΣΗ, Μονοκατοικία, etc.). With ?language=en,
  content is English (SALE, Detached house, etc.). NB: this differs from
  Sithonia (?lang=en) — distinct query param name.

Listing card structure (.property-item.h-250):
  - <div class="favorite-add" data-id="1236">  — CLEAN site_property_id
  - <a class="prop-link" href="/property/{id}/{slug}">
  - <h2 class="fs-16 fw-bold">{title}</h2>
  - <span><i class="la-map-marker-alt"></i>Chalkidiki, Sithonia, Ormos Panagias</span>
  - <ul class="listing-icons"><li><i la-bed/><span>N beds</span></li>...</ul>
  - <span class="fw-bold">820.000 €</span>  OR
    <span class="oldprice fw-bold">450.000 €</span> + <span class="proprice fw-bold">400.000 €</span>

Pagination signal:
  <li class="pagination-list active" data-id="N">N</li>
  When site falls back to page 1 (out-of-range request), active_page=1.
  We compare with our requested page to detect overflow.

Detail page structure:
  - <h1>{title}</h1>
  - .property-price span — main price
  - .property-badges span — "FOR SALE"
  - #property-desc p — description (often 500–1500 chars)
  - .information-list with .inf-item children — INFO block
    Each <li>: <div class="inf-item"><span>Label:</span></div><span>Value</span>
  - .information-list WITHOUT .inf-item children — DISTANCE block
    Each <li>: <i></i>Label: 300 meters.
  - .property-feautures li span — FEATURES block
    Either "Label: Value" (e.g. "Area: 270 sq.m") OR bool ("A/C")
  - .slider-wrapper.gallery .slider-property-img a[href] — photo gallery
  - <script>var lat = X; var long = Y;</script> — Leaflet GPS
  - L.circle([lat, long], 400, ...) — privacy obfuscation (radius 400m typical)

Key architectural decisions
===========================

1) Force ?language=en — stable English labels everywhere

2) Card ID from data-id attribute (cleanest source), NOT URL parsing.
   data-id="1236" → site_property_id="1236". The URL slug carries it too but
   includes transliterated category making URL parsing fragile.

3) Discounted-price handling:
   - If .proprice exists → use that (active reduced price)
   - Else first .fw-bold WITHOUT .oldprice class
   .oldprice is the strike-through original price; we never use it.

4) Info vs Distance discrimination (both use .information-list):
   - INFO  li has .inf-item child → label/value via two <span>s
   - DIST  li has NO .inf-item    → plain text "Label: 300 meters."

5) Energy class is in Greek (Γ, Δ, Ε...) — translate to Latin (C, D, E).
   The <span data-id="N"> attribute is also available as fallback signal.

6) GPS = L.circle (privacy obfuscation, 400m typical, same approach as
   Sithonia/Estate+). Coords stored to lat/lng; gps_type='circle',
   gps_radius_m=400 → extra_features. 400m precision works for building-level
   clustering.

7) Photo gallery uses .slider-property-img a[href]. Slick carousel would
   inject clones at runtime BUT server returns pre-render HTML (no clones).
   Defensive dedup-by-URL anyway. Fallback: og:image meta tag.

8) Card title contains marketing copy not category name ("Detached house
   with sea view", "Luxury Beachfront Villa..."). We scan for known
   keywords (villa/maisonette/apartment) at card level; canonical category
   comes from category filter URL (always "Residential" for our use).

9) Whitelist auto-match: location_raw is "Chalkidiki, Sithonia, X" — contains
   "Chalkidiki" matching the Halkidiki whitelist (alt spelling). We force-
   append "Halkidiki" if not present.
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

# English labels (after ?language=en) routed to first-class Property columns.
# Lowercase, no trailing colons. NOTE: 'Rooms' on this site means bedrooms.
_LABEL_TO_PROPERTY_COLUMN: Dict[str, str] = {
    # Info block
    "rooms":               "bedrooms",
    "bathroom":            "bathrooms",
    "year of manufacture": "year_built",
    "year of construction": "year_built",  # defensive synonym
    "levels":              "levels",
    # Features block
    "area":                "size_sqm",
    "plot area":           "land_size_sqm",
}

# Labels we explicitly DROP — always-same values or duplicates.
_DROP_LABELS: set = {
    "code",   # already captured upstream as site_property_id from data-id
}

# Integer count fields stored as extra_features.<slug>_count
# (e.g. 'living room' → living_room_count). Must be checked BEFORE yes/no
# routing because '1' is in _YES_VALUES (count=1 would hijack to True).
_COUNT_LABELS: set = {
    "kitchen",
    "living room",
    "wc",
}

# Greek energy class labels → Latin equivalents (Greek law since 2010):
#   1: A+, 2: A, 3: B+, 4: B, 5: C, 6: D, 7: E, 8: F, 9: G
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

# data-id → Latin energy class. Used as fallback if Greek translation fails.
_ENERGY_CLASS_BY_INDEX: Dict[int, str] = {
    1: "A+", 2: "A", 3: "B+", 4: "B", 5: "C", 6: "D", 7: "E", 8: "F", 9: "G",
}

# Greek-transliterated category prefixes in URL slugs.
# Slug format: /property/{id}/{Transliterated-Category}-{Place}
# This is autogenerated server-side and IS NOT translated by ?language=en,
# so it's a reliable category source when card title is just marketing copy
# (e.g. "Luxury Living Just 100m from the Sea" — no type keyword).
_CATEGORY_FROM_SLUG: Dict[str, str] = {
    "monokatoikia":  "Detached House",
    "bila":          "Villa",
    "mezoneta":      "Maisonette",
    "diamerisma":    "Apartment",
    "oikopedo":      "Land",
    "agrotemachio":  "Agricultural Land",
    "studio":        "Studio",
    "loft":          "Loft",
    "ktirio":        "Building",
    "katastima":     "Shop",
    "grafeio":       "Office",
    "ksenodochio":   "Hotel",
}

# Yes/No normalisation. English-only — site is forced to ?language=en.
_YES_VALUES = {"yes", "y", "true", "1"}
_NO_VALUES  = {"no",  "n", "false", "0"}

# Halkidiki bounding box. GPS outside is rejected as malformed/default.
_HALKIDIKI_LAT_RANGE = (39.0, 41.0)
_HALKIDIKI_LNG_RANGE = (22.0, 24.0)

# Maximum pages to walk. 64 listings ÷ 12 per page = 6 pages real. Cap is
# defensive against pagination bugs (infinite loops on broken HTML).
_PAGE_SAFETY_CAP = 30


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

      "820.000 €"      -> 820000
      "1.250.000 €"    -> 1250000
      "400.000,00€"    -> 400000  (cents dropped)
      ""               -> None

    Cap >€200M as malformed.
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
    """Parse area-in-square-meters strings. '270 sq.m' → 270.0"""
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
    """'Yes'/'No' → bool; None for anything else."""
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
    "Area: 270 sq.m"  -> ("Area", "270 sq.m")
    "A/C"             -> ("A/C", None)  -- boolean amenity
    """
    text = text.strip().rstrip(".")  # strip trailing period (distance section)
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
    # Already Latin (A–G with optional +)
    if re.match(r"^[A-G][+]?$", t, re.IGNORECASE):
        return t.upper()
    return _GREEK_ENERGY_CLASS.get(t.lower(), t)


def _guess_category_from_title(title: str) -> Optional[str]:
    """
    Card title is marketing copy. We scan for category keywords:
      "Detached house with sea view"        -> "Detached House"
      "Luxury Beachfront Villa - First Line" -> "Villa"
      "Stunning 110 sq.m. Maisonette..."    -> "Maisonette"
      "Investment property of 7 studios..." -> "Studio"

    Returns None if no keyword matched.
    """
    if not title:
        return None
    lower = title.lower()
    # Order matters: more specific first
    for keyword, category in [
        ("detached house",  "Detached House"),
        ("apartment complex", "Apartment Complex"),
        ("maisonette",      "Maisonette"),
        ("villa",           "Villa"),
        ("apartment",       "Apartment"),
        ("studio",          "Studio"),
        ("loft",            "Loft"),
        ("building",        "Building"),
        ("country house",   "Country House"),
        ("house",           "House"),
    ]:
        if keyword in lower:
            return category
    return None


def _build_location_raw(card_location_text: Optional[str]) -> str:
    """
    Build whitelist-safe location_raw. Card text format:
      "Chalkidiki, Sithonia, Ormos Panagias"

    We force-append " Halkidiki" if neither Chalkidiki nor Halkidiki is
    present (defensive against future label changes).
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

class HalkidikiEstateScraper(EnrichmentMixin, BaseScraper):
    # Same rationale as grekodom: detail page may not have explicit category;
    # NLP would fill None → "Land/Plot" (NLP's canonical name), overwriting
    # the cleaner card-derived "Land" on next daily_sync save.
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )
    
    """
    Halkidiki Estate scraper — RealStatus/iarts CMS platform, Halkidiki agent.

    At min_price=400000, for/1 (Sale), category/1 (Residential):
    ~64 listings across 6 pages of 12 cards each. Categories include
    Detached House, Villa, Maisonette, Apartment, Studio.
    """

    BASE_URL = "https://www.halkidikiestate.com"

    def __init__(self):
        super().__init__()
        self.source_domain = "halkidikiestate.com"

    async def fetch_listings(self):
        """Backwards-compatible entry point for the dispatcher."""
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # URL builders
    # ---------------------------------------------------------------

    def _build_list_url(self, page: int, min_price: int) -> str:
        """
        RESTful path-segment URL. `for/1` = Sale, `category/1` = Residential.
        ?language=en forces English content (default is Greek).
        """
        return (
            f"{self.BASE_URL}"
            f"/listings/priceFrom/{min_price}/for/1/category/1"
            f"/page/{page}"
            f"?language=en"
        )

    def _construct_detail_url(self, site_id: str) -> str:
        """
        Detail URL with bare ID (slug omitted — server resolves on ID alone).
        ?language=en forces English structured labels.
        """
        return f"{self.BASE_URL}/property/{site_id}?language=en"

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs
    # ---------------------------------------------------------------

    async def collect_urls(self, min_price: int = 400000) -> List[PropertyTemplate]:
        """
        Walk paginated listing. Three stop conditions:
          1. .property-item.h-250 returns empty
          2. .pagination-list.active reports back a page number lower than
             requested (site is silently falling back to page 1)
          3. PAGE_SAFETY_CAP exceeded
        """
        all_properties: List[PropertyTemplate] = []
        page = 1

        while page <= _PAGE_SAFETY_CAP:
            url = self._build_list_url(page, min_price)
            logger.info(f"[{self.source_domain}] Парсинг страницы {page}...")

            try:
                response = await self.client.get(url)
                parser = LexborHTMLParser(response.text)
                cards = parser.css(".property-item.h-250")

                if not cards:
                    logger.info(
                        f"[{self.source_domain}] нет карточек на странице {page} "
                        f"— конец пагинации"
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
                                f"[{self.source_domain}] запрошена стр.{page}, "
                                f"но сервер вернул стр.{active_n} — конец пагинации"
                            )
                            break

                page_count = 0
                for card in cards:
                    try:
                        prop = self._parse_card(card)
                        if prop:
                            all_properties.append(prop)
                            page_count += 1
                    except Exception as e:
                        logger.error(
                            f"[{self.source_domain}] ошибка парсинга карточки: {e}"
                        )

                logger.info(
                    f"[{self.source_domain}] страница {page}: "
                    f"собрано {page_count} объектов"
                )

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

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """Extract seed PropertyTemplate from one .property-item.h-250 card."""
        # site_property_id — cleanest source is .favorite-add[data-id]
        fa = card.css_first(".favorite-add")
        if not fa:
            return None
        site_id = (fa.attributes.get("data-id") or "").strip()
        if not site_id:
            return None

        full_url = self._construct_detail_url(site_id)

        # Title
        title_h2 = card.css_first("h2.fs-16.fw-bold")
        title = title_h2.text(strip=True) if title_h2 else ""

        # Location: first <span> in .card-body containing Chalkidiki/Halkidiki
        location_raw = "Halkidiki"
        body = card.css_first(".card-body")
        if body:
            for span in body.css("span"):
                text = span.text(separator=" ", strip=True)
                lower = text.lower()
                if "halkidiki" in lower or "chalkidiki" in lower or "χαλκιδική" in lower:
                    location_raw = _build_location_raw(text)
                    break

        # Price: prefer .proprice (active discounted), else .fw-bold without
        # .oldprice (which is strike-through original price)
        price_text = None
        listing_price = card.css_first(".listing-price")
        if listing_price:
            proprice = listing_price.css_first(".proprice")
            if proprice:
                price_text = proprice.text(strip=True)
            else:
                for span in listing_price.css("span.fw-bold"):
                    cls = span.attributes.get("class", "") or ""
                    if "oldprice" not in cls:
                        price_text = span.text(strip=True)
                        break

        # Icons row: beds, baths, parking, size
        bedrooms = None
        bathrooms = None
        size_sqm = None
        for li in card.css(".listing-icons li.icons-list"):
            text = li.text(separator=" ", strip=True).lower()
            if "bed" in text:
                m = re.search(r"(\d+)", text)
                if m:
                    bedrooms = int(m.group(1))
            elif "bathroom" in text or "bath" in text:
                m = re.search(r"(\d+)", text)
                if m:
                    bathrooms = int(m.group(1))
            elif "sq.m" in text or "sq m" in text:
                m = re.search(r"(\d+(?:[.,]\d+)?)", text)
                if m:
                    try:
                        size_sqm = float(m.group(1).replace(",", "."))
                    except ValueError:
                        pass

        category = _guess_category_from_title(title)

        # Fallback: extract category from URL slug if title-keyword failed.
        # Slug format: '/property/{id}/Monokatoikia-Sithonia' — server-side
        # generated, always Greek-transliterated regardless of ?language=en.
        if category is None:
            prop_link = card.css_first(".prop-link")
            if prop_link:
                href = prop_link.attributes.get("href", "") or ""
                m = re.search(r"/property/\d+/([^/?]+)", href)
                if m:
                    slug_first_word = m.group(1).split("-")[0].lower()
                    category = _CATEGORY_FROM_SLUG.get(slug_first_word)

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
          - Description (#property-desc <p>)
          - Information block (.information-list with .inf-item children)
          - Distance block (.information-list without .inf-item)
          - Features block (.property-feautures li)
          - GPS coords from Leaflet JS
          - Photos (.slider-wrapper.gallery)
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

            # 3. Header price + badge (fallback if structured blocks miss)
            self._parse_header(parser, data)

            # 4. Information + Distance + Features blocks
            self._parse_structured_blocks(parser, data)

            # 5. Coordinates from Leaflet JS
            self._parse_coordinates(raw_html, data)

            # 6. Location breakdown from header (Chalkidiki, Sithonia, X)
            self._parse_location_from_header(parser, data)

            # 7. NLP fallback over description (EnrichmentMixin step 5).
            # Fills missing year_built/bedrooms/bathrooms/etc + adds
            # amenity features (sea_view, pool, parking, ...) from text.
            # Never overwrites structural data already filled above.
            self._apply_nlp_fallback(data)

            # 8. Quality Gate (log-only — daily_sync decides whether to retry)
            if not self._passes_quality_gate(data.get("description")):
                logger.warning(
                    f"[{self.source_domain}] description below quality gate "
                    f"for {url}"
                )

            # 9. Drop None values so card seed isn't clobbered.
            return {k: v for k, v in data.items() if v is not None}

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] ошибка fetch_details для {url}: {e}"
            )
            return {}

    # ---------------------------------------------------------------
    # Phase 2 helpers
    # ---------------------------------------------------------------

    def _collect_image_urls(self, parser: LexborHTMLParser) -> List[str]:
        """
        Photos from .slider-wrapper.gallery .slider-property-img a[href].
        Filter to image extensions, dedup by URL.

        Fallback: meta[property="og:image"] for properties without gallery.
        """
        photos: List[str] = []
        for a in parser.css(".slider-wrapper.gallery .slider-property-img a"):
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
        Description from <p> inside #property-desc.

        Falls back to og:description meta tag (via EnrichmentMixin) when
        the main block is missing or has no qualifying paragraphs — better
        than empty, still passes Quality Gate (typically 1-2 sentences).
        """
        desc_block = parser.css_first("#property-desc")
        if desc_block:
            paragraphs = []
            for p in desc_block.css("p"):
                txt = p.text(separator=" ", strip=True)
                if txt and len(txt) >= 30 and txt not in paragraphs:
                    paragraphs.append(txt)
            if paragraphs:
                return "\n\n".join(paragraphs)

        # Fallback: og:description meta tag
        return self._og_description_fallback(parser)

    def _parse_header(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Parse the top header for price (fallback — also covered by features
        section in some properties).
        """
        # Price: <div class="property-price"><span>820.000 €</span></div>
        price_span = parser.css_first(".property-price span")
        if price_span:
            v = _to_int_euro(price_span.text(strip=True))
            if v is not None:
                data["price"] = v

    def _parse_location_from_header(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Header has '.property-title span' with text:
          'Chalkidiki, Sithonia, Ormos Panagias'

        Split into:
          - DROP first segment (prefecture, always 'Chalkidiki')
          - subarea = second segment ('Sithonia' = municipality)
          - area    = third segment ('Ormos Panagias' = neighborhood, most
                                     specific = most useful for clustering)
        """
        title_block = parser.css_first(".property-title")
        if not title_block:
            return
        for span in title_block.css("span"):
            text = span.text(separator=" ", strip=True)
            lower = text.lower()
            if "chalkidiki" in lower or "halkidiki" in lower:
                parts = [p.strip() for p in text.split(",") if p.strip()]
                # Skip the prefecture (always Chalkidiki / Halkidiki)
                non_prefecture = [
                    p for p in parts
                    if "chalkidiki" not in p.lower() and "halkidiki" not in p.lower()
                ]
                if len(non_prefecture) >= 1:
                    # Last part is most specific (neighborhood) → area
                    if data.get("area") is None:
                        data["area"] = non_prefecture[-1]
                if len(non_prefecture) >= 2:
                    # Second-to-last is municipality → subarea
                    if data.get("subarea") is None:
                        data["subarea"] = non_prefecture[-2]
                break

    def _parse_coordinates(self, raw_html: str, data: Dict[str, Any]) -> None:
        """
        Parse Leaflet JS for GPS. Two relevant patterns:

          var lat = 40.230740097202;
          var long = 23.741115326936;
          var icon = 1;  // 1 = privacy circle, 0 = exact marker
          ...
          L.circle([lat,long], 400, {fillColor: 'blue', radius: 40}).addTo(map);

        We extract:
          - latitude/longitude from `var lat = N; var long = N;`
          - radius from L.circle(..., N, ...) — privacy obfuscation indicator
          - extra_features.gps_type = 'circle' | 'exact' (if marker icon)

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
                # Circle — extract radius
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
    # Structured panels — Info / Distance / Features
    # ---------------------------------------------------------------

    def _parse_structured_blocks(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Iterate three structured blocks:
          1. .information-list with .inf-item children → INFO block
          2. .information-list without .inf-item → DISTANCE block
          3. .property-feautures li → FEATURES block

        Block 1+2 share the same parent UL class, so we discriminate per-li
        by presence of .inf-item child.
        """
        # Info + Distance (same selector, different per-li structure)
        for li in parser.css(".information-list li"):
            if li.css_first(".inf-item"):
                self._parse_info_li(li, data)
            else:
                self._parse_distance_li(li, data)

        # Features (separate selector)
        for li in parser.css(".property-feautures li"):
            self._parse_feature_li(li, data)

    def _parse_info_li(self, li: LexborNode, data: Dict[str, Any]) -> None:
        """
        Info li structure:
          <li>
            <div class="inf-item"><i></i><span>Label:</span></div>
            <span>Value</span>     <-- may have extra classes for energy
          </li>

        spans[0] = label (inside .inf-item)
        spans[1] = value (sibling of .inf-item)
        """
        spans = li.css("span")
        if len(spans) < 2:
            return  # malformed or label-only entry

        label_raw = spans[0].text(strip=True).rstrip(":").strip()
        value_span = spans[1]
        value = value_span.text(strip=True)
        label_lower = label_raw.lower()

        # Special handling: Energy Class (Greek letter translation)
        if "energy" in label_lower and "class" in label_lower:
            translated = _translate_energy_class(value)
            # Fallback to data-id mapping if translation didn't yield Latin
            if not re.match(r"^[A-G][+]?$", translated, re.IGNORECASE):
                data_id = value_span.attributes.get("data-id", "") or ""
                if data_id.isdigit():
                    idx = int(data_id)
                    if idx in _ENERGY_CLASS_BY_INDEX:
                        translated = _ENERGY_CLASS_BY_INDEX[idx]
            data["extra_features"]["energy_class"] = translated
            return

        self._route_keyed_field(label_raw, value, data)

    def _parse_distance_li(self, li: LexborNode, data: Dict[str, Any]) -> None:
        """
        Distance li structure:
          <li><i class="la-umbrella-beach"></i>Sea: 300 meters.</li>

        Result: extra_features.distance_from_<slug> = "300 meters"
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

    def _parse_feature_li(self, li: LexborNode, data: Dict[str, Any]) -> None:
        """
        Feature li structure:
          <li><i></i><span>Area: 270 sq.m</span></li>      -- key:value
          <li><i></i><span>A/C</span></li>                  -- boolean amenity
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
        Identical logic shape to Sithonia's _route_field, but mapping table
        is halkidikiestate-specific.
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
        # '1' being in _YES_VALUES (same lesson as Sithonia).
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