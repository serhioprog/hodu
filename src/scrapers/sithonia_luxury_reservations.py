"""
~/hodu/src/scrapers/sithonia_luxury_reservations.py

sithonialuxuryreservations.gr scraper — estateplus.gr-powered CMS,
first scraper for estateplus.gr family in our collection.
Sprint 11 #2.

Anti-bot: clean Stage 0 (curl_cffi chrome120, ~200ms response).

Agent: Sithonia RS Luxury Reservations (info@sithoniars.gr, +306988929459).
Backend: sithoniars.estateplus.gr (estateplus.gr SaaS broker software).

URL patterns
------------
List:    /listings?listingType=for+sale&price_min={min}&search=Search&lang=en
            &page={N}
         No category filter \u2014 captures all property types (residential,
         commercial, land, other). At priceFrom 400000: 36 total across 4 pages
         (~9-10 per page). Pagination &page=N (clean URL pattern).
Detail:  /{site_id}?lang=en   (4-6 digit numeric ID; resolves via <base href="/">)

Mixed-language category labels
------------------------------
With lang=en the title h3 is English ("Parcel 10500 m\u00b2") but the
"Category:" row in .kf_property_detail_Essentail can still show Greek
("\u0391\u03b3\u03c1\u03bf\u03c4\u03b5\u03bc\u03ac\u03c7\u03b9\u03bf" = Land). _CATEGORY_MAP includes Greek
mappings for safety; title-scan still wins when English category word appears.

Coords: JS Leaflet init `L.map('map').setView([lat, lng], ...)` \u2014 bbox-gated.

Images: <a href="..." data-imagelightbox="g"> inside .bxslider li \u2014 full
resolution URLs (not the background-image div).

Property info panel
-------------------
.kf_property_detail_Essentail ul li a holds "Label: Value" rows (with
fa-check-circle icon). Includes top-level fields (Price, Area, Subarea,
Neighborhood, Category, Area as in sqm) plus extras (Zoning, Orientation,
Slope, View, Distance from Sea). Some entries are bare strings without
colon ("Facade", "Furnished") \u2014 treated as boolean flags.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.models.schemas import PropertyTemplate
from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin


# ============================================================================
# Constants
# ============================================================================

_BASE_URL = "https://sithonialuxuryreservations.gr"
_SOURCE_DOMAIN = "sithonialuxuryreservations.gr"

# Pagination safety cap
_MAX_PAGES = 30
_INTER_PAGE_SLEEP_SEC = 1.5

# Halkidiki bbox for coord sanity
_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# Category mapping: English + Greek (Greek leaks into Category: row on lang=en pages)
_CATEGORY_MAP: Dict[str, str] = {
    # English
    "maisonette":   "Maisonette",
    "apartment":    "Apartment",
    "studio":       "Apartment",
    "loft":         "Apartment",
    "penthouse":    "Apartment",
    "flat":         "Apartment",
    "villa":        "Villa",
    "house":        "House",
    "home":         "House",
    "residence":    "House",
    "residency":    "House",
    "cottage":      "House",
    "detached":     "House",
    "bungalow":     "House",
    "land":         "Land",
    "plot":         "Land",
    "parcel":       "Land",
    "field":        "Land",
    "office":       "Commercial",
    "shop":         "Commercial",
    "store":        "Commercial",
    "warehouse":    "Commercial",
    "commercial":   "Commercial",
    "professional": "Commercial",
    "hotel":        "Hotel/Commercial",
    "building":     "Building",
    "complex":      "Building",
    # Greek (lowercased)
    "\u03b4\u03b9\u03b1\u03bc\u03ad\u03c1\u03b9\u03c3\u03bc\u03b1":   "Apartment",   # diamerisma
    "\u03bc\u03b5\u03b6\u03bf\u03bd\u03ad\u03c4\u03b1":               "Maisonette",  # mezoneta
    "\u03b2\u03af\u03bb\u03b1":                                       "Villa",       # vila
    "\u03b2\u03af\u03bb\u03bb\u03b1":                                 "Villa",       # villa
    "\u03bc\u03bf\u03bd\u03bf\u03ba\u03b1\u03c4\u03bf\u03b9\u03ba\u03af\u03b1": "House",   # monokatoikia
    "\u03c3\u03c0\u03af\u03c4\u03b9":                                 "House",       # spiti
    "\u03b1\u03b3\u03c1\u03bf\u03c4\u03b5\u03bc\u03ac\u03c7\u03b9\u03bf": "Land",    # agrotemachio
    "\u03bf\u03b9\u03ba\u03cc\u03c0\u03b5\u03b4\u03bf":               "Land",        # oikopedo
    "\u03ba\u03b1\u03c4\u03ac\u03c3\u03c4\u03b7\u03bc\u03b1":         "Commercial",  # katastima
    "\u03b3\u03c1\u03b1\u03c6\u03b5\u03af\u03bf":                     "Commercial",  # grafeio
    "\u03be\u03b5\u03bd\u03bf\u03b4\u03bf\u03c7\u03b5\u03af\u03bf":   "Hotel/Commercial",  # xenodocheio
}


# ============================================================================
# Helpers
# ============================================================================

def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


def _slug(label: str) -> str:
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_int_simple(text: str) -> Optional[int]:
    """First integer; Greek format uses '.' as thousands separator."""
    if not text:
        return None
    m = re.search(r"\d[\d.]*", text)
    if not m:
        return None
    try:
        return int(m.group(0).replace(".", ""))
    except ValueError:
        return None


def _to_float_sqm(text: str) -> Optional[float]:
    """Parse '10500 m\u00b2', '85.5 m\u00b2', '1.200 m\u00b2' formats."""
    if not text:
        return None
    m = re.search(r"\d[\d.,]*", text)
    if not m:
        return None
    raw = m.group(0)
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")
    elif raw.count(".") > 1:
        raw = raw.replace(".", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _category_from_text(text: str) -> Optional[str]:
    """Scan ALL alphabetic words in text against _CATEGORY_MAP."""
    t = _normalize_text(text).lower()
    if not t:
        return None
    # English words (a-z)
    for word in re.findall(r"[a-z]+", t):
        cat = _CATEGORY_MAP.get(word)
        if cat:
            return cat
    # Greek words (\u0386-\u03ce, with diacritics)
    for word in re.findall(r"[\u03b1-\u03c9\u0386-\u03ce\u03ac-\u03ce]+", t):
        cat = _CATEGORY_MAP.get(word)
        if cat:
            return cat
    return None


# ============================================================================
# Scraper
# ============================================================================

class SithoniaLuxuryReservationsScraper(EnrichmentMixin, BaseScraper):
    """sithonialuxuryreservations.gr \u2014 estateplus.gr-powered, paginated walk."""

    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
        # category omitted \u2014 NLP picks marketing buzzwords; we have title scan
        # + Greek Category: row + URL category for solid coverage
    )

    _NLP_TO_STRUCTURAL: Dict[str, Set[str]] = {
        "swimming_pool":    {"pool", "swimming_pool"},
        "sea_view":         {"view_sea", "sea_view"},
        "parking":          {"parking", "private_parking"},
        "air_conditioning": {"air_conditioning", "ac"},
        "fireplace":        {"fire_place", "fireplace"},
        "balcony":          {"balcony", "balconies"},
        "garden":           {"garden", "private_garden"},
        "storage_room":     {"storage", "storage_room"},
        "elevator":         {"elevator", "lift"},
        "furnished":        {"furnished"},
        "alarm":            {"alarm", "security_system"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        # BaseScraper.__init__ writes self.source_domain="" \u2014 restore canonical.
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builders ─────────────────────────────────────────────────

    def _build_list_url(self, page: int, min_price: int) -> str:
        return (
            f"{_BASE_URL}/listings"
            f"?listingType=for+sale&price_min={min_price}"
            f"&search=Search&lang=en&page={page}"
        )

    def _build_detail_url(self, site_id: str) -> str:
        return f"{_BASE_URL}/{site_id}?lang=en"

    # ── Phase 1: collect_urls ────────────────────────────────────────

    async def collect_urls(
        self, min_price: int = 400_000,
    ) -> List[PropertyTemplate]:
        seeds: Dict[str, PropertyTemplate] = {}
        total_pages_known: Optional[int] = None
        page = 1

        while page <= _MAX_PAGES:
            url = self._build_list_url(page, min_price)
            logger.info(f"[{self.source_domain}] Stage 0 GET {url}")

            try:
                resp = await self.client.get(url)
            except Exception as exc:
                logger.error(
                    f"[{self.source_domain}] page {page} fetch failed: {exc!r}"
                )
                break

            if resp.status_code != 200:
                logger.warning(
                    f"[{self.source_domain}] page {page}: HTTP {resp.status_code}"
                )
                break

            parser = LexborHTMLParser(resp.text)

            # Detect last page from pagination on page 1
            if page == 1 and total_pages_known is None:
                total_pages_known = self._detect_total_pages(parser)
                if total_pages_known:
                    logger.info(
                        f"[{self.source_domain}] pagination: {total_pages_known} pages"
                    )

            cards = parser.css(".kf_listing_outer_wrap")
            if not cards:
                logger.info(
                    f"[{self.source_domain}] page {page}: 0 cards \u2014 end"
                )
                break

            page_added = 0
            for card in cards:
                try:
                    seed = self._parse_card(card)
                except Exception as exc:
                    logger.error(
                        f"[{self.source_domain}] card parse: {exc!r}"
                    )
                    continue
                if seed and seed.site_property_id not in seeds:
                    seeds[seed.site_property_id] = seed
                    page_added += 1

            logger.info(
                f"[{self.source_domain}] page {page}: {len(cards)} cards "
                f"(+{page_added} new, total {len(seeds)})"
            )

            if total_pages_known and page >= total_pages_known:
                logger.info(
                    f"[{self.source_domain}] reached last page {page}/{total_pages_known}"
                )
                break

            if page_added == 0 and page > 1:
                logger.info(
                    f"[{self.source_domain}] page {page}: all duplicates \u2014 end"
                )
                break

            await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)
            page += 1

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} unique seeds"
        )
        return list(seeds.values())

    def _detect_total_pages(self, parser: LexborHTMLParser) -> Optional[int]:
        """Parse <ul class='pagination'> for max page link."""
        max_n = 0
        for a in parser.css("ul.pagination li a"):
            text = a.text(strip=True)
            if text.isdigit():
                n = int(text)
                if n > max_n:
                    max_n = n
        return max_n if max_n > 0 else None

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        # site_id: from any anchor href inside card (e.g. <a href="104000">)
        site_id: Optional[str] = None
        for a in card.css("a[href]"):
            href = (a.attributes.get("href") or "").strip()
            if href.isdigit():
                site_id = href
                break
        if not site_id:
            return None

        # Detail URL
        detail_url = self._build_detail_url(site_id)

        # Title: h5 inside .kf_property_caption (first h5)
        title = ""
        caption = card.css_first(".kf_property_caption")
        if caption:
            h5 = caption.css_first("h5")
            if h5:
                title = _normalize_text(h5.text(strip=False))

        category = _category_from_text(title)

        # Size from title trailing "{N} m\u00b2"
        size_sqm: Optional[float] = None
        m = re.search(r"([\d,.]+)\s*m\s*[\u00b2]?", title)
        if m:
            size_sqm = _to_float_sqm(m.group(1))

        # Location: text after fa-map-marker icon
        location_raw: Optional[str] = None
        if caption:
            p = caption.css_first("p")
            if p:
                # "Code <strong>104000</strong>  <i class='fa-map-marker'></i> Sithonia"
                ptext = _normalize_text(p.text(strip=False))
                # Extract text after the Code+id portion
                # Cleanup: remove "Code N" prefix
                cleaned = re.sub(r"^Code\s*\S+\s*", "", ptext).strip()
                if cleaned:
                    location_raw = cleaned

        # Price: standalone h5 outside .kf_property_caption (second h5)
        price: Optional[int] = None
        all_h5 = card.css("h5")
        if len(all_h5) >= 2:
            price = self._to_int_euro_safe(
                _normalize_text(all_h5[1].text(strip=False))
            )

        seed = PropertyTemplate(
            site_property_id=str(site_id),
            source_domain=self.source_domain,
            url=detail_url,
            price=price,
            size_sqm=size_sqm,
            location_raw=location_raw,
        )
        if category:
            try:
                seed.category = category
            except Exception:
                pass
        return seed

    # ── Phase 2: fetch_details ───────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        fetch_url = url if "lang=en" in url else f"{url}{'&' if '?' in url else '?'}lang=en"

        try:
            resp = await self.client.get(fetch_url)
        except Exception as exc:
            logger.warning(
                f"[{self.source_domain}] detail fetch failed {url}: {exc!r}"
            )
            return {}

        if resp.status_code != 200:
            logger.warning(
                f"[{self.source_domain}] HTTP {resp.status_code} on {url}"
            )
            return {}

        parser = LexborHTMLParser(resp.text)
        data: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # Step 1: structured panels
        self._parse_title_block(parser, data, extra)
        self._parse_property_info_panel(parser, data, extra)

        # Step 2: description (multiple <p> inside .kf_property_detail_uptwon)
        description = self._extract_description(parser)
        if description:
            data["description"] = description
        else:
            og = self._og_description_fallback(parser)
            if og:
                data["description"] = og

        # Step 3: coords from L.map(...).setView(...)
        lat, lng = self._extract_coords(resp.text)
        if lat is not None and lng is not None and _bbox_check(lat, lng):
            data["latitude"] = lat
            data["longitude"] = lng

        # Step 4: images from .bxslider li a[href]
        images = self._extract_images(parser)
        if images:
            data["images"] = images

        # Land normalization
        if data.get("category") == "Land":
            sz = data.get("size_sqm")
            if sz and not data.get("land_size_sqm"):
                data["land_size_sqm"] = sz

        if extra:
            data["extra_features"] = extra

        # Step 5: NLP fallback
        self._apply_nlp_fallback(data)

        # Step 7: Quality gate (log-only)
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )

        return data

    # ── Step 1.a: title block (.kf_property_detail_uptwon) ───────────

    def _parse_title_block(
        self, parser: LexborHTMLParser,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first(".kf_property_detail_uptwon")
        if not block:
            return

        # Title h3.pull-left + h3.pull-right (price)
        title_h3 = block.css_first("h3.pull-left")
        if title_h3:
            title = _normalize_text(title_h3.text(strip=False))
            cat = _category_from_text(title)
            if cat:
                data["category"] = cat
            # Trailing "{N} m\u00b2"
            m = re.search(r"([\d,.]+)\s*m\s*[\u00b2]?", title)
            if m and "size_sqm" not in data:
                v = _to_float_sqm(m.group(1))
                if v is not None:
                    data["size_sqm"] = v

        price_h3 = block.css_first("h3.pull-right")
        if price_h3:
            v = self._to_int_euro_safe(_normalize_text(price_h3.text(strip=False)))
            if v is not None:
                data["price"] = v

    # ── Step 1.b: property info panel ────────────────────────────────

    def _parse_property_info_panel(
        self, parser: LexborHTMLParser,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        """
        .kf_property_detail_Essentail ul li a holds "Label: Value" rows.
        Some entries are bare strings (e.g. "Facade") \u2014 boolean flags.
        """
        for block in parser.css(".kf_property_detail_Essentail"):
            for li in block.css("li"):
                text = _normalize_text(li.text(strip=False))
                if not text:
                    continue
                if ":" in text:
                    label, _, value = text.partition(":")
                    self._route_info_row(
                        label.strip().lower().rstrip(":"),
                        value.strip(),
                        data, extra,
                    )
                else:
                    # boolean flag
                    sl = _slug(text)
                    if sl:
                        extra[sl] = True

    def _route_info_row(
        self, label: str, value: str,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        if not value:
            return

        # Strip leading "#" from IDs
        if label in ("property id", "id"):
            extra["agent_code"] = value.lstrip("#")
            return
        if label == "price":
            v = self._to_int_euro_safe(value)
            if v is not None and "price" not in data:
                data["price"] = v
            return
        if label in ("price per m\u00b2", "price per m2", "price per sqm"):
            v = self._to_int_euro_safe(value)
            if v is not None:
                extra["price_per_sqm"] = v
            return
        if label == "category":
            # Category may be Greek even on lang=en (e.g. "\u0391\u03b3\u03c1\u03bf\u03c4\u03b5\u03bc\u03ac\u03c7\u03b9\u03bf")
            cat = _category_from_text(value)
            if cat and "category" not in data:
                data["category"] = cat
            else:
                extra["category_raw"] = value
            return
        if label == "type":
            # "For Sale" / "For Rent" \u2014 not category, just sale type
            extra["listing_type"] = value
            return
        if label == "area":
            # Could be either sqm OR prefecture name ("Chalkidiki"). Distinguish:
            # if value contains digits and "m" \u2192 sqm; else \u2192 prefecture
            if re.search(r"\d", value) and ("m" in value.lower() or "\u00b2" in value):
                v = _to_float_sqm(value)
                if v is not None and "size_sqm" not in data:
                    data["size_sqm"] = v
            else:
                # prefecture name \u2014 store but don't override location_raw
                extra["prefecture_raw"] = value
            return
        if label == "subarea":
            extra["subarea"] = value
            return
        if label == "neighborhood":
            data["area"] = value  # municipality-ish for GeoMatcher
            return
        if label == "bedrooms":
            v = _to_int_simple(value)
            if v is not None and "bedrooms" not in data:
                data["bedrooms"] = v
            return
        if label == "bathrooms":
            v = _to_int_simple(value)
            if v is not None and "bathrooms" not in data:
                data["bathrooms"] = v
            return
        if label in ("year of manufacture", "construction year", "year built"):
            v = _to_int_simple(value)
            if v is not None and 1900 <= v <= 2100:
                data["year_built"] = v
            return
        if label == "floor":
            extra["floor"] = value
            return
        if label == "levels":
            v = _to_int_simple(value)
            if v is not None:
                data["levels"] = v
            return

        # Distance fields \u2014 normalize to integer meters
        if label.startswith("distance from"):
            m = re.search(r"(\d+)", value)
            if m:
                key_suffix = label.replace("distance from", "").strip()
                sl = _slug(key_suffix)
                if sl:
                    extra[f"distance_{sl}_m"] = int(m.group(1))
            return

        # Default: extras with slugified key
        sl = _slug(label)
        if sl:
            extra[sl] = value

    # ── Step 2: description ──────────────────────────────────────────

    def _extract_description(
        self, parser: LexborHTMLParser,
    ) -> Optional[str]:
        block = parser.css_first(".kf_property_detail_uptwon")
        if not block:
            return None
        paragraphs: List[str] = []
        for p in block.css("p"):
            txt = _normalize_text(p.text(strip=False))
            if txt:
                paragraphs.append(txt)
        if paragraphs:
            return "\n\n".join(paragraphs)
        return None

    # ── Step 3: coords from L.map(...).setView(...) ──────────────────

    def _extract_coords(
        self, html_text: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        # L.map('map').setView([lat, lng], zoom)
        m = re.search(
            r"setView\s*\(\s*\[\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\]",
            html_text,
        )
        if not m:
            # Also try L.circle([lat, lng], ...)
            m = re.search(
                r"L\.circle\s*\(\s*\[\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\]",
                html_text,
            )
        if not m:
            return None, None
        try:
            lat = float(m.group(1))
            lng = float(m.group(2))
        except ValueError:
            return None, None
        if lat == 0.0 or lng == 0.0:
            return None, None
        return lat, lng

    # ── Step 4: images ───────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        """
        Gallery: .bxslider li a[href] (excluding clones which have class
        bx-clone). Each href is the full-resolution image URL.
        """
        seen: set = set()
        out: List[str] = []
        for a in parser.css(".bxslider li a[href]"):
            href = (a.attributes.get("href") or "").strip()
            if not href or href.endswith(".svg"):
                continue
            # Only http(s) URLs (skip "#" etc)
            if not href.startswith(("http://", "https://")):
                continue
            if href not in seen:
                seen.add(href)
                out.append(href)
        return out
