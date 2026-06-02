"""
~/hodu/src/scrapers/realgreece_net.py

realgreece.net scraper \u2014 Real Greece Real Estate Network, a network
aggregator that shows properties from multiple partner agencies (partner
codes XK, KZ, COX, ...).
Sprint 11 #3.

Anti-bot: clean Stage 0 (curl_cffi chrome120, ~250ms response).
reCAPTCHA only on contact form (non-blocking for scraping).

URL patterns
------------
List:    /search?purpose=1&city=1401386573&pricemin={min}&lng=en&p={N}
         purpose=1 = sale; city=1401386573 = Halkidiki (confirmed from title).
         category= empty captures all property types.
         At priceFrom 400k: 10 properties total (single page).

Detail:  /en/{Location}-{Category}-for-Sale_{AGENT}-{ID}.html
         Where AGENT-ID is the pcode (e.g. KZ-15, XK-37, COX-54).
         The URL contains URL-encoded hyphens (%2D) in slugs.

Network aggregator semantics
----------------------------
realgreece.net aggregates listings from partner agencies, each with a 1-3
letter partner code. The pcode (e.g. "KZ-15") is the agency's internal
property code; we use it as site_property_id since it's stable and unique
within realgreece.net's namespace.

Properties may duplicate listings from partner agencies' own websites
(e.g. KZ-* may also be on kassandra-estate.com). Engine 2 handles
cross-source deduplication.

Card anatomy (.rec-col)
-----------------------
.rec > .rec1 > .photo a[href] \u2192 detail URL (relative, en/... prefix)
.rec1 .divlink .caption .pcode \u2192 site_property_id ("KZ-15", "XK-37")
.rec1 .divlink .caption p.price \u2192 "\u20ac 625.000"
.rec1 .divlink .caption h3 a \u2192 title "Villa for Sale - Kassandra"
.rec1 .divlink .caption .ofields .ofield \u2192 rooms / baths / sq.m.

Detail page (#propertydetails)
------------------------------
.areaspath              \u2192 breadcrumb "Halkidiki / Kassandra / Kassandra"
.property-main-title    \u2192 h1 title + .ofield (price/bedroom/bathroom/size/pcode)
.property-description   \u2192 p.ad with the full text
.summary-list ul.list   \u2192 key-value rows (Code/Area/Sub-area/Type/Size/Price/
                          Levels/Construction Year/Bedrooms/Baths/wc/Heating)
.property-moredetails   \u2192 boolean feature spans (Parking/Fireplace/Storeroom)
JS Leaflet              \u2192 `var latitude = N; var longitude = N;`
#AllPhotos .photo       \u2192 [data-src] = full resolution image path
                          (img src may have "s" or "xs" suffix = thumbnail)
                          Prepend base URL to get absolute URL.
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

_BASE_URL = "https://www.realgreece.net"
_SOURCE_DOMAIN = "realgreece.net"

# Halkidiki numeric city ID (confirmed from page title)
_CITY_HALKIDIKI = "1401386573"

# Pagination safety cap
_MAX_PAGES = 30
_INTER_PAGE_SLEEP_SEC = 1.5

# Halkidiki bbox for coord sanity
_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# Category mapping for h3/title scan and Summary List Type field
_CATEGORY_MAP: Dict[str, str] = {
    # English (the site is English on lang=en)
    "maisonette":    "Maisonette",
    "apartment":     "Apartment",
    "studio":        "Apartment",
    "loft":          "Apartment",
    "penthouse":     "Apartment",
    "flat":          "Apartment",
    "villa":         "Villa",
    "house":         "House",
    "home":          "House",
    "residence":     "House",
    "cottage":       "House",
    "detached":      "House",
    "bungalow":      "House",
    "land":          "Land",
    "plot":          "Land",
    "parcel":        "Land",
    "field":         "Land",
    "office":        "Commercial",
    "shop":          "Commercial",
    "store":         "Commercial",
    "warehouse":     "Commercial",
    "commercial":    "Commercial",
    "professional":  "Commercial",
    "hotel":         "Hotel/Commercial",
    "building":      "Building",
    "complex":       "Building",
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
    """Parse '7.200 sq.m.', '85.5 m\u00b2', '150 sq.m.' formats."""
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
    elif raw.count(".") == 1:
        # Greek thousand sep: "7.200" \u2192 7200, but "150.5" \u2192 150.5
        # If the integer part is 1-3 digits and fractional is exactly 3, treat as thousands
        parts = raw.split(".")
        if len(parts) == 2 and 1 <= len(parts[0]) <= 3 and len(parts[1]) == 3:
            raw = raw.replace(".", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _category_from_text(text: str) -> Optional[str]:
    """Scan ALL alphabetic words against _CATEGORY_MAP."""
    t = _normalize_text(text).lower()
    if not t:
        return None
    for word in re.findall(r"[a-z]+", t):
        cat = _CATEGORY_MAP.get(word)
        if cat:
            return cat
    return None


def _strip_image_size_suffix(path: str) -> str:
    """
    realgreece.net thumbnails have 's' or 'xs' suffix before extension:
      KZ-119s.jpg  \u2192 thumbnail (~250px wide)
      KZ-119xs.jpg \u2192 extra small
      KZ-119.jpg   \u2192 full resolution
    Used when data-src isn't present and we must derive full-res from src.
    """
    return re.sub(r"(xs|s)(\.\w+)$", r"\2", path)


# ============================================================================
# Scraper
# ============================================================================

class RealGreeceNetScraper(EnrichmentMixin, BaseScraper):
    """realgreece.net \u2014 partner network aggregator."""

    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
        # category from "Type" field is reliable \u2014 no NLP fallback
    )

    _NLP_TO_STRUCTURAL: Dict[str, Set[str]] = {
        "swimming_pool":    {"pool", "swimming_pool"},
        "sea_view":         {"sea_view", "view_sea"},
        "parking":          {"parking", "covered_parking"},
        "air_conditioning": {"air_conditioning", "ac", "central_air_conditioning"},
        "fireplace":        {"fire_place", "fireplace"},
        "balcony":          {"balcony", "balconies"},
        "garden":           {"garden", "roof_garden"},
        "storage_room":     {"storeroom", "storage", "storage_room"},
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
            f"{_BASE_URL}/search"
            f"?purpose=1&city={_CITY_HALKIDIKI}&category="
            f"&pricemin={min_price}&pricemax="
            f"&sizemin=&sizemax=&id1="
            f"&bedroomsmin=&bedroomsmax=&furnished="
            f"&floormin=&floormax=&cyearmin=&cyearmax="
            f"&seadistancemin=&seadistancemax="
            f"&lng=en&p={page}"
        )

    def _absolutize(self, href: str) -> str:
        """Convert relative href ('en/...html') to absolute URL."""
        if href.startswith(("http://", "https://")):
            return href
        # Site uses URL-encoded hyphens (%2D) in slugs \u2014 leave as-is
        return f"{_BASE_URL}/{href.lstrip('/')}"

    # ── Phase 1: collect_urls ────────────────────────────────────────

    async def collect_urls(
        self, min_price: int = 400_000,
    ) -> List[PropertyTemplate]:
        seeds: Dict[str, PropertyTemplate] = {}
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
            cards = parser.css(".rec-col")
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

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        # pcode = site_property_id (e.g. "KZ-15", "XK-37")
        pcode_el = card.css_first(".pcode")
        if not pcode_el:
            return None
        site_id = _normalize_text(pcode_el.text(strip=False))
        if not site_id:
            return None

        # Detail URL from any anchor with .html
        detail_href: Optional[str] = None
        for a in card.css("a[href]"):
            href = (a.attributes.get("href") or "").strip()
            if href.endswith(".html"):
                detail_href = href
                break
        if not detail_href:
            return None
        detail_url = self._absolutize(detail_href)

        # Title: <h3><a>...</a></h3>
        title = ""
        h3 = card.css_first(".caption h3")
        if h3:
            title = _normalize_text(h3.text(strip=False))

        category = _category_from_text(title)

        # location_raw: "{Category} for Sale - {Location}" \u2014 take part after " - "
        location_raw: Optional[str] = None
        if " - " in title:
            location_raw = title.split(" - ", 1)[1].strip()

        # Price
        price: Optional[int] = None
        price_el = card.css_first("p.price")
        if price_el:
            price = self._to_int_euro_safe(
                _normalize_text(price_el.text(strip=False))
            )

        # ofields: rooms / baths / sq.m.
        bedrooms: Optional[int] = None
        bathrooms: Optional[int] = None
        size_sqm: Optional[float] = None
        for of in card.css(".ofields .ofield"):
            text = _normalize_text(of.text(strip=False))
            if not text:
                continue
            ltext = text.lower()
            if "room" in ltext and bedrooms is None:
                bedrooms = _to_int_simple(text)
            elif "bath" in ltext and bathrooms is None:
                bathrooms = _to_int_simple(text)
            elif ("sq.m" in ltext or "m\u00b2" in ltext) and size_sqm is None:
                size_sqm = _to_float_sqm(text)
                # Also check 'title' attribute for cleaner value
                t = of.attributes.get("title", "")
                if t and ("sq.m" in t.lower() or "m\u00b2" in t):
                    tv = _to_float_sqm(t)
                    if tv:
                        size_sqm = tv

        seed = PropertyTemplate(
            site_property_id=str(site_id),
            source_domain=self.source_domain,
            url=detail_url,
            price=price,
            size_sqm=size_sqm,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
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
        try:
            resp = await self.client.get(url)
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

        # Step 1.a: breadcrumb \u2192 location_raw (highest fidelity location)
        path_el = parser.css_first(".areaspath")
        if path_el:
            path_text = _normalize_text(path_el.text(strip=False))
            if path_text:
                extra["breadcrumb"] = path_text
                # Last segment is the most specific area
                parts = [p.strip() for p in path_text.split("/") if p.strip()]
                if parts:
                    data["location_raw"] = parts[-1]

        # Step 1.b: title block (.property-main-title)
        self._parse_main_title(parser, data, extra)

        # Step 1.c: Summary list (.summary-list ul.list li)
        self._parse_summary_list(parser, data, extra)

        # Step 1.d: Additional features (boolean flags)
        self._parse_additional_features(parser, extra)

        # Step 2: description (.property-description p.ad)
        desc_el = parser.css_first(".property-description p.ad")
        if desc_el:
            description = _normalize_text(desc_el.text(strip=False))
            if description:
                data["description"] = description
        if not data.get("description"):
            og = self._og_description_fallback(parser)
            if og:
                data["description"] = og

        # Step 3: coords from JS `var latitude = N; var longitude = N;`
        lat, lng = self._extract_coords(resp.text)
        if lat is not None and lng is not None and _bbox_check(lat, lng):
            data["latitude"] = lat
            data["longitude"] = lng

        # Step 4: images from #AllPhotos
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

    # ── Step 1.b: title block ────────────────────────────────────────

    def _parse_main_title(
        self, parser: LexborHTMLParser,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first(".property-main-title")
        if not block:
            return

        # h1: "{Category} for Sale - {Location}"
        h1 = block.css_first("h1")
        if h1:
            title = _normalize_text(h1.text(strip=False))
            cat = _category_from_text(title)
            if cat:
                data["category"] = cat

        # .ofield.price
        p_el = block.css_first(".ofield.price")
        if p_el:
            v = self._to_int_euro_safe(_normalize_text(p_el.text(strip=False)))
            if v is not None:
                data["price"] = v

        # .ofield.bedroom (text begins with number "4 Rooms")
        b_el = block.css_first(".ofield.bedroom")
        if b_el:
            v = _to_int_simple(_normalize_text(b_el.text(strip=False)))
            if v is not None:
                data["bedrooms"] = v

        # .ofield.bathroom
        bath_el = block.css_first(".ofield.bathroom")
        if bath_el:
            v = _to_int_simple(_normalize_text(bath_el.text(strip=False)))
            if v is not None:
                data["bathrooms"] = v

        # .ofield.size
        s_el = block.css_first(".ofield.size")
        if s_el:
            v = _to_float_sqm(_normalize_text(s_el.text(strip=False)))
            if v is not None:
                data["size_sqm"] = v

        # .ofield.pcode
        c_el = block.css_first(".ofield.pcode")
        if c_el:
            extra["agent_code"] = _normalize_text(c_el.text(strip=False))

    # ── Step 1.c: Summary list ───────────────────────────────────────

    def _parse_summary_list(
        self, parser: LexborHTMLParser,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        """
        .summary-list ul.list li: each <li> has <span>label</span><strong>value</strong>
        Skip li.d-none (hidden, empty values).
        """
        for li in parser.css(".summary-list ul.list li"):
            cls = li.attributes.get("class", "") or ""
            if "d-none" in cls:
                continue
            span = li.css_first("span")
            strong = li.css_first("strong")
            if not span or not strong:
                continue
            label = _normalize_text(span.text(strip=False)).lower()
            value = _normalize_text(strong.text(strip=False))
            if not label or not value:
                continue
            self._route_summary_row(label, value, data, extra)

    def _route_summary_row(
        self, label: str, value: str,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        if label == "code":
            extra["agent_code"] = value
            return
        if label == "code b":
            extra["agent_code_b"] = value
            return
        if label == "area":
            extra["area_label"] = value
            return
        if label in ("sub-area", "subarea", "sub area"):
            # Specific area for geo matching
            data["area"] = value
            extra["subarea"] = value
            return
        if label == "type":
            cat = _category_from_text(value)
            if cat:
                data["category"] = cat
            else:
                extra["type_raw"] = value
            return
        if label == "purpose":
            extra["listing_type"] = value
            return
        if label == "size":
            v = _to_float_sqm(value)
            if v is not None and "size_sqm" not in data:
                data["size_sqm"] = v
            return
        if label == "price":
            v = self._to_int_euro_safe(value)
            if v is not None and "price" not in data:
                data["price"] = v
            return
        if label in ("price/sq.m.", "price/sqm", "price per sq.m.", "price per sqm"):
            v = self._to_int_euro_safe(value)
            if v is not None:
                extra["price_per_sqm"] = v
            return
        if label == "levels":
            # Sometimes "Ground floor-1st" \u2014 store raw + extract digit count
            extra["levels_raw"] = value
            m = re.findall(r"\d+|ground|1st|2nd|3rd|\d+th", value, re.IGNORECASE)
            if m:
                data["levels"] = len(m)
            return
        if label == "construction year":
            v = _to_int_simple(value)
            if v is not None and 1900 <= v <= 2100:
                data["year_built"] = v
            return
        if label in ("bedrooms", "rooms"):
            v = _to_int_simple(value)
            if v is not None and "bedrooms" not in data:
                data["bedrooms"] = v
            return
        if label in ("baths", "bathrooms"):
            v = _to_int_simple(value)
            if v is not None and "bathrooms" not in data:
                data["bathrooms"] = v
            return
        if label == "wc":
            v = _to_int_simple(value)
            if v is not None:
                extra["wc"] = v
            return
        if label == "heating":
            extra["heating"] = value
            return

        # Default: extras with slugified key
        sl = _slug(label)
        if sl:
            extra[sl] = value

    # ── Step 1.d: Additional features (boolean flags) ────────────────

    def _parse_additional_features(
        self, parser: LexborHTMLParser, extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first(".property-moredetails")
        if not block:
            return
        for span in block.css("span"):
            cls = span.attributes.get("class", "") or ""
            if "d-none" in cls:
                continue
            text = _normalize_text(span.text(strip=False))
            if not text:
                continue
            sl = _slug(text)
            if sl:
                extra[sl] = True

    # ── Step 3: coords from JS `var latitude = N; var longitude = N;` ─

    def _extract_coords(
        self, html_text: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        m_lat = re.search(r"var\s+latitude\s*=\s*([-\d.]+)\s*;", html_text)
        m_lng = re.search(r"var\s+longitude\s*=\s*([-\d.]+)\s*;", html_text)
        if not m_lat or not m_lng:
            return None, None
        try:
            lat = float(m_lat.group(1))
            lng = float(m_lng.group(1))
        except ValueError:
            return None, None
        if lat == 0.0 or lng == 0.0:
            return None, None
        return lat, lng

    # ── Step 4: images ───────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        """
        Gallery: #AllPhotos .photo[data-src] \u2014 data-src holds full-res path.
        Some entries are inside .photos.d-none containers but still hold valid
        photos for the gallery, so we don't filter by parent visibility.

        Fallback: if data-src missing, use img src with size-suffix stripped.
        """
        seen: set = set()
        out: List[str] = []
        all_photos = parser.css_first("#AllPhotos")
        if not all_photos:
            return out

        for photo in all_photos.css(".photo"):
            data_src = (photo.attributes.get("data-src") or "").strip()
            if not data_src:
                # Fallback to inner img src
                img = photo.css_first("img")
                if img:
                    data_src = (img.attributes.get("src") or "").strip()
                    if data_src:
                        data_src = _strip_image_size_suffix(data_src)
            if not data_src:
                continue
            full_url = (
                data_src
                if data_src.startswith(("http://", "https://"))
                else f"{_BASE_URL}/{data_src.lstrip('/')}"
            )
            if full_url not in seen:
                seen.add(full_url)
                out.append(full_url)
        return out
