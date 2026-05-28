"""
~/hodu/src/scrapers/halkidiki_imoti.py

halkidiki-imoti.com scraper — Strategy A (paginated server-rendered walk).

Tech
----
WordPress + "Essential Real Estate" (ere) plugin + benaa theme. Fully
server-side-rendered (WP), NO Cloudflare / PerimeterX — Stage 0 curl_cffi
handles both listing and detail. The ere plugin is widely used by GR/BG
agencies, so this parser is largely reusable across other ere-based sites
(selectors are the plugin's, not this agency's).

URL patterns
------------
List:    /advanced-search/?status=for-sale&state=halkidiki&min-price={p}      (page 1)
         /advanced-search/page/{N}/?status=for-sale&state=halkidiki&min-price={p}
            -> server-side filtered to the wanted set (~54 @ ≥400k), so no
               client-side price filtering is needed.
Detail:  /property/{slug}/    e.g. /property/nea-iraklia-halkidiki-greece-35/

Listing cards
-------------
`.ere-item-wrap` (12/page) inside `.ere-property.property-grid`. Each card:
  * .property-link[href] / .property-title a[href]  -> detail URL
  * .property-title a                                -> location string (heading)
  * .property-price                                  -> "€900,000"
Pagination walks ?page in path until a page yields no cards.

Detail blocks (ere plugin — clean structured spans)
---------------------------------------------------
Overview tab (`.ere__list-2-col`):
  * .ere__property-type        -> category (taxonomy display name)
  * .property-price            -> price (first occurrence = header)
  * .ere__property-bedrooms    -> bedrooms
  * .ere__property-bathrooms   -> bathrooms
  * .ere__property-size        -> "250 кв.м."  (RU кв.м — BG agency)
  * .ere__property-land-size   -> "1,800 SqM"  (comma = thousands)
  * .ere__property-identity    -> agency ref (e.g. S0501)
Address (`.ere__property-address-list`): .city / .state / .country
Description: .ere__single-property-description .ere-property-element
Features:    .property-feature-wrap a.feature-checked  (+ Energy Class X)
Images:      .single-property-image-main .property-gallery-item a.zoomGallery[href]
             (full originals; scoped to the main gallery, not thumbs/sidebar)

Caveats / decisions
-------------------
* Coordinates: the ere map geocodes the ADDRESS client-side ("types":"geocode")
  — no lat/lng in the served HTML. coords therefore default to None and the
  pipeline's name-based GeoMatcher resolves location from area (e.g. Nea
  Kallikratia). A best-effort coord regex is still tried + bbox-checked.
* category: ere type display name -> canonical hodu vocabulary.
* area = .city (locality/municipality); state (prefecture) goes into location_raw.
* size & land use a comma-as-thousands sqm parser ("1,800 SqM" -> 1800).
* site_property_id = URL slug (stable, present in both card and detail).
"""
from __future__ import annotations

import asyncio
import random
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

_BASE_URL = "https://halkidiki-imoti.com"
_SOURCE_DOMAIN = "halkidiki-imoti.com"

_MAX_PAGES = 12               # ~5 pages of 12 @ ≥400k; headroom
_INTER_PAGE_SLEEP_SEC = 1.5

_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# ere taxonomy display name (lower-cased) -> canonical hodu category.
_CATEGORY_MAP: Dict[str, str] = {
    "villa":              "Villa",
    "detached house":     "House",
    "house":              "House",
    "bungalow":           "House",
    "apartment":          "Apartment",
    "studio":             "Apartment",
    "loft":               "Apartment",
    "room":               "Apartment",
    "maisonette":         "Maisonette",
    "apartments complex": "Business",
    "apartment complex":  "Business",
    "building":           "Business",
    "hotel":              "Hotel",
    "store":              "Business",
    "office":             "Business",
    "plot":               "Land",
    "land":               "Land",
    "parcel":             "Land",
}


# =============================================================================
# Helpers
# =============================================================================

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
    """First integer in text. '6', '250 кв.м.' -> int."""
    if not text:
        return None
    m = re.search(r"\d+", text.replace(",", "").replace(".", "").replace(" ", ""))
    return int(m.group(0)) if m else None


def _to_sqm(text: str) -> Optional[float]:
    """
    Parse sqm where comma is a THOUSANDS separator on this site:
    '1,800 SqM' -> 1800.0 ; '250 кв.м.' -> 250.0
    """
    if not text:
        return None
    cleaned = text.replace("\xa0", " ").replace(",", "")
    m = re.search(r"\d+(?:\.\d+)?", cleaned)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _slug_from_url(url: str) -> Optional[str]:
    """'/property/nea-iraklia-halkidiki-greece-35/' -> 'nea-iraklia-halkidiki-greece-35'."""
    if not url:
        return None
    m = re.search(r"/property/([a-z0-9\-]+)/?", url)
    return m.group(1) if m else None


def _map_category(type_text: str) -> Optional[str]:
    t = _normalize_text(type_text).lower()
    if not t:
        return None
    return _CATEGORY_MAP.get(t, t.title())


# =============================================================================
# Scraper
# =============================================================================

class HalkidikiImotiScraper(EnrichmentMixin, BaseScraper):
    """halkidiki-imoti.com — WordPress/ere Strategy A paginated walk."""

    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )

    _NLP_TO_STRUCTURAL: Dict[str, set] = {
        "swimming_pool":    {"pool", "swimming_pool"},
        "sea_view":         {"sea_view", "view"},
        "parking":          {"garage", "parking"},
        "air_conditioning": {"air_condition", "air_conditioning", "ac"},
        "fireplace":        {"fire_place"},
        "balcony":          {"balconies"},
        "garden":           {"private_garden"},
        "storage_room":     {"storage", "storage_space"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builder ───────────────────────────────────────────────────────

    def _construct_search_url(self, *, page: int, min_price: int) -> str:
        q = f"status=for-sale&state=halkidiki&min-price={min_price}"
        if page <= 1:
            return f"{_BASE_URL}/advanced-search/?{q}"
        return f"{_BASE_URL}/advanced-search/page/{page}/?{q}"

    # ── Phase 1: collect_urls ─────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        """Walk the server-filtered advanced-search, parse `.ere-item-wrap` cards."""
        seeds: Dict[str, PropertyTemplate] = {}

        for page in range(1, max_pages + 1):
            url = self._construct_search_url(page=page, min_price=min_price)
            logger.info(f"[{self.source_domain}] page {page}: GET {url}")
            try:
                resp = await self.client.get(url)
            except Exception as exc:
                logger.error(f"[{self.source_domain}] page {page} fetch failed: {exc!r}")
                break

            if getattr(resp, "status_code", 200) == 404 or not resp.text:
                break

            parser = LexborHTMLParser(resp.text)
            cards = parser.css(".ere-item-wrap")
            if not cards:
                logger.info(f"[{self.source_domain}] page {page}: no cards — end of results")
                break

            page_added = 0
            for card in cards:
                try:
                    seed = self._parse_card(card)
                except Exception as exc:
                    logger.error(f"[{self.source_domain}] card parse error: {exc!r}")
                    continue
                if not seed or seed.site_property_id in seeds:
                    continue
                seeds[seed.site_property_id] = seed
                page_added += 1

            logger.info(
                f"[{self.source_domain}] page {page}: {len(cards)} cards "
                f"(+{page_added} new, total: {len(seeds)})"
            )
            if page_added == 0:
                break
            await asyncio.sleep(_INTER_PAGE_SLEEP_SEC + random.uniform(0.5, 1.5))

        logger.info(f"[{self.source_domain}] collect_urls done: {len(seeds)} unique seeds")
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        link = card.css_first(".property-link[href]") or card.css_first(".property-title a[href]")
        href = link.attributes.get("href") if link else None
        if not href:
            for a in card.css("a[href]"):
                h = a.attributes.get("href") or ""
                if re.search(r"/property/[a-z0-9\-]+/?$", h):
                    href = h
                    break
        if not href:
            return None
        if href.startswith("/"):
            href = f"{_BASE_URL}{href}"

        slug = _slug_from_url(href)
        if not slug:
            return None

        title_a = card.css_first(".property-title a") or link
        location_raw = _normalize_text(title_a.text(strip=False)) if title_a else None

        price_node = card.css_first(".property-price")
        price_text = _normalize_text(price_node.text(strip=False)) if price_node else None

        return PropertyTemplate(
            site_property_id=slug,
            source_domain=self.source_domain,
            url=href,
            price=price_text,             # validator cleans "€900,000" -> 900000
            location_raw=location_raw,
        )

    # ── Phase 2: fetch_details ────────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        try:
            resp = await self.client.get(url)
        except Exception as exc:
            logger.error(f"[{self.source_domain}] detail fetch failed {url}: {exc!r}")
            return {}
        if not resp.text:
            return {}

        parser = LexborHTMLParser(resp.text)
        data: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # Step 1: structured ere blocks
        self._parse_overview(parser, data, extra)
        self._parse_address(parser, data)
        self._parse_features(parser, extra)

        # Step 2: description -> og fallback
        description = self._extract_description(parser) or self._og_description_fallback(parser)
        if description:
            data["description"] = description

        # Step 3: coordinates (best-effort; site geocodes client-side -> usually None)
        lat, lng = self._extract_coordinates(resp.text)
        if lat is not None and lng is not None:
            data["latitude"] = lat
            data["longitude"] = lng

        # Step 4: images -> og fallback
        images = self._extract_images(parser)
        if not images:
            og = self._og_image_fallback(parser)
            if og:
                images = [og]
        if images:
            data["images"] = images

        if extra:
            data["extra_features"] = extra

        # Step 5: NLP fallback (missing metric columns only)
        self._apply_nlp_fallback(data)

        # Step 7: quality gate (log-only)
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(f"[{self.source_domain}] description below quality gate for {url}")

        return data

    # ── Step 1 helpers ────────────────────────────────────────────────────

    def _parse_overview(self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any]) -> None:
        cat = parser.css_first(".ere__property-type")
        if cat:
            c = _map_category(cat.text(strip=False))
            if c:
                data["category"] = c

        price = parser.css_first(".property-price")
        if price:
            p = self._to_int_euro_safe(_normalize_text(price.text(strip=False)))
            if p is not None:
                data["price"] = p

        bn = parser.css_first(".ere__property-bedrooms")
        if bn:
            n = _to_int_simple(bn.text(strip=False))
            if n is not None:
                data["bedrooms"] = n

        ba = parser.css_first(".ere__property-bathrooms")
        if ba:
            n = _to_int_simple(ba.text(strip=False))
            if n is not None:
                data["bathrooms"] = n

        sz = parser.css_first(".ere__property-size")
        if sz:
            v = _to_sqm(sz.text(strip=False))
            if v is not None:
                data["size_sqm"] = v

        ls = parser.css_first(".ere__property-land-size")
        if ls:
            v = _to_sqm(ls.text(strip=False))
            if v is not None:
                data["land_size_sqm"] = v

        ref = parser.css_first(".ere__property-identity")
        if ref:
            r = _normalize_text(ref.text(strip=False))
            if r:
                extra["site_ref"] = r

    def _parse_address(self, parser: LexborHTMLParser, data: Dict[str, Any]) -> None:
        addr = parser.css_first(".ere__property-address-list")
        if not addr:
            return
        parts: List[str] = []
        city = addr.css_first(".city span")
        state = addr.css_first(".state span")
        country = addr.css_first(".country span")
        if city:
            data["area"] = _normalize_text(city.text(strip=False))
            if data["area"]:
                parts.append(data["area"])
        if state:
            st = _normalize_text(state.text(strip=False))
            if st:
                parts.append(st)
        if country:
            ct = _normalize_text(country.text(strip=False))
            if ct:
                parts.append(ct)
        if parts:
            data["location_raw"] = ", ".join(parts)

    def _parse_features(self, parser: LexborHTMLParser, extra: Dict[str, Any]) -> None:
        for a in parser.css(".property-feature-wrap a.feature-checked"):
            t = _normalize_text(a.text(strip=False))
            if not t:
                continue
            low = t.lower()
            if low.startswith("energy class"):
                val = t[len("Energy Class"):].strip() or t
                extra["energy_class"] = val
            else:
                extra[_slug(t)] = True

    # ── Step 2: description ────────────────────────────────────────────────

    def _extract_description(self, parser: LexborHTMLParser) -> Optional[str]:
        cont = parser.css_first(".ere__single-property-description .ere-property-element")
        if not cont:
            cont = parser.css_first(".ere__single-property-description")
        if cont:
            txt = _normalize_text(cont.text(separator="\n", strip=True))
            if txt and len(txt) >= 50:
                return txt
        return None

    # ── Step 3: coordinates (best-effort) ─────────────────────────────────

    def _extract_coordinates(self, html_text: str) -> Tuple[Optional[float], Optional[float]]:
        patterns = (
            r'data-(?:lat|latitude)=["\']([0-9]{2}\.[0-9]+)["\'][^>]*?'
            r'data-(?:lng|long|longitude)=["\']([0-9]{2}\.[0-9]+)["\']',
            r'"(?:lat|latitude)"\s*:\s*"?([0-9]{2}\.[0-9]+)"?\s*,\s*'
            r'"(?:lng|long|longitude)"\s*:\s*"?([0-9]{2}\.[0-9]+)"?',
        )
        for pat in patterns:
            m = re.search(pat, html_text)
            if not m:
                continue
            try:
                lat, lng = float(m.group(1)), float(m.group(2))
            except ValueError:
                continue
            if _bbox_check(lat, lng):
                return lat, lng
        return None, None

    # ── Step 4: images ─────────────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        seen: set = set()
        out: List[str] = []
        gallery = parser.css_first(".single-property-image-main")
        if not gallery:
            return out
        for a in gallery.css("a.zoomGallery[href]"):
            h = (a.attributes.get("href") or "").strip()
            if h and h not in seen and not h.endswith(".svg"):
                seen.add(h)
                out.append(h)
        if not out:  # fallback to <img> if no lightbox anchors
            for img in gallery.css("img[src]"):
                h = (img.attributes.get("src") or "").strip()
                if h and h not in seen and not h.endswith(".svg"):
                    seen.add(h)
                    out.append(h)
        return out
