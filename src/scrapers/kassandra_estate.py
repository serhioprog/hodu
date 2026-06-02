"""
~/hodu/src/scrapers/kassandra_estate.py

kassandra-estate.gr scraper — Spitogatos agentwebsites/template7.

Fourth Spitogatos template variant in the platform:
- template1 → casaproperties + engelvoelkers (Bootstrap 3, .listing-item cards)
- template3 → neamesitiki (Bootstrap 3, .listing-item with .listing-item-body)
- template7 → kassandra-estate (Bootstrap 4, .ts-result cards, swiper gallery)
                                NEW in Sprint 10 #4

Agent: Kassandra Estate, Kassandria Halkidiki (Mr Karagiannis).

URL patterns
------------
List:    /en/property/search?region=196&listingType=sale&category={cat}
            &propertyTypes%5B%5D=&priceLow={min}
         region 196 = Chalkidiki. Empty index `[]=` not `[0]=` as other templates.
Detail:  /en/propertyDetails/{8-digit-id}

⚠️ ANTI-BOT — Distil Networks (SAME as casa/neamesitiki).
   Stage 0 returns 2752-char "Pardon Our Interruption". Stage 1 Playwright
   passes via stock stealth.

Template7 selector inventory (verified from HTML probe)
-------------------------------------------------------
List card (RICH — has location + size + rooms + bathrooms already):
  Container: a.ts-result[href]
  Image:     .card-img[style*="background-image"]  → URL via regex
  Price:     .ts-item__info-badge                  → "€ 700,000"
  Title:     figure.ts-item__info h4
  Location:  figure.ts-item__info aside            → "Kassandra, Poseidi"
  Specs:     .ts-description-lists dl → <dt>label</dt><dd>value</dd>
             (Area / Rooms / Bathrooms)

Detail:
  Title:       #page-title h1
  Location:    #page-title h5 span
  Price:       #page-title .badge.badge-primary
  Coords:      #location .marker[data-lat][data-lng]  data-type="exact"|"offset"
  Description: #description p (first p — main prose; dl is sibling)
  Features:    #description dl.ts-description-list__line → dt/dd pairs
               (Area, Price per m², Neighborhood, Rooms, Floor, Parking spot,
                Construction year, Heating System, Energy class, Levels,
                Kitchens, Living rooms, Bathrooms, Status, Type)
  Amenities:   #amenities ul li  (boolean OR "Label: <span>value</span>")
               (Distance from sea, Lot size, balconies, etc.)
  Gallery:     #gallery-carousel .swiper-slide a.popup-image[href]
               (Full-size 1600x1200 jpg → strip _1600x1200 → original)

Numbers: US format (comma=thousands). "€ 700,000" → 700000. "144 m²" → 144.

Coordinates: data-type="exact" = REAL property coords (high value).
             data-type="offset" = privacy-fuzzed by ~200m (still usable).
             No HQ trap concern (markers are property-specific, not agency).

Energy class quirk: some properties have "Energy Performance Certificate not
required for this property" as the <dd> text — extract via regex \b([A-G][+-]?)\b
only when match found, otherwise leave unset.
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
from src.scrapers.fetchers.browser_pool import browser_pool


# =============================================================================
# Constants
# =============================================================================

_BASE_URL = "https://kassandra-estate.gr"
_SOURCE_DOMAIN = "kassandra-estate.gr"

_REGION_ID = 196  # Chalkidiki

_CATEGORIES: Tuple[str, ...] = ("residential", "commercial", "land")

_PAGES_PER_CATEGORY_LIMIT = 12
_INTER_PAGE_SLEEP_SEC = 2.0

_PAGE_TIMEOUT_MS = 45_000

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# Distil challenge signature
_DISTIL_TITLE_FRAGMENT = "Pardon Our Interruption"

# Title-prefix lowercase → canonical hodu category. Longest first via sort.
_CATEGORY_MAP: Dict[str, str] = {
    "villa":             "Villa",
    "detached house":    "House",
    "detached":          "House",
    "house":             "House",
    "maisonette":        "Maisonette",
    "apartment complex": "Apartment",
    "apartment":         "Apartment",
    "studio":            "Apartment",
    "loft":              "Apartment",
    "penthouse":         "Apartment",
    "bungalow":          "House",
    "building":          "Building",
    "farm":              "Land",
    "office":            "Commercial",
    "store":             "Commercial",
    "shop":              "Commercial",
    "warehouse":         "Commercial",
    "industrial":        "Commercial",
    "craft":             "Commercial",
    "hotel":             "Hotel/Commercial",
    "showroom":          "Commercial",
    "business":          "Commercial",
    "hall":              "Commercial",
    "land plot":         "Land",
    "parcel":            "Land",
    "plot":              "Land",
    "island":            "Land",
    "agricultural":      "Land",
}

# URL-category fallback when card title lacks any keyword.
# residential URLs are too heterogeneous (Villa/House/Maisonette/Apartment),
# so we leave them alone and rely on detail page resolution. commercial
# and land URLs are narrow enough to safely default.
_URL_CATEGORY_FALLBACK: Dict[str, str] = {
    "commercial": "Commercial",
    "land":       "Land",
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
    if not text:
        return None
    m = re.search(r"\d[\d,]*", text)
    if not m:
        return None
    try:
        return int(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _to_sqm(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"\d[\d,]*(?:\.\d+)?", text.replace("\xa0", " "))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/propertyDetails/(\d+)", url)
    return m.group(1) if m else None


def _category_from_title(title: str) -> Optional[str]:
    t = _normalize_text(title).lower()
    if not t:
        return None
    for key in sorted(_CATEGORY_MAP, key=len, reverse=True):
        if key in t:
            return _CATEGORY_MAP[key]
    return None


_BG_URL_RE = re.compile(r"url\(\s*['\"]?(.*?)['\"]?\s*\)", re.IGNORECASE)


def _bg_url(style: str) -> Optional[str]:
    if not style:
        return None
    m = _BG_URL_RE.search(style)
    return m.group(1).strip() if m else None


def _strip_spitogatos_size(url: str) -> str:
    """OBSOLETE — kept for call-site compatibility. Spitogatos CDN
    serves ONLY sized variants (_1600x1200, _800x600, etc.); stripping
    the suffix produces 404s. Gallery already supplies _1600x1200 URLs
    which are high quality (~1600px wide), so we pass through unchanged.

    Verified 2026-05-30:
      m3.spitogatos.gr/{id}_1600x1200.jpg?v=...  → 200 OK
      m3.spitogatos.gr/{id}.jpg?v=...            → 404
    """
    return url


# =============================================================================
# Scraper
# =============================================================================

class KassandraEstateScraper(EnrichmentMixin, BaseScraper):
    """kassandra-estate.gr — Spitogatos template7 walk with forced Stage 1."""

    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )

    _NLP_TO_STRUCTURAL: Dict[str, set] = {
        "swimming_pool":    {"pool", "swimming_pool"},
        "sea_view":         {"sea_view"},
        "parking":          {"garage", "parking", "private_parking", "parking_spot"},
        "air_conditioning": {"air_conditioned", "air_condition", "air_conditioning", "ac"},
        "fireplace":        {"fire_place", "fireplace"},
        "balcony":          {"balconies", "balcony"},
        "garden":           {"private_garden", "garden"},
        "storage_room":     {"storage_space", "storage", "storage_room"},
        "elevator":         {"lift", "elevator"},
        "furnished":        {"furnished"},
        "alarm":            {"alarm", "security_system"},
        "solar_heater":     {"solar_heater", "solar_water_heating"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── Stage 1 fetch helpers ─────────────────────────────────────────────

    @staticmethod
    async def _make_context(browser):
        return await browser.new_context(
            user_agent=_USER_AGENT,
            viewport={"width": 1366, "height": 800},
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )

    async def _stage1_get_in_context(
        self, context, url: str, *, timeout_ms: int = _PAGE_TIMEOUT_MS,
    ) -> Optional[str]:
        """Stage 1 GET via Playwright. Needs networkidle for Distil to resolve.

        Pages with many third-party scripts (recaptcha + analytics) may never
        reach true networkidle within the timeout. On timeout, we harvest the
        partial DOM and validate: if Distil stub is gone and content is rich
        (>10k chars), the content is usable even though network is still busy.
        """
        page = await context.new_page()
        try:
            timed_out = False
            try:
                resp = await page.goto(
                    url, wait_until="networkidle", timeout=timeout_ms,
                )
                if not resp:
                    logger.warning(f"[{self.source_domain}] no response for {url}")
                    return None
                if resp.status >= 400:
                    logger.warning(
                        f"[{self.source_domain}] HTTP {resp.status} for {url}"
                    )
                    return None
            except Exception as exc:
                if "Timeout" not in repr(exc):
                    logger.warning(
                        f"[{self.source_domain}] page.goto failed {url}: {exc!r}"
                    )
                    return None
                timed_out = True
                logger.warning(
                    f"[{self.source_domain}] networkidle timeout for {url} "
                    f"— harvesting partial DOM"
                )

            content = await page.content()
            if _DISTIL_TITLE_FRAGMENT in content[:2000]:
                logger.error(
                    f"[{self.source_domain}] Distil challenge served on {url} "
                    f"— Stage 1 stealth insufficient"
                )
                return None
            # On timeout, require rich content as proof Distil resolved
            if timed_out and len(content) < 10_000:
                logger.warning(
                    f"[{self.source_domain}] timeout + thin DOM ({len(content)} chars) "
                    f"for {url} — discard"
                )
                return None
            return content
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _stage1_get_one_shot(
        self, url: str, *, timeout_ms: int = _PAGE_TIMEOUT_MS,
    ) -> Optional[str]:
        async with browser_pool.acquire() as browser:
            context = await self._make_context(browser)
            try:
                return await self._stage1_get_in_context(
                    context, url, timeout_ms=timeout_ms,
                )
            finally:
                try:
                    await context.close()
                except Exception:
                    pass

    # ── URL builder ───────────────────────────────────────────────────────

    def _construct_search_url(
        self, *, category: str, page: int, min_price: int,
    ) -> str:
        url = (
            f"{_BASE_URL}/en/property/search"
            f"?region={_REGION_ID}"
            f"&listingType=sale"
            f"&category={category}"
            f"&propertyTypes%5B%5D="
            f"&priceLow={min_price}"
            f"&priceHigh="
            f"&livingAreaLow="
            f"&livingAreaHigh="
            f"&roomsLow=nd"
            f"&roomsHigh=nd"
        )
        if page > 1:
            url += f"&page={page}"
        return url

    # ── Phase 1: collect_urls ─────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _PAGES_PER_CATEGORY_LIMIT,
    ) -> List[PropertyTemplate]:
        """Walk all 3 categories, paginated. One browser+context for the walk."""
        seeds: Dict[str, PropertyTemplate] = {}

        async with browser_pool.acquire() as browser:
            context = await self._make_context(browser)
            try:
                for category in _CATEGORIES:
                    logger.info(
                        f"[{self.source_domain}] === category: {category} ==="
                    )
                    for page_num in range(1, max_pages + 1):
                        url = self._construct_search_url(
                            category=category, page=page_num, min_price=min_price,
                        )
                        logger.info(
                            f"[{self.source_domain}] {category} page {page_num}: "
                            f"Stage1 GET {url}"
                        )
                        html = await self._stage1_get_in_context(context, url)
                        if not html:
                            break

                        parser = LexborHTMLParser(html)
                        cards = parser.css("a.ts-result")
                        if not cards:
                            logger.info(
                                f"[{self.source_domain}] {category} page "
                                f"{page_num}: 0 cards — end"
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
                            if not seed or seed.site_property_id in seeds:
                                continue
                            # URL-hint fallback for commercial/land cards
                            # whose titles lack any keyword (e.g. "Property in...")
                            if not getattr(seed, "category", None):
                                fb = _URL_CATEGORY_FALLBACK.get(category)
                                if fb:
                                    try:
                                        seed.category = fb
                                    except Exception:
                                        pass
                            seeds[seed.site_property_id] = seed
                            page_added += 1

                        logger.info(
                            f"[{self.source_domain}] {category} page {page_num}: "
                            f"{len(cards)} cards (+{page_added} new, total {len(seeds)})"
                        )

                        if page_added == 0 and page_num > 1:
                            logger.info(
                                f"[{self.source_domain}] {category} page "
                                f"{page_num}: all duplicates — end"
                            )
                            break

                        await asyncio.sleep(
                            _INTER_PAGE_SLEEP_SEC + random.uniform(0.5, 1.5)
                        )
            finally:
                try:
                    await context.close()
                except Exception:
                    pass

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} unique seeds"
        )
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        href = (card.attributes.get("href") or "").strip()
        if not href:
            return None
        if href.startswith("/"):
            href = f"{_BASE_URL}{href}"

        site_id = _id_from_url(href)
        if not site_id:
            return None

        # Price — .ts-item__info-badge
        price_int: Optional[int] = None
        badge = card.css_first(".ts-item__info-badge")
        if badge:
            price_int = self._to_int_euro_safe(
                _normalize_text(badge.text(strip=False))
            )

        # Title — figure.ts-item__info h4
        title = ""
        h4 = card.css_first("figure.ts-item__info h4")
        if not h4:
            h4 = card.css_first("h4")
        if h4:
            title = _normalize_text(h4.text(strip=False))

        cat = _category_from_title(title)

        # Location — figure.ts-item__info aside (e.g. "Kassandra, Poseidi")
        location_raw: Optional[str] = None
        aside = card.css_first("figure.ts-item__info aside")
        if aside:
            txt = _normalize_text(aside.text(strip=False))
            if txt:
                location_raw = txt

        # Specs — .ts-description-lists dl → dt/dd pairs
        size_sqm: Optional[float] = None
        bedrooms: Optional[int] = None
        bathrooms: Optional[int] = None
        for dl in card.css(".ts-description-lists dl"):
            dt = dl.css_first("dt")
            dd = dl.css_first("dd")
            if not dt or not dd:
                continue
            label = _normalize_text(dt.text(strip=False)).rstrip(":").lower()
            value = _normalize_text(dd.text(strip=False))
            if not label or not value:
                continue
            if label == "area" and size_sqm is None:
                size_sqm = _to_sqm(value)
            elif label == "rooms" and bedrooms is None:
                bedrooms = _to_int_simple(value)
            elif label == "bathrooms" and bathrooms is None:
                bathrooms = _to_int_simple(value)

        seed = PropertyTemplate(
            site_property_id=str(site_id),
            source_domain=self.source_domain,
            url=href,
            price=price_int,
            size_sqm=size_sqm,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            location_raw=location_raw,
        )
        if cat:
            try:
                seed.category = cat
            except Exception:
                pass
        return seed

    # ── Phase 2: fetch_details ────────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        html = await self._stage1_get_one_shot(url)
        if not html:
            return {}

        parser = LexborHTMLParser(html)
        data: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # Step 1: structured blocks
        self._parse_title_block(parser, data, extra)
        self._parse_features_dl(parser, data, extra)
        self._parse_amenities(parser, data, extra)

        # Step 2: description
        description = self._extract_description(parser)
        if description:
            data["description"] = description

        # Step 3: coordinates (per-property, exact or offset)
        coords = self._extract_coords(parser)
        if coords:
            lat, lng = coords
            if _bbox_check(lat, lng):
                data["latitude"] = lat
                data["longitude"] = lng

        # Step 4: images
        images = self._extract_images(parser)
        if images:
            data["images"] = images

        if extra:
            data["extra_features"] = extra

        # Heuristic: large size + no bedrooms ⇒ likely Land plot
        if "category" not in data:
            sz = data.get("size_sqm")
            bd = data.get("bedrooms")
            if sz and sz >= 1000 and not bd:
                data["category"] = "Land"

        # Land normalization
        if data.get("category") == "Land":
            sz = data.get("size_sqm")
            if sz and not data.get("land_size_sqm"):
                data["land_size_sqm"] = sz

        # Step 5: NLP fallback
        self._apply_nlp_fallback(data)

        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )

        return data

    # ── Step 1.a: title (h1, location, price) ─────────────────────────────

    def _parse_title_block(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        # H1
        h1 = parser.css_first("#page-title h1") or parser.css_first("h1")
        if h1:
            title = _normalize_text(h1.text(strip=False))
            cat = _category_from_title(title)
            if cat:
                data["category"] = cat

        # Location header (h5 span)
        loc = parser.css_first("#page-title h5 span")
        if loc:
            t = _normalize_text(loc.text(strip=False))
            if t:
                # raw location goes to area at this stage; Neighborhood dl row may
                # override below with parenthesised form "Poseidi (Kassandra)"
                extra["location_header"] = t

    # ── Step 1.b: description list (dl.ts-description-list__line) ─────────

    def _parse_features_dl(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        dl = parser.css_first("#description dl.ts-description-list__line") or \
             parser.css_first("dl.ts-description-list__line")
        if not dl:
            return
        # Iterate dt/dd pairs in order
        children = [c for c in dl.iter() if c.tag in ("dt", "dd")]
        i = 0
        while i < len(children) - 1:
            if children[i].tag == "dt" and children[i + 1].tag == "dd":
                dt_node = children[i]
                dd_node = children[i + 1]
                label = _normalize_text(dt_node.text(strip=False)).rstrip(":").lower()
                value_text = _normalize_text(dd_node.text(strip=False))
                if label and value_text:
                    self._route_dl_row(label, value_text, dd_node, data, extra)
                i += 2
            else:
                i += 1

    def _route_dl_row(
        self,
        label: str,
        value_text: str,
        dd_node: LexborNode,
        data: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        if label == "id":
            extra["agent_code"] = value_text
            return
        if label == "area":
            v = _to_sqm(value_text)
            if v is not None and "size_sqm" not in data:
                data["size_sqm"] = v
            return
        if label == "price per m²":
            n = _to_int_simple(value_text)
            if n is not None:
                extra["price_per_sqm"] = n
            return
        if label == "neighborhood":
            data["area"] = value_text
            return
        if label == "rooms":
            if "bedrooms" not in data:
                n = _to_int_simple(value_text)
                if n is not None:
                    data["bedrooms"] = n
            return
        if label == "bathrooms":
            n = _to_int_simple(value_text)
            if n is not None and "bathrooms" not in data:
                data["bathrooms"] = n
            return
        if label == "wc":
            n = _to_int_simple(value_text)
            if n is not None:
                extra["wc_count"] = n
            return
        if label == "kitchens":
            n = _to_int_simple(value_text)
            if n is not None:
                extra["kitchens_count"] = n
            return
        if label == "living rooms":
            n = _to_int_simple(value_text)
            if n is not None:
                extra["living_rooms_count"] = n
            return
        if label == "construction year":
            n = _to_int_simple(value_text)
            if n is not None and 1900 <= n <= 2100:
                data["year_built"] = n
            return
        if label == "renovation year":
            n = _to_int_simple(value_text)
            if n is not None and 1900 <= n <= 2100:
                extra["renovation_year"] = n
            return
        if label == "levels":
            n = _to_int_simple(value_text)
            if n is not None:
                data["levels"] = n
            return
        if label == "floor":
            extra["floor"] = value_text
            return
        if label == "heating system":
            extra["heating_system"] = value_text
            return
        if label == "parking spot":
            extra["parking_spot"] = value_text
            return
        if label == "energy class":
            # dd text might be a valid class letter OR noise like
            # "Energy Performance Certificate not required for this property"
            m = re.search(r"\b([A-G][\+\-]?)\b", value_text)
            if m:
                extra["energy_class"] = m.group(1)
            return
        if label == "status":
            for tag in (t.strip() for t in value_text.split(",")):
                if tag:
                    sl = _slug(tag)
                    if sl:
                        extra[sl] = True
            return
        if label == "type":
            for tag in (t.strip() for t in value_text.split(",")):
                if tag:
                    sl = _slug(tag)
                    if sl:
                        extra[sl] = True
            return
        # Generic fallback
        sl = _slug(label)
        if sl:
            extra[sl] = value_text

    # ── Step 1.c: amenities (#amenities ul li) ────────────────────────────

    def _parse_amenities(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first("#amenities ul") or \
                parser.css_first("ul.ts-list-colored-bullets")
        if not block:
            return
        for li in block.css("li"):
            text = _normalize_text(li.text(strip=False))
            if not text:
                continue
            low = text.lower()

            if low.startswith("lot size"):
                v = _to_sqm(text)
                if v is not None and "land_size_sqm" not in data:
                    data["land_size_sqm"] = v
                continue

            if "distance from sea" in low:
                m = re.search(r"(\d+)", text)
                if m:
                    extra["distance_sea_m"] = int(m.group(1))
                continue

            if low.startswith("size of balconies"):
                v = _to_sqm(text)
                if v is not None:
                    extra["balcony_size_sqm"] = v
                continue

            if "monthly shared expenses" in low:
                continue

            if ":" in text:
                label, _, value = text.partition(":")
                label = label.strip().lower()
                value = value.strip()
                if not value:
                    continue
                sl = _slug(label)
                if sl:
                    extra[sl] = value
                continue

            sl = _slug(text)
            if sl:
                extra[sl] = True

    # ── Step 2: description (#description p — first p, not dl) ────────────

    def _extract_description(self, parser: LexborHTMLParser) -> Optional[str]:
        # First <p> inside #description (avoid the trailing <dl>)
        desc_block = parser.css_first("#description")
        if not desc_block:
            return None
        p = desc_block.css_first("p")
        if not p:
            return None
        txt = _normalize_text(p.text(separator=" ", strip=True))
        # Trim "Contact information:" / "Contact us:" footer noise
        m = re.search(r"\bcontact\s+(?:information|us)\b", txt, re.IGNORECASE)
        if m:
            txt = txt[: m.start()].strip()
        return txt if (txt and len(txt) >= 50) else None

    # ── Step 3: coordinates (.marker[data-lat][data-lng]) ─────────────────

    def _extract_coords(
        self, parser: LexborHTMLParser,
    ) -> Optional[Tuple[float, float]]:
        marker = parser.css_first("#location .marker[data-lat]") or \
                 parser.css_first(".ts-map .marker[data-lat]") or \
                 parser.css_first(".marker[data-lat]")
        if not marker:
            return None
        lat_s = marker.attributes.get("data-lat") or ""
        lng_s = marker.attributes.get("data-lng") or marker.attributes.get("data-lon") or ""
        try:
            lat = float(lat_s)
            lng = float(lng_s)
        except ValueError:
            return None
        if lat == 0.0 or lng == 0.0:
            return None
        return (lat, lng)

    # ── Step 4: images (swiper gallery, popup-image with full-size) ───────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        seen: set = set()
        out: List[str] = []
        gallery = parser.css_first("#gallery-carousel")
        if not gallery:
            return out
        # Prefer <a class="popup-image" href="..."> as the full-size URL
        for a in gallery.css("a.popup-image[href]"):
            href = (a.attributes.get("href") or "").strip()
            if not href or href.endswith(".svg"):
                continue
            u = _strip_spitogatos_size(href)
            if u not in seen:
                seen.add(u)
                out.append(u)
        # Fallback: <img src="..."> inside slides
        if not out:
            for img in gallery.css(".swiper-slide img[src]"):
                src = (img.attributes.get("src") or "").strip()
                if not src or src.endswith(".svg"):
                    continue
                u = _strip_spitogatos_size(src)
                if u not in seen:
                    seen.add(u)
                    out.append(u)
        return out
