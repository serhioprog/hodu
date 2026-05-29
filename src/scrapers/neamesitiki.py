"""
~/hodu/src/scrapers/neamesitiki.py

neamesitiki.gr scraper — Strategy A (paginated walk) WITH forced Stage 1
(Playwright via browser_pool) for Distil Networks anti-bot bypass.

Tech
----
Spitogatos `agentwebsites/template3` SaaS website (same family as
engelvoelkers's template1). Identical Spitogatos backend, different
CSS skin. Selector inventory below is reusable for other template3 agents.

⚠️ ANTI-BOT — Distil Networks (a.k.a. Imperva).
   Stage 0 curl_cffi returns a 2.7k-char "Pardon Our Interruption" challenge
   stub (HTTP 200 — so the funnel WON'T auto-escalate). Only Stage 1
   Playwright passes the JS+cookie challenge. We bypass the funnel and
   invoke `browser_pool.acquire()` directly. One context per collect_urls
   walk (cookie reuse helps Distil rate-limiter); fresh context per
   fetch_details (isolation).

   If Distil ever tightens and Playwright stops passing, we'd need Tier 3+
   (CDP / mouse / residential proxy) — same class as PerimeterX sites that
   are currently deferred (sousouras, ergon, kanata).

Agent identity
--------------
Real Estate Petia Sheitanova, L. Ampatzoglou 42, Nea Kallikratia, Halkidiki.
The same agent operates `halkidiki-imoti.com` on a separate (WordPress-ere)
backend. Listings on the two sites can overlap — Engine 1 handles dedup.

URL patterns
------------
List:    /en/property/search?listingType=sale&region=196&category={cat}
            &propertyTypes[0]=&priceLow={min}&page={N}
            categories: residential | commercial | land | other
            region 196 = Chalkidiki (verified via JS var phpjs.geographies)
            10 cards/page, terminates on empty page
Detail:  /en/propertyDetails/{id}    e.g. /en/propertyDetails/15892147
            id is Spitogatos autoincrement (8-digit numeric, stable)

Listing card (.listing-item)
----------------------------
.listing-item-body[href]                -> detail URL → 8-digit id
h3                                       -> title (type + village)
.property-image[style background-image]  -> main thumbnail (Spitogatos CDN, _WxH suffix)
.listing-item-details b .pull-right      -> price "€ 450,000" (US-format)
ul.property-specs li[N]                  -> [0]=size, [1]=€/sqm, [2]=bedrooms, [3]=bathrooms
p (NOT .listing-item-code)               -> location_raw (first line) + teaser desc
p.listing-item-code                      -> "Code : 0481" — agent's human ref

Detail blocks
-------------
.page-head-bck h1                        -> "Villa for sale Village (Muni), € 900,000, 250 m²"
.property-top .amenities-list            -> type-icon + bedrooms + bathrooms (quick stats)
.property-top .property-id               -> "Code 0501"
.property-features-table table tr        -> label-value spec rows (th + td)
.property-amenities li                   -> boolean flags + "Lot size: N m²" special-case
.entry .panel-body                       -> full description
#property-gallery img[src]               -> gallery (strip _900x675 → original)
.energy-container .energy[data-value]    -> energy class (A+/A/B+/.../G)

Number format (US-style)
------------------------
`€ 900,000` (comma = thousands). `_to_int_euro_safe` and `_to_sqm` (local)
both handle this. Different from sani-realestate's European format.

Caveats / decisions
-------------------
* Coordinates: NOT provided per-property. The only data-lat on detail
  pages is the agency HQ pin (HQ TRAP). We return (None, None) and rely
  on pipeline GeoMatcher via location_raw.
* size_sqm: from H1 title's trailing ", N m²". Card-level size also
  captured into seed.size_sqm as faster source.
* Multi-category: site requires category=X. We iterate residential /
  commercial / land. 'other' skipped (parking etc, rarely ≥400k).
* site_property_id = 8-digit URL id (Spitogatos backend, stable).
* "Rooms" spec-table column = combined kitchen+living+bedroom count;
  used as bedrooms ONLY if .property-top quick-stat missing.
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

_BASE_URL = "https://neamesitiki.gr"
_SOURCE_DOMAIN = "neamesitiki.gr"

_REGION_ID = 196   # Chalkidiki, per phpjs.geographies

_CATEGORIES: Tuple[str, ...] = ("residential", "commercial", "land")

_PAGES_PER_CATEGORY_LIMIT = 12
_INTER_PAGE_SLEEP_SEC = 2.0   # Politeness — Stage 1 is heavier, be courteous

# Playwright timing — Distil challenge can take 5-15s on cold session.
_PAGE_TIMEOUT_MS = 45_000

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# Agency HQ — defensive HQ-trap reject if coords ever surface.
_HQ_LATLNG = (40.3128, 23.0648)
_HQ_TOLERANCE = 0.02

# Distil challenge signature — fail fast if Stage 1 stealth ever stops working.
_DISTIL_TITLE_FRAGMENT = "Pardon Our Interruption"

# Title-prefix lowercase → canonical hodu category. Longest first.
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
    "bungalow":          "House",
    "building":          "Building",
    "farm":              "Land",
    "houseboat":         "Other",
    "office":            "Commercial",
    "store":             "Commercial",
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
}


# =============================================================================
# Helpers — pure functions
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
    """First integer; US thousands-grouping. '1,500' → 1500."""
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
    """Parse sqm in US format (comma=thousands, dot=decimal)."""
    if not text:
        return None
    cleaned = text.replace("\xa0", " ")
    m = re.search(r"\d[\d,]*(?:\.\d+)?", cleaned)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _is_hq(lat: float, lng: float) -> bool:
    return (abs(lat - _HQ_LATLNG[0]) <= _HQ_TOLERANCE and
            abs(lng - _HQ_LATLNG[1]) <= _HQ_TOLERANCE)


def _id_from_url(url: str) -> Optional[str]:
    """'/en/propertyDetails/15892147' → '15892147'."""
    if not url:
        return None
    m = re.search(r"/propertyDetails/(\d+)", url)
    return m.group(1) if m else None


def _category_from_title(title: str) -> Optional[str]:
    """'Villa for sale Nea Irakleia' → 'Villa'."""
    t = _normalize_text(title).lower()
    if not t:
        return None
    head = re.split(r"\bfor\s+(?:sale|rent)\b", t)[0].strip()
    head = head or t
    for key in sorted(_CATEGORY_MAP, key=len, reverse=True):
        if head.startswith(key) or f" {key}" in f" {head}":
            return _CATEGORY_MAP[key]
    return None


_BG_URL_RE = re.compile(r"url\(\s*['\"]?(.*?)['\"]?\s*\)", re.IGNORECASE)


def _bg_url(style: str) -> Optional[str]:
    if not style:
        return None
    m = _BG_URL_RE.search(style)
    return m.group(1).strip() if m else None


def _strip_spitogatos_size(url: str) -> str:
    """'.../287524055_900x675.jpg?v=...' → '.../287524055.jpg?v=...'."""
    return re.sub(r"_\d+x\d+(?=\.\w+(?:\?|$))", "", url)


# =============================================================================
# Scraper
# =============================================================================

class NeamesitikiScraper(EnrichmentMixin, BaseScraper):
    """neamesitiki.gr — Spitogatos template3 walk with forced Stage 1."""

    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )

    _NLP_TO_STRUCTURAL: Dict[str, set] = {
        "swimming_pool":    {"pool", "swimming_pool"},
        "sea_view":         {"sea_view", "view"},
        "parking":          {"garage", "parking", "private_parking"},
        "air_conditioning": {"air_conditioned", "air_condition", "air_conditioning", "ac"},
        "fireplace":        {"fire_place", "fireplace"},
        "balcony":          {"balconies", "balcony"},
        "garden":           {"private_garden", "garden"},
        "storage_room":     {"storage", "storage_space", "storage_room"},
        "elevator":         {"lift", "elevator"},
        "furnished":        {"furnished"},
        "alarm":            {"alarm", "security_system"},
        "solar_heater":     {"solar_heater", "solar_water_heater"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── Stage 1 fetch helpers ─────────────────────────────────────────────

    @staticmethod
    async def _make_context(browser):
        """Configure a Playwright context with realistic Chrome fingerprint."""
        return await browser.new_context(
            user_agent=_USER_AGENT,
            viewport={"width": 1366, "height": 800},
            locale="en-US",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

    async def _stage1_get_in_context(
        self, context, url: str, *, timeout_ms: int = _PAGE_TIMEOUT_MS,
    ) -> Optional[str]:
        """
        Fetch one URL using the given Playwright context (cookies preserved).
        Returns HTML on success, None on failure / Distil challenge.
        """
        page = await context.new_page()
        try:
            try:
                resp = await page.goto(
                    url, wait_until="networkidle", timeout=timeout_ms,
                )
            except Exception as exc:
                logger.warning(
                    f"[{self.source_domain}] page.goto failed {url}: {exc!r}"
                )
                return None
            if not resp:
                logger.warning(f"[{self.source_domain}] no response for {url}")
                return None
            if resp.status >= 400:
                logger.warning(
                    f"[{self.source_domain}] HTTP {resp.status} for {url}"
                )
                return None
            content = await page.content()
            if _DISTIL_TITLE_FRAGMENT in content[:2000]:
                logger.error(
                    f"[{self.source_domain}] Distil challenge served on {url} "
                    f"— Stage 1 stealth insufficient"
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
        """Acquire browser + context just for this one URL. Used by fetch_details."""
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

    def _construct_search_url(self, *, category: str, page: int, min_price: int) -> str:
        # propertyTypes%5B0%5D= (explicit index 0). Without the 0, Laravel returns 405.
        url = (
            f"{_BASE_URL}/en/property/search"
            f"?listingType=sale"
            f"&region={_REGION_ID}"
            f"&category={category}"
            f"&propertyTypes%5B0%5D="
            f"&roomsLow=nd"
            f"&priceLow={min_price}"
            f"&priceHigh="
            f"&livingAreaLow="
            f"&livingAreaHigh="
            f"&myCode="
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
        """
        Walk all 3 categories, paginated 10/page. ONE browser+context for the
        whole walk — cookies accumulate, helps Distil not re-challenge.
        """
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
                            # Failure or Distil challenge — abandon this category
                            break

                        parser = LexborHTMLParser(html)
                        cards = parser.css(".listing-item")
                        if not cards:
                            logger.info(
                                f"[{self.source_domain}] {category} page "
                                f"{page_num}: 0 cards — end"
                            )
                            break

                        page_added = 0
                        for card in cards:
                            try:
                                seed = self._parse_card(card, category=category)
                            except Exception as exc:
                                logger.error(
                                    f"[{self.source_domain}] card parse: {exc!r}"
                                )
                                continue
                            if not seed or seed.site_property_id in seeds:
                                continue
                            seeds[seed.site_property_id] = seed
                            page_added += 1

                        logger.info(
                            f"[{self.source_domain}] {category} page {page_num}: "
                            f"{len(cards)} cards (+{page_added} new, total {len(seeds)})"
                        )
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

    def _parse_card(
        self, card: LexborNode, *, category: str,
    ) -> Optional[PropertyTemplate]:
        link = card.css_first("a.listing-item-body[href]")
        if not link:
            return None
        href = (link.attributes.get("href") or "").strip()
        if not href:
            return None
        if href.startswith("/"):
            href = f"{_BASE_URL}{href}"

        site_id = _id_from_url(href)
        if not site_id:
            return None

        h3 = card.css_first("h3")
        title = _normalize_text(h3.text(strip=False)) if h3 else ""
        cat = _category_from_title(title)

        price_node = card.css_first(".listing-item-details b .pull-right")
        price_text = _normalize_text(price_node.text(strip=False)) if price_node else ""
        price_int = self._to_int_euro_safe(price_text)

        # Card quick stats: [0]=size, [1]=€/sqm, [2]=bedrooms, [3]=bathrooms
        size_sqm: Optional[float] = None
        bedrooms: Optional[int] = None
        bathrooms: Optional[int] = None
        for li in card.css("ul.property-specs li"):
            txt = _normalize_text(li.text(strip=False))
            if not txt:
                continue
            low = txt.lower()
            if "/sq" in low:
                continue   # price-per-sqm — ignore at seed stage
            if size_sqm is None and re.search(r"\d[\d,]*\s*m[²2]\b", txt):
                size_sqm = _to_sqm(txt)
            elif bedrooms is None and "bedroom" in low:
                bedrooms = _to_int_simple(txt)
            elif bathrooms is None and "bathroom" in low:
                bathrooms = _to_int_simple(txt)

        # Location: first non-empty <p> that's not the code line.
        location_raw: Optional[str] = None
        for p in card.css("p"):
            cls = p.attributes.get("class") or ""
            if "listing-item-code" in cls:
                continue
            multiline = _normalize_text(p.text(separator="\n", strip=True))
            if multiline:
                first_line = multiline.split("\n", 1)[0].strip()
                if first_line:
                    location_raw = first_line
                break

        seed = PropertyTemplate(
            site_property_id=str(site_id),
            source_domain=self.source_domain,
            url=href,
            price=(price_int if price_int is not None else None),
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
        """One-shot Stage 1 fetch + parse. Daily_sync calls us per-property."""
        html = await self._stage1_get_one_shot(url)
        if not html:
            return {}

        parser = LexborHTMLParser(html)
        data: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # Step 1: structured blocks
        self._parse_title_block(parser, data, extra)
        self._parse_property_top(parser, data, extra)
        self._parse_features_table(parser, data, extra)
        self._parse_amenities(parser, data, extra)

        # Step 2: description (og fallback)
        description = (self._extract_description(parser)
                       or self._og_description_fallback(parser))
        if description:
            data["description"] = description

        # Step 3: coords — INTENTIONALLY NONE on this template (HQ trap).

        # Step 4: images
        images = self._extract_images(parser)
        if not images:
            og = self._og_image_fallback(parser)
            if og:
                images = [og]
        if images:
            data["images"] = images

        if extra:
            data["extra_features"] = extra

        # Step 5: NLP fallback
        self._apply_nlp_fallback(data)

        # Step 7: quality gate (log-only)
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )

        return data

    # ── Step 1: title (H1 with embedded size) ─────────────────────────────

    def _parse_title_block(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        h1 = parser.css_first(".page-head-bck h1") or parser.css_first("h1")
        if not h1:
            return
        title = _normalize_text(h1.text(strip=False))

        cat = _category_from_title(title)
        if cat:
            data["category"] = cat

        # Trailing ", N m²" in H1
        m = re.search(r",\s*([\d,]+(?:\.\d+)?)\s*m[²2]\b", title)
        if m:
            v = _to_sqm(m.group(1))
            if v is not None and "size_sqm" not in data:
                data["size_sqm"] = v

    # ── Step 1: property-top quick stats ──────────────────────────────────

    def _parse_property_top(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        top = parser.css_first(".property-top")
        if not top:
            return

        for li in top.css(".amenities-list li"):
            ic = li.css_first("i")
            cls = (ic.attributes.get("class") or "") if ic else ""
            text = _normalize_text(li.text(strip=False))
            if not text:
                continue
            if "fa-bed" in cls:
                n = _to_int_simple(text)
                if n is not None and "bedrooms" not in data:
                    data["bedrooms"] = n
            elif "fa-tint" in cls:
                n = _to_int_simple(text)
                if n is not None and "bathrooms" not in data:
                    data["bathrooms"] = n
            elif "fa-home" in cls and "category" not in data:
                cat = _category_from_title(text)
                if cat:
                    data["category"] = cat

        pid = top.css_first(".property-id")
        if pid:
            txt = _normalize_text(pid.text(strip=False))
            cleaned = re.sub(r"^\s*code\s*", "", txt, flags=re.IGNORECASE).strip()
            m = re.match(r"([A-Z0-9]{2,12})", cleaned)
            if m:
                extra["agent_code"] = m.group(1)

    # ── Step 1: features table ────────────────────────────────────────────

    def _parse_features_table(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        table = parser.css_first(".property-features-table table")
        if not table:
            return
        for tr in table.css("tr"):
            th = tr.css_first("th")
            td = tr.css_first("td")
            if not th or not td:
                continue
            label = _normalize_text(th.text(strip=False)).rstrip(":").lower()
            value_text = _normalize_text(td.text(strip=False))
            if not value_text:
                continue
            self._route_table_row(label, value_text, td, data, extra)

    def _route_table_row(
        self,
        label: str,
        value_text: str,
        td_node: LexborNode,
        data: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
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
        if label == "energy class":
            div = (td_node.css_first(".energy[data-value]")
                   or td_node.css_first(".energy"))
            if div:
                ec_text = _normalize_text(div.text(strip=False))
                if ec_text:
                    extra["energy_class"] = ec_text
                else:
                    cls = div.attributes.get("class") or ""
                    m = re.search(r"energy-([a-z\-]+)", cls)
                    if m:
                        extra["energy_class"] = (
                            m.group(1).replace("-plus", "+").upper()
                        )
            else:
                m = re.search(r"\b([A-G][\+\-]?)\b", value_text)
                if m:
                    extra["energy_class"] = m.group(1)
            return
        if label == "status":
            for tag in [t.strip() for t in value_text.split(",")]:
                if tag:
                    extra[_slug(tag)] = True
            return
        if label == "type":
            for tag in [t.strip() for t in value_text.split(",")]:
                if tag:
                    extra[_slug(tag)] = True
            return
        if label == "neighborhood":
            data["area"] = value_text
            return
        if label == "price per m²":
            n = _to_int_simple(value_text)
            if n is not None:
                extra["price_per_sqm"] = n
            return
        extra[_slug(label)] = value_text

    # ── Step 1: amenities list ────────────────────────────────────────────

    def _parse_amenities(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first(".property-amenities")
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
            extra[_slug(text)] = True

    # ── Step 2: description ──────────────────────────────────────────────

    def _extract_description(self, parser: LexborHTMLParser) -> Optional[str]:
        body = parser.css_first(".entry .panel-body")
        if not body:
            return None
        txt = _normalize_text(body.text(separator="\n", strip=True))
        return txt if (txt and len(txt) >= 50) else None

    # ── Step 4: images ────────────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        seen: set = set()
        out: List[str] = []

        gallery = parser.css_first("#property-gallery")
        if gallery:
            for img in gallery.css("img[src]"):
                src = (img.attributes.get("src") or "").strip()
                if not src:
                    continue
                u = _strip_spitogatos_size(src)
                if u and u not in seen and not u.endswith(".svg"):
                    seen.add(u)
                    out.append(u)

        if not out:
            thumbs = parser.css_first("#property-gallery-thumbnails")
            if thumbs:
                for div in thumbs.css(".item.bg-image[style]"):
                    bg = _bg_url(div.attributes.get("style") or "")
                    if not bg:
                        continue
                    u = _strip_spitogatos_size(bg)
                    if u and u not in seen and not u.endswith(".svg"):
                        seen.add(u)
                        out.append(u)
        return out