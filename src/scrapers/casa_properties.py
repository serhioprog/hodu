"""
~/hodu/src/scrapers/casa_properties.py

casaproperties.gr scraper — Spitogatos agentwebsites/template1 SaaS.
SAME backend as neamesitiki (template3) and engelvoelkers (template1).

Agent: CASA PROPERTIES, Nikiti (Sithonia, Halkidiki).
Owner Georgios Tzeprailidis. casahalkidiki@hotmail.com.

URL patterns (identical to neamesitiki — Spitogatos backend)
-----------------------------------------------------------
List:    /en/property/search?listingType=sale&category={cat}&region=196
            &roomsLow=nd&priceLow={min}&propertyTypes%5B0%5D=&page={N}
         region 196 = Chalkidiki. 12 cards/page. propertyTypes[0]=
         (explicit index 0) — without it, Laravel returns 405.
         categories: residential | commercial | land
Detail:  /en/propertyDetails/{id}  (8-digit Spitogatos id, e.g. 15830965)

⚠️ ANTI-BOT — Distil Networks (SAME as neamesitiki).
   Stage 0 returns 2752-char "Pardon Our Interruption" challenge stub
   with HTTP 200 — funnel does not auto-escalate. Forced Stage 1 via
   browser_pool.acquire(). Detection guard fails fast if stealth ever
   stops working.

Template1 vs Template3 (neamesitiki)
------------------------------------
List card structure:
  template3: .listing-item > a.listing-item-body[href]
  template1: .listing-item > a.listing-image[href] + h4 > a (title)

Detail page differences:
  template3: .page-head-bck h1     ; template1: .page-head h1
  template3: .entry .panel-body    ; template1: .entry directly
  template3: NO per-property coords (HQ pin only)
  template1: .map .marker[data-lat][data-lng]  ← REAL coords (offset for privacy)
  template3: agent-code "0501"     ; template1: "234563" (pure numeric)

Number format: US (comma = thousands). "€ 1,250,000" = 1_250_000.
              "300 m²", "4,167 €/sq.m." — same comma format.

HQ trap
-------
Agency office at (40.224329, 23.666925), Nikiti. With data-type="offset"
on per-property markers, listings are intentionally fuzzed ~500m. HQ
tolerance set at 0.005 (~500m) — properties near office may need manual
review, but most are sufficiently distant.
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

_BASE_URL = "https://casaproperties.gr"
_SOURCE_DOMAIN = "casaproperties.gr"

_REGION_ID = 196   # Chalkidiki

_CATEGORIES: Tuple[str, ...] = ("residential", "commercial", "land")

_PAGES_PER_CATEGORY_LIMIT = 12
_INTER_PAGE_SLEEP_SEC = 2.0

_PAGE_TIMEOUT_MS = 45_000

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# Agency HQ — Nikiti office (per footer's "Locate our office on the map" link).
_HQ_LATLNG = (40.224329, 23.666925)
_HQ_TOLERANCE = 0.005   # ~500m

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


def _is_hq(lat: float, lng: float) -> bool:
    return (abs(lat - _HQ_LATLNG[0]) <= _HQ_TOLERANCE and
            abs(lng - _HQ_LATLNG[1]) <= _HQ_TOLERANCE)


def _id_from_url(url: str) -> Optional[str]:
    """'/en/propertyDetails/15830965' → '15830965'."""
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
    serves ONLY sized variants (_900x675, _1600x1200, etc.); stripping
    the suffix produces 404s. Sized URLs from gallery are passed through.

    Verified 2026-05-30:
      m3.spitogatos.gr/{id}_900x675.jpg?v=...  → 200 OK
      m3.spitogatos.gr/{id}.jpg?v=...          → 404
    Same fix applied to kassandra_estate.py.
    """
    return url


# =============================================================================
# Scraper
# =============================================================================

class CasaPropertiesScraper(EnrichmentMixin, BaseScraper):
    """casaproperties.gr — Spitogatos template1 walk with forced Stage 1."""

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
            f"?listingType=sale"
            f"&category={category}"
            f"&region={_REGION_ID}"
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
                                seed = self._parse_card(card)
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
        # URL — prefer a.listing-image[href], fallback h4>a[href]
        link = card.css_first("a.listing-image[href]")
        if not link:
            link = card.css_first("h4 a[href]")
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

        # Title from h4 (which contains an <a>)
        h4 = card.css_first("h4")
        title = ""
        if h4:
            anchor = h4.css_first("a")
            title = _normalize_text(
                anchor.text(strip=False) if anchor else h4.text(strip=False)
            )

        cat = _category_from_title(title)

        # Price + size from <p><b>...€ 1,250,000<span class="pull-right">300 m²</span></b></p>
        price_int: Optional[int] = None
        size_sqm: Optional[float] = None
        for b in card.css("p b"):
            size_span = b.css_first(".pull-right")
            size_text = ""
            if size_span:
                size_text = _normalize_text(size_span.text(strip=False))
                if "m" in size_text.lower():
                    size_sqm = _to_sqm(size_text)
            # Price is the b text minus the size span text
            full = _normalize_text(b.text(strip=False))
            if size_text and size_text in full:
                price_text = full.replace(size_text, "").strip()
            else:
                price_text = full
            if "€" in price_text or price_text.replace(",", "").replace(" ", "").isdigit():
                price_int = self._to_int_euro_safe(price_text)
                if price_int:
                    break

        # Specs list — <ul><li>4 Bedrooms</li><li>2 Bathrooms</li><li>X €/sq.m.</li></ul>
        bedrooms: Optional[int] = None
        bathrooms: Optional[int] = None
        for li in card.css("ul li"):
            txt = _normalize_text(li.text(strip=False))
            low = txt.lower()
            if not txt or "€/sq" in low:
                continue
            if bedrooms is None and "bedroom" in low:
                bedrooms = _to_int_simple(txt)
            elif bathrooms is None and "bathroom" in low:
                bathrooms = _to_int_simple(txt)

        # Agent code — p.listing-item-code "Code\xa0234563"
        agent_code: Optional[str] = None
        code_p = card.css_first("p.listing-item-code")
        if code_p:
            m = re.search(r"Code\s*(\d+)", _normalize_text(code_p.text(strip=False)))
            if m:
                agent_code = m.group(1)

        # Card-level location not exposed in template1 list. "Halkidiki" used
        # as placeholder so daily_sync's HALKIDIKI_REGIONS_WHITELIST filter passes
        # (it checks substring match in url+location_raw). Detail-page Neighborhood
        # overrides this with the real area (e.g. "Nikiti") before save.
        # Placeholder is factual: CASA PROPERTIES is a Halkidiki-only agency
        # (office in Nikiti, Sithonia).
        seed = PropertyTemplate(
            site_property_id=str(site_id),
            source_domain=self.source_domain,
            url=href,
            price=price_int,
            size_sqm=size_sqm,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            location_raw="Halkidiki",
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
        self._parse_property_top(parser, data, extra)
        self._parse_features_table(parser, data, extra)
        self._parse_amenities(parser, data, extra)
        self._parse_energy_class(parser, extra)

        # Step 2: description
        description = self._extract_description(parser)
        if description:
            data["description"] = description

        # Step 3: coordinates (PER-PROPERTY!)
        coords = self._extract_coords(parser)
        if coords:
            lat, lng = coords
            if _bbox_check(lat, lng) and not _is_hq(lat, lng):
                data["latitude"] = lat
                data["longitude"] = lng

        # Step 4: images
        images = self._extract_images(parser)
        if images:
            data["images"] = images

        if extra:
            data["extra_features"] = extra

        # Heuristic: large size + no bedrooms ⇒ likely Land plot.
        # Some agency titles lack any category keyword ("FOR SALE PROPERTY IN ...")
        # and H1 / property-top detection both fail. Size signal disambiguates.
        if "category" not in data:
            sz = data.get("size_sqm")
            bd = data.get("bedrooms")
            if sz and sz >= 1000 and not bd:
                data["category"] = "Land"

        # Land normalization: Area = plot area, replicate to land_size_sqm
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

    # ── Step 1.a: title (h1 in .page-head) ────────────────────────────────

    def _parse_title_block(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        h1 = parser.css_first(".page-head h1") or parser.css_first("h1")
        if not h1:
            return
        title = _normalize_text(h1.text(strip=False))
        cat = _category_from_title(title)
        if cat:
            data["category"] = cat
        # Trailing ", N m²" inside H1
        m = re.search(r",\s*([\d,]+(?:\.\d+)?)\s*m[²2]\b", title)
        if m:
            v = _to_sqm(m.group(1))
            if v is not None and "size_sqm" not in data:
                data["size_sqm"] = v

    # ── Step 1.b: property-top (amenities + code) ─────────────────────────

    def _parse_property_top(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        top = parser.css_first(".property-top")
        if not top:
            return

        for li in top.css(".amenities-list li"):
            icon = li.css_first("i")
            cls = (icon.attributes.get("class") or "") if icon else ""
            text = _normalize_text(li.text(strip=False))
            if not text:
                continue
            if "fa-home" in cls and "category" not in data:
                cat = _category_from_title(text)
                if cat:
                    data["category"] = cat
            elif "fa-bed" in cls:
                n = _to_int_simple(text)
                if n is not None and "bedrooms" not in data:
                    data["bedrooms"] = n
            elif "fa-tint" in cls:
                n = _to_int_simple(text)
                if n is not None and "bathrooms" not in data:
                    data["bathrooms"] = n

        pid = top.css_first(".property-id")
        if pid:
            txt = _normalize_text(pid.text(strip=False))
            m = re.search(r"Code\s*(\d+)", txt)
            if m:
                extra["agent_code"] = m.group(1)

    # ── Step 1.c: features table (IDENTICAL to template3) ─────────────────

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
            div = td_node.css_first(".energy")
            if div:
                ec = _normalize_text(div.text(strip=False))
                if ec:
                    extra["energy_class"] = ec
            else:
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
        if label == "neighborhood":
            data["area"] = value_text
            return
        if label == "price per m²":
            n = _to_int_simple(value_text)
            if n is not None:
                extra["price_per_sqm"] = n
            return
        sl = _slug(label)
        if sl:
            extra[sl] = value_text

    # ── Step 1.d: amenities list (with "Lot size", "Distance from sea") ───

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

            # "Lot size: N m²" → land_size_sqm
            if low.startswith("lot size"):
                v = _to_sqm(text)
                if v is not None and "land_size_sqm" not in data:
                    data["land_size_sqm"] = v
                continue

            # "Distance from sea (m): N"
            if "distance from sea" in low:
                m = re.search(r"(\d+)", text)
                if m:
                    extra["distance_sea_m"] = int(m.group(1))
                continue

            # "Size of balconies: N m²"
            if low.startswith("size of balconies"):
                v = _to_sqm(text)
                if v is not None:
                    extra["balcony_size_sqm"] = v
                continue

            # "Average Monthly shared expenses: N €" — skip noise
            if "monthly shared expenses" in low:
                continue

            # Generic "Label: value" splits
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

            # Boolean flag
            sl = _slug(text)
            if sl:
                extra[sl] = True

    # ── Step 1.e: energy class (fallback if not parsed in table) ──────────

    def _parse_energy_class(
        self, parser: LexborHTMLParser, extra: Dict[str, Any],
    ) -> None:
        if "energy_class" in extra:
            return
        ec = parser.css_first(".energy-container .energy")
        if ec:
            txt = _normalize_text(ec.text(strip=False))
            if txt:
                extra["energy_class"] = txt

    # ── Step 2: description (.entry directly, no .panel-body) ─────────────

    def _extract_description(self, parser: LexborHTMLParser) -> Optional[str]:
        entry = parser.css_first(".entry")
        if not entry:
            return None
        txt = _normalize_text(entry.text(separator=" ", strip=True))
        # Trim "Contact information:" footer
        m = re.search(r"\bcontact\s+information\b", txt, re.IGNORECASE)
        if m:
            txt = txt[: m.start()].strip()
        return txt if (txt and len(txt) >= 50) else None

    # ── Step 3: coordinates (.marker[data-lat][data-lng]) ─────────────────

    def _extract_coords(
        self, parser: LexborHTMLParser,
    ) -> Optional[Tuple[float, float]]:
        marker = parser.css_first(".map .marker[data-lat]") or \
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

    # ── Step 4: images ────────────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        seen: set = set()
        out: List[str] = []
        gallery = parser.css_first("#property-gallery")
        if gallery:
            for img in gallery.css("img[src]"):
                src = (img.attributes.get("src") or "").strip()
                if not src or src.endswith(".svg"):
                    continue
                u = _strip_spitogatos_size(src)
                if u not in seen:
                    seen.add(u)
                    out.append(u)
        # Also try thumbnails carousel
        thumbs = parser.css_first("#property-gallery-thumbnails")
        if thumbs:
            for img in thumbs.css("img[src]"):
                src = (img.attributes.get("src") or "").strip()
                if not src or src.endswith(".svg"):
                    continue
                u = _strip_spitogatos_size(src)
                if u not in seen:
                    seen.add(u)
                    out.append(u)
            # Some templates store thumbnails as background-image
            for div in thumbs.css("[style*='background-image']"):
                bg = _bg_url(div.attributes.get("style") or "")
                if not bg or bg.endswith(".svg"):
                    continue
                u = _strip_spitogatos_size(bg)
                if u not in seen:
                    seen.add(u)
                    out.append(u)
        return out
