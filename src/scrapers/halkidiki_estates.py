"""
~/hodu/src/scrapers/halkidiki_estates.py

halkidikiestates.com scraper — WordPress + Estatik plugin (estatik.net).
Sprint 10 #5. FIRST non-Spitogatos / non-WPestate WordPress scraper in hodu.

Platform: WordPress + Astra + Elementor + Estatik plugin v3.10.3.
Agent: Meliton Properties, Thessaloniki.
Stage: 0 (curl_cffi). No Distil / Cloudflare. ~36 properties at €400k+ (4 pages × 9).

URL patterns
------------
List (p1): /?s&es_search[price][min]=400000&...&post_type=properties
List (pN): /?paged-1=N&s&es_search[price][min]=400000&...&post_type=properties
Detail:    /property/{slug}/

Selectors (verified via HTML probe)
-----------------------------------
List card:
  Container: div.properties.type-properties (has es_type-X classes)
  URL:       .es-thumbnail a[href] | h2 a[href]
  Title:     h2 a.es-property-link
  Price:     .es-price
  Sold-out:  .es-property-label-sold-out (skip)
  Category:  es_type-{villa|plots|new-projects}
  Specs:     .es-bottom-icon__list .es-bottom-icon (beds, baths only — area UNRELIABLE)

Detail:
  Title:     h1.elementor-heading-title
  Fields:    .es-property-fields ul li → <strong>Label:</strong> Value
             (Type/Status/Bedrooms/Bathrooms/Floors/Area/Lot size/Year built)
  Desc:      #es-description p (+ ul li)
  Features:  .es-features-list-wrap ul li
  Images:    .es-gallery img[data-magnific-img] (slick clones — dedupe by URL)
  Coords:    NONE (map tab hidden site-wide via display:none)

Quirks
------
- SOLD-OUT TRAP: detail page may show es_status-for-sale despite card label.
  Visible card label is truth. Also re-check 'status' field on detail.
- CARD AREA UNRELIABLE: "X sq ft" typos for clearly m² Greek villas; sometimes
  shows garden size not building. Always use detail page Area field.
- IMAGE DEDUP: slick carousel adds .slick-cloned clones at edges; dedupe by URL.
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

_BASE_URL = "https://halkidikiestates.com"
_SOURCE_DOMAIN = "halkidikiestates.com"

_MAX_PAGES = 8
_PER_PAGE = 9
_INTER_REQUEST_SLEEP = 0.5
_MIN_PAGE_BYTES = 5_000
_MIN_DETAIL_BYTES = 10_000

_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

_ES_TYPE_CATEGORY: Dict[str, str] = {
    "villa":         "Villa",
    "plots":         "Land",
    "plot":          "Land",
    "new-projects":  "New Build",
    "apartment":     "Apartment",
    "maisonette":    "Maisonette",
    "house":         "House",
    "commercial":    "Commercial",
}

_CATEGORY_FROM_TITLE: Dict[str, str] = {
    "villa":       "Villa",
    "villas":      "Villa",
    "maisonette":  "Maisonette",
    "apartment":   "Apartment",
    "house":       "House",
    "plot":        "Land",
    "land":        "Land",
    "shop":        "Commercial",
    "store":       "Commercial",
    "office":      "Commercial",
}

_LOCATION_KEYWORDS: Tuple[str, ...] = (
    "Vourvourou", "Pefkochori", "Sithonia", "Kassandra",
    "Nikiti", "Sani", "Halkidiki",
)

_WP_SIZE_SUFFIX_RE = re.compile(r'-\d+x\d+(?=\.(?:jpe?g|png|webp)$)', re.I)
_ES_TYPE_RE = re.compile(r'\bes_type-([a-z0-9\-]+)')


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


def _strip_wp_size(url: str) -> str:
    return _WP_SIZE_SUFFIX_RE.sub('', url or '')


def _slug_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/property/([^/?#]+)/?", url)
    return m.group(1) if m else None


def _category_from_es_type(class_str: str) -> Optional[str]:
    if not class_str:
        return None
    m = _ES_TYPE_RE.search(class_str)
    if not m:
        return None
    return _ES_TYPE_CATEGORY.get(m.group(1).lower())


def _category_from_title(title: str) -> Optional[str]:
    t = _normalize_text(title).lower()
    if not t:
        return None
    for key in sorted(_CATEGORY_FROM_TITLE, key=len, reverse=True):
        if key in t:
            return _CATEGORY_FROM_TITLE[key]
    return None


def _extract_location(*texts: str) -> Optional[str]:
    haystack = " ".join((t or "") for t in texts)
    if not haystack:
        return None
    for needle in sorted(_LOCATION_KEYWORDS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(needle)}\b", haystack, re.IGNORECASE):
            return needle
    return None


def _is_sold_out(card: LexborNode) -> bool:
    return card.css_first(".es-property-label-sold-out") is not None


# =============================================================================
# Scraper
# =============================================================================

class HalkidikiEstatesScraper(EnrichmentMixin, BaseScraper):
    """halkidikiestates.com — WordPress + Estatik plugin, Stage 0 (curl_cffi)."""

    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels", "category",
    )

    _NLP_TO_STRUCTURAL: Dict[str, set] = {
        "swimming_pool":    {"pool", "swimming_pool", "private_pool", "heated_pool"},
        "sea_view":         {"sea_view", "view_to_the_sea", "sea_view_from_balconies"},
        "parking":          {"parking", "private_parking", "parking_space", "parking_1", "parking_2"},
        "air_conditioning": {"a_c", "ac", "air_conditioning", "air_conditioning_installation_in_operation"},
        "fireplace":        {"fire_place", "fireplace"},
        "balcony":          {"balcony", "balconies"},
        "garden":            {"private_garden", "garden"},
        "elevator":          {"lift", "elevator"},
        "furnished":         {"furnished", "fully_furnished"},
        "alarm":             {"alarm", "full_alarm_system"},
        "solar_heater":      {"solar_heater", "solar_water_heater"},
        "barbecue":          {"bbq"},
        "storage_room":      {"storage_room"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    @staticmethod
    def _construct_search_url(*, page: int, min_price: int) -> str:
        common = (
            "s"
            f"&es_search%5Bprice%5D%5Bmin%5D={min_price}"
            "&es_search%5Bprice%5D%5Bmax%5D"
            "&es_search%5Bbedrooms%5D%5Bmin%5D"
            "&es_search%5Bbedrooms%5D%5Bmax%5D"
            "&post_type=properties"
        )
        if page == 1:
            return f"{_BASE_URL}/?{common}"
        return f"{_BASE_URL}/?paged-1={page}&{common}"

    async def _stage0_get(self, url: str) -> Optional[str]:
        try:
            r = await self.client.get(url)
        except Exception as exc:
            logger.warning(f"[{self.source_domain}] GET error {url}: {exc!r}")
            return None
        if r.status_code != 200:
            logger.warning(f"[{self.source_domain}] HTTP {r.status_code} for {url}")
            return None
        html = r.text
        if len(html) < _MIN_PAGE_BYTES:
            logger.warning(f"[{self.source_domain}] short {len(html)}B for {url}")
            return None
        return html

    # ── Phase 1: collect_urls ────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        seeds: Dict[str, PropertyTemplate] = {}

        for page_num in range(1, max_pages + 1):
            url = self._construct_search_url(page=page_num, min_price=min_price)
            logger.info(f"[{self.source_domain}] page {page_num}: Stage0 GET {url}")

            html = await self._stage0_get(url)
            if not html:
                break

            parser = LexborHTMLParser(html)
            cards = parser.css("div.properties.type-properties")
            if not cards:
                logger.info(f"[{self.source_domain}] page {page_num}: 0 cards — end")
                break

            page_added = 0
            page_sold = 0
            for card in cards:
                if _is_sold_out(card):
                    page_sold += 1
                    continue
                try:
                    seed = self._parse_card(card)
                except Exception as exc:
                    logger.error(f"[{self.source_domain}] card parse: {exc!r}")
                    continue
                if not seed or seed.site_property_id in seeds:
                    continue
                seeds[seed.site_property_id] = seed
                page_added += 1

            logger.info(
                f"[{self.source_domain}] page {page_num}: {len(cards)} cards "
                f"(+{page_added} new, total {len(seeds)}, -{page_sold} sold-out)"
            )

            if page_added == 0 and page_num > 1:
                break
            if '<a class="next page-numbers"' not in html:
                break

            await asyncio.sleep(_INTER_REQUEST_SLEEP + random.uniform(0.1, 0.5))

        logger.info(f"[{self.source_domain}] collect_urls: {len(seeds)} seeds")
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        classes = card.attributes.get("class", "") or ""

        a = card.css_first(".es-thumbnail a[href]") or card.css_first("h2 a[href]")
        if not a:
            return None
        href = (a.attributes.get("href") or "").strip()
        if not href:
            return None
        if href.startswith("/"):
            href = f"{_BASE_URL}{href}"
        if "/property/" not in href:
            return None

        slug = _slug_from_url(href)
        if not slug:
            return None

        price: Optional[int] = None
        price_node = card.css_first(".es-price")
        if price_node:
            price = self._to_int_euro_safe(_normalize_text(price_node.text(strip=False)))

        title = ""
        tn = card.css_first("h2 a.es-property-link") or card.css_first("h2 a")
        if tn:
            title = _normalize_text(tn.text(strip=False))

        excerpt = ""
        ex = card.css_first(".es-property-excerpt p")
        if ex:
            excerpt = _normalize_text(ex.text(strip=False))

        location = _extract_location(title, excerpt)

        bedrooms: Optional[int] = None
        bathrooms: Optional[int] = None
        for icon in card.css(".es-bottom-icon__list .es-bottom-icon"):
            ihtml = icon.html or ""
            itext = _normalize_text(icon.text(strip=False))
            n = _to_int_simple(itext)
            if n is None:
                continue
            if "icon--bedrooms" in ihtml or "beds" in itext.lower():
                bedrooms = n
            elif "icon--bathrooms" in ihtml or "baths" in itext.lower():
                bathrooms = n

        category = _category_from_es_type(classes) or _category_from_title(title)

        seed = PropertyTemplate(
            site_property_id=str(slug),
            source_domain=self.source_domain,
            url=href,
            price=price,
            size_sqm=None,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            location_raw=location,
        )
        if category:
            try:
                seed.category = category
            except Exception:
                pass
        return seed

    # ── Phase 2: fetch_details ───────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        html = await self._stage0_get(url)
        if not html or len(html) < _MIN_DETAIL_BYTES:
            return {}

        parser = LexborHTMLParser(html)
        data: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # Step 1: title + property fields
        self._parse_title_block(parser, data, extra)
        fields = self._parse_property_fields(parser)
        self._route_fields(fields, data, extra)

        # Sold-out trap re-check
        status = (fields.get("status") or "").strip().lower()
        if "sold" in status:
            logger.info(f"[{self.source_domain}] skip {url}: status='{status}'")
            return {}

        # Step 2: description + og fallback
        description = self._extract_description(parser)
        if not description:
            description = self._og_description_fallback(parser)
        if description:
            data["description"] = description

        # Step 3: NO coords (Estatik map hidden site-wide)

        # Features
        for f in self._extract_features(parser):
            sl = _slug(f)
            if sl:
                extra[sl] = True

        # Step 4: images + og fallback
        images = self._extract_images(parser)
        if not images:
            og = self._og_image_fallback(parser)
            if og:
                images = [og]
        if images:
            data["images"] = images

        # Category fallback from page class container
        if "category" not in data:
            container = parser.css_first("div.elementor.elementor-location-single")
            if container is not None:
                cat = _category_from_es_type(container.attributes.get("class", "") or "")
                if cat:
                    data["category"] = cat

        if extra:
            data["extra_features"] = extra

        # Land heuristic
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

        # Step 7: quality gate (log-only)
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(f"[{self.source_domain}] thin desc for {url}")

        return data

    # ── Step helpers ─────────────────────────────────────────────────────

    def _parse_title_block(self, parser, data, extra):
        h1 = parser.css_first("h1.elementor-heading-title") or parser.css_first("h1")
        if not h1:
            return
        title = _normalize_text(h1.text(strip=False))
        if not title:
            return
        cat = _category_from_title(title)
        if cat:
            data["category"] = cat
        loc = _extract_location(title)
        if loc:
            data["area"] = loc

    def _parse_property_fields(self, parser) -> Dict[str, str]:
        result: Dict[str, str] = {}
        block = parser.css_first(".es-property-fields ul")
        if not block:
            return result
        for li in block.css("li"):
            strong = li.css_first("strong")
            if not strong:
                continue
            key = _normalize_text(strong.text(strip=False)).rstrip(":").lower()
            if not key:
                continue
            link = li.css_first('a[rel="tag"]')
            if link:
                value = _normalize_text(link.text(strip=False))
            else:
                full = _normalize_text(li.text(strip=False))
                stxt = _normalize_text(strong.text(strip=False))
                value = full.replace(stxt, "", 1).strip()
            if value:
                result[key] = value
        return result

    def _route_fields(self, fields, data, extra):
        for key, value in fields.items():
            if key == "type":
                cat = _ES_TYPE_CATEGORY.get(value.lower()) or _category_from_title(value)
                if cat and "category" not in data:
                    data["category"] = cat
                continue
            if key == "status":
                extra["listing_status"] = value
                continue
            if key == "bedrooms":
                n = _to_int_simple(value)
                if n is not None and "bedrooms" not in data:
                    data["bedrooms"] = n
                continue
            if key == "bathrooms":
                n = _to_int_simple(value)
                if n is not None and "bathrooms" not in data:
                    data["bathrooms"] = n
                continue
            if key == "floors":
                n = _to_int_simple(value)
                if n is not None:
                    data["levels"] = n
                continue
            if key == "area":
                v = _to_sqm(value)
                if v is not None and "size_sqm" not in data:
                    data["size_sqm"] = v
                continue
            if key == "lot size":
                v = _to_sqm(value)
                if v is not None and "land_size_sqm" not in data:
                    data["land_size_sqm"] = v
                continue
            if key == "year built":
                n = _to_int_simple(value)
                if n is not None and 1900 <= n <= 2100:
                    data["year_built"] = n
                continue
            if key == "date added":
                extra["date_added"] = value
                continue
            if key == "post updated":
                extra["post_updated"] = value
                continue
            sl = _slug(key)
            if sl:
                extra[sl] = value

    def _extract_description(self, parser) -> Optional[str]:
        block = parser.css_first("#es-description")
        if not block:
            return None
        parts: List[str] = []
        for p in block.css("p"):
            txt = _normalize_text(p.text(separator=" ", strip=True))
            if txt:
                parts.append(txt)
        for li in block.css("ul li"):
            txt = _normalize_text(li.text(separator=" ", strip=True))
            if txt and 3 < len(txt) < 200:
                parts.append(f"• {txt}")
        text = "\n".join(parts).strip()
        text = re.sub(r"\bcontact\s+(?:information|us)\b.*$", "", text,
                      flags=re.IGNORECASE | re.DOTALL).strip()
        return text if (text and len(text) >= 50) else None

    def _extract_features(self, parser) -> List[str]:
        out: List[str] = []
        block = parser.css_first(".es-features-list-wrap ul")
        if not block:
            return out
        for li in block.css("li"):
            txt = _normalize_text(li.text(strip=False)).lstrip("•").strip()
            if txt:
                out.append(txt)
        return out

    def _extract_images(self, parser) -> List[str]:
        seen: set = set()
        out: List[str] = []
        gallery = parser.css_first(".es-gallery")
        if not gallery:
            return out
        for img in gallery.css("img"):
            u = (img.attributes.get("data-magnific-img") or "").strip()
            if not u:
                u = (img.attributes.get("data-src") or "").strip()
                if u and not u.startswith("data:"):
                    u = _strip_wp_size(u)
            if not u or u.startswith("data:"):
                continue
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
        return out
