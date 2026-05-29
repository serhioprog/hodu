"""
Scraper for remax-metron.gr (RE/MAX Metron Halkidiki agency).
Sprint 10 #7. ASP.NET Blazor SSR shell + jQuery AJAX populates results.

Architecture (Stage 0 hybrid):
- List: POST /en-US/search-results with JSON filter body. Direct GET on
  /en-US/results returns an empty container; the backend endpoint that
  jQuery posts to is publicly accessible and returns the same HTML
  fragment the browser would render into #estatesResults.
- Detail: GET /en-US/property/{numeric_id} — fully server-rendered.

For filter aim=1 (Sale) + area=196 (Chalkidiki top-level) + priceFrom=400k
the agency currently has 11 properties — single-page response, no
pagination needed at this filter. If filter widens (e.g. priceFrom
removed), pagination via 'pageClicked' field in JSON body would activate
(empty body returned 18/page out of 102 total in recon).

URL patterns:
  List POST:  /en-US/search-results  (Content-Type: application/json)
  Detail GET: /en-US/property/{numeric_id}

Card selectors:
  div.listing-item[data-property-id]
  .geodir-category-content_price > span      → '1.300.000 €'
  .title-sin_item:nth-of-type(1) span × 1-2  → 'Village, SubRegion'
  .title-sin_item:nth-of-type(2)             → property type text
  li > i.fa-cube / .fa-bed-front / .fa-bath  → size/bedrooms/bathrooms

Detail page anchors:
  let title = '...'                             → real semantic title (JS)
  let lat = N\nlet lon = N                     → Leaflet coords (JS)
  .geodir-category-location span.color1 × 2    → village + sub-region
  p.par_break                                   → full description prose
  .pxp-single-property-gallery figure a[href]  → full-res gallery URLs
  .details-list li → <b>K</b> + div.textright  → basic chars
  .listing-features li → <b>K:</b> + value     → additional chars
  span[class^='class']                          → energy class A-G

Status filter:
  Card: .infoboxdown span text → must be 'Sale' (else skip)
  Detail: <h1><span>Type, </span><span>for sale</span>... — second span
"""
from __future__ import annotations

import html as html_lib
import json
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional

from loguru import logger
from selectolax.lexbor import LexborHTMLParser

from src.models.schemas import PropertyTemplate
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.scrapers.base import BaseScraper


# =============================================================================
# Constants
# =============================================================================
_BASE = "https://www.remax-metron.gr"
_LIST_ENDPOINT = f"{_BASE}/en-US/search-results"
_LIST_REFERER = f"{_BASE}/en-US/results"
_DEFAULT_MIN_PRICE = 400_000

# Site region/filter codes (extracted via DOM dropdown inspection in recon)
_AIM_FOR_SALE = "1"              # aim=1 (For Sale), aim=2 (For Rent)
_AREA_HALKIDIKI = "196"          # top-level Chalkidiki (encompasses 14 sub-regions)

# Halkidiki coord sanity bounds (lat 39-41, lng 22-25)
_HALKIDIKI_LAT_MIN, _HALKIDIKI_LAT_MAX = 39.0, 41.0
_HALKIDIKI_LNG_MIN, _HALKIDIKI_LNG_MAX = 22.0, 25.0

# Property type → MDM canonical category
_CATEGORY_MAP = {
    "villa": "Villa",
    "detached house": "House",
    "maisonette": "House",
    "residential building": "House",
    "residential complex": "Hotel",  # multi-unit investment (e.g. 15BR/18BA)
    "apartment": "Apartment",
    "studio": "Apartment",
    "parcel": "Land",
    "land": "Land",
    "plot": "Land",
    "hotel": "Hotel",
}


# =============================================================================
# Module-level parsers
# =============================================================================
def _classify_category(prop_type_text: Optional[str]) -> str:
    if not prop_type_text:
        return "House"
    return _CATEGORY_MAP.get(prop_type_text.strip().lower(), "House")


def _parse_price_eu(text: Optional[str]) -> Optional[int]:
    """'1.300.000 €' → 1300000  (EU thousand separator = period)."""
    if not text:
        return None
    clean = re.sub(r"[€\s]", "", text).replace(".", "").replace(",", ".")
    try:
        return int(Decimal(clean)) if clean else None
    except Exception:
        return None


def _parse_int_first(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"\d+", text.replace(",", ""))
    return int(m.group(0)) if m else None


def _parse_float_eu(text: Optional[str]) -> Optional[float]:
    """'159.6 sq.m' → 159.6  /  '8.145,36 €' → 8145.36 (EU format)."""
    if not text:
        return None
    m = re.search(r"\d[\d.,]*", text)
    if not m:
        return None
    val = m.group(0)
    if "," in val and "." in val:
        val = val.replace(".", "").replace(",", ".")
    elif "," in val:
        val = val.replace(",", ".")
    try:
        return float(val)
    except Exception:
        return None


# =============================================================================
# Scraper
# =============================================================================
class RemaxMetronScraper(EnrichmentMixin, BaseScraper):
    source_domain = "remax-metron.gr"

    def __init__(self) -> None:
        super().__init__()
        # BaseScraper.__init__ sets self.source_domain = "" which shadows
        # the class attribute. Restore from class for downstream consumers
        # (daily_sync line 327 reads scraper.source_domain, _parse_card uses
        # it to populate PropertyTemplate.source_domain on seeds).
        self.source_domain = type(self).source_domain

    async def collect_urls(
        self,
        min_price: int = _DEFAULT_MIN_PRICE,
    ) -> List[PropertyTemplate]:
        """
        POST JSON filter to backend search endpoint, parse returned HTML
        fragment. Single POST suffices for the current filter (11 results,
        totalPages=1).
        """
        filter_body = {
            "aim": _AIM_FOR_SALE,
            "area": _AREA_HALKIDIKI,
            "priceFrom": str(min_price),
        }
        logger.info(
            f"[remax-metron] POST {_LIST_ENDPOINT} filter={filter_body}"
        )

        try:
            response = await self.client.post(
                _LIST_ENDPOINT,
                data=json.dumps(filter_body),
                headers={
                    "Content-Type": "application/json",
                    "Origin": _BASE,
                    "Referer": _LIST_REFERER,
                },
            )
        except Exception as exc:
            logger.error(f"[remax-metron] list POST failed: {exc}")
            return []

        if response.status_code != 200:
            logger.error(
                f"[remax-metron] list POST HTTP {response.status_code}"
            )
            return []

        body = response.text
        parsed = LexborHTMLParser(body)

        m = re.search(r"Found <b>(\d+)</b>", body)
        announced = int(m.group(1)) if m else None
        if announced is not None:
            logger.info(
                f"[remax-metron] site announces {announced} total results "
                f"(filter priceFrom={min_price})"
            )

        seeds: List[PropertyTemplate] = []
        skipped_status = 0
        for card in parsed.css("div.listing-item[data-property-id]"):
            seed = self._parse_card(card)
            if seed is None:
                skipped_status += 1
            else:
                seeds.append(seed)

        logger.info(
            f"[remax-metron] collect_urls done: {len(seeds)} seeds, "
            f"skipped {skipped_status} status-filtered (announced={announced})"
        )
        return seeds

    def _parse_card(self, card) -> Optional[PropertyTemplate]:
        prop_id = card.attributes.get("data-property-id")
        if not prop_id:
            return None

        # Card-level status filter
        status_node = card.css_first(".infoboxdown span")
        if status_node:
            status = status_node.text(strip=True).lower()
            if status and status not in ("sale", "for sale"):
                logger.info(
                )
                return None

        detail_url = f"{_BASE}/en-US/property/{prop_id}"

        # Price from card price span
        price = None
        price_node = card.css_first(".geodir-category-content_price > span")
        if price_node:
            price = _parse_price_eu(price_node.text(strip=True))

        # Title h3 items: [0]=location, [1]=property type
        title_items = card.css(".geodir-category-content h3.title-sin_item")
        region_text = ""
        prop_type_text = ""
        if title_items:
            spans = title_items[0].css("span")
            parts = [s.text(strip=True).rstrip(",").strip() for s in spans]
            region_text = ", ".join(p for p in parts if p)
        if len(title_items) >= 2:
            raw = title_items[1].text()
            prop_type_text = re.sub(r"\s+", " ", raw).strip().rstrip("&").strip()

        # Size + bedrooms + bathrooms via icon classes
        size_sqm = None
        bedrooms = None
        bathrooms = None
        for li in card.css(".geodir-category-content-details li"):
            if li.css_first("i.fa-cube"):
                size_sqm = _parse_float_eu(li.text())
            elif li.css_first("i.fa-bed-front"):
                bedrooms = _parse_int_first(li.text())
            elif li.css_first("i.fa-bath"):
                bathrooms = _parse_int_first(li.text())

        category = _classify_category(prop_type_text)

        # First image (thumbnail; detail page will replace with full-res gallery)
        img_node = card.css_first(
            ".carousel-item img[src*='ilist-cdn.e-agents.cloud']"
        )
        thumb_url = img_node.attributes.get("src") if img_node else None
        images = [thumb_url] if thumb_url else []

        # Build seed
        extras = [f"code: {prop_id}"]
        if prop_type_text:
            extras.append(f"property_type_raw: {prop_type_text}")
        if bathrooms is not None:
            extras.append(f"bathrooms: {bathrooms}")

        return PropertyTemplate(
            site_property_id=prop_id,
            source_domain=self.source_domain,
            url=detail_url,
            category=category,
            price=price,
            location_raw=region_text or None,
            size_sqm=size_sqm if category != "Land" else None,
            land_size_sqm=size_sqm if category == "Land" else None,
            bedrooms=bedrooms,
            images=images,
            extras=extras,
        )

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Fetch detail page and parse all fields.
        Returns dict with keys matching the EnrichmentMixin contract.
        """
        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.warning(f"[remax-metron] detail fetch failed {url}: {exc}")
            return {}

        if response.status_code != 200:
            logger.warning(
                f"[remax-metron] detail HTTP {response.status_code} for {url}"
            )
            return {}

        html_text = response.text
        parsed = LexborHTMLParser(html_text)
        result: Dict[str, Any] = {}
        extras: List[str] = []

        # Defense-in-depth status (h1 second span)
        h1_node = parsed.css_first("h1")
        if h1_node:
            spans = h1_node.css("span")
            if len(spans) >= 2:
                status = spans[1].text(strip=True).lower()
                if status and "sale" not in status:
                    logger.warning(
                        f"(should have been filtered at card level)"
                    )
                    extras.append(f"_detail_status: {status}")

        # Real semantic title from JS literal (richer than h1)
        title_match = re.search(
            r'let\s+title\s*=\s*"((?:[^"\\]|\\.)*)"', html_text
        )
        if title_match:
            extras.append(f"title: {title_match.group(1).strip()}")

        # Price from data-propertyprise attribute (cleanest — numeric integer)
        price_node = parsed.css_first("[data-propertyprise]")
        if price_node:
            raw = price_node.attributes.get("data-propertyprise")
            if raw:
                try:
                    result["price"] = int(raw)
                except (ValueError, TypeError):
                    pass

        # Description
        desc_node = parsed.css_first("p.par_break")
        if desc_node:
            desc = html_lib.unescape(desc_node.text()).strip()
            if desc:
                result["description"] = desc

        # Coordinates from JS literals with Halkidiki bounds sanity
        coords_match = re.search(
            r"let\s+lat\s*=\s*([-\d.]+)\s+let\s+lon\s*=\s*([-\d.]+)",
            html_text,
            re.DOTALL,
        )
        if coords_match:
            try:
                lat = float(coords_match.group(1))
                lng = float(coords_match.group(2))
                if (_HALKIDIKI_LAT_MIN <= lat <= _HALKIDIKI_LAT_MAX
                        and _HALKIDIKI_LNG_MIN <= lng <= _HALKIDIKI_LNG_MAX):
                    result["latitude"] = lat
                    result["longitude"] = lng
                else:
                    logger.info(
                        f"[remax-metron] coords ({lat:.4f}, {lng:.4f}) "
                        f"outside Halkidiki bounds for {url}"
                    )
            except (ValueError, TypeError):
                pass

        # Region refinement (village + sub-region from header)
        loc_spans = parsed.css(".geodir-category-location span.color1")
        if loc_spans:
            parts = [s.text(strip=True).rstrip(",").strip() for s in loc_spans]
            joined = ", ".join(p for p in parts if p)
            if joined:
                result["area"] = joined

        # Gallery (full-res from figure[href], not thumb URLs in background-image)
        gallery_links = parsed.css(
            ".pxp-single-property-gallery figure a[href*='ilist-cdn.e-agents.cloud']"
        )
        images = []
        for a in gallery_links:
            href = a.attributes.get("href")
            if href and href not in images:
                images.append(href)
        if images:
            result["images"] = images

        # Basic Characteristics (.details-list)
        detail_category = None
        for li in parsed.css(".details-list li"):
            b_node = li.css_first("b")
            div_node = li.css_first("div.textright")
            if not b_node or not div_node:
                continue
            key = b_node.text(strip=True)
            value_full = div_node.text(strip=True)
            value = value_full.replace(key, "", 1).strip()
            if not value:
                continue
            kl = key.lower()

            if kl == "property type":
                detail_category = _classify_category(value)
                extras.append(f"property_type_raw: {value}")
            elif kl == "bedrooms":
                v = _parse_int_first(value)
                if v is not None:
                    result["bedrooms"] = v
            elif kl == "bathrooms":
                v = _parse_int_first(value)
                if v is not None:
                    extras.append(f"bathrooms: {v}")
            elif kl == "area":
                v = _parse_float_eu(value)
                if v:
                    if detail_category == "Land":
                        result["land_size_sqm"] = v
                    else:
                        result["size_sqm"] = v
            elif kl == "code":
                extras.append(f"code: {value}")
            elif kl == "year built":
                yr = _parse_int_first(value)
                if yr and 1800 <= yr <= 2030:
                    result["year_built"] = yr
            elif kl == "floor":
                extras.append(f"floor: {value}")
            elif kl == "parking":
                p_int = _parse_int_first(value)
                if p_int is not None:
                    extras.append(f"parking_spaces: {p_int}")
            elif kl == "energy class":
                m = re.search(r"\b([A-G][+\-]?)\b", value)
                if m:
                    extras.append(f"energy_class: {m.group(1)}")
            elif kl == "category":
                extras.append(f"site_category: {value}")
            elif kl == "distance from":
                # Multi-line: 'Seaside: 0 Meters / Airport: 104 Kilometers / City: 16 Kilometers'
                # NB: must use `value` (without "Distance from" prefix) — otherwise
                # the [A-Za-z]+ greedy-matches "fromSeaside" as one label.
                for label, num, unit in re.findall(
                    r"([A-Za-z]+)\s*:\s*(\d+)\s*(Meters|Kilometers)",
                    value,
                ):
                    factor = 1 if unit.lower() == "meters" else 1000
                    extras.append(
                        f"distance_to_{label.lower()}_m: {int(num) * factor}"
                    )
            elif kl == "price per sq.m":
                pps = _parse_float_eu(value)
                if pps:
                    extras.append(f"price_per_sqm: {pps}")

        # Override category if detail page has more specific property type
        if detail_category:
            result["category"] = detail_category

        # Additional Characteristics (.listing-features)
        for li in parsed.css(".listing-features li"):
            b_node = li.css_first("b")
            if not b_node:
                continue
            key = b_node.text(strip=True).rstrip(":").strip()
            full = li.text(strip=True)
            value = full.replace(b_node.text(strip=True), "", 1).lstrip(":").strip()
            if not key or not value:
                continue
            key_snake = (
                key.lower()
                .replace(" ", "_")
                .replace("/", "_")
                .replace(".", "")
                .replace("(", "")
                .replace(")", "")
                .replace(":", "")
            )
            extras.append(f"{key_snake}: {value}")

        # Exclusive flag
        if parsed.css_first(".ribbonstr"):
            extras.append("exclusive: true")

        if extras:
            result["extras"] = extras

        return result
