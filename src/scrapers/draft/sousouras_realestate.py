"""
Sousouras Real Estate scraper — Spitogatos template5 (agent ID 12379).

This is the FIRST template5 scraper. The same Bootstrap-based HTML layout
powers HUNDREDS of agent sites on the spitogatos.gr platform. Future
template5 scrapers should clone this file and change ONLY:
    self.source_domain  + self.BASE_URL_LIST

Architecture:
  * Stage 0 (curl_cffi) is sufficient — no JS rendering needed, no
    Cloudflare WAF, just Laravel + CloudFront CDN.
  * GET-only. List pages paginate via &page=N. No AJAX.
  * Detail pages are single-shot — Gallery/Details/Map/Contact tabs are
    all in one HTML response; JS only does client-side show/hide.

Filter strategy:
  * URL: ?listingType=sale&category=residential&region=196&priceLow=400000
    where region=196 = Chalkidiki prefecture.
  * Pagination: 9 cards per page. At min_price=400000 we get ~44 listings
    across 5 pages.
  * Halkidiki whitelist is applied in daily_sync._run_scrapers using
    location_raw + url. We guarantee a whitelist match by appending
    "Halkidiki" to location_raw (all listings ARE Halkidiki by the
    region=196 URL param anyway).

Field extraction philosophy (matches GLRealEstateScraper):
  * Structural panel = source of truth: <table.info-table> rows
  * Amenities list = additional features: <ul.property-amenities>
  * Description = <p> children of .property-content (after <h1>)
  * GPS coordinates = <div.marker[data-lat][data-lng][data-type]>
    (gps_type="exact" vs "offset" saved to extra_features for cluster matching)
  * Photos = <div.swiper-slide> img src/data-src, prefer 1600x1200 size

Key vocabulary mapping decision:
  * Their "Rooms" (info table) = our "bedrooms" (column).
    Confirmed by cross-referencing list page "5 Bedrooms" with detail
    page info-table "Rooms: 5" for the same property.
  * Their "Type" (info table) = "Holiday home, Investment, ..." marketing
    tags, NOT category. Stored in extra_features.type.
  * Category (Villa/Maisonette/Detached House/...) is parsed from list
    page <h3> first word — always reliable. Detail page <h1> sometimes
    follows the same pattern but sometimes is marketing copy
    ("150 m from a unique beach") — collect_urls' value wins via the
    None-filter at the end of fetch_details.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.scrapers.base import BaseScraper
from src.models.schemas import PropertyTemplate


# =============================================================
# Mappings — pure data, no logic
# =============================================================

# Map info-table <th> labels (lowercase, stripped) to PropertyTemplate columns.
_INFO_LABEL_TO_COLUMN: Dict[str, str] = {
    "rooms":             "bedrooms",         # SEE module docstring
    "bathrooms":         "bathrooms",
    "construction year": "year_built",
    "neighborhood":      "area",
    "levels":            "levels",
}

# Info-table labels that always go to extra_features (slugged keys).
# Intentionally NOT mapped to first-class columns — too domain-specific.
_INFO_LABELS_TO_EXTRA = {
    "price per m²", "price per m2", "zone", "parking spot",
    "energy class", "kitchens", "living rooms", "wc",
    "status", "type", "extra", "heating system",
}

# Map first word of title to canonical category. Used for both list page
# <h3> ("Villa for sale Kriopigi (Kassandra)") and detail page <h1>.
_CATEGORY_KEYWORDS: Dict[str, str] = {
    "villa":       "Villa",
    "maisonette":  "Maisonette",
    "apartment":   "Apartment",
    "studio":      "Studio",
    "detached":    "Detached House",
    "house":       "Detached House",
    "bungalow":    "Bungalow",
    "loft":        "Loft",
    "building":    "Building",
}


# =============================================================
# Helpers — pure functions, no scraper state
# =============================================================

def _slug(label: str) -> str:
    """Convert a free-form HTML label into a stable extra_features key."""
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_int_euro(text: str) -> Optional[int]:
    """Parse "€ 1,000,000" / "€ 4,762" / "1.500.000€" into integer euros.

    Defensive cap at €200M — concatenated price strings ("1.500.0001.400.000")
    are silently rejected (they'd overflow Property.price INTEGER column).
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
    """First integer in string. "Rooms: 5" → 5, "10" → 10."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """Parse "210 m²" / "1500" / "127 m²" → float."""
    if not text:
        return None
    m = re.search(r"\d+(?:[.,]\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def _extract_id_from_url(url: str) -> Optional[str]:
    """Pull the property id from .../propertyDetails/19853678 → "19853678"."""
    if not url:
        return None
    m = re.search(r"/propertyDetails/(\d+)", url)
    return m.group(1) if m else None


def _normalize_image_url(url: str) -> str:
    """Force HD (1600x1200) variant. Spitogatos images come in
    _200x100 (thumb), _900x675 (list card), _1600x1200 (HD)."""
    return re.sub(r"_\d+x\d+\.jpg", "_1600x1200.jpg", url)


def _extract_category(title: str) -> Optional[str]:
    """
    "Villa for sale Kriopigi (Kassandra)"        → "Villa"
    "Detached House for sale ..."                → "Detached House"
    "150 m from a unique beach"                  → None  (no keyword match)
    """
    if not title:
        return None
    words = title.strip().split()
    if not words:
        return None
    return _CATEGORY_KEYWORDS.get(words[0].lower())


def _extract_location_from_title(title: str) -> Optional[str]:
    """
    Parse area/neighborhood text from list-page <h3> or detail-page <h1>.

    "Villa for sale Kriopigi (Kassandra)"
        → "Kriopigi (Kassandra)"
    "Villa for sale in Polichrono (Chalkidiki) with swimming pool..."
        → "Polichrono (Chalkidiki)"
    "150 m from a unique beach"
        → None (caller falls back to "Halkidiki")
    """
    if not title:
        return None
    m = re.search(
        r"for sale\s+(?:in\s+)?(.+?)"
        r"(?:\s+(?:with|and|featuring|having|near|close|located).+)?$",
        title,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    return None


# =============================================================
# Scraper
# =============================================================

class SousourasRealEstateScraper(BaseScraper):
    """
    Halkidiki listings from sousouras-realestate.gr (Spitogatos agent 12379).

    At min_price=400000: ~44 listings across 5 pages.
    Without price filter, the agent inventory is ~147 residential sale items.
    """

    BASE_URL_LIST = "https://www.sousouras-realestate.gr/en/property/search"
    PAGE_SAFETY_CAP = 30  # real value is ~5 pages; cap protects against pagination bugs

    def __init__(self):
        super().__init__()
        self.source_domain = "sousouras-realestate.gr"

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs from list pages
    # ---------------------------------------------------------------
    async def collect_urls(self, min_price: int = 400000) -> List[PropertyTemplate]:
        """
        Walk pagination ?page=1..N. Stop on <li class="last disabled"> or
        empty card list. Extract seed PropertyTemplate per card.

        location_raw is constructed to GUARANTEE a Halkidiki whitelist
        match downstream. Either parsed neighborhood + " Halkidiki" suffix,
        or just "Halkidiki" fallback. All sousouras listings are Halkidiki
        by region=196 URL param anyway.
        """
        all_properties: List[PropertyTemplate] = []
        page = 1

        while page <= self.PAGE_SAFETY_CAP:
            url = (
                f"{self.BASE_URL_LIST}"
                f"?listingType=sale"
                f"&category=residential"
                f"&region=196"
                f"&priceLow={min_price}"
                f"&page={page}"
            )
            logger.info(f"[{self.source_domain}] Парсинг страницы {page}...")

            try:
                response = await self.client.get(url)
                parser = LexborHTMLParser(response.text)

                cards = parser.css("a.property-item")
                if not cards:
                    logger.info(
                        f"[{self.source_domain}] нет карточек на странице {page} — конец пагинации"
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
                        logger.error(f"[{self.source_domain}] ошибка парсинга карточки: {e}")

                logger.info(
                    f"[{self.source_domain}] страница {page}: собрано {page_count} объектов"
                )

                # Stop condition: pagination's "last" button is disabled →
                # we just processed the last page.
                if parser.css_first("li.last.disabled"):
                    logger.info(
                        f"[{self.source_domain}] последняя страница достигнута ({page})"
                    )
                    break

                await asyncio.sleep(2)
                page += 1

            except Exception as e:
                logger.error(f"[{self.source_domain}] критическая ошибка на странице {page}: {e}")
                break

        logger.info(
            f"[{self.source_domain}] Phase 1 завершён: {len(all_properties)} URLs за {page} стр."
        )
        return all_properties

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """Extract PropertyTemplate seed from one <a class="property-item">."""
        href = card.attributes.get("href")
        if not href:
            return None

        site_id = _extract_id_from_url(href)
        if not site_id:
            logger.warning(f"[{self.source_domain}] не извлёк ID из {href}")
            return None

        # Title — e.g. "Villa for sale Kriopigi (Kassandra)"
        title_node = card.css_first("h3")
        title = title_node.text(strip=True) if title_node else ""

        # location_raw — see method docstring on whitelist guarantee
        parsed_loc = _extract_location_from_title(title)
        location_raw = f"{parsed_loc} Halkidiki" if parsed_loc else "Halkidiki"

        # Price + size from card (validator in PropertyTemplate handles strings)
        price_node = card.css_first("span.price")
        price_text = price_node.text(strip=True) if price_node else None

        area_node = card.css_first("span.area")
        size_sqm = _to_float_sqm(area_node.text(strip=True)) if area_node else None

        # Bedrooms / bathrooms from "<p><b>5 Bedrooms <span>3 Bathrooms</span></b></p>"
        bedrooms = None
        bathrooms = None
        text_block = card.css_first(".property-item-text p")
        if text_block:
            # separator=" " to ensure <span> content isn't glued to "Bedrooms"
            full_text = text_block.text(separator=" ", strip=True)
            m_bed = re.search(r"(\d+)\s+Bedrooms?", full_text, re.IGNORECASE)
            if m_bed:
                bedrooms = int(m_bed.group(1))
            m_bath = re.search(r"(\d+)\s+Bathrooms?", full_text, re.IGNORECASE)
            if m_bath:
                bathrooms = int(m_bath.group(1))

        # Category from h3 — list page is most reliable source
        category = _extract_category(title)

        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=href,
            price=price_text,  # PropertyTemplate.clean_price strips €/spaces
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
        Fetch one property detail page and extract structured data:
          * Info table → most Property columns + several extra_features keys
          * Amenities list → extra_features (booleans + key/value)
          * GPS coordinates from <div.marker[data-lat][data-lng]>
          * Photos from <div.swiper-slide> (1600x1200 size)
          * Description from <p> children of .property-content

        Returns dict with keys matching daily_sync's `base_data.update(details)`
        expectation. CRUCIALLY: None values are filtered out at the end so
        seed values from collect_urls (especially category from <h3>) aren't
        clobbered by detail-page parsing failures.
        """
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)

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

            # ───────────────────────────────────────────────
            # 1. TITLE → category (best-effort; collect_urls is authoritative)
            # ───────────────────────────────────────────────
            h1 = parser.css_first("h1")
            if h1:
                title = h1.text(strip=True)
                if title:
                    data["category"] = _extract_category(title)

            # ───────────────────────────────────────────────
            # 2. PRICE + SIZE + AGENT CODE — header strip
            # ───────────────────────────────────────────────
            price_node = parser.css_first("span.price.color2")
            if price_node:
                data["price"] = _to_int_euro(price_node.text(strip=True))

            area_node = parser.css_first("span.area.color2")
            if area_node:
                data["size_sqm"] = _to_float_sqm(area_node.text(strip=True))

            # Agent's internal code: "Code J848" → "J848". Saved for
            # potential future cross-reference with other spitogatos sources.
            code_node = parser.css_first("span.property-code")
            if code_node:
                code_text = code_node.text(strip=True)
                m = re.search(r"Code\s*(\S+)", code_text)
                if m:
                    data["extra_features"]["agent_code"] = m.group(1)

            # ───────────────────────────────────────────────
            # 3. DESCRIPTION
            # ───────────────────────────────────────────────
            data["description"] = self._parse_description(parser)

            # ───────────────────────────────────────────────
            # 4. INFO TABLE — structured panel (high-confidence)
            # ───────────────────────────────────────────────
            self._parse_info_table(parser, data)

            # ───────────────────────────────────────────────
            # 5. AMENITIES LIST — additional features
            # ───────────────────────────────────────────────
            self._parse_amenities(parser, data)

            # ───────────────────────────────────────────────
            # 6. GPS COORDINATES
            # ───────────────────────────────────────────────
            self._parse_coordinates(parser, data)

            # ───────────────────────────────────────────────
            # 7. PHOTOS — swiper carousel
            # ───────────────────────────────────────────────
            data["images"] = self._collect_image_urls(parser)

            # ───────────────────────────────────────────────
            # 8. FINAL CLEANUP
            # Drop None values so seed from collect_urls isn't clobbered
            # when daily_sync does `base_data.update(details)`.
            # Empty list / dict / string are KEPT (signal "successfully empty").
            # ───────────────────────────────────────────────
            return {k: v for k, v in data.items() if v is not None}

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] ошибка fetch_details для {url}: {e}"
            )
            return {}

    # ---------------------------------------------------------------
    # Phase 2 helpers
    # ---------------------------------------------------------------
    def _parse_description(self, parser: LexborHTMLParser) -> str:
        """
        Description = <p> children of .property-content, joined with blank
        lines. Filters out the AddToAny share-widget paragraphs (they
        contain <a class="a2a_dd"> markup).
        """
        content = parser.css_first(".property-content")
        if not content:
            return ""

        paragraphs: List[str] = []
        for p in content.css("p"):
            # Skip AddToAny share widget paragraphs
            inner_html = (p.html or "").lower()
            if "a2a_dd" in inner_html or "addtoany" in inner_html:
                continue
            txt = p.text(separator=" ", strip=True)
            if not txt or len(txt) < 30:
                continue
            paragraphs.append(txt)

        if paragraphs:
            return "\n\n".join(paragraphs)

        # Fallback to og:description (1-2 sentences) — still better than empty
        og = parser.css_first('meta[property="og:description"]')
        if og:
            content_text = og.attributes.get("content", "") or ""
            return content_text.strip()

        return ""

    def _parse_info_table(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Each row: <tr><th>Label</th><td>Value</td></tr>

        Energy class is special — value is <div class="energy energy-e">E</div>
        but td.text(strip=True) collapses to just "E", so no special handling.
        """
        for row in parser.css("table.info-table tr"):
            th = row.css_first("th")
            td = row.css_first("td")
            if not th or not td:
                continue

            label = th.text(strip=True)
            value = td.text(strip=True)
            if not label or not value:
                continue

            label_lower = label.lower()
            column = _INFO_LABEL_TO_COLUMN.get(label_lower)

            if column == "bedrooms":
                v = _to_int_simple(value)
                if v is not None:
                    data["bedrooms"] = v
            elif column == "bathrooms":
                v = _to_int_simple(value)
                if v is not None:
                    data["bathrooms"] = v
            elif column == "year_built":
                v = _to_int_simple(value)
                if v is not None:
                    data["year_built"] = v
            elif column == "levels":
                # Property.levels is String — store verbatim
                data["levels"] = value
            elif column == "area":
                data["area"] = value
            elif label_lower in _INFO_LABELS_TO_EXTRA:
                key = _slug(label)
                # Type-coerce numeric labels
                if label_lower in {"kitchens", "living rooms", "wc"}:
                    n = _to_int_simple(value)
                    data["extra_features"][f"{key}_count"] = n if n is not None else value
                elif label_lower in {"price per m²", "price per m2"}:
                    n = _to_int_euro(value)
                    data["extra_features"][key] = n if n is not None else value
                else:
                    data["extra_features"][key] = value
            # else: unknown label — silently skip (defensive against
            # spitogatos adding new fields without scraper update)

    def _parse_amenities(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Two <li> shapes in <ul class="property-amenities">:
          1. Boolean:    <li>Airy</li>
          2. Key-value:  <li>Distance from sea (m): <span class="text-red">1500</span></li>

        For key-value form, numeric values are coerced to int (1500 → 1500,
        not "1500"). String values like "East" or "Synthetic" stay as strings.
        """
        for li in parser.css("ul.property-amenities li"):
            value_span = li.css_first("span.text-red")
            full_text = li.text(separator=" ", strip=True)

            if value_span:
                # Key-value form
                value_text = value_span.text(strip=True)
                # Label = full_text with value chunk removed
                label_text = full_text.replace(value_text, "").strip().rstrip(":").strip()
                key = _slug(label_text)
                if not key:
                    continue
                # Try numeric coercion (most values: 1500, 60 m², ...)
                if re.fullmatch(r"[\d,.]+(?:\s*m²?)?", value_text):
                    n = _to_float_sqm(value_text)
                    if n is not None:
                        data["extra_features"][key] = int(n) if n == int(n) else n
                        continue
                # String form (e.g. "East", "Synthetic", "Double Glass")
                data["extra_features"][key] = value_text
            else:
                # Boolean form
                if not full_text:
                    continue
                key = _slug(full_text)
                if key:
                    data["extra_features"][key] = True

    def _parse_coordinates(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        <div class="marker" data-lat="40.028412" data-lng="23.470201" data-type="exact">

        data-type="exact"  → real GPS (preferred for cluster matching)
        data-type="offset" → privacy circle ~200m radius (less reliable)

        Both are saved; gps_type is recorded in extra_features so the
        GeoMatcher / cluster-pair generator can weight them appropriately.
        """
        marker = parser.css_first("div.marker[data-lat]")
        if not marker:
            return

        lat = marker.attributes.get("data-lat")
        lng = marker.attributes.get("data-lng")
        gps_type = marker.attributes.get("data-type", "offset")

        if lat and lng:
            try:
                data["latitude"] = float(lat)
                data["longitude"] = float(lng)
                data["extra_features"]["gps_type"] = gps_type
            except ValueError:
                logger.warning(
                    f"[{self.source_domain}] невалидные координаты: lat={lat}, lng={lng}"
                )

    def _collect_image_urls(self, parser: LexborHTMLParser) -> List[str]:
        """
        Walk <div class="swiper-slide"> skipping duplicate slides (the
        swiper carousel pre-renders first/last twice for infinite-loop UX).

        Two img patterns appear:
          * Active slide: <img src="https://m1.spitogatos.gr/..._1600x1200.jpg">
          * Lazy slides:  <img data-src="https://...">

        URLs are normalised to _1600x1200.jpg for HD download by MediaDownloader.
        """
        photos: List[str] = []
        seen_indices = set()

        for slide in parser.css(".swiper-slide"):
            classes = slide.attributes.get("class", "")
            if "swiper-slide-duplicate" in classes:
                continue

            # Dedupe via data-swiper-slide-index (the swiper assigns same
            # index to original + clone slides; first occurrence wins)
            index = slide.attributes.get("data-swiper-slide-index")
            if index and index in seen_indices:
                continue
            if index:
                seen_indices.add(index)

            img = slide.css_first("img")
            if not img:
                continue

            src = img.attributes.get("src") or img.attributes.get("data-src")
            if not src:
                continue

            # Limit to spitogatos CDN images (skip any tracking pixels etc.)
            if "spitogatos.gr" not in src:
                continue

            src = _normalize_image_url(src)
            if src not in photos:
                photos.append(src)

        return photos