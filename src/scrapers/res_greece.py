"""
Scraper for res-greece.com (RES Greece — Thessaloniki real estate agency).

Sprint 10 #6. Custom CMS (ASP.NET-style routing /JSONFrontend/...).
Stage 0 sufficient (curl_cffi chrome120 returns 200 OK, no Distil/CF).

Strategy A: paginated walk through /catalog with Halkidiki region filter
(regions[]=572 Kassandra + 592 Sithonia + 612 Athos) and €400k-5M price
filter. ~268 properties across 15 pages (~18 main + ~3 Golden Visa promo
per page).

URL patterns:
  Page 1:  /catalog?regions[]=...&price_from=400.000+€&price_to=5.000.000+€
  Page N:  /catalog/page/N?regions[]=...&price_from=...&price_to=...
  Detail:  /catalog/{site_id}/{slug}    e.g. /catalog/7241/adrian

Status flags on cards (skip):
  .cardCaption.label1082  → "Sold"
  .cardCaption.label1085  → "Out of market"
  .cardCaption.labelVNJ   → "Golden Visa" — INFORMATIONAL only, keep card

Defense-in-depth: detail page also has .projectCaption.label1082/.label1085
which we re-check (race condition: card filter missed it).

Gallery: <img src="/content/Image/gallery/..."> inside .gallery-wrapper.
Selectolax scope by `.gallery-wrapper img[src]` excludes Similar Objects
and Have-a-Look carousels (those use catalog-cover images elsewhere).
"""

from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from loguru import logger
from selectolax.lexbor import LexborHTMLParser

from src.models.schemas import PropertyTemplate
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.scrapers.base import BaseScraper


# =============================================================================
# Constants
# =============================================================================

_BASE = "https://res-greece.com"
_DEFAULT_MIN_PRICE = 400_000
_DEFAULT_MAX_PRICE = 5_000_000

# Halkidiki region IDs on res-greece.com
#   572 = Halkidiki-Kassandra
#   592 = Halkidiki-Sithonia
#   612 = Halkidiki-Athos
_HALKIDIKI_PREFIX = "Halkidiki"  # location_raw whitelist prefix

# URL templates: site expects price as European-formatted string with literal €
# (space=+, €=%E2%82%AC, dots as thousand separator).
_LIST_PATH_P1 = (
    "/catalog?regions%5B%5D=572&regions%5B%5D=592&regions%5B%5D=612"
    "&price_from={pf}&price_to={pt}"
)
_LIST_PATH_PN = (
    "/catalog/page/{n}?regions%5B%5D=572&regions%5B%5D=592&regions%5B%5D=612"
    "&price_from={pf}&price_to={pt}"
)

# Status label classes
_CARD_LABEL_SOLD = "label1082"
_CARD_LABEL_OOM = "label1085"
_CARD_LABEL_VNJ = "labelVNJ"  # Golden Visa — informational only

# Pagination safety cap (expected ~15)
_MAX_PAGES = 30

# NLP fallback fillable columns
_NLP_FILLABLE_COLUMNS = ("category", "year_built", "levels", "bathrooms")

# Category normalization from card "Type:" text
_CATEGORY_RULES = (
    # (substring, normalized) — checked in order, most-specific first
    ("land plot", "Land"),
    ("land",      "Land"),
    ("plot",      "Land"),
    ("villa",     "Villa"),
    ("mezonette", "House"),
    ("maisonette","House"),
    ("house",     "House"),
    ("apartment", "Apartment"),
    ("flat",      "Apartment"),
    ("hotel",     "Hotel"),
    ("commercial","Commercial"),
)

# Word → integer for "Two floored" / "Three storey" patterns
_LEVELS_WORD_MAP = {
    "single": 1, "ground": 1, "one": 1,
    "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
}


# =============================================================================
# Helpers
# =============================================================================

def _format_price_for_url(price: int) -> str:
    """
    int 400000 → URL-encoded "400.000+%E2%82%AC".
    """
    formatted = f"{price:,}".replace(",", ".")
    return f"{formatted}+%E2%82%AC"


def _build_list_url(n: int, min_price: int, max_price: int) -> str:
    """Build list page URL for page n (1-indexed)."""
    pf = _format_price_for_url(min_price)
    pt = _format_price_for_url(max_price)
    path = (
        _LIST_PATH_P1.format(pf=pf, pt=pt)
        if n == 1
        else _LIST_PATH_PN.format(n=n, pf=pf, pt=pt)
    )
    return _BASE + path


# =============================================================================
# Scraper
# =============================================================================

class ResGreeceScraper(EnrichmentMixin, BaseScraper):
    """
    Scraper for res-greece.com (RES Greece agency, Thessaloniki).
    Sprint 10 #6. Stage 0. Strategy A (paginated walk).
    """

    source_domain = "res-greece.com"
    _NLP_FILLABLE_COLUMNS = _NLP_FILLABLE_COLUMNS

    # -------------------------------------------------------------------------
    # collect_urls
    # -------------------------------------------------------------------------

    async def collect_urls(
        self,
        min_price: int = _DEFAULT_MIN_PRICE,
        max_price: int = _DEFAULT_MAX_PRICE,
    ) -> List[PropertyTemplate]:
        """
        Walk the paginated catalog and return PropertyTemplate seeds.
        Skips Sold/OoM cards on card-level filter. Dedupes by site_property_id
        (Golden Visa promo block can duplicate main listings).
        """
        all_seeds: Dict[str, PropertyTemplate] = {}  # site_id → seed
        total_pages_known: Optional[int] = None
        page = 1

        while page <= _MAX_PAGES:
            url = _build_list_url(page, min_price, max_price)
            try:
                response = await self.client.get(url)
            except Exception as exc:
                logger.warning(
                    f"[res-greece] page {page} fetch failed: {exc}"
                )
                break

            if response.status_code != 200:
                logger.warning(
                    f"[res-greece] page {page} HTTP {response.status_code}; "
                    f"stopping pagination"
                )
                break

            parsed = LexborHTMLParser(response.text)

            # Detect last page from pagination (only on first page)
            if page == 1 and total_pages_known is None:
                total_pages_known = self._detect_total_pages(parsed)
                if total_pages_known:
                    logger.info(
                        f"[res-greece] pagination detected: "
                        f"{total_pages_known} pages"
                    )

            # Parse all cards on this page
            page_seeds = self._parse_list_page(parsed)

            # If page returned zero cards AND not first page → stop
            if not page_seeds and page > 1:
                logger.info(
                    f"[res-greece] page {page}: zero cards, stopping"
                )
                break

            # Dedup by site_id (Golden Visa promo cross-page dup safety)
            new_on_page = 0
            for seed in page_seeds:
                sid = seed.site_property_id
                if sid not in all_seeds:
                    all_seeds[sid] = seed
                    new_on_page += 1

            logger.info(
                f"[res-greece] page {page}: {len(page_seeds)} cards parsed, "
                f"{new_on_page} new (running total: {len(all_seeds)})"
            )

            # Termination via pagination
            if total_pages_known and page >= total_pages_known:
                logger.info(
                    f"[res-greece] reached last page {page}/"
                    f"{total_pages_known}"
                )
                break

            page += 1

        logger.info(
            f"[res-greece] collect_urls done: {len(all_seeds)} unique seeds "
            f"across {page} page(s)"
        )
        return list(all_seeds.values())

    def _detect_total_pages(self, parsed: LexborHTMLParser) -> Optional[int]:
        """Parse <ul class="pagination"> for last page number."""
        max_n = 0
        for a in parsed.css("ul.pagination li.page-item a.page-link"):
            href = a.attributes.get("href", "")
            match = re.search(r"/catalog/page/(\d+)", href)
            if match:
                n = int(match.group(1))
                if n > max_n:
                    max_n = n
        return max_n if max_n > 0 else None

    def _parse_list_page(
        self, parsed: LexborHTMLParser
    ) -> List[PropertyTemplate]:
        """
        Parse all property cards inside <section class="searchResults">.
        Includes main listings + Golden Visa promo block (.vng-wrapper).
        Excludes "Have a look" section (which is outside searchResults).
        """
        section = parsed.css_first("section.searchResults")
        if not section:
            logger.warning("[res-greece] section.searchResults not found")
            return []

        seeds: List[PropertyTemplate] = []
        for card_div in section.css("div.col-md-4"):
            try:
                seed = self._parse_card(card_div)
                if seed:
                    seeds.append(seed)
            except Exception as exc:
                logger.warning(
                    f"[res-greece] card parse failed: {exc}"
                )
                continue
        return seeds

    def _parse_card(self, card_div) -> Optional[PropertyTemplate]:
        """Parse a single card. Returns None if should skip."""
        link = card_div.css_first('a[href^="/catalog/"]')
        if not link:
            return None

        href = link.attributes.get("href", "")
        match = re.match(r"^/catalog/(\d+)/", href)
        if not match:
            return None
        site_id = match.group(1)
        url = urljoin(_BASE, href)

        # Status filter — skip Sold + Out of market
        if link.css_first(f".cardCaption.{_CARD_LABEL_SOLD}"):
            logger.debug(
                f"[res-greece] skip site_id={site_id}: Sold"
            )
            return None
        if link.css_first(f".cardCaption.{_CARD_LABEL_OOM}"):
            logger.debug(
                f"[res-greece] skip site_id={site_id}: Out of market"
            )
            return None
        # Golden Visa is informational — keep card
        is_vnj = bool(link.css_first(f".cardCaption.{_CARD_LABEL_VNJ}"))

        # Region path (2 spans inside .cardRegion)
        region_spans = link.css(".cardRegion span.text-nowrap")
        region_main = (
            region_spans[0].text(strip=True) if region_spans else ""
        )
        village = (
            region_spans[1].text(strip=True)
            if len(region_spans) > 1 else ""
        )

        # Halkidiki whitelist
        if not region_main.startswith(_HALKIDIKI_PREFIX):
            logger.debug(
                f"[res-greece] skip site_id={site_id}: non-Halkidiki "
                f"({region_main!r})"
            )
            return None

        # Name
        name_el = link.css_first(".cardName")
        name = name_el.text(strip=True) if name_el else ""

        # Card description: 3 .option lines (Type, Size, Sea)
        type_text = ""
        size_text = ""
        for opt in link.css(".cardDescription .option"):
            txt = opt.text(strip=True)
            if txt.startswith("Type:"):
                type_text = txt[len("Type:"):].strip()
            elif txt.startswith("Size:"):
                size_text = txt[len("Size:"):].strip()
            # Sea distance also extracted at detail level — skip here

        # Price block
        price_block = link.css_first(".cardPrice")
        price = self._parse_price_block(price_block)

        # Size (m²) with sanity filter (some land cards have weird
        # text like "to build 2 floors" — first digit becomes 2, not size)
        size_sqm = self._parse_int_first(size_text)
        if size_sqm is not None and size_sqm < 10:
            logger.debug(
                f"[res-greece] suspect card size {size_sqm} m² for "
                f"site_id={site_id}; ignoring (detail page may have it)"
            )
            size_sqm = None

        # Build seed
        location_raw = (
            f"{region_main} → {village}".strip(" →")
            if village else region_main
        )
        seed = PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=url,
            price=price,
            size_sqm=size_sqm,
            location_raw=location_raw,
        )
        # Category hint from card Type — matches kassandra/halkidiki pattern
        seed.category = self._normalize_category(type_text)

        # Stash Golden Visa flag for detail merge (Pydantic-extra attr)
        if is_vnj:
            try:
                seed._is_golden_visa = True
            except Exception:
                pass

        return seed

    def _parse_price_block(self, price_block) -> Optional[int]:
        """
        Parse .cardPrice block:
          - .priceNew (discount overrides .priceOld <strike>)
          - .priceRegular: single ("2.700.000 €") or range
            ("800.000 – 1.300.000 €") → take low bound
          - .priceRequest: "Price by request" → None
        """
        if not price_block:
            return None
        new_el = price_block.css_first(".priceNew")
        if new_el:
            return self._to_int_euro_safe(new_el.text())
        reg_el = price_block.css_first(".priceRegular")
        if reg_el:
            text = reg_el.text()
            match = re.search(r"([\d.]+)", text.replace(" ", ""))
            if match:
                return self._to_int_euro_safe(match.group(1))
        # priceRequest or empty
        return None

    def _parse_int_first(self, text: str) -> Optional[int]:
        """Extract first integer from text. Used for size '385 m²' etc."""
        if not text:
            return None
        match = re.search(r"(\d+)", text)
        return int(match.group(1)) if match else None

    def _normalize_category(self, type_text: str) -> str:
        """Card 'Type:' text → hodu category. Empty string if unknown."""
        if not type_text:
            return ""
        t = type_text.lower().strip()
        for needle, normalized in _CATEGORY_RULES:
            if needle in t:
                return normalized
        return ""

    # -------------------------------------------------------------------------
    # fetch_details
    # -------------------------------------------------------------------------

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Fetch detail page and parse all fields.
        Returns dict with keys matching the EnrichmentMixin contract.
        """
        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.warning(
                f"[res-greece] detail fetch failed {url}: {exc}"
            )
            return {}
        if response.status_code != 200:
            logger.warning(
                f"[res-greece] detail HTTP {response.status_code} for {url}"
            )
            return {}

        html_text = response.text
        parsed = LexborHTMLParser(html_text)

        result: Dict[str, Any] = {}
        extras: List[str] = []

        # Status check (defense in depth — card filter should have caught)
        detail_status = self._detect_detail_status(parsed)
        if detail_status:
            logger.warning(
                f"[res-greece] DETAIL STATUS = {detail_status} for {url} "
                f"(should have been filtered at card level)"
            )
            extras.append(f"_detail_status: {detail_status}")

        # Region / area
        _region_main, village = self._extract_region_path(parsed)
        if village:
            result["area"] = village

        # Description
        description = self._extract_description(parsed)
        if description:
            result["description"] = description

        # Coordinates from Mapbox
        lat, lng = self._extract_coords(html_text)
        if lat is not None:
            result["latitude"] = lat
        if lng is not None:
            result["longitude"] = lng

        # Gallery images
        images = self._extract_gallery(parsed)
        if images:
            result["images"] = images

        # Sidebar projectOptions
        options = self._parse_sidebar_options(parsed)

        # Size (sidebar overrides card)
        sidebar_size = self._parse_int_first(options.get("Size", ""))
        if sidebar_size:
            result["size_sqm"] = sidebar_size

        # Bedrooms
        bedrooms = self._parse_int_first(options.get("Bedrooms", ""))
        if bedrooms is not None:
            result["bedrooms"] = bedrooms

        # Land size from "Private land: Yes (4200m2)" pattern
        land_text = options.get("Private land", "")
        land_match = re.search(r"\((\d+)\s*m", land_text)
        if land_match:
            result["land_size_sqm"] = float(land_match.group(1))

        # Year built from description "Built in 2012, renovated 2018"
        if description:
            year_match = re.search(
                r"[Bb]uilt\s+in\s+(19\d{2}|20\d{2})", description
            )
            if year_match:
                result["year_built"] = int(year_match.group(1))

        # Levels from "Object type" ("Two floored plus semibasement")
        levels = self._parse_levels(options.get("Object type", ""))
        if levels:
            result["levels"] = levels

        # Price from sidebar (overrides card if found)
        sidebar_price_block = parsed.css_first(
            ".projectOptions .optionValue.cardPrice"
        )
        if not sidebar_price_block:
            sidebar_price_block = parsed.css_first(".projectOptions .cardPrice")
        sidebar_price = self._parse_price_block(sidebar_price_block)
        if sidebar_price:
            result["price"] = sidebar_price

        # extra_features — amalgamate sidebar data + flags
        if options.get("Object"):
            extras.append(f"Tagline: {options['Object']}")
        if options.get("Object type"):
            extras.append(f"Architecture: {options['Object type']}")
        if options.get("Property type"):
            extras.append(f"Condition: {options['Property type']}")
        if options.get("Sea"):
            extras.append(f"Sea distance: {options['Sea']}")
        if options.get("Sea view"):
            extras.append(f"Sea view: {options['Sea view']}")
        if options.get("Infrastructure"):
            extras.append(
                f"Infrastructure distance: {options['Infrastructure']}"
            )
        if options.get("Airport"):
            extras.append(f"Airport distance: {options['Airport']}")
        if options.get("Options"):
            for amenity in options["Options"].split(","):
                amenity = amenity.strip()
                if amenity:
                    extras.append(f"Amenity: {amenity}")
        # Energy class certificate (Latin OR Greek E variant on this site)
        for key in (
            "Energy class certificate",
            "Εnergy class certificate",  # Greek capital Epsilon Ε
        ):
            if options.get(key):
                extras.append(f"Energy class: {options[key]}")
                break

        if extras:
            result["extra_features"] = extras

        return result

    # -------------------------------------------------------------------------
    # Detail page helpers
    # -------------------------------------------------------------------------

    def _detect_detail_status(self, parsed) -> Optional[str]:
        """Return 'sold' / 'out_of_market' / None from detail caption."""
        if parsed.css_first(f".projectCaption.{_CARD_LABEL_SOLD}"):
            return "sold"
        if parsed.css_first(f".projectCaption.{_CARD_LABEL_OOM}"):
            return "out_of_market"
        return None

    def _extract_region_path(self, parsed) -> Tuple[str, str]:
        """Extract (region_main, village) from .projectRegion anchors."""
        anchors = parsed.css(".projectRegion a")
        region_main = anchors[0].text(strip=True) if anchors else ""
        village = anchors[1].text(strip=True) if len(anchors) > 1 else ""
        return region_main, village

    def _extract_description(self, parsed) -> str:
        """Parse <article class="textBlock"> preserving paragraph breaks."""
        article = parsed.css_first("article.textBlock")
        if not article:
            return ""
        text = article.text(deep=True, separator="\n", strip=True)
        text = html_lib.unescape(text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _extract_coords(
        self, html_text: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract (lat, lng) from Mapbox script.
        Pattern: center: [lng, lat] (Mapbox order). Whitespace + newlines
        may appear inside brackets — use DOTALL.
        Sanity: Halkidiki ~ lat 39-41, lng 22-25.
        """
        # Primary: center: [lng, lat]
        match = re.search(
            r"center:\s*\[\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\]",
            html_text,
            re.DOTALL,
        )
        if match:
            try:
                lng = float(match.group(1))
                lat = float(match.group(2))
                if 35 < lat < 45 and 19 < lng < 30:
                    return lat, lng
            except ValueError:
                pass
        # Fallback: setLngLat
        match = re.search(
            r"setLngLat\(\s*\[\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\]",
            html_text,
            re.DOTALL,
        )
        if match:
            try:
                lng = float(match.group(1))
                lat = float(match.group(2))
                if 35 < lat < 45 and 19 < lng < 30:
                    return lat, lng
            except ValueError:
                pass
        return None, None

    def _extract_gallery(self, parsed) -> List[str]:
        """
        Extract all <img src> inside .gallery-wrapper.
        Filter: paths must start with /content/Image/gallery/.
        Dedup by URL. Return absolute URLs.
        """
        images: List[str] = []
        seen: set = set()
        wrapper = parsed.css_first(".gallery-wrapper")
        if not wrapper:
            return images
        for img in wrapper.css("img"):
            src = img.attributes.get("src", "").strip()
            if not src.startswith("/content/Image/gallery/"):
                continue
            if src in seen:
                continue
            seen.add(src)
            images.append(urljoin(_BASE, src))
        return images

    def _parse_sidebar_options(self, parsed) -> Dict[str, str]:
        """
        Parse .projectOptions .optionItem → {name: value}.
        First value wins per key (skip duplicates).
        """
        options: Dict[str, str] = {}
        sidebar = parsed.css_first(".projectOptions")
        if not sidebar:
            return options
        for item in sidebar.css(".optionItem"):
            name_el = item.css_first(".optionName")
            val_el = item.css_first(".optionValue")
            if not (name_el and val_el):
                continue
            name = name_el.text(strip=True)
            value = val_el.text(strip=True)
            if name and name not in options:
                options[name] = value
        return options

    def _parse_levels(self, object_type: str) -> Optional[int]:
        """
        Extract levels from descriptive text:
          "Two floored plus semibasement" → 2
          "Single floor"                  → 1
          "2-storey building"             → 2
        """
        if not object_type:
            return None
        t = object_type.lower()
        for word, n in _LEVELS_WORD_MAP.items():
            if word in t:
                return n
        match = re.search(r"(\d+)\s*(?:floor|storey|stor)", t)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        return None
