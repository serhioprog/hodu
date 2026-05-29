"""
~/hodu/src/scrapers/dimitris_realestate.py

dimitrisrealestate.com scraper — Stage 0 (curl_cffi). Custom CMS by
realstatus.gr / iarts.gr ("Broker Program Real Status").

Agent
-----
Dimitris Chatzis Real Estate, Pefkochori (Kassandra, Halkidiki), since 1997.
NEW agent — no overlap with existing sources (Petia Sheitanova / others).

URL patterns
------------
List:    /{slug}/timi_apo-{min_price}?page={N}
         slugs:
           poliseis-katoikies              → residential (villa/maisonette/...)
           poliseis-gi                     → land
           poliseis-epaggelmatikoi_xoroi   → commercial (hotel/store/...)
           poliseis-loipes_katigories      → other (often empty)
         filter "timi_apo-400000" = price ≥ 400k
         Pagination: assume `?page=N` (Laravel default). If breaks, try /sel-N.
         12 cards/page.
Detail:  /property/{numeric_id}    e.g. /property/1312
         id = ascending. Agent display code = "1" + URL_id (e.g. "11312").
         "1" prefix likely = sale-flag in their internal coding.

Card selectors (list page)
--------------------------
.property-item                                   → card root
  a.prop-link[href]                              → /property/{id}
  .favorite-add[data-id]                         → numeric id (alt source)
  .card-body h2                                  → title
  .card-body > span (with i.la-map-marker-alt)   → location_raw
  .listing-icons li i.la-bed + span              → "4 beds"
  .listing-icons li i.la-bath + span             → "2 bathrooms"
  .listing-icons li i.la-parking + span          → "4 spots"
  .listing-icons li i.la-object-group + span     → "110 sq.m"
  .price-p .fw-bold:not(.oldprice)               → "2.450.000 €" current
  .price-p .proprice                             → discounted price (alt)
  .price-p .oldprice                             → original (when discounted)
  .prop-id span                                  → "Code 11312" (agent code)
  img.listing-img[src]                           → main thumbnail

Detail selectors
----------------
.property-title h1                       → title (for category refinement)
.property-title .property-price span     → price (sanity check)
.property-details                        → description (concat <p>, trim boilerplate)
.property-inform .information-list li    → label-value rows
.feautures .property-feautures li        → additional features (typo "feautures"!)
.energy-section .energy.active           → energy class (e.g. "A")
<script>: var lat=N; var long=N;         → coordinates (regex)
.gallery .item-img a.card-img[href]      → image URLs (count ≈ 23)

Number format
-------------
European (dot = thousands). E.g. "2.450.000 €" = 2_450_000. Sizes mostly
<1000 sqm without separator, but large lands may be "1.800 sq.m".

Coordinates
-----------
Per-property in inline <script>. Site uses icon=1 (40m circle, privacy-blur)
but circle center IS the property location.
Bbox: (39.0..41.5, 22.0..25.0) for Halkidiki.
HQ trap NOT enforced initially (no precise HQ coords known) — but tests
should verify Stage 3 properties have DIFFERENT coords (else add HQ reject).

Category detection
------------------
1) Title-based scan (Villa/Maisonette/Apartment/Penthouse/House/Hotel/etc).
2) URL slug fallback (poliseis-gi → Land, etc).
Title wins over URL hint.
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

_BASE_URL = "https://www.dimitrisrealestate.com"
_SOURCE_DOMAIN = "dimitrisrealestate.com"

# (URL slug, default category fallback if title detection fails)
_CATEGORIES: Tuple[Tuple[str, str], ...] = (
    ("poliseis-katoikies",            "House"),
    ("poliseis-gi",                   "Land"),
    ("poliseis-epaggelmatikoi_xoroi", "Commercial"),
    ("poliseis-loipes_katigories",    "Other"),
)

_PAGES_PER_CATEGORY_LIMIT = 12
_INTER_PAGE_SLEEP_SEC = 1.5

_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# Title prefix lowercase → canonical hodu category. Longest first via sort.
_CATEGORY_MAP: Dict[str, str] = {
    "villa":              "Villa",
    "detached house":     "House",
    "detached":           "House",
    "house":              "House",
    "maisonette":         "Maisonette",
    "apartment complex":  "Apartment",
    "apartment":          "Apartment",
    "studio":             "Apartment",
    "loft":               "Apartment",
    "penthouse":          "Apartment",
    "bungalow":           "House",
    "building":           "Building",
    "farm":               "Land",
    "office":             "Commercial",
    "store":              "Commercial",
    "shop":               "Commercial",
    "warehouse":          "Commercial",
    "industrial":         "Commercial",
    "craft":              "Commercial",
    "hotel":              "Hotel/Commercial",
    "showroom":           "Commercial",
    "business":           "Commercial",
    "hall":               "Commercial",
    "land plot":          "Land",
    "parcel":             "Land",
    "plot":               "Land",
    "island":             "Land",
    "agricultural":       "Land",
}

# Coordinates extraction from inline <script>
_COORDS_RE = re.compile(
    r"var\s+lat\s*=\s*(-?\d+\.?\d*)\s*;\s*var\s+long\s*=\s*(-?\d+\.?\d*)\s*;",
    re.IGNORECASE,
)


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
    """First plain integer (no separators)."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def _to_int_euro(text: str) -> Optional[int]:
    """
    Parse integer in European or US thousands format.
      '2.450.000 €' → 2450000   (European: dot = thousands)
      '1,500'        → 1500     (US:       comma = thousands)
      '450'          → 450      (plain)
      '1.800 sq.m'   → 1800     (European thousands)
      '1.5 km'       → 1        (US decimal, take int part)

    Heuristic for ambiguous '1.NNN':
      - If exactly 3 digits AFTER each dot → thousands group
      - Otherwise (1-2 digits after dot) → decimal
    """
    if not text:
        return None
    m = re.search(r"-?\d[\d,\.]*", text.replace("\xa0", " "))
    if not m:
        return None
    s = m.group(0).strip()

    if "," in s and "." not in s:
        return int(s.replace(",", ""))
    if "." in s and "," not in s:
        parts = s.split(".")
        # Heuristic: all groups after first must be exactly 3 digits → thousands
        if len(parts) > 1 and all(len(p) == 3 and p.isdigit() for p in parts[1:]):
            try:
                return int(s.replace(".", ""))
            except ValueError:
                return None
        # Otherwise decimal
        try:
            return int(float(s))
        except ValueError:
            return None
    if "." in s and "," in s:
        # Mixed: assume US format with both: 1,234.56 → 1234
        try:
            return int(float(s.replace(",", "")))
        except ValueError:
            return None
    try:
        return int(s)
    except ValueError:
        return None


def _to_sqm(text: str) -> Optional[float]:
    """Parse sqm value (uses European thousands)."""
    n = _to_int_euro(text)
    return float(n) if n is not None else None


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _id_from_url(url: str) -> Optional[str]:
    """'/property/1312' or 'https://.../property/1312' → '1312'."""
    if not url:
        return None
    m = re.search(r"/property/(\d+)", url)
    return m.group(1) if m else None


def _category_from_title(title: str) -> Optional[str]:
    t = _normalize_text(title).lower()
    if not t:
        return None
    for key in sorted(_CATEGORY_MAP, key=len, reverse=True):
        if key in t:
            return _CATEGORY_MAP[key]
    return None


# =============================================================================
# Scraper
# =============================================================================

class DimitrisRealestateScraper(EnrichmentMixin, BaseScraper):
    """dimitrisrealestate.com — Pefkochori real estate, 4-category iteration."""

    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )

    _NLP_TO_STRUCTURAL: Dict[str, set] = {
        "swimming_pool":    {"pool", "swimming_pool", "pisina"},
        "sea_view":         {"sea_view"},          # set explicitly when value="Sea"
        "parking":          {"parking", "private_parking", "garage"},
        "air_conditioning": {"a_c", "air_conditioning", "air_conditioner", "ac"},
        "fireplace":        {"fireplace", "tzaki"},
        "balcony":          {"balcony", "balconies"},
        "garden":           {"garden", "private_garden"},
        "storage_room":     {"warehouse", "storage", "apothiki"},
        "elevator":         {"elevator", "lift"},
        "furnished":        {"furnished"},
        "alarm":            {"alarm"},
        "solar_heater":     {"solar"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builder ───────────────────────────────────────────────────────
    def _construct_search_url(
            self, *, slug: str, page: int, min_price: int,
        ) -> str:
            # Pagination = slug "/page-N" (decoded from site's listings.js pushState).
            # ?language=en forces English content (default is Greek; verified working).
            url = f"{_BASE_URL}/{slug}/timi_apo-{min_price}"
            if page > 1:
                url += f"/page-{page}"
            url += "?language=en"
            return url

    # ── Phase 1: collect_urls ─────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _PAGES_PER_CATEGORY_LIMIT,
    ) -> List[PropertyTemplate]:
        """Walk all 4 categories, paginated. Stage 0 (curl_cffi)."""
        seeds: Dict[str, PropertyTemplate] = {}

        for slug, default_category in _CATEGORIES:
            logger.info(f"[{self.source_domain}] === category: {slug} ===")

            for page_num in range(1, max_pages + 1):
                url = self._construct_search_url(
                    slug=slug, page=page_num, min_price=min_price,
                )
                logger.info(
                    f"[{self.source_domain}] {slug} page {page_num}: GET {url}"
                )

                try:
                    response = await self.client.get(url)
                except Exception as exc:
                    logger.warning(
                        f"[{self.source_domain}] fetch failed: {exc!r}"
                    )
                    break

                if response.status_code != 200:
                    logger.warning(
                        f"[{self.source_domain}] HTTP {response.status_code}"
                    )
                    break

                parser = LexborHTMLParser(response.text)
                # NB: detail page also has .property-item ("similar properties")
                # but on list pages those don't appear. Be defensive anyway.
                cards = [
                    c for c in parser.css(".property-item")
                    if "similar-prop" not in (c.attributes.get("class") or "")
                ]
                if not cards:
                    logger.info(
                        f"[{self.source_domain}] {slug} page {page_num}: "
                        f"0 cards — end of category"
                    )
                    break

                page_added = 0
                for card in cards:
                    try:
                        seed = self._parse_card(
                            card, default_category=default_category,
                        )
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
                    f"[{self.source_domain}] {slug} page {page_num}: "
                    f"{len(cards)} cards (+{page_added} new, total {len(seeds)})"
                )

                # Termination: all duplicates (pagination loop) or empty
                if page_added == 0 and page_num > 1:
                    logger.info(
                        f"[{self.source_domain}] {slug} page {page_num}: "
                        f"all duplicates — pagination exhausted"
                    )
                    break

                await asyncio.sleep(
                    _INTER_PAGE_SLEEP_SEC + random.uniform(0.3, 0.8)
                )

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} unique seeds"
        )
        return list(seeds.values())

    def _parse_card(
        self, card: LexborNode, *, default_category: str,
    ) -> Optional[PropertyTemplate]:
        link = card.css_first("a.prop-link[href]")
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

        h2 = card.css_first("h2")
        title = _normalize_text(h2.text(strip=False)) if h2 else ""

        # Category: title-based first, then URL hint fallback
        cat = _category_from_title(title) or default_category

        # Price — prefer .proprice (discounted current), else fw-bold (not oldprice)
        price_int = None
        proprice = card.css_first(".price-p .proprice")
        if proprice:
            price_int = _to_int_euro(_normalize_text(proprice.text(strip=False)))
        else:
            for span in card.css(".price-p .fw-bold"):
                cls = span.attributes.get("class") or ""
                if "oldprice" in cls:
                    continue
                price_int = _to_int_euro(_normalize_text(span.text(strip=False)))
                if price_int is not None:
                    break

        # Listing icons (bed/bath/parking/object-group)
        bedrooms = None
        bathrooms = None
        size_sqm = None
        parking_spots = None
        for li in card.css(".listing-icons li"):
            icon = li.css_first("i")
            cls = (icon.attributes.get("class") or "") if icon else ""
            txt = _normalize_text(li.text(strip=False))
            if "la-bed" in cls and bedrooms is None:
                bedrooms = _to_int_simple(txt)
            elif "la-bath" in cls and bathrooms is None:
                bathrooms = _to_int_simple(txt)
            elif "la-parking" in cls and parking_spots is None:
                parking_spots = _to_int_simple(txt)
            elif "la-object-group" in cls and size_sqm is None:
                size_sqm = _to_sqm(txt)

        # Location: first <span> in .card-body with map-marker icon
        location_raw = None
        body = card.css_first(".card-body")
        if body:
            for span in body.css("span"):
                if span.css_first("i.la-map-marker-alt"):
                    txt = _normalize_text(span.text(strip=False))
                    if txt:
                        location_raw = txt
                    break

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
        # Force English content
        fetch_url = url + ("&" if "?" in url else "?") + "language=en"
        try:
            response = await self.client.get(fetch_url)
        except Exception as exc:
            logger.warning(
                f"[{self.source_domain}] fetch_details fetch failed: {exc!r}"
            )
            return {}

        if response.status_code != 200:
            logger.warning(
                f"[{self.source_domain}] HTTP {response.status_code} for {url}"
            )
            return {}

        html = response.text
        parser = LexborHTMLParser(html)
        data: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # Step 1: structured blocks
        self._parse_title_block(parser, data, extra)
        self._parse_information_list(parser, data, extra)
        self._parse_additional_features(parser, data, extra)
        self._parse_energy_class(parser, extra)
        self._parse_distance_block(parser, extra)

        # Step 2: description
        description = self._extract_description(parser)
        if description:
            data["description"] = description

        # Step 3: coordinates (inline <script>)
        coords = self._extract_coords(html)
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

        # Land normalization: "Area" on a Land detail page is the PLOT size,
        # not a building size. Replicate to land_size_sqm so MDM matchers
        # can compare plots properly.
        if data.get("category") == "Land":
            sz = data.get("size_sqm")
            if sz and not data.get("land_size_sqm"):
                data["land_size_sqm"] = sz

        # Step 5: NLP fallback
        self._apply_nlp_fallback(data)

        # Step 6: quality gate (log-only)
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )

        return data

    # ── Step 1.a: title block ─────────────────────────────────────────────

    def _parse_title_block(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        h1 = parser.css_first(".property-title h1") or parser.css_first("h1")
        if h1:
            title = _normalize_text(h1.text(strip=False))
            if title:
                cat = _category_from_title(title)
                if cat:
                    data["category"] = cat

        # Agent code from "Code: 11312"
        code_node = parser.css_first(".property-title .prices-section span")
        if code_node:
            txt = _normalize_text(code_node.text(strip=False))
            m = re.search(r"code\s*:?\s*(\d+)", txt, re.IGNORECASE)
            if m:
                extra["agent_code"] = m.group(1)

    # ── Step 1.b: information-list ────────────────────────────────────────

    def _parse_information_list(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        ul = parser.css_first(".property-inform ul.information-list")
        if not ul:
            return
        for li in ul.css("li"):
            span = li.css_first("span")
            if not span:
                continue
            value_text = _normalize_text(span.text(strip=False))
            if not value_text:
                continue
            full = _normalize_text(li.text(strip=False))
            # Label = text BEFORE the value span
            if value_text not in full:
                continue
            label = full[: full.rfind(value_text)].strip()
            label = label.rstrip(":").strip().lower()
            if not label:
                continue
            self._route_information_row(label, value_text, data, extra)

    def _route_information_row(
        self,
        label: str,
        value_text: str,
        data: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        if label == "code":
            if "agent_code" not in extra:
                extra["agent_code"] = value_text
            return
        if label == "rooms":
            # In Greek listings "Rooms" = bedrooms (verified against title desc)
            n = _to_int_simple(value_text)
            if n is not None:
                data["bedrooms"] = n
            return
        if label in ("bedroom", "bedrooms"):
            n = _to_int_simple(value_text)
            if n is not None:
                data["bedrooms"] = n
            return
        if label in ("bathroom", "bathrooms"):
            n = _to_int_simple(value_text)
            if n is not None:
                data["bathrooms"] = n
            return
        if label in ("living room", "living rooms"):
            n = _to_int_simple(value_text)
            if n is not None:
                extra["living_rooms_count"] = n
            return
        if label in ("kitchen", "kitchens"):
            n = _to_int_simple(value_text)
            if n is not None:
                extra["kitchens_count"] = n
            return
        if label == "wc":
            n = _to_int_simple(value_text)
            if n is not None:
                extra["wc_count"] = n
            return
        if label in ("balcony", "balconies"):
            v = _to_sqm(value_text)
            if v is not None:
                extra["balcony_size_sqm"] = v
            return
        if label == "floor":
            extra["floor"] = value_text
            return
        if label == "heating":
            extra["heating_system"] = value_text
            return
        if label == "status":
            for tag in (t.strip() for t in value_text.split(",")):
                if tag:
                    extra[_slug(tag)] = True
            return
        if label == "placement":
            extra["placement"] = value_text
            return
        if label == "year of manufacture":
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
        # Catch-all (skip empty/non-ASCII labels defensively)
        sl = _slug(label)
        if sl:
            extra[sl] = value_text

    # ── Step 1.c: additional features (typo class "feautures") ────────────

    def _parse_additional_features(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        ul = (
            parser.css_first(".feautures ul.property-feautures")
            or parser.css_first(".features ul.property-features")
        )
        if not ul:
            return
        for li in ul.css("li"):
            span = li.css_first("span")
            if not span:
                continue
            text = _normalize_text(span.text(strip=False))
            if not text:
                continue
            self._route_additional_feature(text, data, extra)

    def _route_additional_feature(
        self,
        text: str,
        data: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        if ":" in text:
            label, _, value = text.partition(":")
            label = label.strip().lower()
            value = value.strip()
            if not value:
                return

            # sqm-bearing labels
            if label == "area":
                v = _to_sqm(value)
                if v is not None and "size_sqm" not in data:
                    data["size_sqm"] = v
                return
            if label in ("garden area", "garden size"):
                v = _to_sqm(value)
                if v is not None:
                    extra["garden_size_sqm"] = v
                extra["garden"] = True
                return
            if label == "warehouse":
                v = _to_sqm(value)
                if v is not None:
                    extra["warehouse_size_sqm"] = v
                extra["warehouse"] = True
                return
            if label in ("plot", "plot area", "lot size", "land area", "land size"):
                v = _to_sqm(value)
                if v is not None and "land_size_sqm" not in data:
                    data["land_size_sqm"] = v
                return

            # View — promote "Sea" to structural sea_view flag
            if label == "view":
                if "sea" in value.lower():
                    extra["sea_view"] = True
                extra["view"] = value
                return

            # Generic label: value
            sl = _slug(label)
            if sl:
                extra[sl] = value
            return

        # Boolean flag (no colon)
        sl = _slug(text)
        if sl:
            extra[sl] = True

    # ── Step 1.d: energy class ────────────────────────────────────────────

    def _parse_energy_class(
        self, parser: LexborHTMLParser, extra: Dict[str, Any],
    ) -> None:
        active = parser.css_first(".energy-section .energy.active")
        if active:
            txt = _normalize_text(active.text(strip=False))
            if txt:
                extra["energy_class"] = txt

    # ── Step 1.e: Distance from ───────────────────────────────────────────

    def _parse_distance_block(
        self, parser: LexborHTMLParser, extra: Dict[str, Any],
    ) -> None:
        for ul in parser.css("ul.information-list"):
            for li in ul.css("li"):
                full = _normalize_text(li.text(strip=False))
                if not full:
                    continue
                # "Airport: 90 kilometers." / "City: 5 meters." / "Village: 2 km."
                m = re.match(
                    r"([A-Za-z][A-Za-z\s]*?)\s*:\s*(\d+)\s*(kilometers|meters|km|m)\.?\s*$",
                    full, re.IGNORECASE,
                )
                if not m:
                    continue
                label, value, unit = m.groups()
                v = int(value)
                v_km = float(v) if unit.lower().startswith("k") else v / 1000.0
                extra[f"distance_{_slug(label)}_km"] = v_km

    # ── Step 2: description ───────────────────────────────────────────────

    def _extract_description(self, parser: LexborHTMLParser) -> Optional[str]:
        block = parser.css_first(".property-details")
        if not block:
            return None
        parts: List[str] = []
        for p in block.css("p"):
            t = _normalize_text(p.text(separator=" ", strip=True))
            if t:
                parts.append(t)
        full = " ".join(parts).strip()
        # Trim footer boilerplate: "The estate agency 'Dimitris Real Estate'..."
        m = re.search(r"the\s+estate\s+agency", full, re.IGNORECASE)
        if m:
            full = full[: m.start()].strip()
        # Trim "For more information, contact us." trailing prompt
        full = re.sub(
            r"\s*for\s+more\s+information,\s+contact\s+us\.?\s*$",
            "", full, flags=re.IGNORECASE,
        )
        return full if len(full) >= 50 else None

    # ── Step 3: coordinates ───────────────────────────────────────────────

    def _extract_coords(self, html: str) -> Optional[Tuple[float, float]]:
        m = _COORDS_RE.search(html)
        if not m:
            return None
        try:
            lat = float(m.group(1))
            lng = float(m.group(2))
        except ValueError:
            return None
        if lat == 0.0 or lng == 0.0:
            return None
        return (lat, lng)

    # ── Step 4: images ────────────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        seen: set = set()
        out: List[str] = []
        gallery = parser.css_first(".gallery") or parser.css_first(".hide-imgs")
        if not gallery:
            return out
        for a in gallery.css(".item-img a.card-img[href]"):
            href = (a.attributes.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("/"):
                href = f"{_BASE_URL}{href}"
            if href in seen or href.endswith(".svg"):
                continue
            seen.add(href)
            out.append(href)
        return out
