"""
~/hodu/src/scrapers/halkidiki_realtors.py

halkidikirealtors.com scraper — Greek/EN realstatus.gr-powered listings.
Sprint 11 (post-Sprint 10 bug-fix batch).

Anti-bot: clean Stage 0 (curl_cffi chrome120, ~1s, 200 OK).
reCAPTCHA present but only on contact form widget — non-blocking.

Agent: Halkidiki Realtor's, Kassandra Chalkidiki (Konstantinos Kouziokas).
Powered by realstatus.gr broker software.

URL patterns
------------
List:    /listings/for/1/areas/N196/category/{cat}/sortby/dateDesc
            /priceFrom/{min}?language=en
         area=N196 → Halkidiki. cat: 1=Residential, 2=Commercial,
         3=Land, 4=Others (skipped). for=1 → sale.
         With priceFrom 400000: 10+1+1 = 12 properties total across 3 cats.
Detail:  /property/{id}/?language=en   (numeric ID)

Internal display code format: "1-{id}" — visible in .property-inform
Code: span and contact form p_code hidden input.

Pagination
----------
<ul class="pagination"> uses <li data-id="N"> JS-driven nav (no href).
At priceFrom 400000 each category fits one page. If H1 count exceeds
cards parsed → log warning (future-proofing); pagination not implemented.

Content language
----------------
?language=en gives mostly clean English labels but some persist in Greek:
- English: "Code:", "Area:", "Rooms:", "Bathroom:", "Floor:"
- Greek:   "Αριθμός ορόφων κτιρίου", "Τύπος Κτιρίου", "Αριθμός θέσεων πάρκινγκ"
English-labelled rows route to top-level columns; Greek to extras as-is.

Coords: JS globals `var lat = X; var long = Y;` (OSM Leaflet). Bbox-gated.

Images: .row.gallery .item-img a.card-img[href] — includes hidden
display:none entries (full set of photos including +N "more" lightbox).
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

_BASE_URL = "https://www.halkidikirealtors.com"
_SOURCE_DOMAIN = "halkidikirealtors.com"

# Area code N196 = Halkidiki (region "code" with N prefix)
_AREA_CODE = "N196"

# (cat_id, cat_name, default_category). Category 4 "Others" skipped
# (vague + empty for our floor). default_category fills seeds where the
# card title doesn't match any _CATEGORY_MAP word (e.g. title "Unique
# property for sale..." has no recognizable category keyword).
_CATEGORIES: Tuple[Tuple[int, str, str], ...] = (
    (1, "Residential", "House"),       # Residential bucket — House is the
                                       # safest generic for unknown titles
    (2, "Commercial",  "Commercial"),
    (3, "Land",        "Land"),
)

_INTER_PAGE_SLEEP_SEC = 1.5

# Halkidiki bbox for coord sanity
_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# h1/h2 first word → canonical hodu category
_CATEGORY_MAP: Dict[str, str] = {
    "maisonette":   "Maisonette",
    "apartment":    "Apartment",
    "studio":       "Apartment",
    "loft":         "Apartment",
    "penthouse":    "Apartment",
    "flat":         "Apartment",
    "villa":        "Villa",
    "house":        "House",
    "home":         "House",
    "residence":    "House",
    "residency":    "House",
    "cottage":      "House",
    "detached":     "House",
    "bungalow":     "House",
    "land":         "Land",
    "plot":         "Land",
    "parcel":       "Land",
    "field":        "Land",
    "office":       "Commercial",
    "shop":         "Commercial",
    "store":        "Commercial",
    "warehouse":    "Commercial",
    "commercial":   "Commercial",
    "professional": "Commercial",
    "hotel":        "Hotel/Commercial",
    "building":     "Building",
    "complex":      "Building",
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
    """First integer; site uses '.' as thousands separator (Greek format)."""
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
    """
    Parse '85 sq.m', '1.200 sq.m' (Greek '.' thousands), '4.705,88' (',' decimal).
    """
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
    try:
        return float(raw)
    except ValueError:
        return None


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _category_from_title(title: str) -> Optional[str]:
    """
    Scan ALL alphabetic words in title against _CATEGORY_MAP, return first
    match. Site titles sometimes lead with "For sale" or "Newly built" before
    the category word, so first-word-only matching misses those (e.g. card
    h2 "For sale maisonette of 98 sq m. first to the sea.").
    """
    t = _normalize_text(title).lower()
    if not t:
        return None
    for word in re.findall(r"[a-z]+", t):
        cat = _CATEGORY_MAP.get(word)
        if cat:
            return cat
    return None


# ============================================================================
# Scraper
# ============================================================================

class HalkidikiRealtorsScraper(EnrichmentMixin, BaseScraper):
    """halkidikirealtors.com — realstatus.gr powered, paginated walk per category."""

    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
        # category omitted — NLP description analysis picks marketing
        # buzzwords ("hotel potential", "commercial opportunity") over the
        # actual property type. URL category bucket (set in collect_urls)
        # provides the authoritative fallback when h1/h2 title scan misses.
    )

    _NLP_TO_STRUCTURAL: Dict[str, Set[str]] = {
        "swimming_pool":    {"pool", "swimming_pool"},
        "sea_view":         {"view_sea", "sea_view", "view"},
        "parking":          {"parking", "private_parking"},
        "air_conditioning": {"air_conditioning", "ac"},
        "fireplace":        {"fire_place", "fireplace"},
        "balcony":          {"balcony", "balconies"},
        "garden":           {"garden", "private_garden"},
        "storage_room":     {"storage", "storage_room"},
        "elevator":         {"elevator", "lift"},
        "furnished":        {"furnished"},
        "alarm":             {"alarm", "preset_alarm", "security_system"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        # BaseScraper.__init__ writes self.source_domain="" — restore canonical.
        # Same fix pattern as remax_metron, res_greece, casa_properties.
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builders ─────────────────────────────────────────────────

    def _build_list_url(self, category: int, min_price: int) -> str:
        return (
            f"{_BASE_URL}/listings/for/1/areas/{_AREA_CODE}"
            f"/category/{category}/sortby/dateDesc/priceFrom/{min_price}"
            f"?language=en"
        )

    def _build_detail_url(self, site_id: str) -> str:
        return f"{_BASE_URL}/property/{site_id}/?language=en"

    # ── Phase 1: collect_urls ────────────────────────────────────────

    async def collect_urls(
        self, min_price: int = 400_000,
    ) -> List[PropertyTemplate]:
        seeds: Dict[str, PropertyTemplate] = {}

        for cat_id, cat_name, default_cat in _CATEGORIES:
            url = self._build_list_url(cat_id, min_price)
            logger.info(
                f"[{self.source_domain}] === category: {cat_name} (id={cat_id}) ==="
            )
            logger.info(f"[{self.source_domain}] Stage 0 GET {url}")

            try:
                resp = await self.client.get(url)
            except Exception as exc:
                logger.error(
                    f"[{self.source_domain}] {cat_name} fetch failed: {exc!r}"
                )
                continue

            if resp.status_code != 200:
                logger.warning(
                    f"[{self.source_domain}] {cat_name}: HTTP {resp.status_code}"
                )
                continue

            parser = LexborHTMLParser(resp.text)
            cards = [
                c for c in parser.css(".property-item")
                if "similar-prop" not in (c.attributes.get("class") or "")
            ]

            # Pagination future-proofing: warn if results count > cards parsed.
            h1 = parser.css_first("h1")
            if h1:
                m = re.search(r"\d+", h1.text(strip=True))
                if m:
                    total = int(m.group(0))
                    if total > len(cards):
                        logger.warning(
                            f"[{self.source_domain}] {cat_name}: H1 says {total}"
                            f" results, but only {len(cards)} cards on first page."
                            f" Pagination not implemented."
                        )

            logger.info(
                f"[{self.source_domain}] {cat_name}: {len(cards)} cards parsed"
            )

            added = 0
            for card in cards:
                try:
                    seed = self._parse_card(card)
                except Exception as exc:
                    logger.error(
                        f"[{self.source_domain}] card parse: {exc!r}"
                    )
                    continue
                if not seed:
                    continue
                # URL category fallback if title scan missed
                if not getattr(seed, "category", None):
                    try:
                        seed.category = default_cat
                    except Exception:
                        pass
                if seed.site_property_id not in seeds:
                    seeds[seed.site_property_id] = seed
                    added += 1

            logger.info(
                f"[{self.source_domain}] {cat_name}: +{added} new "
                f"(total: {len(seeds)})"
            )
            await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} unique seeds"
        )
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        site_id = card.attributes.get("data-id")
        if not site_id:
            return None

        link = card.css_first("a.prop-link[href]")
        href = (link.attributes.get("href") if link else "") or ""
        if not href:
            href = self._build_detail_url(site_id)
        href = href.split("?")[0]  # strip session lang params

        h2 = card.css_first("h2")
        title = _normalize_text(h2.text(strip=False)) if h2 else ""
        category = _category_from_title(title)

        # Location: span containing la-map-marker-alt icon
        location_raw: Optional[str] = None
        for span in card.css("span"):
            if span.css_first("i.la-map-marker-alt"):
                text = _normalize_text(span.text(strip=False))
                if text:
                    location_raw = text
                    break

        # Price
        price: Optional[int] = None
        price_node = card.css_first(".listing-price .price-p .fw-bold")
        if price_node:
            price = self._to_int_euro_safe(
                _normalize_text(price_node.text(strip=False))
            )

        # Icon list: sqm, bedrooms, bathrooms (Greek 'δ' = δωμάτια = bedrooms)
        size_sqm: Optional[float] = None
        bedrooms: Optional[int] = None
        bathrooms: Optional[int] = None
        for li in card.css("ul.listing-icons li.icons-list"):
            icon = li.css_first("i")
            cls = (icon.attributes.get("class") or "") if icon else ""
            text = _normalize_text(li.text(strip=False))
            if not text:
                continue
            if "la-ruler-combined" in cls and size_sqm is None:
                size_sqm = _to_float_sqm(text)
            elif "la-bed" in cls and bedrooms is None:
                bedrooms = _to_int_simple(text)
            elif "la-bath" in cls and bathrooms is None:
                bathrooms = _to_int_simple(text)

        seed = PropertyTemplate(
            site_property_id=str(site_id),
            source_domain=self.source_domain,
            url=href,
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
        # Force ?language=en even if seed URL lacks it
        fetch_url = (
            url if "language=en" in url
            else f"{url.rstrip('/')}/?language=en"
        )

        try:
            resp = await self.client.get(fetch_url)
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

        # Step 1: structured panels
        self._parse_title_block(parser, data, extra)
        self._parse_information_panel(parser, data, extra)
        self._parse_features_panel(parser, extra)
        self._parse_distance_section(parser, extra)
        self._parse_energy_class(parser, extra)
        self._parse_agent_code(parser, extra)

        # Step 2: description
        description = self._extract_description(parser)
        if description:
            data["description"] = description
        else:
            og = self._og_description_fallback(parser)
            if og:
                data["description"] = og

        # Step 3: coords from JS globals
        lat, lng = self._extract_coords(resp.text)
        if lat is not None and lng is not None and _bbox_check(lat, lng):
            data["latitude"] = lat
            data["longitude"] = lng

        # Step 4: images
        images = self._extract_images(parser)
        if images:
            data["images"] = images

        # Land normalization: replicate size to land_size if Land category
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

    # ── Step 1.a: title block (.property-title) ──────────────────────

    def _parse_title_block(
        self, parser: LexborHTMLParser,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first(".property-title")
        if not block:
            return

        h1 = block.css_first("h1")
        if h1:
            title_text = _normalize_text(h1.text(strip=False))
            cat = _category_from_title(title_text)
            if cat:
                data["category"] = cat
            # trailing "N sq.m." in title
            m = re.search(r"([\d,.]+)\s*sq\.?\s*m", title_text, re.IGNORECASE)
            if m and "size_sqm" not in data:
                v = _to_float_sqm(m.group(1))
                if v is not None:
                    data["size_sqm"] = v

        price_node = block.css_first(".property-price span")
        if price_node:
            v = self._to_int_euro_safe(
                _normalize_text(price_node.text(strip=False))
            )
            if v is not None:
                data["price"] = v

        pps = block.css_first(".prices-section span")
        if pps:
            v = self._to_int_euro_safe(_normalize_text(pps.text(strip=False)))
            if v is not None:
                extra["price_per_sqm"] = v

    # ── Step 1.b: information panel (.property-inform) ──────────────

    def _parse_information_panel(
        self, parser: LexborHTMLParser,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first(".property-inform ul.information-list")
        if not block:
            return
        for li in block.css("li"):
            text = _normalize_text(li.text(strip=False))
            if not text or ":" not in text:
                continue
            label, _, value = text.partition(":")
            self._route_info_row(
                label.strip().lower().rstrip(":"),
                value.strip(),
                data, extra,
            )

    def _route_info_row(
        self, label: str, value: str,
        data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        if not value:
            return
        if label == "area":
            v = _to_float_sqm(value)
            if v is not None and "size_sqm" not in data:
                data["size_sqm"] = v
            return
        if label == "rooms":
            v = _to_int_simple(value)
            if v is not None and "bedrooms" not in data:
                data["bedrooms"] = v
            return
        if label in ("bathroom", "bathrooms"):
            v = _to_int_simple(value)
            if v is not None and "bathrooms" not in data:
                data["bathrooms"] = v
            return
        if label in ("year of manufacture", "construction year"):
            v = _to_int_simple(value)
            if v is not None and 1900 <= v <= 2100:
                data["year_built"] = v
            return
        if label == "code":
            extra["agent_code"] = value
            return
        if label == "floor":
            extra["floor"] = value
            return
        sl = _slug(label)
        if sl:
            extra[sl] = value

    # ── Step 1.c: features panel (.feautures) ───────────────────────

    def _parse_features_panel(
        self, parser: LexborHTMLParser, extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first(".feautures ul.property-feautures")
        if not block:
            return
        for li in block.css("li"):
            text = _normalize_text(li.text(strip=False))
            if not text:
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
            else:
                sl = _slug(text)
                if sl:
                    extra[sl] = True

    # ── Step 1.d: distance section ──────────────────────────────────

    def _parse_distance_section(
        self, parser: LexborHTMLParser, extra: Dict[str, Any],
    ) -> None:
        for h4 in parser.css("h4"):
            if "distance" not in (h4.text(strip=True) or "").lower():
                continue
            nxt = h4.next
            while nxt is not None and nxt.tag != "ul":
                nxt = nxt.next
            if nxt is None:
                break
            for li in nxt.css("li"):
                text = _normalize_text(li.text(strip=False))
                if not text or ":" not in text:
                    continue
                label, _, value = text.partition(":")
                label = label.strip().lower()
                m = re.search(r"(\d+)", value)
                if not m:
                    continue
                sl = _slug(label)
                if sl:
                    extra[f"distance_{sl}_m"] = int(m.group(1))
            break

    # ── Step 1.e: energy class ──────────────────────────────────────

    def _parse_energy_class(
        self, parser: LexborHTMLParser, extra: Dict[str, Any],
    ) -> None:
        active = parser.css_first(".energy-section .energy.active")
        if active:
            ec = _normalize_text(active.text(strip=False))
            if ec:
                extra["energy_class"] = ec

    # ── Step 1.f: agent code from contact form hidden input ─────────

    def _parse_agent_code(
        self, parser: LexborHTMLParser, extra: Dict[str, Any],
    ) -> None:
        if "agent_code" in extra:
            return
        code_input = parser.css_first('input[name="p_code"]')
        if code_input:
            code = (code_input.attributes.get("value") or "").strip()
            if code:
                extra["agent_code"] = code

    # ── Step 2: description ─────────────────────────────────────────

    def _extract_description(
        self, parser: LexborHTMLParser,
    ) -> Optional[str]:
        details = parser.css_first(".property-details")
        if not details:
            return None
        paragraphs: List[str] = []
        for p in details.css("p"):
            txt = _normalize_text(p.text(strip=False))
            if txt:
                paragraphs.append(txt)
        if paragraphs:
            return "\n\n".join(paragraphs)
        txt = _normalize_text(details.text(separator=" ", strip=True))
        return txt if (txt and len(txt) >= 50) else None

    # ── Step 3: coords from <script> globals ────────────────────────

    def _extract_coords(
        self, html_text: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        lat_m = re.search(r"var\s+lat\s*=\s*([-\d.]+)\s*;", html_text)
        lng_m = re.search(r"var\s+long\s*=\s*([-\d.]+)\s*;", html_text)
        if not (lat_m and lng_m):
            return None, None
        try:
            lat = float(lat_m.group(1))
            lng = float(lng_m.group(1))
        except ValueError:
            return None, None
        if lat == 0.0 or lng == 0.0:
            return None, None
        return lat, lng

    # ── Step 4: images ──────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        seen: set = set()
        out: List[str] = []
        for a in parser.css(".row.gallery .item-img a.card-img[href]"):
            href = (a.attributes.get("href") or "").strip()
            if not href or href.endswith(".svg"):
                continue
            if href.startswith("/"):
                href = f"{_BASE_URL}{href}"
            if href not in seen:
                seen.add(href)
                out.append(href)
        return out
