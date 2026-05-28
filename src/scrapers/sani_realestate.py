"""
~/hodu/src/scrapers/sani_realestate.py

sani-realestate.gr scraper — Strategy A (paginated server-rendered walk).

Tech
----
WordPress + "wpresidence" theme + "WPestate" plugin. Fully server-side
rendered — Stage 0 curl_cffi handles both listing and detail (recon: 200,
15 `.listing_wrapper` cards/page on raw HTML, no JS needed). The wpresidence
/ WPestate stack is a widely-used commercial real-estate theme, so this
parser is largely reusable across other wpresidence-based GR agencies
(selectors are the theme's, not this agency's).

URL patterns
------------
List:    /en/property-listings/            (page 1)
         /en/property-listings/page/{N}/   (server pagination — `pagination_nojax`)
            -> 4 pages, ~57 objects total. The site has NO price filter, so
               we collect every card and CLIENT-SIDE filter to >= min_price.
               "Price upon request" cards are KEPT (price=None) — these are
               super-luxury listings the owner explicitly wants ingested.
Detail:  /en/estate_property/{slug}/   e.g. /en/estate_property/villa-for-sale-in-pefkochori-1054/

Listing cards
-------------
`.listing_wrapper` (15/page) inside `#listing_ajax_container`. Each card:
  * data-modal-link   -> detail URL
  * data-listid       -> stable post id (used as site_property_id)
  * data-modal-title  -> title -> category
  * .listing_unit_price_wrapper -> "1.950.000 €" | "Price upon request"
  * .property_location_image a  -> area link, city link
Pagination walks /page/{N}/ until a page yields no cards.

Detail blocks (WPestate — clean class-keyed `.listing_detail` rows)
------------------------------------------------------------------
Title:       h1.entry-title.entry-prop          -> "Villa for sale in ..."  -> category
Details accordion (#accordion_property_details):
  * .property_default_price        -> "Price: 1.400.000 €"
  * .property_default_property_size span -> "150,00 m²"
  * .property_default_lot_size span      -> "151,00 m²"   (plot / land)
  * .property_default_bedrooms     -> "Bedrooms: 3"
  * .property_default_bathrooms    -> "Bathrooms: 4"
  * .property-year                 -> "Year Built: 2025"
  * .stories-number                -> "Floors: 2"
Address accordion (#accordion_property_address): strong-keyed rows
  (Address / City / Area / State-County / Zip / Country).
Energy:      .indicator-energy[data-energyclass]  -> "A+"
Features:    #accordion_features_details .feature_block_others .listing_detail
Description: #wpestate_property_description_section .panel-body  (full <p> blocks)
Images:      .property_multi_image_slider .multi_image_slider_image[style]
             (server-rendered bg-image; size suffix -WxH stripped to original)

Number format (IMPORTANT — opposite of halkidiki-imoti)
-------------------------------------------------------
European: dot = thousands, comma = decimal.
  "1.400.000 €"  -> 1400000   (handled by mixin _to_int_euro_safe)
  "4.500,00 m²"  -> 4500      (handled by local _to_sqm_eu)
  "150,00 m²"    -> 150

Caveats / decisions
-------------------
* Coordinates: WPestate stores no per-property lat/lng for these listings
  (`markers:"[]"`); the detail JS `general_latitude/longitude` falls back to
  the AGENCY HQ (Nea Fokea ~40.1343, 23.3976). Using it would pin all 57
  objects on the office — fatal for clustering. We therefore IGNORE the HQ
  fallback: a best-effort data-cur_lat regex is tried, bbox-checked AND
  HQ-rejected; in practice coords default to None and the pipeline's
  name-based GeoMatcher resolves location from city/area (e.g. Pefkochori,
  Halkidiki).
* No server price filter -> client-side >= min_price in collect_urls,
  "Price upon request" (price=None) always kept.
* category derived from the title prefix ("Villa/Land Plot/Land Parcel/
  Maisonette/Apartment/Detached House"), with breadcrumb fallback.
* site_property_id = data-listid (post id; stable, present on the card).
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

_BASE_URL = "https://sani-realestate.gr"
_SOURCE_DOMAIN = "sani-realestate.gr"

_MAX_PAGES = 8                 # site has 4; headroom for growth
_INTER_PAGE_SLEEP_SEC = 1.5

_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# Agency HQ (Nea Fokea office) — WPestate uses this as the no-coords fallback.
# Any extracted coord within _HQ_TOLERANCE of this is the office, NOT the
# property, and must be rejected.
_HQ_LATLNG = (40.1343, 23.3976)
_HQ_TOLERANCE = 0.02          # ~2 km; office fallback cluster

# Title-prefix (lower) -> canonical hodu category. Longest keys first at match.
_CATEGORY_MAP: Dict[str, str] = {
    "villa":          "Villa",
    "detached house": "House",
    "house":          "House",
    "maisonette":     "Maisonette",
    "apartment":      "Apartment",
    "studio":         "Apartment",
    "land plot":      "Land",
    "land parcel":    "Land",
    "agricultural":   "Land",
    "plot":           "Land",
    "parcel":         "Land",
    "land":           "Land",
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
    """First integer in text, ignoring grouping/space. 'Bedrooms: 3' -> 3."""
    if not text:
        return None
    m = re.search(r"\d+", text.replace(",", "").replace(".", "").replace(" ", ""))
    return int(m.group(0)) if m else None


def _to_sqm_eu(text: str) -> Optional[float]:
    """
    Parse sqm in EUROPEAN format on this site (dot=thousands, comma=decimal):
        '150,00 m²'    -> 150.0
        '4.500,00 m²'  -> 4500.0
        '15.000,00 m²' -> 15000.0
    """
    if not text:
        return None
    cleaned = text.replace("\xa0", " ").replace(".", "").replace(",", ".")
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


def _is_hq(lat: float, lng: float) -> bool:
    return (abs(lat - _HQ_LATLNG[0]) <= _HQ_TOLERANCE and
            abs(lng - _HQ_LATLNG[1]) <= _HQ_TOLERANCE)


def _slug_from_url(url: str) -> Optional[str]:
    """'/en/estate_property/villa-for-sale-in-pefkochori-1054/' -> slug."""
    if not url:
        return None
    m = re.search(r"/estate_property/([a-z0-9\-]+)/?", url)
    return m.group(1) if m else None


def _category_from_title(title: str) -> Optional[str]:
    """'Land Plot for sale in Sani (Kassandra)' -> 'Land'."""
    t = _normalize_text(title).lower()
    if not t:
        return None
    # strip trailing "for sale ..." so only the leading type words remain
    head = re.split(r"\bfor\s+sale\b", t)[0].strip()
    head = head or t
    # longest-key match (e.g. "land plot" before "land")
    for key in sorted(_CATEGORY_MAP, key=len, reverse=True):
        if head.startswith(key) or f" {key}" in f" {head}":
            return _CATEGORY_MAP[key]
    return None


def _strip_img_size(url: str) -> str:
    """'.../NVD0691-1-488x790.jpg' -> '.../NVD0691-1.jpg' (WP original)."""
    return re.sub(r"-\d+x\d+(?=\.\w+$)", "", url)


_BG_URL_RE = re.compile(r"url\(\s*['\"]?(.*?)['\"]?\s*\)", re.IGNORECASE)


def _bg_url(style: str) -> Optional[str]:
    if not style:
        return None
    m = _BG_URL_RE.search(style)
    return m.group(1).strip() if m else None


# =============================================================================
# Scraper
# =============================================================================

class SaniRealestateScraper(EnrichmentMixin, BaseScraper):
    """sani-realestate.gr — WordPress/wpresidence(WPestate) Strategy A walk."""

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
        "balcony":          {"balconies", "balcony", "veranda", "porch"},
        "garden":           {"private_garden", "garden"},
        "storage_room":     {"storage", "storage_space", "storage_room"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builder ───────────────────────────────────────────────────────

    def _construct_search_url(self, *, page: int) -> str:
        if page <= 1:
            return f"{_BASE_URL}/en/property-listings/"
        return f"{_BASE_URL}/en/property-listings/page/{page}/"

    # ── Phase 1: collect_urls ─────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        """Walk the listing pages, parse `.listing_wrapper` cards, client-filter."""
        seeds: Dict[str, PropertyTemplate] = {}
        skipped_cheap = 0

        for page in range(1, max_pages + 1):
            url = self._construct_search_url(page=page)
            logger.info(f"[{self.source_domain}] page {page}: GET {url}")
            try:
                resp = await self.client.get(url)
            except Exception as exc:
                logger.error(f"[{self.source_domain}] page {page} fetch failed: {exc!r}")
                break

            if getattr(resp, "status_code", 200) == 404 or not resp.text:
                break

            parser = LexborHTMLParser(resp.text)
            container = parser.css_first("#listing_ajax_container")
            cards = (container.css(".listing_wrapper") if container
                     else parser.css(".listing_wrapper"))
            if not cards:
                logger.info(f"[{self.source_domain}] page {page}: no cards — end of results")
                break

            page_added = 0
            for card in cards:
                try:
                    seed, cheap = self._parse_card(card, min_price=min_price)
                except Exception as exc:
                    logger.error(f"[{self.source_domain}] card parse error: {exc!r}")
                    continue
                if cheap:
                    skipped_cheap += 1
                    continue
                if not seed or seed.site_property_id in seeds:
                    continue
                seeds[seed.site_property_id] = seed
                page_added += 1

            logger.info(
                f"[{self.source_domain}] page {page}: {len(cards)} cards "
                f"(+{page_added} new, total kept: {len(seeds)}, "
                f"skipped <{min_price}: {skipped_cheap})"
            )
            await asyncio.sleep(_INTER_PAGE_SLEEP_SEC + random.uniform(0.5, 1.5))

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} unique seeds "
            f"(skipped {skipped_cheap} below {min_price})"
        )
        return list(seeds.values())

    def _parse_card(
        self, card: LexborNode, *, min_price: int,
    ) -> Tuple[Optional[PropertyTemplate], bool]:
        """
        Return (seed, cheap). `cheap` True means a numeric price below
        min_price -> skip. "Price upon request" (no number) is NOT cheap;
        it is kept with price=None.
        """
        href = card.attributes.get("data-modal-link") or ""
        if not href:
            link = card.css_first(".property-unit-information-wrapper h4 a[href]")
            href = (link.attributes.get("href") if link else "") or ""
        if not href:
            return None, False
        if href.startswith("/"):
            href = f"{_BASE_URL}{href}"

        listid = (card.attributes.get("data-listid") or "").strip()
        site_id = listid or _slug_from_url(href)
        if not site_id:
            return None, False

        title = _normalize_text(card.attributes.get("data-modal-title") or "")
        if not title:
            h4 = card.css_first(".property-unit-information-wrapper h4 a")
            title = _normalize_text(h4.text(strip=False)) if h4 else ""

        category = _category_from_title(title)

        price_node = card.css_first(".listing_unit_price_wrapper")
        price_text = _normalize_text(price_node.text(strip=False)) if price_node else ""
        price_int = self._to_int_euro_safe(price_text)

        # client-side price filter (site has none); upon-request kept
        if price_int is not None and price_int < min_price:
            return None, True

        # location from card (area link, city link)
        location_raw: Optional[str] = None
        loc = card.css_first(".property_location_image")
        if loc:
            parts = [_normalize_text(a.text(strip=False)) for a in loc.css("a")]
            parts = [p for p in parts if p]
            if parts:
                location_raw = ", ".join(parts)

        seed = PropertyTemplate(
            site_property_id=str(site_id),
            source_domain=self.source_domain,
            url=href,
            price=(price_int if price_int is not None else None),
            location_raw=location_raw,
        )
        if category:
            try:
                seed.category = category
            except Exception:
                pass
        return seed, False

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

        # Step 1: structured WPestate blocks
        self._parse_title_category(parser, data)
        self._parse_details_accordion(parser, data, extra)
        self._parse_address(parser, data)
        self._parse_energy(parser, extra)
        self._parse_features(parser, data, extra)

        # Step 2: description -> og fallback
        description = self._extract_description(parser) or self._og_description_fallback(parser)
        if description:
            data["description"] = description

        # Step 3: coordinates (best-effort; HQ fallback rejected -> usually None)
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

    def _parse_title_category(self, parser: LexborHTMLParser, data: Dict[str, Any]) -> None:
        h1 = parser.css_first("h1.entry-title")
        title = _normalize_text(h1.text(strip=False)) if h1 else ""
        cat = _category_from_title(title)
        if not cat:
            # breadcrumb / label fallback ("Villas", "Agricultural Plots", ...)
            lab = parser.css_first(".single_property_labels .actioncat a")
            if not lab:
                for a in parser.css(".breadcrumb li a[rel='tag']"):
                    lab = a
                    break
            if lab:
                cat = _category_from_title(_normalize_text(lab.text(strip=False)))
        if cat:
            data["category"] = cat

    def _row_value(self, node: Optional[LexborNode]) -> str:
        """Text of a `.listing_detail` row minus its <strong>label:</strong>."""
        if not node:
            return ""
        full = _normalize_text(node.text(strip=False))
        strong = node.css_first("strong")
        if strong:
            label = _normalize_text(strong.text(strip=False))
            if label and full.startswith(label):
                return full[len(label):].strip()
        return full

    def _parse_details_accordion(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        det = parser.css_first("#accordion_property_details")
        scope = det if det else parser

        price = scope.css_first(".property_default_price")
        if price:
            p = self._to_int_euro_safe(self._row_value(price))
            if p is not None:
                data["price"] = p

        sz = scope.css_first(".property_default_property_size span")
        if sz:
            v = _to_sqm_eu(sz.text(strip=False))
            if v is not None:
                data["size_sqm"] = v

        lot = scope.css_first(".property_default_lot_size span")
        if lot:
            v = _to_sqm_eu(lot.text(strip=False))
            if v is not None:
                data["land_size_sqm"] = v

        bd = scope.css_first(".property_default_bedrooms")
        if bd:
            n = _to_int_simple(self._row_value(bd))
            if n is not None:
                data["bedrooms"] = n

        ba = scope.css_first(".property_default_bathrooms")
        if ba:
            n = _to_int_simple(self._row_value(ba))
            if n is not None:
                data["bathrooms"] = n

        yr = scope.css_first(".property-year")
        if yr:
            n = _to_int_simple(self._row_value(yr))
            if n is not None and 1900 <= n <= 2100:
                data["year_built"] = n

        fl = scope.css_first(".stories-number")
        if fl:
            n = _to_int_simple(self._row_value(fl))
            if n is not None:
                data["levels"] = n

        pid = parser.css_first("#agent_property_id")
        if pid:
            ref = (pid.attributes.get("value") or "").strip()
            if ref:
                extra["site_ref"] = ref
        mls = scope.css_first(".mls")
        if mls:
            m = self._row_value(mls)
            if m:
                extra["mls_id"] = m

    def _parse_address(self, parser: LexborHTMLParser, data: Dict[str, Any]) -> None:
        addr = parser.css_first("#accordion_property_address")
        if not addr:
            return
        fields: Dict[str, str] = {}
        for row in addr.css(".listing_detail"):
            strong = row.css_first("strong")
            if not strong:
                continue
            label = _normalize_text(strong.text(strip=False)).rstrip(":").lower()
            value = self._row_value(row)
            if value:
                fields[label] = value

        # area: prefer City (locality), fall back to Address line
        area = fields.get("city") or fields.get("address")
        if area:
            data["area"] = area

        parts: List[str] = []
        for key in ("city", "area", "state/county", "country"):
            v = fields.get(key)
            if v and v not in parts:
                parts.append(v)
        if parts:
            data["location_raw"] = ", ".join(parts)

    def _parse_energy(self, parser: LexborHTMLParser, extra: Dict[str, Any]) -> None:
        ind = parser.css_first(".indicator-energy[data-energyclass]")
        if ind:
            val = (ind.attributes.get("data-energyclass") or "").strip()
            if val:
                extra["energy_class"] = val
                return
        row = parser.css_first(".listing_detail_energy .listing_detail")
        if row:
            txt = _normalize_text(row.text(strip=False))
            m = re.search(r"energy class[:\s]+([A-Ga-g]\+?)", txt, re.IGNORECASE)
            if m:
                extra["energy_class"] = m.group(1).upper()

    def _parse_features(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        block = parser.css_first("#accordion_features_details .feature_block_others")
        if not block:
            return
        for fd in block.css(".listing_detail"):
            cls = fd.attributes.get("class") or ""
            # skip the chapter heading and any container row that itself wraps
            # nested .listing_detail items (its text is an aggregate of all
            # features -> junk key). Real feature rows are leaf nodes.
            if "feature_chapter_name" in cls or "feature_block_others" in cls:
                continue
            if fd.css_first(".listing_detail") is not None:
                continue
            t = _normalize_text(fd.text(strip=False))
            if not t:
                continue
            low = t.lower()
            # "Distance from the sea (m.): 60"
            if low.startswith("distance from the sea"):
                m = re.search(r"\d+", t)
                if m:
                    extra["sea_distance_m"] = int(m.group(0))
                continue
            # "Plot area: 151 sq.m." -> land fallback if not already set
            if low.startswith("plot area") and "land_size_sqm" not in data:
                v = _to_sqm_eu(t)
                if v is not None:
                    data["land_size_sqm"] = v
                continue
            extra[_slug(t)] = True

    # ── Step 2: description ────────────────────────────────────────────────

    def _extract_description(self, parser: LexborHTMLParser) -> Optional[str]:
        body = parser.css_first("#wpestate_property_description_section .panel-body")
        if not body:
            return None
        paras = [_normalize_text(p.text(strip=False)) for p in body.css("p")]
        paras = [p for p in paras if p]
        txt = "\n".join(paras) if paras else _normalize_text(
            body.text(separator="\n", strip=True)
        )
        if txt and len(txt) >= 50:
            return txt
        return None

    # ── Step 3: coordinates (best-effort; HQ rejected) ────────────────────

    def _extract_coordinates(self, html_text: str) -> Tuple[Optional[float], Optional[float]]:
        # Per-property data-cur_lat / data-cur_long on #gmap_wrapper only.
        # The JS `general_latitude/longitude` is the agency HQ fallback and is
        # deliberately NOT consulted here.
        m = re.search(
            r'data-cur_lat=["\']([0-9]{2}\.[0-9]+)["\'][^>]*?'
            r'data-cur_long=["\']([0-9]{2}\.[0-9]+)["\']',
            html_text,
        )
        if not m:
            return None, None
        try:
            lat, lng = float(m.group(1)), float(m.group(2))
        except ValueError:
            return None, None
        if not _bbox_check(lat, lng):
            return None, None
        if _is_hq(lat, lng):
            logger.debug(f"[{self.source_domain}] coords == HQ fallback, dropping")
            return None, None
        return lat, lng

    # ── Step 4: images ─────────────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        seen: set = set()
        out: List[str] = []

        def _collect(nodes) -> None:
            for n in nodes:
                raw = _bg_url(n.attributes.get("style") or "")
                if not raw:
                    continue
                u = _strip_img_size(raw.strip())
                if u and u not in seen and not u.endswith(".svg") and "/logo" not in u.lower():
                    seen.add(u)
                    out.append(u)

        slider = parser.css_first(".property_multi_image_slider")
        if slider:
            _collect(slider.css(".multi_image_slider_image[style]"))
        if not out:
            lb = parser.css_first(".lightbox_property_slider")
            if lb:
                _collect(lb.css(".item[style]"))
        if not out:
            for img in parser.css(".wpestate_property_header_extended img[src]"):
                h = (img.attributes.get("src") or "").strip()
                u = _strip_img_size(h)
                if u and u not in seen and not u.endswith(".svg") and "/logo" not in u.lower():
                    seen.add(u)
                    out.append(u)
        return out
