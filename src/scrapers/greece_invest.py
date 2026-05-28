"""
~/hodu/src/scrapers/greece_invest.py

greece-invest.com scraper — Strategy A (paginated server-rendered walk).

Tech / anti-bot
---------------
Plain Apache, server-side-rendered HTML, NO Cloudflare / PerimeterX. Stage 0
(curl_cffi via self.client.get) is sufficient for BOTH listing and detail —
no Playwright, no AJAX, no nonce. Part of the AKTIS GROUP network; the same
listings are mirrored across greece-invest.{com,gr,ru,de,...} (language
domains) — we scrape the English .com.

URL patterns
------------
List:    /search?sType={1|2|3}&region=10&p1={min_price}&...&page={N}
            region=10   = Halkidiki (site's region taxonomy)
            p1          = min price, formatted "400.000" (dot thousand-sep)
            sType=1     = residential   (~156 @ ≥400k)
            sType=2/3   = commercial / land variants (extra res_*/comm_* params)
Detail:  /realty/{id}     e.g. /realty/7549

Strategy notes
--------------
The /search results are server-rendered (region + price filter applied
server-side), so collect_urls just walks ?page=N until a page yields no NEW
cards. Cards are `.pcard` elements carrying everything we need for the seed:
detail URL, category, location, price. We paginate every requested sType and
dedupe by site_property_id (a property can only live in one sType, but the
dedupe is cheap insurance).

Detail pages are also server-rendered — clean structured blocks:
  * <h1>          "Townhouse № 7549"            → category (word before №)
  * .text-primary "800 000 - 980 000€"          → price (range → take first)
  * .prop-main-icons li (.ttl3 + label)         → bedrooms / bathrooms / area / sea-view
  * .prop-props tr (td/td)                      → plot area, distances, energy class
  * .prop-location a[href^=/greece/]            → area, municipality, prefecture
  * .prop-txt p                                 → description
  * .prop-amenities li                          → amenity flags
  * a[data-fslightbox] href                     → full-size gallery (…/uploads/properties/wm/…)
  * <script> const center={lat:..,lng:..}       → coordinates (bbox-checked)

Mapping decisions
-----------------
* category: site type-word → canonical hodu vocabulary (_CATEGORY_MAP).
  "Townhouse" → "Maisonette" (row-house ≡ maisonette in the hodu vocab; keeps
  cross-engine clustering aligned with kw_greece / grekodom). Unknown types are
  Title-cased verbatim so Engine 1 still gets a non-null category.
* area  = locality (1st /greece/ crumb), subarea = municipality (2nd crumb).
* Plot area → land_size_sqm (top-level). Living area → size_sqm.
* Distances / energy class / amenities → extra_features.
* NLP fallback (mixin) fills only missing metric columns from the description.
"""
from __future__ import annotations

import asyncio
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

_BASE_URL = "https://greece-invest.com"
_SEARCH_PATH = "/search"
_SOURCE_DOMAIN = "greece-invest.com"

# Site region taxonomy: 10 = Halkidiki.
_REGION_HALKIDIKI = 10

# ~156 residential @ ≥400k → ~10 pages. 30 gives ample headroom for sType 2/3.
_MAX_PAGES = 30
_INTER_PAGE_SLEEP_SEC = 1.5

# Halkidiki bbox sanity (lat_min, lng_min, lat_max, lng_max) — coords outside
# this are wrong (default map centre / other region) and would poison GeoMatcher.
_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# sType → query-param template. {region}/{price}/{page} injected. The full
# param sets replicate the site's three search forms (verified from live URLs);
# missing required params can make the backend ignore the price/region filter.
_STYPE_PARAMS: Dict[int, str] = {
    1: ("sType=1&region={region}&bed=&comm_type=&propId=&s1=&s2="
        "&p1={price}&p2=&page={page}"),
    2: ("sType=2&region={region}&bed=&p1={price}&p2=&comm_sq=&s1=&s2="
        "&comm_type=0&propId=&res_type=0&res_age=0&res_sq_from=&res_sq_to="
        "&res_fl_from=&res_fl_to=&dsea=0-1500&dcity=0-10&page={page}"),
    3: ("sType=3&region={region}&bed=&p1={price}&p2=&comm_sq=&s1=&s2="
        "&comm_type=0&propId=&res_type=0&res_age=0&res_sq_from=&res_sq_to="
        "&res_fl_from=&res_fl_to=&dsea=0-1500&dcity=0-10&page={page}"),
}

# Site type-word → canonical hodu category. Lower-cased keys.
_CATEGORY_MAP: Dict[str, str] = {
    "townhouse":        "Maisonette",   # row house ≡ maisonette in hodu vocab
    "maisonette":       "Maisonette",
    "villa":            "Villa",
    "apartment":        "Apartment",
    "flat":             "Apartment",
    "studio":           "Apartment",
    "penthouse":        "Apartment",
    "house":            "House",
    "detached house":   "House",
    "cottage":          "House",
    "bungalow":         "House",
    "land":             "Land",
    "plot":             "Land",
    "plot of land":     "Land",
    "hotel":            "Hotel",
    "commercial":       "Business",
    "commercial property": "Business",
    "business":         "Business",
    "office":           "Business",
    "shop":             "Business",
    "store":            "Business",
    "building":         "Business",
    "warehouse":        "Business",
}

# Halkidiki locality → municipality (best-effort; .prop-location usually gives
# the municipality directly as the 2nd crumb, this is a fallback for extras).
_CITY_TO_MUNICIPALITY: Dict[str, str] = {
    "kassandra": "Kassandra", "kallithea": "Kassandra", "afytos": "Kassandra",
    "kriopigi": "Kassandra", "polychrono": "Kassandra", "hanioti": "Kassandra",
    "chanioti": "Kassandra", "pefkohori": "Kassandra", "pefkochori": "Kassandra",
    "paliouri": "Kassandra", "nea skioni": "Kassandra", "sani": "Kassandra",
    "fourka": "Kassandra", "siviri": "Kassandra", "possidi": "Kassandra",
    "kalandra": "Kassandra", "nea fokea": "Kassandra", "elani": "Kassandra",
    "sithonia": "Sithonia", "nikiti": "Sithonia", "neos marmaras": "Sithonia",
    "marmaras": "Sithonia", "sarti": "Sithonia", "toroni": "Sithonia",
    "sikia": "Sithonia", "vourvourou": "Sithonia", "porto koufo": "Sithonia",
    "ormos panagias": "Sithonia", "metamorfosi": "Sithonia", "psakoudia": "Sithonia",
    "ouranoupoli": "Aristotelis", "ierissos": "Aristotelis", "ammouliani": "Aristotelis",
    "nea roda": "Aristotelis", "stratoni": "Aristotelis", "olympiada": "Aristotelis",
    "polygyros": "Polygyros", "gerakini": "Polygyros", "ormylia": "Polygyros",
    "moudania": "Nea Propontida", "nea moudania": "Nea Propontida",
    "kallikrateia": "Nea Propontida", "flogita": "Nea Propontida",
    "nea triglia": "Nea Propontida", "nea plagia": "Nea Propontida",
}


# =============================================================================
# Helpers (pure functions)
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
    """First integer in text. '3', '15 Bedrooms', '250 sq.m.' → int."""
    if not text:
        return None
    m = re.search(r"\d+", text.replace(".", "").replace(",", "").replace(" ", ""))
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """Parse a sqm value: '250 sq.m.' / '500   sq.m.' / '110,5' → float."""
    if not text:
        return None
    m = re.search(r"\d+(?:[.,]\d+)?", text.replace("\xa0", " "))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _extract_id_from_url(url: str) -> Optional[str]:
    """'/realty/7549' → '7549'."""
    if not url:
        return None
    m = re.search(r"/realty/(\d+)", url)
    return m.group(1) if m else None


def _map_category(type_word: str) -> Optional[str]:
    """Site type-word → canonical hodu category (Title-case fallback)."""
    tw = _normalize_text(type_word).lower()
    if not tw:
        return None
    return _CATEGORY_MAP.get(tw, tw.title())


# =============================================================================
# Scraper
# =============================================================================

class GreeceInvestScraper(EnrichmentMixin, BaseScraper):
    """
    greece-invest.com — Strategy A paginated server-rendered walk.

      Phase 1: collect_urls(min_price)  → List[PropertyTemplate] seeds
                 walks /search?...&page=N over sType 1/2/3, parses .pcard
      Phase 2: fetch_details(url)        → Dict[str, Any] (7-step canonical)

    Stage 0 (curl_cffi via self.client) handles everything — no Playwright.
    """

    # Breadcrumb / h1 give an authoritative category; structured blocks give the
    # metrics. NLP only fills metric columns that remain None.
    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )

    _NLP_TO_STRUCTURAL: Dict[str, set] = {
        "swimming_pool":    {"pool", "private_pool", "private pool"},
        "sea_view":         {"sea_view", "view"},
        "parking":          {"garage", "parking_spot"},
        "air_conditioning": {"air_condition", "a_c", "ac"},
        "fireplace":        {"fire_place"},
        "balcony":          {"balconies"},
        "garden":           {"private_garden"},
        "storage_room":     {"storage"},
    }

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builder ───────────────────────────────────────────────────────

    def _construct_search_url(self, *, s_type: int, page: int, min_price: int) -> str:
        # Site expects "400.000" (dot thousands sep) in p1.
        price_str = f"{min_price:,}".replace(",", ".")
        params = _STYPE_PARAMS[s_type].format(
            region=_REGION_HALKIDIKI, price=price_str, page=page,
        )
        return f"{_BASE_URL}{_SEARCH_PATH}?{params}"

    # ── Phase 1: collect_urls ─────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        s_types: Tuple[int, ...] = (1, 2, 3),
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        """
        Walk every requested sType's paginated /search, parse `.pcard` seeds,
        dedupe by site_property_id. Stops a sType after 2 consecutive pages
        with no NEW cards (end of results / pagination clamp).
        """
        seeds: Dict[str, PropertyTemplate] = {}

        for s_type in s_types:
            if s_type not in _STYPE_PARAMS:
                continue
            consecutive_empty = 0
            for page in range(1, max_pages + 1):
                url = self._construct_search_url(
                    s_type=s_type, page=page, min_price=min_price,
                )
                logger.info(
                    f"[{self.source_domain}] sType={s_type} page {page}: GET {url}"
                )
                try:
                    resp = await self.client.get(url)
                except Exception as exc:
                    logger.error(
                        f"[{self.source_domain}] sType={s_type} page {page} "
                        f"fetch failed: {exc!r}"
                    )
                    break

                if getattr(resp, "status_code", 200) == 404:
                    break
                if not resp.text:
                    break

                parser = LexborHTMLParser(resp.text)
                cards = parser.css(".pcard")
                page_added = 0

                if cards:
                    for card in cards:
                        try:
                            seed = self._parse_card(card)
                        except Exception as exc:
                            logger.error(
                                f"[{self.source_domain}] card parse error: {exc!r}"
                            )
                            continue
                        if not seed or seed.site_property_id in seeds:
                            continue
                        seeds[seed.site_property_id] = seed
                        page_added += 1
                else:
                    # Backstop: scan bare /realty/ links if .pcard markup changed.
                    for a in parser.css("a[href*='/realty/']"):
                        href = (a.attributes.get("href") or "").strip()
                        site_id = _extract_id_from_url(href)
                        if not site_id or site_id in seeds:
                            continue
                        if href.startswith("/"):
                            href = f"{_BASE_URL}{href}"
                        seeds[site_id] = PropertyTemplate(
                            site_property_id=site_id,
                            source_domain=self.source_domain,
                            url=href,
                        )
                        page_added += 1

                logger.info(
                    f"[{self.source_domain}] sType={s_type} page {page}: "
                    f"{len(cards)} cards (+{page_added} new, total: {len(seeds)})"
                )

                if page_added == 0:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        logger.info(
                            f"[{self.source_domain}] sType={s_type}: end of results"
                        )
                        break
                else:
                    consecutive_empty = 0

                await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} unique seeds"
        )
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """
        Parse one `.pcard`. ptitle text is "{Category} | {area}, {prefecture},
        {municipality}". Detail page is authoritative; the card seeds enough for
        the Halkidiki whitelist + price pre-filter.
        """
        title_a = card.css_first(".ptitle a[href]")
        href = title_a.attributes.get("href") if title_a else None
        if not href:
            for a in card.css("a[href]"):
                if "/realty/" in (a.attributes.get("href") or ""):
                    href = a.attributes.get("href")
                    break
        if not href:
            return None
        if href.startswith("/"):
            href = f"{_BASE_URL}{href}"

        site_id = _extract_id_from_url(href)
        if not site_id:
            return None

        category: Optional[str] = None
        location_raw: Optional[str] = None
        if title_a:
            ptext = _normalize_text(title_a.text(strip=False))
            if "|" in ptext:
                cat_part, _, loc_part = ptext.partition("|")
                category = _map_category(cat_part)
                location_raw = loc_part.strip() or None
            elif ptext:
                category = _map_category(ptext)

        price_node = card.css_first(".price")
        price_text = _normalize_text(price_node.text(strip=False)) if price_node else None
        # Range (e.g. "800 000 - 980 000 €") → take the first amount.
        if price_text and "-" in price_text:
            price_text = price_text.split("-", 1)[0].strip()

        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=href,
            price=price_text,           # validator cleans "249 000 €" → 249000
            category=category,
            location_raw=location_raw,
        )

    # ── Phase 2: fetch_details ────────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """Canonical 7-step pipeline (see EnrichmentMixin)."""
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

        # ── Step 1: structured site-specific extraction ────────────────────
        self._parse_title_and_price(parser, data)
        self._parse_main_icons(parser, data, extra)
        self._parse_props_table(parser, data, extra)
        self._parse_location(parser, data, extra)
        self._parse_amenities(parser, extra)

        # ── Step 2: description → og:description fallback ──────────────────
        description = self._extract_description(parser)
        if not description:
            description = self._og_description_fallback(parser)
        if description:
            data["description"] = description

        # ── Step 3: coordinates (bbox-checked) ─────────────────────────────
        lat, lng = self._extract_coordinates(parser, resp.text)
        if lat is not None and lng is not None:
            data["latitude"] = lat
            data["longitude"] = lng

        # ── Step 4: images → og:image fallback ─────────────────────────────
        images = self._extract_images(parser)
        if not images:
            og = self._og_image_fallback(parser)
            if og:
                images = [og]
        if images:
            data["images"] = images

        # Merge extras BEFORE NLP so dedup can inspect them.
        if extra:
            data["extra_features"] = extra

        # ── Step 5: NLP fallback (fills missing metric columns only) ───────
        self._apply_nlp_fallback(data)

        # ── Step 6: LLM fallback — not enabled (no self._llm_extractor) ────

        # ── Step 7: quality gate (log-only) ───────────────────────────────
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )

        return data

    # ── Step 1 helpers ────────────────────────────────────────────────────

    def _parse_title_and_price(self, parser: LexborHTMLParser, data: Dict[str, Any]) -> None:
        """h1 'Townhouse № 7549' → category; '.text-primary' price (range→first)."""
        h1 = parser.css_first("h1")
        if h1:
            h1_text = _normalize_text(h1.text(strip=False))
            # Type-word is everything before the listing number marker (№ / #).
            type_word = re.split(r"[№#]", h1_text, 1)[0].strip()
            cat = _map_category(type_word)
            if cat:
                data["category"] = cat

        # Price sits in a span near the title; "800 000 - 980 000€" → first amount.
        price_node = (
            parser.css_first(".prop-header .text-primary")
            or parser.css_first("h1 ~ div .text-primary")
            or parser.css_first(".text-primary.text-nowrap")
        )
        if price_node:
            price_text = _normalize_text(price_node.text(strip=False))
            if price_text:
                first = price_text.split("-", 1)[0].strip()
                price = self._to_int_euro_safe(first)
                if price is not None:
                    data["price"] = price

    def _parse_main_icons(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        """`.prop-main-icons li`: .ttl3 value + label span (Bedrooms/Bathrooms/Area/...)."""
        for li in parser.css(".prop-main-icons li"):
            val_node = li.css_first(".ttl3")
            if not val_node:
                continue
            value = _normalize_text(val_node.text(strip=False))
            # Label is the span that is NOT .ttl3.
            label = ""
            for sp in li.css("span"):
                if "ttl3" not in (sp.attributes.get("class") or ""):
                    label = _normalize_text(sp.text(strip=False))
            label_low = label.lower()
            if not label_low or not value:
                continue

            if "bedroom" in label_low:
                n = _to_int_simple(value)
                if n is not None:
                    data["bedrooms"] = n
            elif "bathroom" in label_low:
                n = _to_int_simple(value)
                if n is not None:
                    data["bathrooms"] = n
            elif "area" in label_low and "plot" not in label_low and "lot" not in label_low:
                sqm = _to_float_sqm(value)
                if sqm is not None:
                    data["size_sqm"] = sqm
            elif "year" in label_low:
                y = _to_int_simple(value)
                if y is not None and 1800 <= y <= 2100:
                    data["year_built"] = y
            else:
                # Sea view / Floor / etc. → extra (bool for Yes/No).
                key = _slug(label)
                low = value.lower()
                if low in ("yes", "ναι"):
                    extra[key] = True
                elif low in ("no", "όχι", "οχι"):
                    extra[key] = False
                else:
                    extra[key] = value

    def _parse_props_table(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        """`.prop-props tr` (td/td): Plot area, distances, energy class, etc."""
        for tr in parser.css(".prop-props tr"):
            tds = tr.css("td")
            if len(tds) < 2:
                continue
            label = _normalize_text(tds[0].text(strip=False)).lower()
            value = _normalize_text(tds[1].text(strip=False))
            if not label or not value:
                continue

            if "plot area" in label or "land area" in label:
                sqm = _to_float_sqm(value)
                if sqm is not None and not data.get("land_size_sqm"):
                    data["land_size_sqm"] = sqm
            elif "distance" in label and "sea" in label:
                n = _to_int_simple(value)
                if n is not None:
                    extra["distance_to_sea_m"] = n
            elif "distance" in label and "airport" in label:
                n = _to_int_simple(value)   # value is in km on this site
                if n is not None:
                    extra["distance_to_airport_km"] = n
            elif "distance" in label and ("center" in label or "supermarket" in label or "city" in label):
                n = _to_int_simple(value)
                if n is not None:
                    extra["distance_to_center_m"] = n
            elif "distance" in label and "marina" in label:
                n = _to_int_simple(value)
                if n is not None:
                    extra["distance_to_marina_m"] = n
            elif "energy" in label:
                extra["energy_class"] = value
            elif "year" in label:
                y = _to_int_simple(value)
                if y is not None and 1800 <= y <= 2100 and not data.get("year_built"):
                    data["year_built"] = y
            else:
                extra[_slug(label)] = value

    def _parse_location(
        self, parser: LexborHTMLParser, data: Dict[str, Any], extra: Dict[str, Any],
    ) -> None:
        """`.prop-location a[href^=/greece/]`: [locality, municipality, prefecture]."""
        loc = parser.css_first(".prop-location")
        if not loc:
            return
        parts = [
            _normalize_text(a.text(strip=False))
            for a in loc.css("a[href]")
            if "/greece/" in (a.attributes.get("href") or "")
        ]
        parts = [p for p in parts if p]
        if not parts:
            return
        data["area"] = parts[0]
        if len(parts) >= 2:
            data["subarea"] = parts[1]          # municipality crumb
        data["location_raw"] = ", ".join(parts)
        # Best-effort municipality routing for extras.
        muni = _CITY_TO_MUNICIPALITY.get(parts[0].lower())
        if muni:
            extra["municipality"] = muni

    def _parse_amenities(self, parser: LexborHTMLParser, extra: Dict[str, Any]) -> None:
        """`.prop-amenities li` → boolean amenity flags in extra_features."""
        for li in parser.css(".prop-amenities li"):
            t = _normalize_text(li.text(strip=False))
            if not t:
                continue
            key = _slug(t)
            if key and key not in extra:
                extra[key] = True

    # ── Step 2: description ────────────────────────────────────────────────

    def _extract_description(self, parser: LexborHTMLParser) -> Optional[str]:
        """Body description from `.prop-txt` <p> blocks (amenity <ul> excluded)."""
        container = parser.css_first(".prop-txt") or parser.css_first(".db-txt")
        if not container:
            return None
        paragraphs = [
            _normalize_text(p.text(separator="\n", strip=True))
            for p in container.css("p")
        ]
        paragraphs = [p for p in paragraphs if p and len(p) >= 5]
        if paragraphs:
            text = "\n\n".join(paragraphs)
            if len(text) >= 50:
                return text
        return None

    # ── Step 3: coordinates ────────────────────────────────────────────────

    def _extract_coordinates(
        self, parser: LexborHTMLParser, html_text: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Coords live in a JS object literal:
            const center={ lat: 39.977..., lng: 23.642... }
        lat/lng are on separate lines, so match each independently.
        Always bbox-checked (Halkidiki) before returning.
        """
        lat_m = re.search(r"\blat\s*:\s*([0-9]+\.[0-9]+)", html_text)
        lng_m = re.search(r"\blng\s*:\s*([0-9]+\.[0-9]+)", html_text)
        if lat_m and lng_m:
            try:
                lat = float(lat_m.group(1))
                lng = float(lng_m.group(1))
            except ValueError:
                return None, None
            if _bbox_check(lat, lng):
                return lat, lng
            logger.debug(
                f"[{self.source_domain}] coords {lat},{lng} outside Halkidiki bbox — dropped"
            )
        return None, None

    # ── Step 4: images ─────────────────────────────────────────────────────

    def _extract_images(self, parser: LexborHTMLParser) -> List[str]:
        """
        Full-size gallery images are the `a[data-fslightbox]` hrefs
        (…/uploads/properties/wm/{id}/…). Thumbnails (thmb) are skipped by
        preferring the lightbox href over the inner <img> src.
        """
        seen: set = set()
        images: List[str] = []
        for a in parser.css("a[data-fslightbox]"):
            href = (a.attributes.get("href") or "").strip()
            if not href or href in seen or href.endswith(".svg"):
                continue
            if href.startswith("//"):
                href = "https:" + href
            seen.add(href)
            images.append(href)
        return images
