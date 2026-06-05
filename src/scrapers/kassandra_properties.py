"""kassandra-properties.gr scraper (Kefalidis Real Estate).

Site: Kefalidis Real Estate (https://www.kassandra-properties.gr)
Stack: Custom PHP CMS + Bootstrap + Swiper galleries
Image hosting: Spitogatos CDN (m1/m2/m3.spitogatos.gr)
Anti-bot: PerimeterX (HUMAN Security) — requires Camoufox bypass

ARCHITECTURAL DEVIATION (documented):
=====================================
This is the FIRST hodu scraper that uses Camoufox directly (not via the
funnel). Reason: PerimeterX defeats both curl_cffi (Stage 0) and
playwright-stealth (Stage 1). Camoufox is a custom Firefox build with
deep anti-fingerprinting patches that bypass PerimeterX successfully.

Because Camoufox browser launch is expensive (~30s), this scraper
deviates from the standard collect_urls/fetch_details split:
- `collect_urls` opens a SINGLE Camoufox session, walks search page
  + ALL detail pages within that one session, caches per-URL detail
  data in `self._detail_cache`, returns seeds with full data.
- `fetch_details(url)` returns `self._detail_cache.get(url, {})`
  (instant in-memory lookup — no second browser session).

Future Sprint 8 work: add Camoufox as Stage 2 to fetcher_funnel so this
deviation can be removed and other 3 PerimeterX-blocked sites
(kassandra_properties, ergon_real_estate, kanata_realestate) unlocked.

URL patterns
------------
List:    /en/property/search?listingType=sale&category=residential&region=196&priceLow={N}
Detail:  /en/propertyDetails/{N}

Inventory: ~49 sale listings ≥€400k in Halkidiki across 3 categories: residential (29, 4 pages), commercial (3, 1 page), land (17, 2 pages).
"""
from __future__ import annotations

import asyncio
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.models.schemas import PropertyTemplate
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.scrapers.base import BaseScraper


# ============================================================================
# Constants
# ============================================================================
_SOURCE_DOMAIN = "kassandra-properties.gr"
_BASE_URL = "https://www.kassandra-properties.gr"
_CATEGORIES = ("residential", "commercial", "land")
_SEARCH_URL_TEMPLATE = (
    "https://www.kassandra-properties.gr/en/property/search"
    "?listingType=sale&region=196&category={cat}&priceLow={min_price}"
)

# Halkidiki bbox sanity
_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)

# Category mapping from card title prefix ("Villa for sale ..." → Villa)
_TITLE_PREFIX_TO_CATEGORY: List[Tuple[str, str]] = [
    # Order: most specific first (compound tokens before generic ones)
    # === Residential ===
    ("apartment complex for sale",  "Complex"),
    ("maisonette for sale",         "Maisonette"),
    ("detached house for sale",     "Detached House"),
    ("villa for sale",              "Villa"),
    ("mansion for sale",            "Villa"),
    ("bungalow for sale",           "Detached House"),
    ("house for sale",              "Detached House"),
    ("apartment for sale",          "Apartment"),
    ("studio for sale",             "Apartment"),
    ("loft for sale",               "Apartment"),
    ("building for sale",           "Complex"),
    # === Hotel ===
    ("hotel for sale",              "Hotel"),
    # === Commercial (canonical: Hotel/Commercial) ===
    ("business building for sale",  "Hotel/Commercial"),
    ("industrial space for sale",   "Hotel/Commercial"),
    ("craft space for sale",        "Hotel/Commercial"),
    ("office for sale",             "Hotel/Commercial"),
    ("store for sale",              "Hotel/Commercial"),
    ("shop for sale",               "Hotel/Commercial"),
    ("warehouse for sale",          "Hotel/Commercial"),
    ("showroom for sale",           "Hotel/Commercial"),
    ("hall for sale",               "Hotel/Commercial"),
    # === Land ===
    ("farm parcel for sale",        "Land"),
    ("land plot for sale",          "Land"),
    ("plot for sale",               "Land"),
    ("parcel for sale",             "Land"),
    ("land for sale",               "Land"),
    ("farm for sale",               "Land"),
    ("island for sale",             "Land"),
]


# ============================================================================
# Village → municipality lookup (same pattern as openestate.gr)
# ============================================================================
_VILLAGE_TO_MUNICIPALITY: List[Tuple[str, str, str]] = [
    # TIER 1: SPECIFIC VILLAGES (longest compound first)
    ("nea kallikratia",  "Nea Propontida", "Nea Kallikratia"),
    ("nea moudania",     "Nea Propontida", "Nea Moudania"),
    ("nea potidaia",     "Nea Propontida", "Nea Potidaia"),
    ("nea potidea",      "Nea Propontida", "Nea Potidaia"),
    ("nea iraklia",      "Nea Propontida", "Nea Iraklia"),
    ("neos marmaras",    "Sithonia",       "Neos Marmaras"),
    ("neo marmaras",     "Sithonia",       "Neos Marmaras"),
    ("agios nikolaos",   "Sithonia",       "Agios Nikolaos"),
    ("nea skioni",       "Kassandra",      "Nea Skioni"),
    ("nea fokaia",       "Kassandra",      "Nea Fokea"),
    ("nea fokea",        "Kassandra",      "Nea Fokea"),
    ("kassandreia",      "Kassandra",      "Kassandreia"),
    ("ouranopolis",      "Aristotelis",    "Ouranopolis"),
    ("ouranoupolis",     "Aristotelis",    "Ouranopolis"),
    ("pefkochori",       "Kassandra",      "Pefkochori"),
    ("pefkohori",        "Kassandra",      "Pefkochori"),
    ("polychrono",       "Kassandra",      "Polychrono"),
    ("polichrono",       "Kassandra",      "Polychrono"),
    ("vourvourou",       "Sithonia",       "Vourvourou"),
    ("pyrgadikia",       "Aristotelis",    "Pyrgadikia"),
    ("kallikratia",      "Nea Propontida", "Nea Kallikratia"),
    ("ammouliani",       "Aristotelis",    "Ammouliani"),
    ("chanioti",         "Kassandra",      "Hanioti"),
    ("haniotis",         "Kassandra",      "Hanioti"),
    ("chaniotis",        "Kassandra",      "Hanioti"),
    ("kallithea",        "Kassandra",      "Kallithea"),
    ("kalithea",         "Kassandra",      "Kallithea"),
    ("kalandra",         "Kassandra",      "Kalandra"),
    ("moudania",         "Nea Propontida", "Nea Moudania"),
    ("kriopigi",         "Kassandra",      "Kriopigi"),
    ("paliouri",         "Kassandra",      "Paliouri"),
    ("polygyros",        "Polygyros",      "Polygyros"),
    ("polygiros",        "Polygyros",      "Polygyros"),
    ("gerakini",         "Polygyros",      "Gerakini"),
    ("hanioti",          "Kassandra",      "Hanioti"),
    ("portes",           "Kassandra",      "Portes"),
    ("ierissos",         "Aristotelis",    "Ierissos"),
    ("stratoni",         "Aristotelis",    "Stratoni"),
    ("stagira",          "Aristotelis",    "Stagira"),
    ("flogita",          "Nea Propontida", "Nea Flogita"),
    ("athytos",          "Kassandra",      "Afytos"),
    ("athitos",          "Kassandra",      "Afytos"),
    ("afytos",           "Kassandra",      "Afytos"),
    ("posidi",           "Kassandra",      "Posidi"),
    ("possidi",          "Kassandra",      "Posidi"),
    ("siviri",           "Kassandra",      "Siviri"),
    ("nikiti",           "Sithonia",       "Nikiti"),
    ("fourka",           "Kassandra",      "Fourka"),
    ("toroni",           "Sithonia",       "Toroni"),
    ("skioni",           "Kassandra",      "Nea Skioni"),
    ("sykia",            "Sithonia",       "Sykia"),
    ("sarti",            "Sithonia",       "Sarti"),
    ("pallini",          "Kassandra",      "Pallini"),
    ("sani",             "Kassandra",      "Sani"),
    # TIER 2: bare municipality fallbacks
    ("nea propontida",   "Nea Propontida", "Nea Propontida"),
    ("aristotelis",      "Aristotelis",    "Aristotelis"),
    ("kassandra",        "Kassandra",      "Kassandra"),
    ("sithonia",         "Sithonia",       "Sithonia"),
]


def _resolve_municipality(text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return (None, None)
    norm = unicodedata.normalize("NFD", text.lower())
    norm = "".join(c for c in norm if unicodedata.category(c) != "Mn")
    for needle, muni, display in _VILLAGE_TO_MUNICIPALITY:
        if needle in norm:
            return (muni, display)
    return (None, None)


# ============================================================================
# Helpers
# ============================================================================
def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


def _to_int_simple(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
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
    """`/propertyDetails/13010070` → `13010070`."""
    m = re.search(r"/propertyDetails/(\d+)", url)
    return m.group(1) if m else None


# ============================================================================
# Scraper
# ============================================================================
class KassandraPropertiesScraper(EnrichmentMixin, BaseScraper):
    """Kefalidis Real Estate scraper (Camoufox-based, PerimeterX bypass)."""

    name = "kassandra_properties"
    source_domain = _SOURCE_DOMAIN
    base_url = _BASE_URL

    # NLP fills only these (omit category — derived from title prefix)
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = type(self).source_domain
        # Cache populated in collect_urls, consumed in fetch_details
        self._detail_cache: Dict[str, Dict[str, Any]] = {}

    # ========================================================================
    # PHASE 1: collect_urls — opens Camoufox once, walks search + ALL details
    # ========================================================================
    async def collect_urls(
        self, min_price: int = 400_000,
    ) -> List[PropertyTemplate]:
        from camoufox.async_api import AsyncCamoufox

        seeds: List[PropertyTemplate] = []
        logger.info(f"[{self.source_domain}] Camoufox launching...")

        async with AsyncCamoufox(headless=True, humanize=False, geoip=True) as browser:
            page = await browser.new_page()

            # ---- Phase 1.1: walk 3 category search URLs, each paginated ----
            cards: List[Dict[str, Any]] = []
            seen_urls: set = set()
            max_pages = 20  # safety ceiling per category

            for category in _CATEGORIES:
                # Fresh page per category — fixes Playwright driver crash from
                # accumulated JS errors across category transitions
                page = await browser.new_page()
                # Swallow JS errors so driver doesn't crash on malformed pageError.location
                page.on("pageerror", lambda e: None)
                search_url = _SEARCH_URL_TEMPLATE.format(cat=category, min_price=min_price)
                logger.info(f"[{self.source_domain}] === category: {category} ===")
                page_num = 1
                cat_card_count_before = len(cards)

                while page_num <= max_pages:
                    page_url = (
                        search_url if page_num == 1
                        else f"{search_url}&page={page_num}"
                    )
                    logger.info(f"[{self.source_domain}] GET page {page_num}: {page_url}")
                    try:
                        await page.goto(page_url, wait_until="domcontentloaded", timeout=90000)
                        # Wait for cards to render. Sousouras renders cards via JS after DOMContentLoaded.
                        try:
                            await page.wait_for_selector('a.property-item', timeout=20000)
                        except Exception:
                            logger.warning(
                                f"[{self.source_domain}] page {page_num}: 'a.property-item' "
                                "didn't appear within 20s — page may be empty or selector changed"
                            )
                        await asyncio.sleep(2)
                    except Exception as exc:
                        logger.error(f"[{self.source_domain}] page {page_num} fetch: {exc!r}")
                        break

                    page_cards = await page.evaluate("""() => {
                    const out = [];
                    for (const a of document.querySelectorAll('a.property-item')) {
                        const href = a.getAttribute('href');
                        if (!href || !href.includes('/propertyDetails/')) continue;
                        const h3 = a.querySelector('h3');
                        const priceNode = a.querySelector('.price');
                        const areaNode = a.querySelector('.area');
                        const bg = a.querySelector('.bg-image');
                        let img = null;
                        if (bg) {
                            const m = (bg.getAttribute('style') || '').match(/url\\(["']?(https?:[^"')]+)/);
                            if (m) img = m[1];
                        }
                        out.push({
                            url: href.startsWith('http') ? href : ('https://www.kassandra-properties.gr' + href),
                            title: h3 ? h3.textContent.trim() : null,
                            price_text: priceNode ? priceNode.textContent.trim() : null,
                            area_text: areaNode ? areaNode.textContent.trim() : null,
                            thumb: img,
                            card_body: a.textContent.replace(/\\s+/g, ' ').trim().slice(0, 200),
                        });
                    }
                    return out;
                }""")
                    # Filter new URLs only (dedup across pages)
                    new_count = 0
                    for c in page_cards:
                        if c["url"] not in seen_urls:
                            seen_urls.add(c["url"])
                            cards.append(c)
                            new_count += 1

                    logger.info(
                        f"[{self.source_domain}] page {page_num}: {len(page_cards)} cards, "
                        f"{new_count} new (total seen: {len(cards)})"
                    )

                    # Stop conditions:
                    # - empty page → no more results
                    # - no new cards on this page → duplicate page, stop
                    if not page_cards or new_count == 0:
                        break

                    # Check pagination: is this the last page?
                    last_page = await page.evaluate("""() => {
                        const next = document.querySelector('.pagination .next');
                        return next ? next.classList.contains('disabled') : true;
                    }""")
                    if last_page:
                        logger.info(f"[{self.source_domain}] reached last page ({page_num})")
                        break

                    page_num += 1

                cat_new = len(cards) - cat_card_count_before
                logger.info(f"[{self.source_domain}] category {category}: {cat_new} new cards")
                # Close page to release resources before next category
                try:
                    await page.close()
                except Exception:
                    pass

            if not cards:
                logger.warning(f"[{self.source_domain}] empty search — no listings or selector changed")
                return []

            # ---- Phase 1.2: for each card, visit detail page, parse rich data ----
            # Fresh page for detail phase (previous category pages closed)
            page = await browser.new_page()
            page.on("pageerror", lambda e: None)
            for i, card in enumerate(cards, 1):
                url = card["url"]
                site_id = _extract_id_from_url(url)
                if not site_id:
                    logger.warning(f"[{self.source_domain}] cannot extract id from {url}")
                    continue
                title = _normalize_text(card.get("title"))

                logger.info(
                    f"[{self.source_domain}] [{i}/{len(cards)}] detail: {url} "
                    f"({card.get('price_text')} | {title[:40]})"
                )

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await asyncio.sleep(3)
                    html = await page.content()
                except Exception as exc:
                    logger.error(f"[{self.source_domain}] detail fetch error {url}: {exc!r}")
                    continue

                # Parse detail page → full data dict
                try:
                    detail_data = self._parse_detail_page(
                        html=html,
                        url=url,
                        site_id=site_id,
                        card_title=title,
                        card_price_text=card.get("price_text") or "",
                        card_area_text=card.get("area_text") or "",
                        card_body=card.get("card_body") or "",
                    )
                except Exception as exc:
                    logger.exception(f"[{self.source_domain}] detail parse error {url}")
                    continue

                # Cache for fetch_details() lookup
                self._detail_cache[url] = detail_data

                # Build PropertyTemplate seed with minimum required (URL + id + price hint
                # + location_raw for Halkidiki whitelist)
                seeds.append(PropertyTemplate(
                    site_property_id=site_id,
                    source_domain=self.source_domain,
                    url=url,
                    price=detail_data.get("price"),
                    location_raw=detail_data.get("location_raw"),
                ))

        logger.info(
            f"[{self.source_domain}] collect_urls done: "
            f"{len(seeds)} seeds, {len(self._detail_cache)} cached details"
        )
        return seeds

    # ========================================================================
    # PHASE 2: fetch_details — instant cache lookup
    # ========================================================================
    async def fetch_details(self, url: str) -> Dict[str, Any]:
        data = self._detail_cache.get(url, {}).copy()
        if not data:
            logger.warning(f"[{self.source_domain}] no cached detail for {url}")
            return {}
        # NLP fallback runs HERE (was deferred from collect_urls so it can act
        # on the final description). Then quality gate.
        self._apply_nlp_fallback(data)
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )
        return data

    # ========================================================================
    # Detail page parser
    # ========================================================================
    def _parse_detail_page(
        self,
        *,
        html: str,
        url: str,
        site_id: str,
        card_title: str,
        card_price_text: str,
        card_area_text: str,
        card_body: str,
    ) -> Dict[str, Any]:
        parser = LexborHTMLParser(html)
        data: Dict[str, Any] = {}
        extras: Dict[str, Any] = {}

        # ── title (detail h1 usually richer than card h3) ──
        h1 = parser.css_first(".property-content h1") or parser.css_first("h1")
        detail_title = _normalize_text(h1.text(strip=False)) if h1 else None
        title = detail_title or card_title
        if title:
            extras["title"] = title

        # ── price ──
        price_node = parser.css_first(".property-price-area .price") or parser.css_first(".price")
        price_text = price_node.text(strip=True) if price_node else card_price_text
        v = self._to_int_euro_safe(price_text)
        if v is not None:
            data["price"] = v

        # ── size (m²) ──
        area_node = parser.css_first(".property-price-area .area") or parser.css_first(".area")
        area_text = area_node.text(strip=True) if area_node else card_area_text
        v = _to_float_sqm(area_text)
        if v is not None:
            data["size_sqm"] = v

        # ── property code ──
        code_node = parser.css_first(".property-code")
        if code_node:
            code = re.sub(r"\D+", "", code_node.text(strip=True))
            if code:
                extras["property_code"] = code

        # ── category from card-title prefix ──
        if card_title:
            tl = card_title.lower()
            for needle, cat in _TITLE_PREFIX_TO_CATEGORY:
                if tl.startswith(needle):
                    data["category"] = cat
                    break
            # Fallback: substring match (detail h1 may not match prefix)
            if "category" not in data:
                for needle, cat in _TITLE_PREFIX_TO_CATEGORY:
                    if needle.replace(" for sale", "") in tl:
                        data["category"] = cat
                        break

        # ── location_raw — use card title (always contains "X (Municipality)") ──
        # detail h1 often more descriptive ("Paliouri-Pefkohori, Halkidiki: ...")
        # both are useful, combine for whitelist matching
        loc_parts = []
        if card_title:
            loc_parts.append(card_title)
        # Extract "in X" or "(X)" from titles
        if loc_parts:
            data["location_raw"] = " ".join(loc_parts)

        # ── description (narrative <p> inside .property-content if present) ──
        desc_node = parser.css_first(".property-content p")
        if desc_node:
            desc = _normalize_text(desc_node.text(strip=False))
            if desc and len(desc) > 50:
                data["description"] = desc
        if not data.get("description"):
            og = self._og_description_fallback(parser)
            if og:
                data["description"] = og

        # ── coords from map marker (.marker[data-lat][data-lng]) ──
        marker = parser.css_first(".marker[data-lat]")
        if marker:
            try:
                lat = float(marker.attributes.get("data-lat", ""))
                lng = float(marker.attributes.get("data-lng", ""))
                if _bbox_check(lat, lng):
                    data["latitude"] = lat
                    data["longitude"] = lng
            except (ValueError, TypeError):
                pass

        # ── specs table (.info-table tr) ──
        self._parse_specs_table(parser, data, extras)

        # ── amenities list (.property-amenities li) ──
        self._parse_amenities(parser, data, extras)

        # ── gallery (Swiper, data-src or background-image) ──
        images = self._extract_images(parser, html)
        if images:
            data["images"] = images

        # ── agent (parse from .contact-info) ──
        self._parse_agent(parser, extras)

        # ── municipality from card title (Hanioti, Moudania, etc.) ──
        muni, display_area = _resolve_municipality(card_title)
        if muni:
            data["calc_prefecture"] = "Halkidiki"
            data["calc_municipality"] = muni
            if display_area:
                data["calc_area"] = display_area

        # ── description fallback: synthesize from structured data when site
        # has no narrative <p> (most Kefalidis listings are pure spec sheets).
        # This gives hodu's Engine 1 enough text to embed and cluster on.
        if not data.get("description") or len(data.get("description", "")) < 50:
            data["description"] = self._synthesize_description(
                title=extras.get("title"),
                category=data.get("category"),
                area=data.get("calc_area"),
                municipality=data.get("calc_municipality"),
                price=data.get("price"),
                size_sqm=data.get("size_sqm"),
                land_size_sqm=data.get("land_size_sqm"),
                bedrooms=data.get("bedrooms"),
                bathrooms=data.get("bathrooms"),
                year_built=data.get("year_built"),
                extras=extras,
            )

        data["extra_features"] = extras
        return data

    @staticmethod
    def _synthesize_description(
        *,
        title: Optional[str],
        category: Optional[str],
        area: Optional[str],
        municipality: Optional[str],
        price: Optional[int],
        size_sqm: Optional[float],
        land_size_sqm: Optional[float],
        bedrooms: Optional[int],
        bathrooms: Optional[int],
        year_built: Optional[int],
        extras: Dict[str, Any],
    ) -> str:
        """Build a 200-300 char description from structured fields.

        Used when the site has no <p> narrative (typical for Kefalidis
        boutique listings — most are pure spec sheets).
        """
        parts: List[str] = []

        # Opening: location + category
        cat = category or "Property"
        loc_bits = []
        if area:
            loc_bits.append(area)
        if municipality and municipality != area:
            loc_bits.append(municipality)
        loc_bits.append("Halkidiki")
        parts.append(f"{cat} for sale in {', '.join(loc_bits)}.")

        # Sizes
        size_phrase_bits = []
        if size_sqm:
            size_phrase_bits.append(f"{int(size_sqm)}m² of living space")
        if land_size_sqm:
            size_phrase_bits.append(f"on a {int(land_size_sqm)}m² plot")
        if size_phrase_bits:
            parts.append(" ".join(size_phrase_bits) + ".")

        # Rooms
        room_bits = []
        if bedrooms:
            room_bits.append(f"{bedrooms} bedroom{'s' if bedrooms != 1 else ''}")
        if bathrooms:
            room_bits.append(f"{bathrooms} bathroom{'s' if bathrooms != 1 else ''}")
        if room_bits:
            parts.append("Featuring " + " and ".join(room_bits) + ".")

        # Year built / renovation
        if year_built:
            yb = f"Built in {year_built}"
            reno = extras.get("renovation_year")
            if reno and reno != year_built:
                yb += f", renovated in {reno}"
            parts.append(yb + ".")

        # Heating / energy
        if extras.get("heating"):
            parts.append(f"Heating: {extras['heating']}.")
        if extras.get("energy_class"):
            parts.append(f"Energy class: {extras['energy_class']}.")

        # Notable amenity flags (true booleans)
        AMENITY_LABELS = {
            "feature_swimming_pool": "swimming pool",
            "feature_pool": "swimming pool",
            "feature_view": "sea views",
            "feature_private_garden": "private garden",
            "feature_garden": "garden",
            "feature_parking": "parking",
            "feature_fireplace": "fireplace",
            "feature_balcony": "balcony",
            "feature_storage_space": "storage",
            "feature_alarm": "alarm system",
            "feature_solar_water_heating": "solar water heating",
            "feature_air_condition": "air conditioning",
            "feature_secure_door": "secure entry",
            "feature_furnished": "furnished",
            "feature_pets_welcome": "pet-friendly",
            "feature_facade": "street frontage",
            "feature_disabled_access": "disabled access",
            "feature_internal_stairs": "internal stairs",
            "feature_awning": "awning",
            "feature_satellite": "satellite TV",
            "feature_window_screens": "window screens",
        }
        amenities = [
            label for key, label in AMENITY_LABELS.items()
            if extras.get(key) is True
        ]
        if amenities:
            # Limit to top 8 for readability
            shown = amenities[:8]
            parts.append("Amenities: " + ", ".join(shown) + ".")

        # Named features (lot specifics)
        if extras.get("distance_to_sea_m"):
            parts.append(f"Distance to sea: {extras['distance_to_sea_m']}m.")
        if extras.get("orientation"):
            parts.append(f"Orientation: {extras['orientation']}.")
        if extras.get("road_type"):
            parts.append(f"Access: {extras['road_type']}.")

        # Status flags from .info-table (e.g. "Renovated, Painted, Furnished")
        if extras.get("status_flags"):
            parts.append(f"Status: {extras['status_flags']}.")
        if extras.get("use_type"):
            parts.append(f"Suitable for: {extras['use_type']}.")
        if extras.get("extra_flags"):
            parts.append(f"Special features: {extras['extra_flags']}.")

        return " ".join(parts)

    def _parse_specs_table(self, parser, data: Dict[str, Any], extras: Dict[str, Any]) -> None:
        """Walk <table class='info-table'> th/td pairs."""
        for tr in parser.css("table.info-table tr"):
            th = tr.css_first("th")
            td = tr.css_first("td")
            if not th or not td:
                continue
            label = _normalize_text(th.text(strip=False)).lower().rstrip(":")
            value = _normalize_text(td.text(strip=False))
            if not label or not value:
                continue

            if label == "rooms":
                v = _to_int_simple(value)
                if v is not None:
                    data["bedrooms"] = v
            elif label == "bathrooms":
                v = _to_int_simple(value)
                if v is not None:
                    data["bathrooms"] = v
            elif label == "construction year":
                v = _to_int_simple(value)
                if v is not None:
                    data["year_built"] = v
            elif label == "renovation year":
                v = _to_int_simple(value)
                if v is not None:
                    extras["renovation_year"] = v
            elif label == "levels":
                v = _to_int_simple(value)
                if v is not None:
                    data["levels"] = str(v)
            elif label == "kitchens":
                v = _to_int_simple(value)
                if v is not None:
                    extras["kitchens"] = v
            elif label == "living rooms":
                v = _to_int_simple(value)
                if v is not None:
                    extras["living_rooms"] = v
            elif label == "wc":
                v = _to_int_simple(value)
                if v is not None:
                    extras["wc_count"] = v
            elif label == "energy class":
                # Energy class label is text inside .energy div
                e_node = td.css_first(".energy")
                if e_node:
                    extras["energy_class"] = _normalize_text(e_node.text(strip=False))
                else:
                    extras["energy_class"] = value
            elif label == "floor":
                extras["floor"] = value
            elif label == "heating system":
                extras["heating"] = value
            elif label == "neighborhood":
                # Already captured in location_raw via card title — but also set area
                if not data.get("location_raw"):
                    data["location_raw"] = value
            elif label == "zone":
                extras["zone"] = value
            elif label == "price per m²":
                v = self._to_int_euro_safe(value)
                if v is not None:
                    extras["price_per_sqm_eur"] = v
            elif label == "status":
                extras["status_flags"] = value
            elif label == "type":
                extras["use_type"] = value
            elif label == "extra":
                extras["extra_flags"] = value
            elif label == "parking spot":
                extras["feature_parking"] = (value.lower() == "yes")

    def _parse_amenities(self, parser, data: Dict[str, Any], extras: Dict[str, Any]) -> None:
        """Walk .property-amenities li — boolean flags and named numeric features."""
        for li in parser.css(".property-amenities li"):
            raw = _normalize_text(li.text(strip=False))
            if not raw:
                continue
            # Check for "Label: value" patterns
            if ":" in raw:
                k, _, v = raw.partition(":")
                slug = re.sub(r"[^a-z0-9]+", "_", k.strip().lower()).strip("_")
                value = _normalize_text(v)
                # Map known numerics to top-level columns
                if slug == "lot_size":
                    fv = _to_float_sqm(value)
                    if fv is not None:
                        data["land_size_sqm"] = fv
                elif slug == "distance_from_sea_m":
                    extras["distance_to_sea_m"] = _to_int_simple(value)
                elif slug == "size_of_balconies":
                    extras["balcony_area_sqm"] = _to_float_sqm(value)
                elif slug == "orientation":
                    extras["orientation"] = value
                elif slug == "road_type":
                    extras["road_type"] = value
                elif slug == "frames_type":
                    extras["joinery_type"] = value
                elif slug == "glass_type":
                    extras["glass_type"] = value
                elif slug == "floors_type":
                    extras["floor_type"] = value
                else:
                    extras[f"feature_{slug}"] = value
            else:
                # Boolean flag
                slug = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
                if slug:
                    extras[f"feature_{slug}"] = True

    def _extract_images(self, parser, html_text: str) -> List[str]:
        """Gallery: Swiper carousel with data-src (lazy) or background-image style."""
        seen: set = set()
        images: List[str] = []

        # Try Swiper slides
        for slide in parser.css(".swiper-slide"):
            # Skip duplicate slides used by Swiper for infinite scroll
            cls = slide.attributes.get("class", "") or ""
            if "swiper-slide-duplicate" in cls:
                continue
            # data-src on inner img
            img = slide.css_first("img[data-src]") or slide.css_first("img[src]")
            if img:
                src = (img.attributes.get("data-src") or img.attributes.get("src") or "").strip()
                if src.startswith("http") and src not in seen and not src.endswith(".svg"):
                    seen.add(src)
                    images.append(src)
                    continue
            # background-image fallback
            inner = slide.css_first(".swiper-slide-inner")
            if inner:
                style = inner.attributes.get("style", "") or ""
                m = re.search(r'url\(["\']?(https?:[^"\')]+)', style)
                if m:
                    src = m.group(1)
                    if src not in seen and not src.endswith(".svg"):
                        seen.add(src)
                        images.append(src)

        # Final fallback: all <img> tags inside swiper-container
        if not images:
            for img in parser.css(".swiper-container img"):
                src = (img.attributes.get("data-src") or img.attributes.get("src") or "").strip()
                if src.startswith("http") and src not in seen and not src.endswith(".svg"):
                    seen.add(src)
                    images.append(src)

        return images

    def _parse_agent(self, parser, extras: Dict[str, Any]) -> None:
        """Extract contact info from .contact-info."""
        # Static agency-level defaults
        extras["agent_company"] = "KASSANDRA PROPERTIES VIP"
        extras["agent_email"] = "info@kassandra-properties.gr"

        # Contact person name from first .contact-info li
        for li in parser.css(".contact-info li"):
            txt = _normalize_text(li.text(strip=False))
            low = txt.lower()
            if low.startswith("contact:") or low.startswith("contact :"):
                name = txt.split(":", 1)[1].strip()
                if name:
                    extras["agent_name"] = name
                break

        # Phone numbers — collect all unique tel: links
        phones = []
        for a in parser.css('a[href^="tel:"]'):
            ph = (a.attributes.get("href", "") or "").replace("tel:", "").strip()
            if ph and ph not in phones:
                phones.append(ph)
        if phones:
            extras["agent_phones"] = phones
            extras["agent_phone_1"] = phones[0]
            if len(phones) > 1:
                extras["agent_phone_2"] = phones[1]
