"""poseidonrealestate.gr scraper.

Site: Poseidon Real Estate (https://poseidonrealestate.gr)
Stack: PHP/RealStatus broker CMS (iarts.gr) + nginx + Leaflet maps + jQuery
Scope: Halkidiki (R/4019), for sale (for/1), >= EUR 400k = ~13 properties
Anti-bot: Stage 0 sufficient (curl_cffi chrome120, no CF/PerimeterX)
Language: Greek by default, but ?language=en gives clean English labels
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


# Greek-normalize for fuzzy matching
def _normalize_greek(s: str) -> str:
    if not s:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", s.lower())
        if unicodedata.category(c) != "Mn"
    )


# Halkidiki bbox sanity
_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)


def _bbox_check(lat: float, lng: float) -> bool:
    lat_min, lng_min, lat_max, lng_max = _HALKIDIKI_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


class PoseidonRealEstateScraper(EnrichmentMixin, BaseScraper):
    """Scraper for poseidonrealestate.gr (Poseidon Real Estate, Kassandra HQ)."""

    name = "poseidonrealestate"
    source_domain = "poseidonrealestate.gr"
    base_url = "https://poseidonrealestate.gr"

    _LISTING_URL = (
        "https://poseidonrealestate.gr/listings/for/1/N/196/R/4019"
        "/sortby/dateDesc/priceFrom/{min_price}"
    )
    _MAX_PAGES = 6                   # 13 results across ~2 pages, generous margin
    _INTER_PAGE_SLEEP_SEC = 1.0

    # Categories — site uses /category/{1,2,3} = Residential/Commercial/Land
    # We do NOT filter by category at URL level (capture all). For per-property
    # categorization we use breadcrumb/title heuristics inside fetch_details.

    # Property type slug detection (English labels after ?language=en)
    _TITLE_TO_CATEGORY: List[Tuple[str, str]] = [
        # Specific/premium structural types FIRST so "villa" wins over "apartment"
        # mentioned in description (e.g. "Luxury villa... independent apartment...")
        ("mansion", "Villa"),
        ("villa", "Villa"),
        ("maisonette", "Maisonette"),
        ("detached house", "Detached House"),
        ("detached home", "Detached House"),
        ("hotel", "Hotel"),
        ("complex", "Complex"),
        # Commercial (BEFORE apartment — "Shop + apartment" is primarily a shop)
        ("shop", "Hotel/Commercial"),
        ("store", "Hotel/Commercial"),
        ("commercial", "Hotel/Commercial"),
        ("business", "Hotel/Commercial"),
        ("office", "Hotel/Commercial"),
        # Generic residential
        ("apartment", "Apartment"),
        ("flat", "Apartment"),
        ("studio", "Apartment"),
        ("building", "Complex"),
        # Land
        ("land", "Land"),
        ("plot", "Land"),
        ("agricultural", "Land"),
        ("field", "Land"),
        ("oikopedo", "Land"),
        # Last-resort fallback (very generic "house" word)
        ("house", "Detached House"),
    ]

    # NLP fallback fills only these (omit category — title scan is authoritative)
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = type(self).source_domain

    # ------------------------------------------------------------------
    # Phase 1: collect_urls
    # ------------------------------------------------------------------
    async def collect_urls(
        self, min_price: int = 400_000,
    ) -> List[PropertyTemplate]:
        seeds: Dict[str, PropertyTemplate] = {}
        base = self._LISTING_URL.format(min_price=min_price)

        for page in range(1, self._MAX_PAGES + 1):
            url = base if page == 1 else f"{base}/page/{page}"
            url += "?language=en"
            logger.info(f"[{self.source_domain}] page {page} GET {url}")

            try:
                resp = await self.client.get(url)
            except Exception as exc:
                logger.error(
                    f"[{self.source_domain}] page {page} fetch failed: {exc!r}"
                )
                break
            if resp.status_code != 200:
                logger.warning(
                    f"[{self.source_domain}] page {page}: HTTP {resp.status_code}"
                )
                break

            parser = LexborHTMLParser(resp.text)
            cards = parser.css(".property-item")
            logger.info(
                f"[{self.source_domain}] page {page}: {len(cards)} cards"
            )

            page_new = 0
            for card in cards:
                try:
                    seed = self._parse_card(card)
                except Exception as exc:
                    logger.error(
                        f"[{self.source_domain}] card parse: {exc!r}"
                    )
                    continue
                if not seed or not seed.url:
                    continue
                if seed.url not in seeds:
                    seeds[seed.url] = seed
                    page_new += 1

            logger.info(
                f"[{self.source_domain}] page {page} -> {page_new} new "
                f"(total {len(seeds)})"
            )
            if page_new == 0:
                break
            await asyncio.sleep(self._INTER_PAGE_SLEEP_SEC)

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} seeds"
        )
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        # site_property_id from card's data-id
        site_id = (card.attributes.get("data-id") or "").strip() or None
        if not site_id:
            return None

        # Detail URL from <a class="prop-link" href="...">
        a = card.css_first("a.prop-link")
        if not a:
            return None
        url = (a.attributes.get("href") or "").strip()
        if not url or "/property/" not in url:
            return None
        url = url.split("?")[0].split("#")[0].rstrip("/")
        # Ensure normalized form (no trailing slash)

        # Price hint from .listing-price .price-p span.fw-bold ("695.000 EUR")
        price = None
        price_node = card.css_first(".listing-price .price-p span.fw-bold")
        if price_node:
            price = self._to_int_euro_safe(price_node.text(strip=False))

        # Location chain from <span><i class="la-map-marker-alt"></i> X, Y, Z</span>
        location_raw = None
        for span in card.css(".card-body > span"):
            if span.css_first("i.la-map-marker-alt"):
                location_raw = _normalize_text(span.text(strip=False))
                break

        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=url,
            price=price,
            location_raw=location_raw,
        )

    # ------------------------------------------------------------------
    # Phase 2: fetch_details
    # ------------------------------------------------------------------
    async def fetch_details(self, url: str) -> Dict[str, Any]:
        # Force English for consistent label parsing
        fetch_url = url + ("&" if "?" in url else "?") + "language=en"
        try:
            resp = await self.client.get(fetch_url)
        except Exception as exc:
            logger.error(
                f"[{self.source_domain}] detail fetch error for {url}: {exc!r}"
            )
            return {}
        if resp.status_code != 200:
            logger.warning(
                f"[{self.source_domain}] detail {url}: HTTP {resp.status_code}"
            )
            return {}

        parser = LexborHTMLParser(resp.text)
        data: Dict[str, Any] = {}
        extras: Dict[str, Any] = {}

        # ── title (English h1)
        h1 = parser.css_first(".property-title h1")
        title = _normalize_text(h1.text(strip=False)) if h1 else None
        if title:
            extras["title"] = title

        # ── price ── .property-price span (e.g. "695.000 EUR")
        price_node = parser.css_first(".property-price span")
        if price_node:
            v = self._to_int_euro_safe(price_node.text(strip=False))
            if v is not None:
                data["price"] = v

        # ── location_raw ── .property-title span (with map-marker icon)
        for span in parser.css(".property-title span"):
            if span.css_first("i.la-map-marker"):
                location_raw = _normalize_text(span.text(strip=False))
                if location_raw:
                    data["location_raw"] = location_raw
                break

        # ── category (from title)
        if title:
            tl = title.lower()
            for needle, cat in self._TITLE_TO_CATEGORY:
                if needle in tl:
                    data["category"] = cat
                    break

        # ── description
        desc_p = parser.css_first(".property-details p")
        if desc_p:
            desc = _normalize_text(desc_p.text(strip=False))
            if desc:
                data["description"] = desc
        if not data.get("description"):
            og = self._og_description_fallback(parser)
            if og:
                data["description"] = og

        # ── information panel (structured key:value)
        self._parse_information_panel(parser, data, extras)

        # ── additional features list
        self._parse_features_list(parser, extras)

        # ── distances
        self._parse_distances(parser, extras)

        # ── energy class — `active` class is added client-side via JS,
        # so server-side static HTML has no `.energy.active`. Parse the
        # inline `var energy = '<N>';` then look up the matching data-id node.
        em = re.search(r"\bvar\s+energy\s*=\s*['\"](\d+)['\"]", resp.text)
        if em and em.group(1):
            energy_id = em.group(1)
            energy_node = parser.css_first(f'.energy[data-id="{energy_id}"]')
            if energy_node:
                energy_label = _normalize_text(energy_node.text(strip=False))
                if energy_label:
                    extras["energy_class"] = energy_label

        # ── coordinates from inline JS  (var lat = X; var long = Y;)
        lat, lng = self._extract_coords(resp.text)
        if lat is not None and lng is not None and _bbox_check(lat, lng):
            data["latitude"] = lat
            data["longitude"] = lng

        # ── images (gallery + hidden lazy-loaded)
        images = self._extract_images(parser)
        if images:
            data["images"] = images

        # ── agent metadata (hardcoded — single-agency site)
        extras["agent_name"] = "Poseidon Real Estate"
        extras["agent_phone_1"] = "+30 2374 071097"
        extras["agent_phone_2"] = "+30 6974 580828"
        extras["agent_email"] = "info@poseidonrealestate.gr"

        data["extra_features"] = extras

        # Step 5: NLP fallback
        self._apply_nlp_fallback(data)

        # Step 7: quality gate
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )

        return data

    # ------------------------------------------------------------------
    # Detail extraction helpers
    # ------------------------------------------------------------------
    def _parse_information_panel(self, parser, data: Dict[str, Any], extras: Dict[str, Any]) -> None:
        """Walk .property-inform .information-list li for structured fields."""
        for li in parser.css(".property-inform .information-list li"):
            raw = _normalize_text(li.text(strip=False))
            if ":" not in raw:
                continue
            label, _, value = raw.partition(":")
            label = label.strip().lower()
            value = value.strip()
            if not value:
                continue

            if label == "code":
                extras["property_code"] = value
            elif label == "area":
                # "211 sq.m" -> 211
                m = re.search(r"\d[\d.,]*", value)
                if m:
                    try:
                        data["size_sqm"] = float(m.group(0).replace(",", "").replace(".", ""))
                    except ValueError:
                        pass
            elif label == "plot area":
                m = re.search(r"\d[\d.,]*", value)
                if m:
                    try:
                        data["land_size_sqm"] = float(m.group(0).replace(",", "").replace(".", ""))
                    except ValueError:
                        pass
            elif label == "rooms":
                m = re.search(r"\d+", value)
                if m:
                    data["bedrooms"] = int(m.group(0))
            elif label == "bathroom":
                m = re.search(r"\d+", value)
                if m:
                    data["bathrooms"] = int(m.group(0))
            elif label == "levels":
                m = re.search(r"\d+", value)
                if m:
                    data["levels"] = str(m.group(0))
            elif label == "year of manufacture":
                m = re.search(r"\d{4}", value)
                if m:
                    data["year_built"] = int(m.group(0))
            elif label == "renovation year":
                m = re.search(r"\d{4}", value)
                if m:
                    extras["renovation_year"] = int(m.group(0))
            elif label == "floor":
                extras["floor"] = value
            elif label == "heating":
                extras["heating"] = value
            elif label == "status":
                extras["condition"] = value
            elif label == "last updated":
                extras["site_last_updated_raw"] = value

    def _parse_features_list(self, parser, extras: Dict[str, Any]) -> None:
        """Walk .property-feautures (typo in HTML) li/span and route into extras."""
        for li in parser.css(".property-feautures li span"):
            raw = _normalize_text(li.text(strip=False))
            if not raw:
                continue
            if ":" in raw:
                k, _, v = raw.partition(":")
                slug = re.sub(r"[^a-z0-9]+", "_", k.strip().lower()).strip("_")
                extras[f"feature_{slug}"] = v.strip()
            else:
                slug = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
                if slug:
                    extras[f"feature_{slug}"] = True

    def _parse_distances(self, parser, extras: Dict[str, Any]) -> None:
        """The 'Distance from' block — second .information-list under col-lg-8."""
        # Heuristic: find any <li> with text "Sea:" "Village:" etc.
        for li in parser.css(".information-list li"):
            raw = _normalize_text(li.text(strip=False))
            for needle, key in (
                ("Sea:", "distance_to_sea"),
                ("Village:", "distance_to_village"),
                ("Λεωφορείο:", "distance_to_bus"),
                ("Bus:", "distance_to_bus"),
                ("Airport:", "distance_to_airport"),
            ):
                if raw.lower().startswith(needle.lower()):
                    extras[key] = raw.split(":", 1)[1].strip()
                    break

    @staticmethod
    def _extract_coords(html_text: str) -> Tuple[Optional[float], Optional[float]]:
        """Pull lat/lng from inline script: var lat = X; var long = Y;"""
        lat_m = re.search(r"\bvar\s+lat\s*=\s*(-?\d+\.\d+)", html_text)
        lng_m = re.search(r"\bvar\s+long\s*=\s*(-?\d+\.\d+)", html_text)
        if lat_m and lng_m:
            try:
                return float(lat_m.group(1)), float(lng_m.group(1))
            except ValueError:
                pass
        return None, None

    @staticmethod
    def _extract_images(parser) -> List[str]:
        """Collect all gallery image URLs (includes hidden display:none ones)."""
        seen: set = set()
        images: List[str] = []
        for a in parser.css(".gallery a.card-img[href]"):
            href = (a.attributes.get("href") or "").strip()
            if not href.startswith("http"):
                continue
            if href.lower().endswith(".svg"):
                continue
            if href not in seen:
                seen.add(href)
                images.append(href)
        return images
