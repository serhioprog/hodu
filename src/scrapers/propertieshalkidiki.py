"""propertieshalkidiki.gr scraper — Sprint 11 #4.

Site: GK Real Estate Halkidiki (https://propertieshalkidiki.gr)
Stack: WordPress 7.0 + Elementor + g5ere/g5core custom theme + Cloudflare.
Scope: /property-state/chalkidiki/ — ~98 properties across 10 pages.
Anti-bot: passes Stage 0 (curl_cffi chrome120).
Language: Greek-only (no language toggle).
"""

from __future__ import annotations

import asyncio
import json
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple


def _normalize_greek(s: str) -> str:
    """Lowercase and strip diacritics for fuzzy Greek text matching."""
    if not s:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", s.lower())
        if unicodedata.category(c) != "Mn"
    )


# (Greek place name as it commonly appears in titles, normalized English form)
_TITLE_LOC_PAIRS: List[Tuple[str, str]] = [
    ("Νέα Φλογητά", "Nea Flogita"),
    ("Νέα Φώκαια", "Nea Fokaia"),
    ("Νέα Ηράκλεια", "Nea Iraklia"),
    ("Νέα Καλλικράτεια", "Nea Kallikrateia"),
    ("Καλλικράτεια", "Kallikrateia"),
    ("Νέα Ποτίδαια", "Nea Potidaia"),
    ("Ποτίδαια", "Potidaia"),
    ("Νέα Μουδανιά", "Nea Moudania"),
    ("Μουδανιά", "Moudania"),
    ("Νέα Σκιώνη", "Nea Skioni"),
    ("Νέα Πλάγια", "Nea Plagia"),
    ("Σταυρονικήτα", "Stavronikita"),
    ("Πολύχρονο", "Polychrono"),
    ("Πευκοχώρι", "Pefkochori"),
    ("Χανιώτη", "Chanioti"),
    ("Κρυοπηγή", "Kriopigi"),
    ("Παλιούρι", "Paliouri"),
    ("Φούρκα", "Fourka"),
    ("Αφυτος", "Afytos"),
    ("Άθυτος", "Athytos"),
    ("Καλάνδρα", "Kalandra"),
    ("Κασσάνδρεια", "Kassandreia"),
    ("Κασσάνδρα", "Kassandra"),
    ("Σάνη", "Sani"),
    ("Σάνι", "Sani"),
    ("Νικήτη", "Nikiti"),
    ("Νέος Μαρμαράς", "Neos Marmaras"),
    ("Σάρτη", "Sarti"),
    ("Βουρβουρού", "Vourvourou"),
    ("Ουρανούπολη", "Ouranoupoli"),
    ("Ιερισσός", "Ierissos"),
    ("Στρατώνι", "Stratoni"),
    ("Ολυμπιάδα", "Olympiada"),
    ("Πυργαδίκια", "Pyrgadikia"),
    ("Μεταμόρφωση", "Metamorfosi"),
    ("Καλλιθέα", "Kallithea"),
    ("Άγιος Νικόλαος", "Agios Nikolaos"),
    ("Άγιος Παύλος", "Agios Pavlos"),
    ("Άγιος Μάμας", "Agios Mamas"),
    ("Πορταριά", "Portaria"),
]

# Pre-compute normalized lookup, longest first (so "Νέα Φλογητά" matches before "Νέα")
_TITLE_LOC_LOOKUP: List[Tuple[str, str]] = sorted(
    [(_normalize_greek(gr), en) for gr, en in _TITLE_LOC_PAIRS],
    key=lambda x: -len(x[0]),
)

from curl_cffi.requests import AsyncSession
from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.models.schemas import PropertyTemplate
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.scrapers.base import BaseScraper


class PropertiesHalkidikiScraper(EnrichmentMixin, BaseScraper):
    """Scraper for propertieshalkidiki.gr (GK Real Estate Halkidiki)."""

    name = "propertieshalkidiki"
    source_domain = "propertieshalkidiki.gr"
    base_url = "https://propertieshalkidiki.gr"

    _BUCKET = "https://propertieshalkidiki.gr/property-state/chalkidiki/page/{page}/"
    _MAX_PAGES = 10  # site has exactly 10 pages per pagination HTML
    _INTER_PAGE_SLEEP_SEC = 3.0

    # WordPress property-type taxonomy slug → hodu category
    _CATEGORY_MAP: Dict[str, Optional[str]] = {
        # Canonical hodu vocabulary (per existing DB category column)
        "mezoneta": "Maisonette",
        "monokatoikia": "Detached House",
        "vila": "Villa",
        "diamerisma": "Apartment",
        "diamerismata": "Apartment",
        "oikopedo": "Land",
        "agrotemachio": "Land",
        "archontiko": "Villa",            # mansion → closest canonical
        "xenodocheio": "Hotel",
        "katastimata": "Hotel/Commercial",
        "sygkrotima": "Complex",
        # Adjective-style slugs (always co-tagged with a structural type)
        "neodmito": None,
        "anakainismeno": None,
    }

    # Priority: more specific structural type wins when multiple types are present
    _TYPE_PRIORITY: Tuple[str, ...] = (
        "archontiko", "vila", "xenodocheio", "sygkrotima",
        "mezoneta", "monokatoikia", "diamerismata", "diamerisma",
        "katastimata", "oikopedo", "agrotemachio",
    )

    # Halkidiki city slug → human-readable location
    _CITY_SLUG_OVERRIDES: Dict[str, str] = {
        "nea-fokaia": "Nea Fokaia",
        "nea-flogita": "Nea Flogita",
        "nea-kallikrateia": "Nea Kallikrateia",
        "nea-moudania": "Nea Moudania",
        "nea-potidaia": "Nea Potidaia",
        "nea-iraklia": "Nea Iraklia",
        "nea-irakleia": "Nea Iraklia",
        "nea-skioni": "Nea Skioni",
        "agios-nikolaos-chalkidikis": "Agios Nikolaos",
        "agios-pavlos": "Agios Pavlos",
        "agios-mamas": "Agios Mamas",
        "portaria-chalkidikis": "Portaria",
        "kallithea-chalkidikis": "Kallithea",
        "polychrono": "Polychrono",
        "pefkochori": "Pefkochori",
        "chanioti": "Chanioti",
        "kassandreia": "Kassandreia",
        "kassandra": "Kassandra",
        "sani": "Sani",
        "kalandra": "Kalandra",
        "kriopigi": "Kriopigi",
        "fourka": "Fourka",
        "paliouri": "Paliouri",
        "athytos": "Athytos",
        "afytos": "Afytos",
        "nikiti": "Nikiti",
        "neos-marmaras": "Neos Marmaras",
        "sarti": "Sarti",
        "vourvourou": "Vourvourou",
        "ouranoupoli": "Ouranoupoli",
        "ierissos": "Ierissos",
        "stratoni": "Stratoni",
        "olympiada": "Olympiada",
        "pyrgadikia": "Pyrgadikia",
        "metamorfosi": "Metamorfosi",
        "kalyves-polygyrou": "Kalyves Polygyrou",
        "stavronikita": "Stavronikita",
        "salonikiou": "Akti Salonikiou",
        "kassandrino": "Kassandrino",
        # Agency-specific slug variants observed in full scrape:
        "flogita": "Nea Flogita",          # short form without "nea-" prefix
        "ag-mamas": "Agios Mamas",         # abbreviated "ag-" instead of "agios-"
        "kryopigi": "Kriopigi",            # "y" spelling variant
    }

    # NLP fills only these columns (omit "category" — body taxonomy authoritative)
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
    )
    _NLP_TO_STRUCTURAL: Dict[str, str] = {}

    def __init__(self) -> None:
        super().__init__()
        # BaseScraper.__init__ writes self.source_domain="" — restore class attr
        self.source_domain = type(self).source_domain

    # ------------------------------------------------------------------
    # Direct curl_cffi fetch (bypasses the funnel)
    # ------------------------------------------------------------------
    # The site's WP middleware is buggy: /property-state/chalkidiki/page/2/
    # consistently returns HTTP 500 with valid content (7 cards, full HTML).
    # The funnel treats 500 as non-escalating and gives up, losing real data.
    # We fetch directly, accept 500 IF body contains g5ere markers, retry once.
    async def _fetch_page(self, url: str, retries: int = 1) -> Optional[str]:
        for attempt in range(1, retries + 2):
            try:
                async with AsyncSession() as s:
                    r = await s.get(url, impersonate="chrome120", timeout=60)
            except Exception as exc:
                logger.warning(
                    f"[{self.source_domain}] fetch attempt {attempt} "
                    f"failed for {url}: {exc!r}"
                )
                if attempt > retries:
                    return None
                await asyncio.sleep(2 ** attempt)
                continue
            if r.status_code not in (200, 500):
                logger.warning(
                    f"[{self.source_domain}] {url}: HTTP {r.status_code}"
                )
                if attempt > retries:
                    return None
                await asyncio.sleep(2 ** attempt)
                continue
            # Even 500 may have valid content — check for theme markers
            if "g5ere__" not in r.text:
                logger.warning(
                    f"[{self.source_domain}] {url}: no g5ere markers in body "
                    f"(status={r.status_code}, length={len(r.text)})"
                )
                return None
            if r.status_code == 500:
                logger.info(
                    f"[{self.source_domain}] {url}: HTTP 500 but valid content "
                    f"(WP middleware bug, parsing anyway)"
                )
            return r.text
        return None

    # ------------------------------------------------------------------
    # URL collection (returns List[PropertyTemplate] seeds)
    # ------------------------------------------------------------------
    async def collect_urls(
        self, min_price: int = 400_000,
    ) -> List[PropertyTemplate]:
        """Walk all pages of /property-state/chalkidiki/ and parse cards into seeds.

        ``min_price`` is accepted for runner compatibility but NOT enforced here:
        the site has no native price filter on the state archive, and all 98
        properties go through Engine 1's quality gate downstream.
        """
        seeds: Dict[str, PropertyTemplate] = {}
        failed_pages: List[int] = []

        for page in range(1, self._MAX_PAGES + 1):
            page_url = self._BUCKET.format(page=page)
            logger.info(f"[{self.source_domain}] page {page} GET {page_url}")
            html = await self._fetch_page(page_url)
            if html is None:
                logger.warning(
                    f"[{self.source_domain}] page {page}: skipping (fetch failed)"
                )
                failed_pages.append(page)
                # Don't break — try the next page in case this one is broken
                await asyncio.sleep(self._INTER_PAGE_SLEEP_SEC)
                continue

            parser = LexborHTMLParser(html)
            cards = parser.css(".g5ere__property-item")
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
                f"[{self.source_domain}] page {page} → {page_new} new "
                f"(total {len(seeds)})"
            )
            if page_new == 0:
                break
            await asyncio.sleep(self._INTER_PAGE_SLEEP_SEC)

        # Retry pass for any pages that failed during the main loop.
        # The site is slow/flaky; a single cool-down often unblocks them.
        if failed_pages:
            logger.info(
                f"[{self.source_domain}] retry pass for {len(failed_pages)} "
                f"failed page(s): {failed_pages}"
            )
            await asyncio.sleep(10)  # cool-down
            for page in failed_pages:
                page_url = self._BUCKET.format(page=page)
                logger.info(
                    f"[{self.source_domain}] retry page {page} GET {page_url}"
                )
                html = await self._fetch_page(page_url, retries=2)
                if html is None:
                    logger.warning(
                        f"[{self.source_domain}] retry page {page}: still failing"
                    )
                    continue
                parser = LexborHTMLParser(html)
                cards = parser.css(".g5ere__property-item")
                retry_new = 0
                for card in cards:
                    try:
                        seed = self._parse_card(card)
                    except Exception as exc:
                        logger.error(
                            f"[{self.source_domain}] retry card parse: {exc!r}"
                        )
                        continue
                    if seed and seed.url and seed.url not in seeds:
                        seeds[seed.url] = seed
                        retry_new += 1
                logger.info(
                    f"[{self.source_domain}] retry page {page} → {retry_new} new "
                    f"(total {len(seeds)})"
                )
                await asyncio.sleep(self._INTER_PAGE_SLEEP_SEC)

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} seeds"
        )
        return list(seeds.values())

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """Extract URL + site_property_id + title/price from a list-page card."""
        # URL — try title link first, fall back to thumbnail link
        url: Optional[str] = None
        a = card.css_first(".g5ere__loop-property-title a[href]")
        if not a:
            a = card.css_first(".g5core__entry-thumbnail[href]")
        if a:
            href = (a.attributes.get("href", "") or "").strip()
            if href and "/property/" in href.lower():
                url = href.split("?")[0].split("#")[0]
        if not url:
            return None
        if "__trashed" in url.lower():
            return None
        if url.rstrip("/").lower().endswith("/property"):
            return None

        # site_property_id — from data-id attr, fall back to post-{N} class
        site_id: Optional[str] = (card.attributes.get("data-id") or "").strip() or None
        if not site_id:
            classes = (card.attributes.get("class") or "").split()
            for c in classes:
                m = re.match(r"post-(\d+)$", c)
                if m:
                    site_id = m.group(1)
                    break
        if not site_id:
            return None

        # Optional card-level price (final price comes from detail page)
        price_eur: Optional[float] = None
        price_node = card.css_first(".g5ere__lpp-price")
        if price_node:
            digits = re.sub(r"[^\d]", "", price_node.text(strip=True))
            if digits:
                try:
                    price_eur = float(digits)
                except ValueError:
                    pass

        # Optional title (replaced by detail-page H1)
        title: Optional[str] = None
        title_node = card.css_first(".g5ere__loop-property-title")
        if title_node:
            t = title_node.text(strip=True)
            if t:
                title = t

        # Don't put card_title in seed.extra_features — fetch_details will
        # set authoritative "title" key and the dict-merge in daily_sync
        # overrides extra_features wholesale (shallow merge).
        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=url,
            price=int(price_eur) if price_eur is not None else None,
        )

    # ------------------------------------------------------------------
    # Detail extraction
    # ------------------------------------------------------------------
    async def fetch_details(self, url: str) -> Dict[str, Any]:
        html = await self._fetch_page(url)
        if html is None:
            logger.warning(
                f"[{self.source_domain}] detail {url}: fetch failed"
            )
            return {}

        parser = LexborHTMLParser(html)
        body = parser.css_first("body")
        body_classes = (body.attributes.get("class", "") if body else "") or ""
        # Property taxonomies (type/state/city/feature) live on inner content div,
        # NOT on the <body> element. Body only holds postid-{N}.
        content_div = parser.css_first('[id^="property-"]')
        content_classes = (
            content_div.attributes.get("class", "") if content_div else ""
        ) or ""

        # --- site_property_id ----------------------------------------
        m = re.search(r"\bpostid-(\d+)\b", body_classes)
        site_id: Optional[str] = m.group(1) if m else None
        if not site_id and content_div:
            m2 = re.match(r"property-(\d+)", content_div.attributes.get("id", "") or "")
            if m2:
                site_id = m2.group(1)
        if not site_id:
            logger.warning(f"[{self.source_domain}] no site_property_id at {url}")
            # Continue — seed's site_property_id is authoritative anyway

        # --- title (Greek) -------------------------------------------
        title_node = parser.css_first(".g5ere__property-title")
        title = title_node.text(strip=True) if title_node else None

        # --- price ----------------------------------------------------
        price_eur: Optional[float] = None
        price_node = parser.css_first(".g5ere__lpp-price")
        if price_node:
            digits = re.sub(r"[^\d]", "", price_node.text(strip=True))
            if digits:
                try:
                    price_eur = float(digits)
                except ValueError:
                    pass

        # --- category (priority-ordered) -----------------------------
        category: Optional[str] = None
        found_types = set(
            re.findall(r"\bproperty-type-([a-z0-9\-]+)\b", content_classes)
        )
        for slug in self._TYPE_PRIORITY:
            if slug in found_types:
                mapped = self._CATEGORY_MAP.get(slug)
                if mapped:
                    category = mapped
                    break

        # --- numeric structural fields -------------------------------
        bedrooms = self._extract_int(parser, ".g5ere__property-bedrooms")
        bathrooms = self._extract_int(parser, ".g5ere__property-bathrooms")
        size_sqm = self._extract_float(parser, ".g5ere__property-size")
        land_size_sqm = self._extract_float(parser, ".g5ere__property-land-size")
        year_built = self._extract_int(parser, ".g5ere__property-year")

        # --- coordinates ---------------------------------------------
        latitude, longitude = self._extract_coords(parser)

        # --- location ------------------------------------------------
        location_raw = self._extract_location(content_classes, title)

        # --- description ---------------------------------------------
        description = self._extract_description(parser)

        # --- images --------------------------------------------------
        images = self._extract_images(parser)

        # --- features → extras dict ---------------------------------
        extras: Dict[str, Any] = self._extract_features(parser, content_classes)

        # Build the data dict — Step 1 outputs (structured fields + extras)
        data: Dict[str, Any] = {}
        if price_eur is not None:
            data["price"] = int(price_eur)
        if category is not None:
            data["category"] = category
        if bedrooms is not None:
            data["bedrooms"] = bedrooms
        if bathrooms is not None:
            data["bathrooms"] = bathrooms
        if size_sqm is not None:
            data["size_sqm"] = size_sqm
        if land_size_sqm is not None:
            data["land_size_sqm"] = land_size_sqm
        if year_built is not None:
            data["year_built"] = year_built
        if latitude is not None and longitude is not None:
            data["latitude"] = latitude
            data["longitude"] = longitude
        if location_raw is not None:
            data["location_raw"] = location_raw
        if description:
            data["description"] = description
        if images:
            data["images"] = images

        # Non-schema metadata goes in extra_features
        extras["title"] = title or None
        extras["agent_name"] = "GK Real Estate Halkidiki"
        extras["agent_phone"] = "+30 6988 421 740"
        extras["agent_email"] = "info@propertieshalkidiki.gr"
        data["extra_features"] = extras

        # Step 5: NLP fallback (fills missing fields from description)
        self._apply_nlp_fallback(data)

        # Step 7: quality gate (log-only — daily_sync handles retry policy)
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )

        return data

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_int(parser, selector: str) -> Optional[int]:
        node = parser.css_first(selector)
        if not node:
            return None
        # Take first numeric token only — "100 m2" must give 100, not 1002
        m = re.search(r"\d[\d,.]*", node.text(strip=True))
        if not m:
            return None
        try:
            return int(m.group(0).replace(",", "").replace(".", ""))
        except ValueError:
            return None

    @staticmethod
    def _extract_float(parser, selector: str) -> Optional[float]:
        node = parser.css_first(selector)
        if not node:
            return None
        # "5,000 m2" → 5000, "100 m2" → 100 (comma is thousand separator)
        m = re.search(r"\d[\d,.]*", node.text(strip=True))
        if not m:
            return None
        try:
            return float(m.group(0).replace(",", "").replace(".", ""))
        except ValueError:
            return None

    @staticmethod
    def _extract_coords(parser) -> Tuple[Optional[float], Optional[float]]:
        for node in parser.css("[data-location]"):
            raw = node.attributes.get("data-location", "") or ""
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            pos = data.get("position") or {}
            lat_raw, lng_raw = pos.get("lat"), pos.get("lng")
            if lat_raw in (None, "") or lng_raw in (None, ""):
                continue
            try:
                lat = float(lat_raw)
                lng = float(lng_raw)
            except (TypeError, ValueError):
                continue
            # Halkidiki bbox sanity check
            if 39.0 <= lat <= 41.5 and 22.0 <= lng <= 25.0:
                return lat, lng
        return None, None

    def _extract_location(
        self,
        content_classes: str,
        title: Optional[str] = None,
    ) -> str:
        # Primary: WP property-city taxonomy slug
        m = re.search(r"\bproperty-city-([a-z0-9\-]+)\b", content_classes)
        if m:
            slug = m.group(1)
            if slug in self._CITY_SLUG_OVERRIDES:
                return self._CITY_SLUG_OVERRIDES[slug]
            cleaned = re.sub(r"-chalkidikis?$", "", slug)
            return cleaned.replace("-", " ").title()
        # Fallback: scan title for known Greek place names (longest match wins)
        if title:
            norm = _normalize_greek(title)
            for gr_norm, en in _TITLE_LOC_LOOKUP:
                if gr_norm in norm:
                    return en
        return "Halkidiki"

    @staticmethod
    def _extract_description(parser) -> Optional[str]:
        block = parser.css_first(".g5ere__property-block-description .card-body")
        if not block:
            return None
        paragraphs: List[str] = []
        for p in block.css("p"):
            text = p.text(strip=True)
            if text:
                paragraphs.append(text)
        if paragraphs:
            return "\n\n".join(paragraphs)
        combined = block.text(strip=True)
        return combined or None

    @staticmethod
    def _extract_images(parser) -> List[str]:
        urls: List[str] = []
        seen: set = set()
        for a in parser.css(".g5core__zoom-image[href]"):
            href = (a.attributes.get("href", "") or "").strip()
            if not href.startswith("http"):
                continue
            # Skip WordPress thumbnails (e.g. -374x240.jpeg)
            if re.search(r"-\d{2,4}x\d{2,4}\.[a-zA-Z]{3,4}(?:$|\?)", href):
                continue
            if href not in seen:
                seen.add(href)
                urls.append(href)
        return urls

    @staticmethod
    def _extract_features(parser, body_classes: str) -> Dict[str, Any]:
        features: Dict[str, Any] = {}
        # Primary source: feature list anchors with /property-feature/{slug}/ hrefs
        for a in parser.css(".g5ere__property-feature-list a[href]"):
            href = a.attributes.get("href", "") or ""
            m = re.search(r"/property-feature/([a-z0-9\-]+)/?", href)
            if m:
                features[f"feature_{m.group(1)}"] = True
        # Fallback: body class scan if list block was missing
        if not features:
            for m in re.finditer(r"\bproperty-feature-([a-z0-9\-]+)\b", body_classes):
                features[f"feature_{m.group(1)}"] = True
        return features
