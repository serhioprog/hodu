"""openestate.gr scraper.

Site: Open Estate (https://openestate.gr) — agency in Kassandra-Pefkohori
Stack: DLE CMS (DataLifeEngine) + PHP 7.4 + Plesk + jQuery + Cloudflare
Coverage: ~336 properties across Halkidiki (28 pages × 12 cards), all
properties manually filtered for Halkidiki — site itself targets only
Halkidiki + Santorini + Thessaloniki, but >90% are Halkidiki.

Quirks:
- Cards: price rendered client-side via JS (formatter.format(N))
- Details: price rendered server-side in #iddiv span
- No per-property coords (map iframe = agency office on all pages)
- URL pattern: /{cat-slug}/{N}-{slug}.html where {N} = site_property_id
- Site categories (7): Apartment/Villa/Cottage/Townhouse/Land/Business/Hotel
- Title-keyword overrides: e.g. site says "Villa" but title says "Maisonette"
- Mixed Sale/Rent listings — filter to Sale only in card parser
- Per-property agent metadata in `.agent_block` (varies by listing)
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


def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


# Regex shared by listing & detail pages — page-level price hint per object
_PRICE_JS_RX = re.compile(
    r'(?:iddiv|relatednews)_(\d+)\.innerHTML\s*=\s*formatter\.format\((\d+)\)'
)

# Regex extracts site_property_id from detail URL
_URL_ID_RX = re.compile(r'/([a-z]+)/(\d+)-[^/]+\.html$', re.IGNORECASE)


# Halkidiki village → (municipality, normalized display name).
# openestate.gr provides no per-property coordinates (the Maps iframe is the
# agency office on every detail page), so GeoMatcher can't run normally.
# We instead resolve municipality directly from a curated village lookup
# applied to `location_raw` text.
#
# Order matters: SPECIFIC villages first (so "Paliouri, Kassandra" resolves
# to area Paliouri, not Kassandra). Within villages, longer compound names
# first (so "Nea Skioni" beats "Skioni"). Bare municipality keywords are
# fallbacks at the end of the list.
_VILLAGE_TO_MUNICIPALITY: List[Tuple[str, str, str]] = [
    # =========================================================
    # TIER 1: SPECIFIC VILLAGES (longer compound names first)
    # =========================================================
    # Kassandra peninsula
    ("nea kallikratia",  "Nea Propontida","Nea Kallikratia"),
    ("nea moudania",     "Nea Propontida","Nea Moudania"),
    ("nea iraklia",      "Nea Propontida","Nea Iraklia"),
    ("neos marmaras",    "Sithonia",   "Neos Marmaras"),
    ("neo marmaras",     "Sithonia",   "Neos Marmaras"),
    ("agios nikolaos",   "Sithonia",   "Agios Nikolaos"),
    ("nea skioni",       "Kassandra",  "Nea Skioni"),
    ("nea fokea",        "Kassandra",  "Nea Fokea"),
    ("kassandreia",      "Kassandra",  "Kassandreia"),
    ("kassandria",       "Kassandra",  "Kassandreia"),
    ("ouranopolis",      "Aristotelis","Ouranopolis"),
    ("ouranoupolis",     "Aristotelis","Ouranopolis"),
    ("metamorfosi",      "Sithonia",   "Metamorfosi"),
    ("glarokavos",       "Kassandra",  "Glarokavos"),
    ("pefkochori",       "Kassandra",  "Pefkochori"),
    ("pefkohori",        "Kassandra",  "Pefkochori"),
    ("akti salonikiou",  "Sithonia",   "Akti Salonikiou"),
    ("akti salonikiu",   "Sithonia",   "Akti Salonikiou"),
    ("mola kaliva",      "Kassandra",  "Mola Kalyva"),
    ("mola kalyva",      "Kassandra",  "Mola Kalyva"),
    ("moles kalives",    "Kassandra",  "Mola Kalyva"),
    ("neo marmara",      "Sithonia",   "Neos Marmaras"),
    ("polychrono",       "Kassandra",  "Polychrono"),
    ("polihrono",        "Kassandra",  "Polychrono"),
    ("afitos",           "Kassandra",  "Afytos"),
    ("polichrono",       "Kassandra",  "Polychrono"),
    ("vourvourou",       "Sithonia",   "Vourvourou"),
    ("pyrgadikia",       "Aristotelis","Pyrgadikia"),
    ("kallikratia",      "Nea Propontida","Nea Kallikratia"),
    ("ammouliani",       "Aristotelis","Ammouliani"),
    ("chaniotis",        "Kassandra",  "Hanioti"),
    ("haniotis",         "Kassandra",  "Hanioti"),
    ("chanioti",         "Kassandra",  "Hanioti"),
    ("kallithea",        "Kassandra",  "Kallithea"),
    ("kalithea",         "Kassandra",  "Kallithea"),
    ("kalandra",         "Kassandra",  "Kalandra"),
    ("moudania",         "Nea Propontida","Nea Moudania"),
    ("kriopigi",         "Kassandra",  "Kriopigi"),
    ("paliouri",         "Kassandra",  "Paliouri"),
    ("palouri",          "Kassandra",  "Paliouri"),
    ("paliuri",          "Kassandra",  "Paliouri"),
    ("polygyros",        "Polygyros",  "Polygyros"),
    ("polygiros",        "Polygyros",  "Polygyros"),
    ("gerakini",         "Polygyros",  "Gerakini"),
    ("portaria",         "Nea Propontida","Portaria"),
    ("hanioti",          "Kassandra",  "Hanioti"),
    ("iraklia",          "Nea Propontida","Nea Iraklia"),
    ("ierissos",         "Aristotelis","Ierissos"),
    ("stratoni",         "Aristotelis","Stratoni"),
    ("stagira",          "Aristotelis","Stagira"),
    ("vourvouro",        "Sithonia",   "Vourvourou"),
    ("flogita",          "Nea Propontida","Nea Flogita"),
    ("athytos",          "Kassandra",  "Afytos"),
    ("athitos",          "Kassandra",  "Afytos"),
    ("afytos",           "Kassandra",  "Afytos"),
    ("posidi",           "Kassandra",  "Posidi"),
    ("possidi",          "Kassandra",  "Posidi"),
    ("loutra",           "Kassandra",  "Loutra"),
    ("siviri",           "Kassandra",  "Siviri"),
    ("nikiti",           "Sithonia",   "Nikiti"),
    ("fourka",           "Kassandra",  "Fourka"),
    ("toroni",           "Sithonia",   "Toroni"),
    ("skioni",           "Kassandra",  "Nea Skioni"),
    ("sykia",            "Sithonia",   "Sykia"),
    ("sarti",            "Sithonia",   "Sarti"),
    ("sani",             "Kassandra",  "Sani"),
    # =========================================================
    # TIER 2: MUNICIPALITY-LEVEL FALLBACKS (when no village hit)
    # =========================================================
    ("nea propontida",   "Nea Propontida","Nea Propontida"),
    ("aristotelis",      "Aristotelis","Aristotelis"),
    ("kassandra",        "Kassandra",  "Kassandra"),
    ("sithonia",         "Sithonia",   "Sithonia"),
]


def _resolve_municipality(location_raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Return (municipality, normalized_area_name) from location_raw, or (None, None)."""
    if not location_raw:
        return (None, None)
    norm = unicodedata.normalize("NFD", location_raw.lower())
    norm = "".join(c for c in norm if unicodedata.category(c) != "Mn")
    for needle, muni, display in _VILLAGE_TO_MUNICIPALITY:
        if needle in norm:
            return (muni, display)
    return (None, None)


class OpenEstateScraper(EnrichmentMixin, BaseScraper):
    """Scraper for openestate.gr (Open Estate, Kassandra-Pefkohori)."""

    name = "openestate"
    source_domain = "openestate.gr"
    base_url = "https://openestate.gr"

    _LISTING_URL_BASE = "https://openestate.gr"
    _MAX_PAGES = 35                  # 28 actual + 7 safety margin
    _INTER_PAGE_SLEEP_SEC = 0.4
    _INTER_DETAIL_SLEEP_SEC = 0.8

    # Site category slug → canonical hodu vocabulary
    _SITE_CATEGORY_MAP: Dict[str, str] = {
        "apartment": "Apartment",
        "villa": "Villa",
        "cottage": "Detached House",
        "townhouse": "Maisonette",
        "land": "Land",
        "business": "Hotel/Commercial",
        "hotel": "Hotel",
    }

    # Title keyword overrides — applied AFTER site-category mapping. The
    # site uses very broad categories (Villa, Townhouse), but titles often
    # state the precise structural type. Earlier matches in the list win.
    _TITLE_OVERRIDES: List[Tuple[str, str]] = [
        ("mansion", "Villa"),
        ("maisonette", "Maisonette"),
        ("hotel", "Hotel"),
        ("complex", "Complex"),
        ("apartment building", "Complex"),
        ("commercial", "Hotel/Commercial"),
        ("shop", "Hotel/Commercial"),
        ("office", "Hotel/Commercial"),
        ("store", "Hotel/Commercial"),
        ("studio", "Apartment"),
        ("villa", "Villa"),
        ("detached house", "Detached House"),
        ("plot of land", "Land"),
        ("agricultural", "Land"),
    ]

    # NLP fallback fills only these (omit category — title/site is authoritative)
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

        for page in range(1, self._MAX_PAGES + 1):
            url = self._LISTING_URL_BASE if page == 1 else f"{self._LISTING_URL_BASE}/page/{page}/"
            logger.info(f"[{self.source_domain}] page {page} GET {url}")

            try:
                resp = await self.client.get(url)
            except Exception as exc:
                logger.error(f"[{self.source_domain}] page {page} fetch: {exc!r}")
                break
            if resp.status_code != 200:
                logger.warning(f"[{self.source_domain}] page {page}: HTTP {resp.status_code}")
                break

            parser = LexborHTMLParser(resp.text)

            # Build price index from page-level JS (id -> price)
            price_index: Dict[str, int] = {}
            for m in _PRICE_JS_RX.finditer(resp.text):
                price_index[m.group(1)] = int(m.group(2))

            cards = [c for c in parser.css(".item") if c.css_first("a.item-link")]
            logger.info(
                f"[{self.source_domain}] page {page}: {len(cards)} cards "
                f"({len(price_index)} prices in JS index)"
            )

            if not cards:
                break

            page_new = 0
            page_skipped_rent = 0
            page_skipped_lowprice = 0
            for card in cards:
                try:
                    seed = self._parse_card(card, price_index, min_price)
                except Exception as exc:
                    logger.error(f"[{self.source_domain}] card parse: {exc!r}")
                    continue
                if seed is None:
                    # _parse_card returns None for skipped (rent, low price, etc.)
                    continue
                if seed == "rent":
                    page_skipped_rent += 1
                    continue
                if seed == "lowprice":
                    page_skipped_lowprice += 1
                    continue
                if not isinstance(seed, PropertyTemplate):
                    continue
                if seed.url not in seeds:
                    seeds[seed.url] = seed
                    page_new += 1

            logger.info(
                f"[{self.source_domain}] page {page} -> {page_new} new, "
                f"{page_skipped_rent} rent, {page_skipped_lowprice} below {min_price:,} "
                f"(total seeds: {len(seeds)})"
            )
            await asyncio.sleep(self._INTER_PAGE_SLEEP_SEC)

        logger.info(
            f"[{self.source_domain}] collect_urls done: {len(seeds)} seeds"
        )
        return list(seeds.values())

    def _parse_card(
        self,
        card: LexborNode,
        price_index: Dict[str, int],
        min_price: int,
    ) -> Any:
        """Return PropertyTemplate, 'rent', 'lowprice', or None for skip."""
        a = card.css_first("a.item-link")
        if not a:
            return None
        url = (a.attributes.get("href") or "").strip()
        if not url or ".html" not in url:
            return None
        url = url.split("?")[0].split("#")[0]

        # Extract id from URL prefix: /{cat}/{N}-{slug}.html
        m = _URL_ID_RX.search(url)
        if not m:
            return None
        cat_slug = m.group(1).lower()
        site_id = m.group(2)

        # Filter Rent
        type_node = card.css_first(".item-meta.meta-type")
        ptype = _normalize_text(type_node.text(strip=False)) if type_node else ""
        if ptype.lower() == "rent":
            return "rent"

        # Price from page-level JS index
        price = price_index.get(site_id)
        if price is None or price < min_price:
            return "lowprice"

        # Seed location_raw with the title (cards have no explicit location field,
        # but titles consistently reference Halkidiki area names like "in Skioni",
        # "in Pefkohori" — required for the Halkidiki whitelist downstream).
        title_node = card.css_first(".item-title")
        title_text = _normalize_text(title_node.text(strip=False)) if title_node else None

        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=url,
            price=price,
            location_raw=title_text,
        )

    # ------------------------------------------------------------------
    # Phase 2: fetch_details
    # ------------------------------------------------------------------
    async def fetch_details(self, url: str) -> Dict[str, Any]:
        try:
            resp = await self.client.get(url)
        except Exception as exc:
            logger.error(f"[{self.source_domain}] detail fetch error for {url}: {exc!r}")
            return {}
        if resp.status_code != 200:
            logger.warning(f"[{self.source_domain}] detail {url}: HTTP {resp.status_code}")
            return {}

        parser = LexborHTMLParser(resp.text)
        data: Dict[str, Any] = {}
        extras: Dict[str, Any] = {}

        # ── title
        h1 = parser.css_first(".title h1") or parser.css_first("h1.ru_en") or parser.css_first("h1")
        title = _normalize_text(h1.text(strip=False)) if h1 else None
        if title:
            extras["title"] = title

        # ── object code
        oid = parser.css_first(".id_object b")
        if oid:
            extras["property_code"] = _normalize_text(oid.text(strip=False))

        # ── price — `#iddiv` span on detail pages is empty server-side and
        # gets populated client-side via inline JS. Extract from the script:
        #   iddiv.innerHTML = formatter.format(850000);
        pm = re.search(r'\biddiv\.innerHTML\s*=\s*formatter\.format\((\d+)\)', resp.text)
        if pm:
            try:
                data["price"] = int(pm.group(1))
            except ValueError:
                pass

        # ── type tag (Sale/Rent) — for sanity check
        type_node = parser.css_first(".type span")
        if type_node:
            extras["listing_type"] = _normalize_text(type_node.text(strip=False))

        # ── category from URL prefix + title-keyword overrides
        url_match = _URL_ID_RX.search(url)
        site_cat_slug = (url_match.group(1).lower() if url_match else "")
        category = self._SITE_CATEGORY_MAP.get(site_cat_slug)
        if title:
            tl = title.lower()
            for needle, cat in self._TITLE_OVERRIDES:
                if needle in tl:
                    category = cat
                    break
        if category:
            data["category"] = category

        # ── parameters table (.customers tr td:td)
        self._parse_parameters_table(parser, data, extras)

        # ── location address ("Address: Pefkohori - Halkidiki")
        location_raw = self._parse_location_address(parser)
        if location_raw:
            data["location_raw"] = location_raw

        # ── description
        desc_node = parser.css_first(".f-desc.full-text") or parser.css_first(".full-text")
        if desc_node:
            desc = _normalize_text(desc_node.text(strip=False))
            if desc:
                data["description"] = desc
        if not data.get("description"):
            og = self._og_description_fallback(parser)
            if og:
                data["description"] = og

        # ── gallery — collect full-size images
        images = self._extract_images(parser)
        if images:
            data["images"] = images

        # ── agent (per-property — site has multiple agents)
        self._parse_agent_block(parser, extras)
        # Hardcoded agency-level fallbacks
        extras.setdefault("agent_company", "Open Estate")
        extras.setdefault("agent_phone_1", "+30 697 388 5112")
        extras.setdefault("agent_phone_2", "+30 694 853 4479")
        extras.setdefault("agent_phone_3", "+30 237 404 3167")

        data["extra_features"] = extras

        # NOTE: NO per-property coords (Google Maps iframe always points
        # to the agency office, identical across all listings). Instead we
        # resolve municipality directly via village substring lookup on
        # location_raw + title — see _VILLAGE_TO_MUNICIPALITY at module top.
        # Some listings have no "Address:" block on the detail page, so the
        # seed's card title (preserved here in extras["title"]) is the only
        # location hint available.
        loc_for_resolution = " ".join(
            x for x in (data.get("location_raw"), extras.get("title")) if x
        )
        muni, display_area = _resolve_municipality(loc_for_resolution)
        if muni:
            data["calc_prefecture"] = "Halkidiki"
            data["calc_municipality"] = muni
            if display_area:
                data["calc_area"] = display_area

        # Step 5: NLP fallback
        self._apply_nlp_fallback(data)

        # Step 7: quality gate
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate for {url}"
            )

        return data

    # ------------------------------------------------------------------
    # Detail helpers
    # ------------------------------------------------------------------
    def _parse_parameters_table(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
        extras: Dict[str, Any],
    ) -> None:
        """Walk <table class='customers'> rows for structured fields.

        Rows are key/value pairs like:
            <tr><td>Year of construction:</td><td>2013</td></tr>
            <tr><td>Plot area:</td><td>550 м²</td></tr>
            <tr><td>Bedrooms:</td><td>4</td></tr>
            <tr><td>To the sea:</td><td>650 м</td></tr>
        """
        for tr in parser.css("table.customers tr"):
            tds = tr.css("td")
            if len(tds) < 2:
                continue
            label = _normalize_text(tds[0].text(strip=False)).rstrip(":").lower()
            value = _normalize_text(tds[1].text(strip=False))
            if not label or not value:
                continue

            if label == "year of construction":
                m = re.search(r"\d{4}", value)
                if m:
                    data["year_built"] = int(m.group(0))
            elif label == "plot area":
                m = re.search(r"\d[\d.,]*", value)
                if m:
                    try:
                        data["land_size_sqm"] = float(m.group(0).replace(",", "").replace(".", ""))
                    except ValueError:
                        pass
            elif label == "living area":
                m = re.search(r"\d[\d.,]*", value)
                if m:
                    try:
                        data["size_sqm"] = float(m.group(0).replace(",", "").replace(".", ""))
                    except ValueError:
                        pass
            elif label == "number of floors":
                m = re.search(r"\d+", value)
                if m:
                    data["levels"] = m.group(0)
            elif label == "bedrooms":
                m = re.search(r"\d+", value)
                if m:
                    data["bedrooms"] = int(m.group(0))
            elif label == "bathrooms":
                m = re.search(r"\d+", value)
                if m:
                    data["bathrooms"] = int(m.group(0))
            elif label == "rooms":
                m = re.search(r"\d+", value)
                if m:
                    extras["rooms_total"] = int(m.group(0))
            elif label == "garages":
                m = re.search(r"\d+", value)
                if m:
                    extras["garages"] = int(m.group(0))
            elif label == "to the sea":
                extras["distance_to_sea"] = value
            elif label == "to the airport":
                extras["distance_to_airport"] = value
            elif label == "to the city":
                extras["distance_to_city"] = value
            elif label == "to the center":
                extras["distance_to_center"] = value
            else:
                # Capture other key/value pairs as extras with sanitized key
                slug = re.sub(r"[^a-z0-9]+", "_", label).strip("_")
                if slug and len(slug) < 40:
                    extras[f"param_{slug}"] = value

    def _parse_location_address(self, parser: LexborHTMLParser) -> Optional[str]:
        """Extract address text following <h2>Location</h2>.

        DOM: <h2>Location</h2><div>Address: Pefkohori - Halkidiki</div>
        """
        for h2 in parser.css("h2"):
            t = _normalize_text(h2.text(strip=False))
            if t.lower() == "location":
                # Walk next siblings — selectolax has .next iteration
                nxt = h2.next
                # Skip text nodes
                while nxt is not None and getattr(nxt, "tag", None) not in (
                    "div", "p", "table", "h2"
                ):
                    nxt = nxt.next
                if nxt is not None and getattr(nxt, "tag", None) in ("div", "p"):
                    raw = _normalize_text(nxt.text(strip=False))
                    # Strip "Address:" prefix
                    if raw.lower().startswith("address:"):
                        raw = raw.split(":", 1)[1].strip()
                    if raw:
                        return raw
                break
        return None

    @staticmethod
    def _extract_images(parser: LexborHTMLParser) -> List[str]:
        """Gallery images at full size (no /thumbs/).

        Strategy: hero image + gallery thumbs converted to full-size by
        stripping /thumbs/ from the URL path.
        """
        seen: set = set()
        images: List[str] = []

        # Hero image
        prev = parser.css_first(".previmage img")
        if prev:
            src = (prev.attributes.get("src") or "").strip()
            if src.startswith("http") and src not in seen:
                seen.add(src)
                images.append(src)

        # Gallery thumbs — server HTML uses class="lazy-loaded" (the
        # ug-thumb-image class is added client-side by Unitegallery JS).
        # We strip /thumbs/ from the URL path to get the full-size image.
        gallery = parser.css_first("#gallery")
        if gallery is not None:
            for img in gallery.css("img"):
                src = (img.attributes.get("src") or "").strip()
                if not src:
                    continue
                if src.startswith("/"):
                    src = "https://openestate.gr" + src
                full = src.replace("/thumbs/", "/")
                if full.startswith("http") and full not in seen:
                    seen.add(full)
                    images.append(full)

        return images

    def _parse_agent_block(
        self, parser: LexborHTMLParser, extras: Dict[str, Any]
    ) -> None:
        """Per-property agent name + email from .agent_block sidebar."""
        block = parser.css_first(".agent_block")
        if not block:
            return
        for p in block.css("p"):
            txt = _normalize_text(p.text(strip=False))
            if txt.lower().startswith("name:"):
                # "Name: Miron"
                extras["agent_name"] = txt.split(":", 1)[1].strip()
            elif txt.lower().startswith("e-mail:"):
                # "E-Mail: mironsaria@gmail.com"
                extras["agent_email"] = txt.split(":", 1)[1].strip()
