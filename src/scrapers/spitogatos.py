"""spitogatos.gr scraper — URLs-only collect + detail-page enrichment.

Phase 1: collect URLs from search pages, extract full data from detail pages.
Detail HTML provides: title, category, full location hierarchy (breadcrumb),
price, sizes, br/ba, year, levels, description, hi-res images, agent info,
4 feature sections (indoor/outdoor/construction/suitable_for), and coords
embedded in script tags (Halkidiki bbox validated).
"""
import asyncio
import re
from typing import Any, Dict, List, Optional

from loguru import logger
from selectolax.parser import HTMLParser

from src.models.schemas import PropertyTemplate
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.scrapers.base import BaseScraper

try:
    from camoufox.async_api import AsyncCamoufox
except ImportError:
    AsyncCamoufox = None


class SpitogatosScraper(EnrichmentMixin, BaseScraper):
    _SEARCH_URL = "https://www.spitogatos.gr/en/for_sale-homes/chalkidiki/minprice-{min_price}"
    _BASE_URL = "https://www.spitogatos.gr"

    _CATEGORY_MAP = {
        "detached house": "Detached House",
        "house": "House",
        "maisonette": "Maisonette",
        "villa": "Villa",
        "apartment": "Apartment",
        "apartment complex": "Complex",
        "building": "Complex",
        "complex": "Complex",
        "hotel": "Hotel",
        "studio": "Apartment",
        "loft": "Apartment",
        "land": "Land",
        "parcel": "Land",
        "plot": "Land",
        "farm": "Land",
        "office": "Hotel/Commercial",
        "shop": "Hotel/Commercial",
        "store": "Hotel/Commercial",
        "warehouse": "Hotel/Commercial",
    }

    _FEATURE_SECTIONS = {
        "indoor": "internal",
        "outdoor": "external",
        "construction": "construction",
        "goodfor": "suitable_for",
    }

    @staticmethod
    def _area_name_variants(area_name):
        """Generate name variants for location_areas lookup.

        Handles: Paralia prefix, ch→h, i→y, multi-word splitting, trailing s.
        """
        if not area_name:
            return []
        v = [area_name]

        # "Paralia X" → "X" (strip beach prefix)
        if area_name.lower().startswith('paralia '):
            rest = area_name[len('paralia '):].strip()
            v.append(rest)
            if rest.endswith('s'):
                v.append(rest[:-1])

        # Multi-word: try each word individually + last word
        words = area_name.split()
        if len(words) > 1:
            for w in words:
                v.append(w)
                if w.endswith('s'):
                    v.append(w[:-1])

        # ch → h transform ("Chaniotis" → "Haniotis" → "Hanioti")
        if 'ch' in area_name.lower():
            t = area_name.replace('Ch', 'H').replace('ch', 'h')
            v.append(t)
            if t.endswith('s'):
                v.append(t[:-1])

        # i → y transform ("Afitos" → "Afytos")
        if 'i' in area_name:
            v.append(area_name.replace('i', 'y'))

        # General trailing s
        if area_name.endswith('s'):
            v.append(area_name[:-1])

        seen = set()
        return [x for x in v if not (x.lower() in seen or seen.add(x.lower()))]

    @staticmethod
    def _OLD_area_variants(area_name: str) -> List[str]:
        """Generate variants of area name for location_areas lookup.
        
        Spitogatos uses different spellings than refdata:
        - Pefkochori (Spitogatos) → Pefkohori (refdata)
        - Paralia Nikitis (Spitogatos) → Nikiti (refdata)
        - Plural forms may differ
        """
        if not area_name:
            return []
        variants = [area_name]
        # Strip "Paralia " (beach prefix) — e.g. "Paralia Nikitis" → "Nikitis"
        if area_name.lower().startswith('paralia '):
            rest = area_name[len('paralia '):].strip()
            variants.append(rest)
            if rest.endswith('s'):
                variants.append(rest[:-1])  # "Nikitis" → "Nikiti"
        # Spelling: ch → h ("Pefkochori" → "Pefkohori")
        if 'ch' in area_name.lower():
            variants.append(area_name.replace('ch', 'h').replace('Ch', 'H'))
        # General trailing s strip
        if area_name.endswith('s'):
            variants.append(area_name[:-1])
        # Dedupe preserving order
        seen = set()
        return [v for v in variants if not (v.lower() in seen or seen.add(v.lower()))]

    def __init__(self):
        super().__init__()
        self.source_domain = "spitogatos.gr"
        self.name = "Spitogatos"
        self._detail_cache: Dict[str, Dict[str, Any]] = {}

    async def collect_urls(self, min_price: int = 400_000, max_pages: int = 5) -> List[PropertyTemplate]:
        if AsyncCamoufox is None:
            logger.error("[spitogatos.gr] Camoufox not installed")
            return []

        seeds: List[PropertyTemplate] = []
        seen_urls = set()
        # max_pages from caller

        logger.info(f"[spitogatos.gr] Camoufox launching (URLs-only, max_pages={max_pages})...")
        async with AsyncCamoufox(headless=True, humanize=False, geoip=True) as browser:
            for page_num in range(1, max_pages + 1):
                base_url = self._SEARCH_URL.format(min_price=min_price)
                url = base_url if page_num == 1 else f"{base_url}/page_{page_num}"
                logger.info(f"[spitogatos.gr] GET page {page_num}: {url}")
                page = await browser.new_page()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await asyncio.sleep(5)
                    html = await page.content()
                finally:
                    await page.close()

                tree = HTMLParser(html)
                articles = tree.css("article[data-order]")
                if not articles:
                    logger.warning(f"[spitogatos.gr] page {page_num}: 0 cards, stopping")
                    break
                for art in articles:
                    link = art.css_first("a.tile__link[href]")
                    if not link:
                        continue
                    href = link.attributes.get("href") or ""
                    if not href:
                        continue
                    if not href.startswith("http"):
                        href = self._BASE_URL + href
                    if href in seen_urls:
                        continue
                    seen_urls.add(href)
                    site_id = href.rsplit("/", 1)[-1]
                    seed = PropertyTemplate(
                        url=href,
                        site_property_id=site_id,
                        source_domain=self.source_domain,
                    )
                    seeds.append(seed)
                logger.info(f"[spitogatos.gr] page {page_num}: {len(articles)} cards, {len(seeds)} total URLs")
        logger.info(f"[spitogatos.gr] collect_urls done: {len(seeds)} URLs")
        return seeds

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        if url in self._detail_cache:
            return self._detail_cache[url].copy()
        if AsyncCamoufox is None:
            return {}

        for attempt in range(1, 4):
            try:
                async with AsyncCamoufox(headless=True, humanize=False, geoip=True) as browser:
                    page = await browser.new_page()
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                        await asyncio.sleep(5)
                        # Scroll to map to trigger lazy load
                        try:
                            await page.evaluate("document.querySelector('.property__map')?.scrollIntoView()")
                            await asyncio.sleep(2)
                        except Exception:
                            pass
                        html = await page.content()
                    finally:
                        await page.close()
                data = self._parse_detail_html(html, url)
                if data and data.get("title"):
                    self._detail_cache[url] = data
                    return data.copy()
                logger.warning(f"[spitogatos.gr] attempt {attempt}/3 empty for {url}")
            except Exception as e:
                logger.warning(f"[spitogatos.gr] attempt {attempt}/3 error for {url}: {e!r}")
            if attempt < 3:
                await asyncio.sleep(attempt * 3)
        logger.error(f"[spitogatos.gr] ALL 3 attempts failed for {url}")
        return {}

    @staticmethod
    async def lookup_area_coords(area_name, session):
        """Lookup (lat, lng) from location_areas via variants + Halkidiki bbox."""
        from sqlalchemy import text as sa_text
        if not area_name:
            return None
        for v in SpitogatosScraper._area_name_variants(area_name):
            row = (await session.execute(sa_text(
                "SELECT lat, lng FROM location_areas "
                "WHERE LOWER(area_en) = LOWER(:n) "
                "  AND lat BETWEEN 39.5 AND 41.0 AND lng BETWEEN 23 AND 24.5 "
                "LIMIT 1"
            ), {"n": v})).first()
            if row:
                return (float(row[0]), float(row[1]))
        first_word = area_name.split()[0]
        row = (await session.execute(sa_text(
            "SELECT lat, lng FROM location_areas "
            "WHERE LOWER(area_en) ILIKE LOWER(:p) "
            "  AND lat BETWEEN 39.5 AND 41.0 AND lng BETWEEN 23 AND 24.5 "
            "LIMIT 1"
        ), {"p": f"%{first_word}%"})).first()
        if row:
            return (float(row[0]), float(row[1]))
        return None

    def _parse_detail_html(self, html: str, url: str) -> Dict[str, Any]:
        tree = HTMLParser(html)
        data: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # Title + category
        h1 = tree.css_first("h1.property__title")
        if h1:
            data["title"] = h1.text(strip=True)
            cat_match = re.match(r"([A-Za-z ]+?)(?:,|$)", data["title"])
            if cat_match:
                prop_type = cat_match.group(1).strip().lower()
                data["category"] = self._CATEGORY_MAP.get(prop_type, prop_type.title())

        # Main address
        addr = tree.css_first(".property__address")
        if addr:
            data["location_raw"] = addr.text(strip=True)

        # Street address (more granular)
        street = tree.css_first(".property__street")
        if street:
            s_txt = street.text(strip=True)
            if s_txt:
                extra["street_address"] = s_txt

        # Breadcrumb: Homes for sale > Chalkidiki > Sithonia > Paralia Nikitis > Listing 17435057
        crumbs = []
        for el in tree.css(".breadcrumb__item"):
            txt = el.text(strip=True)
            if txt and not txt.startswith("Listing"):
                crumbs.append(txt)
        if crumbs:
            extra["breadcrumb"] = crumbs
            # Breadcrumb format: "Homes for sale" > <prefecture> > <municipality> > <area>
            # e.g. ['Homes for sale', 'Chalkidiki', 'Sithonia', 'Paralia Nikitis']
            # Strip the leading "Homes for sale" header
            geo = [c for c in crumbs if c.lower() not in ('homes for sale', 'sale', 'homes', 'select subarea', 'select area')]
            if len(geo) >= 1:
                data["calc_prefecture"] = geo[0]
            if len(geo) >= 2:
                data["calc_municipality"] = geo[1]
            if len(geo) >= 3:
                data["calc_area"] = geo[2]
                data["area"] = geo[2]  # also set 'area' for admin display
                data["subarea"] = geo[-1]
            elif len(geo) == 2:
                data["calc_area"] = geo[1]
                data["area"] = geo[1]

        # Price
        price_el = tree.css_first(".property__price__text")
        if price_el:
            ptxt = price_el.text(strip=True)
            m = re.search(r"([\d,.]+)", ptxt.replace("\u20ac", ""))
            if m:
                try:
                    data["price"] = int(m.group(1).replace(",", "").replace(".", ""))
                except ValueError:
                    pass

        # Spec table
        spec: Dict[str, str] = {}
        for dt, dd in zip(tree.css("dl.property__details dt"), tree.css("dl.property__details dd")):
            k = dt.text(strip=True).lower()
            v = dd.text(strip=True)
            spec[k] = v

        if "surface" in spec:
            m = re.search(r"(\d[\d,.]*)", spec["surface"])
            if m:
                try: data["size_sqm"] = float(m.group(1).replace(",", ""))
                except ValueError: pass
        if "plot surface" in spec:
            m = re.search(r"(\d[\d,.]*)", spec["plot surface"])
            if m:
                try: data["land_size_sqm"] = float(m.group(1).replace(",", ""))
                except ValueError: pass
        if "construction year" in spec:
            m = re.search(r"(\d{4})", spec["construction year"])
            if m: data["year_built"] = int(m.group(1))
        if "levels" in spec:
            data["levels"] = spec["levels"]
        if "bathrooms" in spec:
            m = re.search(r"(\d+)", spec["bathrooms"])
            if m: data["bathrooms"] = int(m.group(1))
        if spec:
            extra["spec_table"] = spec

        # Bedrooms from quick info icons
        for li in tree.css("ul.property__info li"):
            txt = li.text(strip=True)
            m = re.search(r"(\d+)\s*br", txt, re.I)
            if m:
                data["bedrooms"] = int(m.group(1))
                break

        # Description
        desc = tree.css_first(".property__description p.sliced")
        if desc:
            data["description"] = desc.text(strip=True)

        # Images: extract all property image IDs (incl. lazy-loaded in __NUXT__).
        # Cluster filter drops outliers like agency logo (which has ID far from property cluster).
        # Use _1600x1200.jpg — biggest size Spitogatos serves.
        url_pattern = re.compile(r"spitogatos\.gr(?:/|\\u002F)(\d{7,10})_\d+x\d+")
        url_ids = set(int(x) for x in url_pattern.findall(html))
        if url_ids:
            sorted_ids = sorted(url_ids)
            median_id = sorted_ids[len(sorted_ids) // 2]
            # Find ALL 8-9 digit numbers within ±300 of median (property cluster)
            cluster_ids = set()
            for m in re.finditer(r"\b(\d{8,9})\b", html):
                try:
                    i = int(m.group(1))
                    if abs(i - median_id) <= 300:
                        cluster_ids.add(i)
                except ValueError:
                    pass
            if cluster_ids:
                data["images"] = [
                    f"https://m1.spitogatos.gr/{pid}_1600x1200.jpg"
                    for pid in sorted(cluster_ids)
                ]

        # Features (4 sections by data-test-id)
        features: Dict[str, Dict[str, bool]] = {}
        for section in tree.css("ul.property__features[data-test-id]"):
            tid = section.attributes.get("data-test-id", "")
            key = self._FEATURE_SECTIONS.get(tid)
            if not key:
                continue
            section_feats: Dict[str, bool] = {}
            for li in section.css("li"):
                svg = li.css_first("svg")
                if not svg:
                    continue
                cls_words = (svg.attributes.get("class") or "").split()
                if "on" in cls_words:
                    present = True
                elif "off" in cls_words:
                    present = False
                else:
                    continue
                span = li.css_first("span")
                if span:
                    label = " ".join(span.text(strip=True).split())  # collapse whitespace
                    if label:
                        section_feats[label] = present
            if section_feats:
                features[key] = section_feats
        if features:
            extra["features"] = features

        # Coordinates: extract from __NUXT__ SSR state.
        # Spitogatos has 3+ geocodeType modes:
        #   "exact"  → verified per-property coords (most listings)
        #   "offset" → privacy-shifted real coords (slight blur)
        #   "hidden" → intentionally hidden (luxury/VIP); coords are minified var like `a`
        # For hidden mode, caller falls back to refdata centroid (best we can do).
        coord_match = re.search(
            r'geocodeType\s*:\s*"(\w+)"\s*,\s*longitude\s*:\s*(-?\d+\.\d+|[a-z_]\w*)\s*,\s*latitude\s*:\s*(-?\d+\.\d+|[a-z_]\w*)',
            html
        )
        if coord_match:
            gtype = coord_match.group(1)
            extra["geocode_type"] = gtype  # always store the type
            try:
                lng = float(coord_match.group(2))
                lat = float(coord_match.group(3))
                # Halkidiki bbox sanity (39.5-41 lat, 23-24.5 lng)
                if 39.5 <= lat <= 41.0 and 23 <= lng <= 24.5:
                    data["latitude"] = lat
                    data["longitude"] = lng
                # else: out of bbox — leave lat/lng unset, caller uses refdata centroid
            except (ValueError, IndexError):
                # gtype="hidden" with non-numeric coords (var ref like 'a')
                # data["latitude"] stays unset → caller uses refdata centroid
                pass

        # Agent / agency
        agency_a = tree.css_first(".property__agency h3 a")
        if agency_a:
            extra["agent_company"] = agency_a.text(strip=True)
            ag_href = agency_a.attributes.get("href", "")
            if ag_href:
                extra["agent_profile"] = (
                    self._BASE_URL + ag_href if ag_href.startswith("/") else ag_href
                )
        agency_p = tree.css_first(".property__agency p")
        if agency_p:
            ap_txt = agency_p.text(strip=True)
            if ap_txt:
                extra["agent_address"] = ap_txt
        contact = tree.css_first(".property__agency__contact")
        if contact:
            ct = contact.text(strip=True)
            if ct:
                extra["agent_name"] = ct

        if extra:
            data["extra_features"] = extra

        return data
