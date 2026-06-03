"""spitogatos.gr scraper — URLs-only collect + detail-page enrichment.

VIP cards have full structured title attribute but only ~13% of inventory.
Regular cards have short title (type + size only). Solution: collect URLs
from search pages, extract everything from detail pages (which are clean
and well-structured for all listings, no VIP/regular bifurcation).
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

    def __init__(self):
        super().__init__()
        self.source_domain = "spitogatos.gr"
        self.name = "Spitogatos"
        self._detail_cache: Dict[str, Dict[str, Any]] = {}

    async def collect_urls(self, min_price: int = 400_000) -> List[PropertyTemplate]:
        """Phase 1: collect URLs only from search pages, NO field extraction."""
        if AsyncCamoufox is None:
            logger.error("[spitogatos.gr] Camoufox not installed")
            return []

        seeds: List[PropertyTemplate] = []
        seen_urls = set()
        max_pages = 1  # SMOKE TEST cap — 1 page only

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
                    # Minimal seed — fetch_details fills rest
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
        """Visit detail page, extract ALL fields."""
        if url in self._detail_cache:
            return self._detail_cache[url].copy()

        if AsyncCamoufox is None:
            return {}

        async with AsyncCamoufox(headless=True, humanize=False, geoip=True) as browser:
            page = await browser.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await asyncio.sleep(4)
                html = await page.content()
            finally:
                await page.close()

        data = self._parse_detail_html(html, url)
        self._detail_cache[url] = data
        return data.copy()

    def _parse_detail_html(self, html: str, url: str) -> Dict[str, Any]:
        tree = HTMLParser(html)
        data: Dict[str, Any] = {}

        # Title (e.g. "Detached house, 180m²")
        h1 = tree.css_first("h1.property__title")
        if h1:
            data["title"] = h1.text(strip=True)
            # Extract category from title
            cat_match = re.match(r"([A-Za-z ]+?)(?:,|$)", data["title"])
            if cat_match:
                prop_type = cat_match.group(1).strip().lower()
                data["category"] = self._CATEGORY_MAP.get(prop_type, prop_type.title())

        # Address → location_raw
        addr = tree.css_first(".property__address")
        if addr:
            data["location_raw"] = addr.text(strip=True)

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

        # Spec table dl.property__details
        spec: Dict[str, str] = {}
        dts = tree.css("dl.property__details dt")
        dds = tree.css("dl.property__details dd")
        for dt, dd in zip(dts, dds):
            k = dt.text(strip=True).lower()
            v = dd.text(strip=True)
            spec[k] = v

        # Extract fields from spec
        if "surface" in spec:
            m = re.search(r"(\d[\d,.]*)", spec["surface"])
            if m:
                try:
                    data["size_sqm"] = float(m.group(1).replace(",", ""))
                except ValueError:
                    pass
        if "plot surface" in spec:
            m = re.search(r"(\d[\d,.]*)", spec["plot surface"])
            if m:
                try:
                    data["land_size_sqm"] = float(m.group(1).replace(",", ""))
                except ValueError:
                    pass
        if "construction year" in spec:
            m = re.search(r"(\d{4})", spec["construction year"])
            if m:
                data["year_built"] = int(m.group(1))
        if "levels" in spec:
            data["levels"] = spec["levels"]
        if "bathrooms" in spec:
            m = re.search(r"(\d+)", spec["bathrooms"])
            if m:
                data["bathrooms"] = int(m.group(1))

        # Bedrooms — from ul.property__info quick icons (look for "br" suffix)
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

        # Images (gallery items + carousel — collect any spitogatos.gr image, upgrade to 900x675)
        imgs: List[str] = []
        seen_img = set()
        for img in tree.css("img[src*='spitogatos.gr']"):
            src = img.attributes.get("src", "")
            if not src:
                continue
            # Upgrade thumb to hi-res
            src = re.sub(r"_\d+x\d+\.jpg", "_900x675.jpg", src)
            if src in seen_img:
                continue
            seen_img.add(src)
            imgs.append(src)
        if imgs:
            data["images"] = imgs

        # Agent
        extra: Dict[str, Any] = {}
        agency_a = tree.css_first(".property__agency h3 a")
        if agency_a:
            extra["agent_company"] = agency_a.text(strip=True)
            ag_href = agency_a.attributes.get("href", "")
            if ag_href:
                extra["agent_profile"] = self._BASE_URL + ag_href if ag_href.startswith("/") else ag_href
        contact = tree.css_first(".property__agency__contact")
        if contact:
            ct = contact.text(strip=True)
            if ct:
                extra["agent_name"] = ct
        if extra:
            data["extra_features"] = extra

        return data
