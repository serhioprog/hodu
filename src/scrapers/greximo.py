"""
greximo.gr scraper — WordPress + Inspiry RealHomes theme.

Strategy B: JS array extraction (var propertiesMapData = [...])
- collect_urls: single GET to /property-search/ → all 88 filtered properties
- fetch_details: canonical 7-step pipeline using stable Inspiry selectors
"""
import re
import json
import logging
from typing import List, Optional

from selectolax.parser import HTMLParser

from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin
#from src.scrapers.fetchers import fetch_html_stage0
from src.models.schemas import PropertyTemplate

from src.core.scraper_area_constants import HALKIDIKI_REGIONS_WHITELIST

logger = logging.getLogger(__name__)

_HALKIDIKI_BBOX = (39.0, 22.0, 41.5, 25.0)
_PROPERTIES_ANCHOR_RE = re.compile(r"var\s+propertiesMapData\s*=\s*")
_PROPERTY_MAP_ANCHOR_RE = re.compile(r"var\s+propertyMapData\s*=\s*")


# ---------- helpers ----------

# Use project-canonical whitelist for locality detection.
# Sorted long-to-short for greedy matching (prefer "Nea Fokea" over "Fokea").
_WHITELIST_SORTED = sorted(
    {s.lower() for s in HALKIDIKI_REGIONS_WHITELIST},
    key=len,
    reverse=True,
)

# Bare prefecture/municipality markers — NOT localities (settlement level).
_BARE_REGION_MARKERS = {
    "halkidiki", "chalkidiki", "khalkidhiki", "halkidhiki",
    "kassandra", "cassandra", "kasandra",
    "sithonia", "sidonia",
    "aristotelis", "aristotle",
    "polygyros", "poligiros",
    "nea propontida",
}

# Greximo-specific locality spelling variants not in core whitelist.
# Per briefing: scraper-side normalizer extensions preferred over modifying
# the shared HALKIDIKI_REGIONS_WHITELIST (which is filter-only by design).
# Add new entries here if greximo properties consistently show as 'Halkidiki'
# (no locality) due to transliteration variants.
_GREXIMO_EXTRA_LOCALITIES = {
    "possidi": "Possidi",   # variant of canonical 'posidi' (Greek Ποσείδι, 2-s spelling)
    "poseidi": "Possidi",   # variant of canonical 'posidi' (greximo image filenames)
}


def _extract_locality(text: str) -> Optional[str]:
    """
    Find a Halkidiki LOCALITY (settlement-level) in text via whitelist substring match,
    falling back to greximo-specific spelling variants.
    Returns canonical Title-cased form or None.
    """
    if not text:
        return None
    text_lower = text.lower()

    # Tier 1: project-canonical whitelist (long-to-short for greedy matching)
    for entry in _WHITELIST_SORTED:
        if entry in _BARE_REGION_MARKERS:
            continue
        if entry in text_lower:
            return " ".join(w.capitalize() for w in entry.split())

    # Tier 2: greximo-specific spelling variants
    for key, canonical in _GREXIMO_EXTRA_LOCALITIES.items():
        if key in text_lower:
            return canonical

    return None


def _extract_locality_from_url(url: str) -> Optional[str]:
    """Parse URL slug for locality: /property/villa-pool-elani-g2188/ → 'Elani'."""
    if not url:
        return None
    m = re.search(r"/property/([^/]+)/?", url)
    if not m:
        return None
    slug = m.group(1).replace("-", " ").lower()
    return _extract_locality(slug)

def _extract_balanced_literal(text: str, anchor_re, open_char: str, close_char: str) -> Optional[str]:
    """Walk balanced bracket literal in JS source (string-aware)."""
    m = anchor_re.search(text)
    if not m:
        return None
    start = text.find(open_char, m.end())
    if start == -1:
        return None
    depth = 0
    in_str = False
    str_char = None
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if escape:
            escape = False
            continue
        if in_str:
            if ch == "\\":
                escape = True
            elif ch == str_char:
                in_str = False
        else:
            if ch in ('"', "'"):
                in_str = True
                str_char = ch
            elif ch == open_char:
                depth += 1
            elif ch == close_char:
                depth -= 1
                if depth == 0:
                    return text[start:i+1]
    return None


def _clean_html_tags(text: str) -> Optional[str]:
    """'<small>Villa</small>' -> 'Villa'"""
    if not text:
        return None
    cleaned = re.sub(r"<[^>]+>", "", text).strip()
    return cleaned or None


def _bbox_check(lat: float, lng: float) -> bool:
    return (_HALKIDIKI_BBOX[0] <= lat <= _HALKIDIKI_BBOX[2]
            and _HALKIDIKI_BBOX[1] <= lng <= _HALKIDIKI_BBOX[3])


def _safe_text(node) -> str:
    """Extract text from selectolax node, stripping SVG/icons."""
    if node is None:
        return ""
    # Remove SVG children first to avoid junk text
    for svg in node.css("svg"):
        svg.decompose()
    return (node.text() or "").strip()

def _parse_int_only(raw: str) -> Optional[int]:
    """Extract first integer from '190 m2', '4 Bedrooms', '2027 Year Built'."""
    if not raw:
        return None
    m = re.search(r"\d+", raw)
    return int(m.group(0)) if m else None


def _parse_euro_price(raw: str) -> Optional[int]:
    """
    Parse euro price string to integer.
    Handles US format (€1,000,000.00) AND EU format (€1.000.000,00).
    """
    if not raw:
        return None
    cleaned = re.sub(r"[^\d.,]", "", raw)
    if not cleaned:
        return None
    last_comma = cleaned.rfind(",")
    last_period = cleaned.rfind(".")
    if last_comma == -1 and last_period == -1:
        try:
            return int(cleaned)
        except ValueError:
            return None
    # If last comma > last period → EU format (comma is decimal separator)
    if last_comma > last_period:
        integer_part = cleaned.split(",")[0].replace(".", "")
    else:
        integer_part = cleaned.split(".")[0].replace(",", "")
    try:
        return int(integer_part)
    except ValueError:
        return None


# ---------- scraper ----------

class GreximoScraper(EnrichmentMixin, BaseScraper):
    """
    greximo.gr — WordPress (Inspiry RealHomes theme) realestate.
    Stage 0 sufficient (no anti-bot detected).
    """

    _NLP_FILLABLE_COLUMNS = (
        "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
        "year_built", "levels",
        # category, price, location omitted — authoritative from JS/structured panel
    )

    def __init__(self):
        super().__init__()
        self.source_domain = "greximo.gr"
        self.base_url = "https://greximo.gr"

    # ---------- COLLECT URLS (Strategy B) ----------

    async def collect_urls(self, min_price: int = 400000) -> List[PropertyTemplate]:
        url = (
            f"{self.base_url}/property-search/"
            f"?location[0]=halkidiki-kassandra"
            f"&location[1]=halkidiki-sithonia"
            f"&status=for-sale"
            f"&min-price={min_price}"
        )

        try:
            response = await self.client.get(url)
            html = response.text
        except Exception as e:
            logger.error(f"[greximo.gr] listings fetch failed: {e}")
            return []

        if not html:
            logger.error(f"[greximo.gr] empty response for {url}")
            return []

        raw_json = _extract_balanced_literal(html, _PROPERTIES_ANCHOR_RE, "[", "]")
        if not raw_json:
            logger.error("[greximo.gr] propertiesMapData not found — layout changed?")
            return []

        try:
            items = json.loads(raw_json)
        except json.JSONDecodeError as e:
            logger.error(f"[greximo.gr] propertiesMapData JSON parse error: {e}")
            return []

        seeds = []
        skipped_bbox = 0
        for item in items:
            try:
                lat = lng = None
                try:
                    lat = float(item.get("lat") or 0) or None
                    lng = float(item.get("lng") or 0) or None
                except (TypeError, ValueError):
                    pass

                if lat and lng and not _bbox_check(lat, lng):
                    skipped_bbox += 1
                    continue

                seed = PropertyTemplate(
                    site_property_id=str(item["id"]),
                    source_domain=self.source_domain,
                    url=item["url"],
                    price=item.get("price"),
                    latitude=lat,
                    longitude=lng,
                    category=_clean_html_tags(item.get("propertyType")),
                    location_raw=item.get("title", ""),
                )
                seeds.append(seed)
            except Exception as e:
                logger.warning(f"[greximo.gr] seed parse error id={item.get('id')}: {e}")
                continue

        logger.info(
            f"[greximo.gr] collect_urls: {len(seeds)} seeds "
            f"({len(items)} JS items, {skipped_bbox} skipped by bbox)"
        )
        return seeds

    # ---------- FETCH DETAILS (canonical 7-step) ----------

    async def fetch_details(self, template: PropertyTemplate) -> Optional[PropertyTemplate]:
        # Step 0: Fetch
        try:
            response = await self.client.get(template.url)
            html = response.text
        except Exception as e:
            logger.error(f"[greximo.gr] fetch failed url={template.url}: {e}")
            return None

        if not html:
            return None

        tree = HTMLParser(html)

        # Step 1: Structured panel
        self._parse_structured_panel(tree, template)

        # Step 2: Description
        self._parse_description(tree, template)

        # Step 3: Coords (verify against listings seed)
        self._parse_coords(html, template)

        # Step 4: Images (gallery)
        template.images = self._parse_images(tree)

        # Step 4.5: Features list
        features = []
        for a in tree.css(".features ul.arrow-bullet-list li a"):
            text = (a.text() or "").strip()
            if text:
                features.append(text)
        if features:
            # Flatten amenities into top-level snake_case boolean keys so
            # quality metric (counts extra_features keys) reflects actual features.
            flattened = {}
            for amenity in features:
                key = re.sub(r"[^a-z0-9]+", "_", amenity.lower()).strip("_")
                if key:
                    flattened[key] = True
            template.extra_features = flattened

        # Step 5: Address / location_raw enrichment
        self._parse_address(tree, template)

        # Step 6: NLP fallback — SKIPPED.
        # EnrichmentMixin._apply_nlp_fallback operates on dicts (data.get("description")),
        # not on PropertyTemplate objects. Greximo's Inspiry RealHomes structured panel
        # reliably populates size_sqm/bedrooms/bathrooms/year_built/land_size_sqm,
        # so NLP fallback isn't required.
        # TODO: refactor to dict-based internal state (matching gl_real_estate.py) if
        # a future property type emerges with empty structured panel.

        # Step 7: Quality gate enforced by daily_sync (eligibility check)
        return template

    # ---------- field extractors ----------

    def _parse_structured_panel(self, tree, template: PropertyTemplate) -> None:
        """
        Parse .property-meta spans into INTEGER fields.

        Note: Pydantic validate_assignment=False on PropertyTemplate, so setattr() does
        NOT run field validators. We must parse to int explicitly before set, otherwise
        raw strings ("190 m2", "4 Bedrooms") would be written and break DB INSERT.
        """
        int_selectors = {
            "size_sqm":      "span.property-meta-size",
            "land_size_sqm": "span.property-meta-lot-size",
            "bedrooms":      "span.property-meta-bedrooms",
            "bathrooms":     "span.property-meta-bath",
            "year_built":    "span.property-meta-year-built",
        }

        for field, sel in int_selectors.items():
            node = tree.css_first(sel)
            if not node:
                continue
            raw = _safe_text(node)
            value = _parse_int_only(raw)
            if value is None:
                continue
            try:
                setattr(template, field, value)
            except Exception as e:
                logger.warning(
                    f"[greximo.gr] {field} setattr failed url={template.url} "
                    f"raw={raw!r} parsed={value}: {e}"
                )

        # Price re-confirm from detail page.
        # Sanity gate: ignore parses < 10000 (likely promo "from €400" text
        # rather than actual price). Seed price from listings JS is authoritative.
        price_node = tree.css_first("span.price-and-type")
        if price_node:
            raw_price = _safe_text(price_node)
            value = _parse_euro_price(raw_price)
            if value is not None and value >= 10000:
                try:
                    template.price = value
                except Exception as e:
                    logger.warning(
                        f"[greximo.gr] price setattr failed url={template.url}: {e}"
                    )
            elif value is not None:
                logger.info(
                    f"[greximo.gr] price re-parse below sanity (got {value}, "
                    f"keeping seed {template.price}) url={template.url}"
                )
            
            # Land remap: greximo shows plot area in property-meta-size (not lot-size).
            # Engine 1 eligibility requires land_size_sqm for Land properties.
            if (template.category or "").strip().lower() == "land":
                if not template.land_size_sqm and template.size_sqm:
                    template.land_size_sqm = template.size_sqm
                    template.size_sqm = None

    def _parse_description(self, tree, template: PropertyTemplate) -> None:
        """Extract description from article.property-item .content."""
        content = tree.css_first("article.property-item div.content")
        if not content:
            return

        # Skip "Additional Details" section to avoid duplication noise
        for extra in content.css("h4.additional-title, ul.additional-details, blockquote"):
            extra.decompose()

        text = content.text(separator="\n", strip=True)
        # Collapse 3+ newlines to 2
        text = re.sub(r"\n{3,}", "\n\n", text).strip()

        if text and len(text) >= 50:
            template.description = text
        else:
            logger.warning(
                f"[greximo.gr] short description ({len(text)} chars) url={template.url}"
            )

    def _parse_coords(self, html: str, template: PropertyTemplate) -> None:
        """Verify lat/lng from var propertyMapData = {...}."""
        raw = _extract_balanced_literal(html, _PROPERTY_MAP_ANCHOR_RE, "{", "}")
        if not raw:
            return
        try:
            data = json.loads(raw)
            lat = float(data.get("lat") or 0) or None
            lng = float(data.get("lng") or 0) or None
            if lat and lng and _bbox_check(lat, lng):
                template.latitude = lat
                template.longitude = lng
        except Exception as e:
            logger.warning(f"[greximo.gr] propertyMapData parse: {e}")

    def _parse_images(self, tree) -> List[str]:
        """
        Extract gallery images from FlexSlider.
        Clone slides have class="clone" AND lack data-fancybox attr →
        filtering by [data-fancybox=gallery-images] auto-excludes them.
        """
        urls = []
        seen = set()
        for a in tree.css(".flexslider a[data-fancybox='gallery-images']"):
            href = a.attributes.get("href")
            if href and href not in seen:
                seen.add(href)
                urls.append(href)
        return urls

    def _parse_address(self, tree, template: PropertyTemplate) -> None:
        """
        Build canonical `location_raw` per project geo-architecture.

        Scraper emits "<Locality>, Halkidiki" — geo_matcher resolves FK chain
        downstream using whitelist text + GPS + Greek-script signals.

        Locality extraction priority:
          1. Detail h1.page-title
          2. JS seed location_raw (originally JS title)
          3. URL slug
          4. address.title element

        Full address text preserved in extra_features['address_streetline']
        as backup signal for geo_matcher's Greek-script matching tier.
        """
        locality = None

        title_node = tree.css_first("h1.page-title span")
        if title_node:
            locality = _extract_locality((title_node.text() or "").strip())

        if not locality and template.location_raw:
            locality = _extract_locality(template.location_raw)

        if not locality:
            locality = _extract_locality_from_url(template.url)

        addr_node = tree.css_first("address.title")
        addr_text = _safe_text(addr_node) if addr_node else ""

        if not locality and addr_text:
            locality = _extract_locality(addr_text)

        # Build canonical location_raw
        if locality:
            template.location_raw = f"{locality}, Halkidiki"
            template.area = locality  # geo_matcher uses as area_name signal
        else:
            template.location_raw = "Halkidiki"
            # leave template.area=None — geo_matcher falls back to lat/lng only

        # Preserve full address for geo_matcher fallback signal
        if addr_text:
            extras = dict(template.extra_features or {})
            extras["address_streetline"] = addr_text
            template.extra_features = extras