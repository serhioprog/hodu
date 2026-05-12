"""
~/hodu/src/scrapers/engel_voelkers.py

Engel & Völkers (engelvoelkers.com) — Halkidiki residential scraper.

Platform: Next.js + React Query SSR hydration
Data extraction: __NEXT_DATA__ JSON parsing (no HTML CSS selectors)
Source: engelvoelkers.com
Target filter: Halkidiki residential properties >= 400_000 EUR

Architecture
------------
collect_urls
    5 paginated requests to /gr/en/propertysearch?...&page={N}
    Each page returns Q0.state.data.listings: list of {restricted, listing} wrappers.
    Page 1 also yields Q1 (geo-coordinates) — ALL 99 GPS coords on every page,
    so we capture them once on page 1.
fetch_details
    GET /gr/en/exposes/{uuid}
    Q0 key=["listing", uuid, ...] returns full 92-field listing object.

ID convention
-------------
- site_property_id  = listing.displayId (e.g. "W-02SB4K") — human-readable, stable
- detail URL        = listing.id (UUID v5) — used as path param

Image URL convention
--------------------
UploadCare CDN hosted on engelvoelkers.com subdomain. Full-quality archive:
    https://uploadcare.engelvoelkers.com/{uuid}/-/format/jpeg/-/stretch/off/
        -/quality/best/-/resize/1920x/
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser

from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.models.schemas import PropertyTemplate


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_BASE_URL = "https://www.engelvoelkers.com"
_LISTING_PATH = "/gr/en/propertysearch"
_DETAIL_PATH_TPL = "/gr/en/exposes/{uuid}"
_PLACE_ID_HALKIDIKI = "ChIJI8OM-QSPqBQRsMO54iy9AAM"
_SOURCE_DOMAIN = "engelvoelkers.com"

# Server returns max 23 listings per page (confirmed empirically)
_PAGE_SIZE = 23

# Safety cap on pagination
_MAX_PAGES = 10

# Polite pause between listing-page fetches
_INTER_PAGE_SLEEP_SEC = 2.0

# UploadCare CDN base URL + transformation chain for full-quality archive
_UPLOADCARE_BASE = "https://uploadcare.engelvoelkers.com"
_IMAGE_TRANSFORMS = "/-/format/jpeg/-/stretch/off/-/quality/best/-/resize/1920x/"

# Default fuzz radius if not present (E&V uses 750m globally)
_DEFAULT_FUZZ_M = 750


# Category mapping: (propertyType, propertySubType) → canonical category.
# Subtype precedence: if (type, subtype) match → use that.
# Else fall back to subtype-as-title-case, then _CATEGORY_BY_TYPE.
_CATEGORY_MAP: Dict[Tuple[str, Optional[str]], str] = {
    # Houses
    ("house", "villa"):          "Villa",
    ("house", "townhouse"):      "Townhouse",
    ("house", "detached_house"): "Detached House",
    ("house", "country_house"):  "Country House",
    ("house", "farmhouse"):      "Country House",
    ("house", "bungalow"):       "Bungalow",
    ("house", "mansion"):        "Mansion",
    ("house", "castle"):         "Castle",
    ("house", "chalet"):         "Chalet",
    ("house", "semi_detached"):  "Semi-detached House",
    ("house", "terraced"):       "Terraced House",
    # Apartments
    ("apartment", "penthouse"):  "Penthouse",
    ("apartment", "studio"):     "Studio",
    ("apartment", "loft"):       "Loft",
    ("apartment", "maisonette"): "Maisonette",
    ("apartment", "duplex"):     "Maisonette",
    # Land
    ("land", "building_plot"):   "Plot",
    ("land", "agricultural"):    "Agricultural Land",
    # Commercial (probably out of scope at this filter but safe to include)
    ("commercial", "office"):    "Office",
    ("commercial", "retail"):    "Shop",
    ("commercial", "hotel"):     "Hotel",
}

# Fallback: propertyType-only when subtype missing or unknown
_CATEGORY_BY_TYPE: Dict[str, str] = {
    "house":      "House",
    "apartment":  "Apartment",
    "land":       "Land",
    "commercial": "Commercial Property",
}

# Boolean amenity key normalization: E&V camelCase → our snake_case
_AMENITY_KEY_MAP: Dict[str, str] = {
    "hasAirConditioning":  "ac",
    "hasBalcony":          "balcony",
    "hasBasement":         "basement",
    "hasBuiltInKitchen":   "built_in_kitchen",
    "hasGarden":           "garden",
    "hasPatio":            "patio",
    "hasSeaOrLakeView":    "sea_or_lake_view",
    "hasSecuritySystem":   "security_system",
    "hasTerrace":          "terrace",
    "hasWaterfront":       "waterfront",
    "hasOpenView":         "open_view",
    "hasGreenView":        "green_view",
    "hasMountainView":     "mountain_view",
    "hasGuestToilet":      "guest_toilet",
    "hasFirePlace":        "fireplace",
    "hasCoveredParking":   "covered_parking",
    "isPetsAllowed":       "pets_allowed",
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers (module-level)
# ─────────────────────────────────────────────────────────────────────────────

def _camel_to_snake(s: str) -> str:
    """`hasAirConditioning` → `has_air_conditioning`."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int; reject bool, None, unparseable strings. Unwrap {min,max} dicts."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, dict):
        v = v.get('min')
        if v is None:
            return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float; reject bool, None, unparseable strings. Unwrap {min,max} dicts."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, dict):
        v = v.get('min')
        if v is None:
            return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _min_or_value(d: Any) -> Optional[Any]:
    """
    For E&V's `{min, max}` range dicts, return `min`.
    For plain scalars, return as-is. For None → None.
    """
    if d is None:
        return None
    if isinstance(d, dict):
        return d.get("min")
    return d


def _extract_next_data(html: str) -> Optional[dict]:
    """Extract and parse the <script id="__NEXT_DATA__"> JSON blob."""
    parser = LexborHTMLParser(html)
    script = parser.css_first('script#__NEXT_DATA__')
    if not script:
        return None
    raw = script.text() or ""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(f"[{_SOURCE_DOMAIN}] __NEXT_DATA__ parse failed: {exc}")
        return None


def _queries_by_key(data: dict, key_prefix: str) -> List[dict]:
    """
    Return all dehydratedState queries whose `queryKey[0] == key_prefix`.
    E.g. `key_prefix='listings'` finds `["listings", {...}]`-keyed queries.
    """
    try:
        queries = data["props"]["pageProps"]["dehydratedState"]["queries"]
    except (KeyError, TypeError):
        return []
    matched: List[dict] = []
    for q in queries:
        qk = q.get("queryKey", [])
        if isinstance(qk, list) and qk and qk[0] == key_prefix:
            matched.append(q)
    return matched


def _derive_category(prop_type: Optional[str], sub_type: Optional[str]) -> Optional[str]:
    """Map (propertyType, propertySubType) → canonical category string."""
    if not prop_type:
        return None
    pt = prop_type.lower()
    st = sub_type.lower() if sub_type else None
    key = (pt, st)
    if key in _CATEGORY_MAP:
        return _CATEGORY_MAP[key]
    if st:
        # Subtype provided but unmapped: capitalize as fallback
        return st.replace("_", " ").title()
    return _CATEGORY_BY_TYPE.get(pt, pt.title())


def _strip_condition_prefix(condition: Optional[str]) -> Optional[str]:
    """`condition.needsRefurbishment` → `Needs Refurbishment`."""
    if not condition:
        return None
    if condition.startswith("condition."):
        condition = condition[len("condition."):]
    spaced = re.sub(r"(?<!^)(?=[A-Z])", " ", condition)
    return spaced.title()


def _construct_image_url(image_id: str) -> str:
    """Build full-quality UploadCare URL from image UUID."""
    if not image_id:
        return ""
    return f"{_UPLOADCARE_BASE}/{image_id.strip()}{_IMAGE_TRANSFORMS}"


def _clean_description_html(text: str) -> str:
    """Decode common HTML entities and strip inline tags from a description string."""
    if not text:
        return ""
    # Common entity decodes
    t = (text
         .replace("&#39;", "'")
         .replace("&apos;", "'")
         .replace("&amp;", "&")
         .replace("&quot;", '"')
         .replace("&lt;", "<")
         .replace("&gt;", ">")
         .replace("&nbsp;", " "))
    # <br> → newline; strip any other simple tags
    t = re.sub(r"<br\s*/?>", "\n", t, flags=re.IGNORECASE)
    t = re.sub(r"<[^>]+>", "", t)
    return t.strip()


def _join_descriptions(*parts: Optional[str]) -> Optional[str]:
    """Join non-empty description chunks separated by a blank line."""
    cleaned = [_clean_description_html(p) for p in parts if p and p.strip()]
    cleaned = [c for c in cleaned if c]
    if not cleaned:
        return None
    return "\n\n".join(cleaned)


def _location_text(address_components: List[dict]) -> str:
    """
    Build a comma-joined location string from addressComponents.
    addressComponents[i] = {placeId, placeType, text, autogenUrl}.
    Sort: country first, then admin levels descending (broadest → narrowest).
    """
    if not address_components:
        return ""
    # Order encountered in samples is already broadest → narrowest.
    # We just collect texts and join.
    parts: List[str] = []
    for comp in address_components:
        text = (comp.get("text") or "").strip()
        if text:
            parts.append(text)
    return ", ".join(parts)


def _ensure_halkidiki_prefix(location_raw: str) -> str:
    """Whitelist matcher expects 'Halkidiki' or 'Chalkidiki' in location_raw."""
    if not location_raw:
        return "Chalkidiki"
    lower = location_raw.lower()
    if "halkidiki" in lower or "chalkidiki" in lower:
        return location_raw
    return f"Chalkidiki, {location_raw}"


# ─────────────────────────────────────────────────────────────────────────────
# Scraper
# ─────────────────────────────────────────────────────────────────────────────

class EngelVoelkersScraper(EnrichmentMixin, BaseScraper):

    # Defensive override (same rationale as grekodom + halkidiki_estate):
    # E&V's JSON normally provides category, but for the rare property
    # where JSON is silent on it, NLP would fill "Land/Plot" (NLP's
    # canonical name) and overwrite the cleaner card-derived value on
    # the next daily_sync save. Card stays authoritative.
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )

    # E&V's JSON uses more specific slug names than the mixin's default
    """
    Engel & Völkers (engelvoelkers.com) — Halkidiki residential, ≥ 400_000 EUR.

    All data extraction via __NEXT_DATA__ JSON (Next.js + React Query SSR).
    No HTML CSS selectors are used for property fields.
    """

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ─── URL builders ────────────────────────────────────────────────────────

    def _construct_listing_url(self, page: int, min_price: int) -> str:
        """Build paginated search URL. Filters must match Q0 queryKey expectations."""
        params = (
            f"businessArea[]=residential"
            f"&currency=EUR"
            f"&hasSessionBounds=true"
            f"&mapMode=place_mode"
            f"&measurementSystem=metric"
            f"&page={page}"
            f"&placeIds[]={_PLACE_ID_HALKIDIKI}"
            f"&placeName=Halkidiki%2C%20Greece"
            f"&price.min={min_price}"
            f"&propertyMarketingType[]=sale"
            f"&searchMode=classic"
            f"&searchRadius=0"
            f"&sortingOptions[]=SALES_PRICE_DESC"
        )
        return f"{_BASE_URL}{_LISTING_PATH}?{params}"

    def _construct_detail_url(self, uuid: str) -> str:
        return f"{_BASE_URL}{_DETAIL_PATH_TPL.format(uuid=uuid)}"

    # ─── Phase 1: collect_urls ───────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        """
        Iterate paginated listings, return PropertyTemplate seeds.

        Page 1's response also includes Q1 (geo-coordinates) which contains GPS
        for ALL listings matching the filter — captured once and indexed by UUID.
        """
        seeds: Dict[str, PropertyTemplate] = {}
        geo_map: Dict[str, Tuple[float, float, int]] = {}
        last_page_processed = 0

        for page in range(1, max_pages + 1):
            url = self._construct_listing_url(page=page, min_price=min_price)
            logger.info(f"[{self.source_domain}] Парсинг страницы {page}...")

            try:
                response = await self.client.get(url)
            except Exception as exc:
                logger.error(f"[{self.source_domain}] page {page} fetch failed: {exc!r}")
                break

            data = _extract_next_data(response.text)
            if not data:
                logger.warning(f"[{self.source_domain}] no __NEXT_DATA__ on page {page} — end")
                break

            # Q0 = listings query
            listings_queries = _queries_by_key(data, "listings")
            if not listings_queries:
                logger.warning(f"[{self.source_domain}] no 'listings' query on page {page}")
                break

            lq = listings_queries[0]

            # Sanity: server-acknowledged page must match what we asked for.
            qk = lq.get("queryKey", [])
            server_page: Optional[int] = None
            if isinstance(qk, list) and len(qk) > 1 and isinstance(qk[1], dict):
                server_page = qk[1].get("options", {}).get("page")
            if server_page is not None and server_page != page:
                logger.info(
                    f"[{self.source_domain}] server returned page {server_page} "
                    f"(asked for {page}) — pagination capped at the source"
                )
                break

            data_obj = lq.get("state", {}).get("data", {}) or {}
            listings = data_obj.get("listings", []) or []
            if not listings:
                logger.info(f"[{self.source_domain}] page {page}: empty listings — end of results")
                break

            # Page 1: capture geo-coordinates for ALL listings (single shot)
            if page == 1:
                self._capture_geo_coords(data, geo_map)
                total = data_obj.get("listingsTotal")
                if total is not None:
                    logger.info(f"[{self.source_domain}] listingsTotal reported: {total}")

            # Process wrapper objects { restricted, listing }
            for wrapper in listings:
                listing = wrapper.get("listing") or {}
                if not listing:
                    continue
                if wrapper.get("restricted"):
                    # Restricted listings have limited card data; still try to seed
                    logger.debug(
                        f"[{self.source_domain}] note: restricted listing {listing.get('id')!r}"
                    )
                seed = self._build_seed(listing, geo_map)
                if seed is None:
                    continue
                seeds.setdefault(seed.site_property_id, seed)

            last_page_processed = page
            logger.info(
                f"[{self.source_domain}] страница {page}: {len(listings)} объектов "
                f"(всего seeds: {len(seeds)})"
            )

            # End-of-results: partial page
            if len(listings) < _PAGE_SIZE:
                logger.info(
                    f"[{self.source_domain}] page {page} is partial "
                    f"({len(listings)} < {_PAGE_SIZE}) — last page"
                )
                break

            await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)

        result = list(seeds.values())
        logger.info(
            f"[{self.source_domain}] Phase 1 завершён: {len(result)} URLs "
            f"за {last_page_processed} стр."
        )
        return result

    def _capture_geo_coords(
        self,
        data: dict,
        geo_map: Dict[str, Tuple[float, float, int]],
    ) -> None:
        """
        Q1 (geo-coordinates) returns ALL listings' GPS on every page. We capture
        once on page 1 — same data on subsequent pages would be redundant.
        """
        geo_queries = _queries_by_key(data, "geo-coordinates")
        if not geo_queries:
            return
        points = geo_queries[0].get("state", {}).get("data", {}).get("points", []) or []
        for pt in points:
            pt_id = pt.get("id")
            loc = pt.get("displayLocation") or {}
            lat = _safe_float(loc.get("lat"))
            lng = _safe_float(loc.get("lng"))
            fuzz = _safe_int(pt.get("fuzzyMoveByMeter")) or _DEFAULT_FUZZ_M
            if pt_id and lat is not None and lng is not None:
                geo_map[pt_id] = (lat, lng, fuzz)
        logger.info(f"[{self.source_domain}] geo-coordinates captured: {len(geo_map)} points")

    def _build_seed(
        self,
        listing: dict,
        geo_map: Dict[str, Tuple[float, float, int]],
    ) -> Optional[PropertyTemplate]:
        """
        Build a PropertyTemplate from a listings-page listing object.
        site_property_id = displayId (stable, human-readable).
        Detail URL uses the UUID `id`.
        """
        uuid = listing.get("id")
        display_id = listing.get("displayId")
        if not uuid or not display_id:
            logger.warning(
                f"[{self.source_domain}] listing missing id/displayId: id={uuid!r}, "
                f"displayId={display_id!r}"
            )
            return None

        url = self._construct_detail_url(uuid)

        # Card-level category (no subtype available at card level for some types)
        category = _derive_category(
            listing.get("propertyType"),
            listing.get("propertySubType"),
        )

        # Areas: prefer usable, then living, then total
        area = listing.get("area") or {}
        size_sqm = (
            _safe_float(area.get("usableSurface"))
            or _safe_float(area.get("livingSurface"))
            or _safe_float(area.get("totalSurface"))
        )
        land_size = _safe_float(area.get("plotSurface"))

        # Price
        price_dict = listing.get("price") or {}
        sales_price = _safe_float(price_dict.get("salesPrice"))
        price = int(round(sales_price)) if sales_price is not None else None

        # Rooms/bed/bath (use .min from {min, max} dicts)
        bedrooms = _safe_int(_min_or_value(listing.get("bedrooms")))
        bathrooms = _safe_int(_min_or_value(listing.get("bathrooms")))

        # Title
        title = (listing.get("profile") or {}).get("title")

        # Location
        location_raw = _ensure_halkidiki_prefix(
            _location_text(listing.get("addressComponents") or [])
        )

        # GPS from Q1 geo-coords
        lat = lng = None
        if uuid in geo_map:
            lat, lng, _fuzz = geo_map[uuid]

        return PropertyTemplate(
            site_property_id=display_id,
            url=url,
            source_domain=self.source_domain,
            category=category,
            price=price,
            size_sqm=size_sqm,
            land_size_sqm=land_size,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            location_raw=location_raw,
            latitude=lat,
            longitude=lng,
            extra_features={'listing_title': title} if title else None,
        )

    # ─── Phase 2: fetch_details ──────────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Fetch /gr/en/exposes/{uuid} and extract full property data.
        Returns a dict that BaseScraper merges with the seed (detail overrides).
        """
        try:
            response = await self.client.get(url)
        except Exception as exc:
            logger.error(f"[{self.source_domain}] detail fetch failed for {url}: {exc!r}")
            return {}

        data = _extract_next_data(response.text)
        if not data:
            logger.warning(f"[{self.source_domain}] no __NEXT_DATA__ on detail {url}")
            return {}

        listing_queries = _queries_by_key(data, "listing")
        if not listing_queries:
            logger.warning(f"[{self.source_domain}] no 'listing' query in detail {url}")
            return {}

        q0_data = listing_queries[0].get("state", {}).get("data", {}) or {}
        status = q0_data.get("status")
        listing = q0_data.get("listing")

        if status != "OK" or not listing:
            logger.warning(
                f"[{self.source_domain}] detail status={status!r}, listing missing for {url}"
            )
            return {}

        return self._build_details(listing)

    def _build_details(self, listing: dict) -> Dict[str, Any]:
        """Project the 92-field listing object to our flat detail dict."""
        profile = listing.get("profile") or {}
        area = listing.get("area") or {}
        features = listing.get("features") or {}

        # ─── Descriptions ───────────────────────────────────────────────
        description = _join_descriptions(
            profile.get("description"),
            profile.get("locationDescription"),
        )

        # ─── Category (full subtype available here) ─────────────────────
        category = _derive_category(
            listing.get("propertyType"),
            listing.get("propertySubType"),
        )

        # ─── Sizes ──────────────────────────────────────────────────────
        size_sqm = (
            _safe_float(area.get("usableSurface"))
            or _safe_float(area.get("livingSurface"))
            or _safe_float(area.get("totalSurface"))
        )
        land_size = _safe_float(area.get("plotSurface"))

        # ─── Price / numerics ───────────────────────────────────────────
        sales_price = _safe_float((listing.get("price") or {}).get("salesPrice"))
        price = int(round(sales_price)) if sales_price is not None else None

        bedrooms = _safe_int(_min_or_value(listing.get("bedrooms")))
        bathrooms = _safe_int(_min_or_value(listing.get("bathrooms")))
        year_built = _safe_int(_min_or_value(listing.get("constructionYear")))
        floors_min = _safe_int(_min_or_value(listing.get("floors")))
        floor_min = _safe_int(_min_or_value(listing.get("floor")))
        rooms_total = _safe_int(_min_or_value(listing.get("rooms")))

        # ─── GPS ────────────────────────────────────────────────────────
        lat = _safe_float(listing.get("displayLat"))
        lng = _safe_float(listing.get("displayLng"))
        fuzz = _safe_int(listing.get("fuzzyMoveByMeter")) or _DEFAULT_FUZZ_M

        # ─── Title / location ───────────────────────────────────────────
        title = profile.get("title")
        location_raw = _ensure_halkidiki_prefix(
            _location_text(listing.get("addressComponents") or [])
        )

        # ─── Images (ordered, deduplicated) ─────────────────────────────
        image_urls: List[str] = []
        for img in listing.get("propertyImages") or []:
            img_id = img.get("id")
            if img_id:
                u = _construct_image_url(img_id)
                if u:
                    image_urls.append(u)
        if not image_urls:
            for img_id in listing.get("uploadCareImageIds") or []:
                u = _construct_image_url(img_id)
                if u:
                    image_urls.append(u)
        # Dedup preserving order
        seen: set = set()
        image_urls = [u for u in image_urls if not (u in seen or seen.add(u))]

        # ─── Extra features ─────────────────────────────────────────────
        extra_features: Dict[str, Any] = {}

        # Boolean amenities — features dict is canonical (17 keys in sample)
        for k, v in features.items():
            if isinstance(v, bool):
                norm = _AMENITY_KEY_MAP.get(k, _camel_to_snake(k))
                extra_features[norm] = v

        # Fallback: also pick up any top-level has*/is* booleans not in features
        for k, v in listing.items():
            if isinstance(v, bool) and (k.startswith("has") or k.startswith("is")):
                norm = _AMENITY_KEY_MAP.get(k, _camel_to_snake(k))
                extra_features.setdefault(norm, v)

        # Condition (i18n key)
        condition = _strip_condition_prefix(listing.get("condition"))
        if condition:
            extra_features["condition"] = condition

        # Flooring (list of strings → comma-joined)
        flooring = listing.get("flooring")
        if flooring and isinstance(flooring, list):
            joined = ", ".join(str(f).replace("_", " ").title() for f in flooring if f)
            if joined:
                extra_features["flooring"] = joined

        # Energy
        energy_val = _min_or_value(listing.get("energyClassNormalized"))
        if energy_val:
            extra_features["energy_class"] = str(energy_val)
        if listing.get("energyCertAvailable"):
            extra_features["energy_cert_available"] = listing.get("energyCertAvailable")

        # Building floor info
        if floors_min is not None:
            extra_features["floors_total"] = floors_min
        if floor_min is not None:
            extra_features["unit_floor"] = floor_min

        # GPS metadata (fuzz/circle privacy radius)
        if lat is not None and lng is not None:
            extra_features["gps_type"] = "circle"
            extra_features["gps_radius_m"] = int(fuzz)

        # Property subtype passthrough
        if listing.get("propertySubType"):
            extra_features["property_subtype"] = listing["propertySubType"]

        # Virtual tour
        vtour = listing.get("vtourUrlNormalized") or listing.get("vtourUrl")
        if vtour:
            extra_features["vtour_url"] = vtour

        # Broker / shop info
        for src, dst in [
            ("shopName",         "shop_name"),
            ("shopAddressCity",  "shop_city"),
            ("shopPhoneNumber",  "shop_phone"),
            ("shopEmail",        "shop_email"),
        ]:
            v = listing.get(src)
            if v:
                extra_features[dst] = v
        agent = listing.get("agent") or {}
        if agent.get("name"):
            extra_features["agent_name"] = agent["name"]
        if agent.get("jobTitle"):
            extra_features["agent_role"] = agent["jobTitle"]

        # Marketing identifiers
        if listing.get("displayId"):
            extra_features["display_id"] = listing["displayId"]
        if listing.get("externalId"):
            extra_features["external_id"] = listing["externalId"]

        # Granular surfaces (in addition to size_sqm)
        if area.get("usableSurface") is not None:
            extra_features["usable_surface_sqm"] = _safe_float(area.get("usableSurface"))
        if area.get("livingSurface") is not None:
            extra_features["living_surface_sqm"] = _safe_float(area.get("livingSurface"))
        if area.get("totalSurface") is not None:
            extra_features["total_surface_sqm"] = _safe_float(area.get("totalSurface"))

        # Total rooms (not the same as bedrooms — habitable rooms)
        if rooms_total is not None:
            extra_features["rooms_count"] = rooms_total

        # ─── Assemble result ────────────────────────────────────────────
        result: Dict[str, Any] = {
            "description": description,
            "images": image_urls,
            "extra_features": extra_features,
        }
        if category:
            result["category"] = category
        if size_sqm is not None:
            result["size_sqm"] = size_sqm
        if land_size is not None:
            result["land_size_sqm"] = land_size
        if price is not None:
            result["price"] = price
        if bedrooms is not None:
            result["bedrooms"] = bedrooms
        if bathrooms is not None:
            result["bathrooms"] = bathrooms
        if year_built is not None:
            result["year_built"] = year_built
        if lat is not None:
            result["latitude"] = lat
        if lng is not None:
            result["longitude"] = lng
        if title:
            extra_features["listing_title"] = title
        if location_raw:
            result["location_raw"] = location_raw
        # For E&V, 'floors' is building floors total. For a villa/townhouse,
        # that's effectively the level count. For an apartment unit, it's
        # the building's floors — keep as 'levels' for consistency with peers.
        if floors_min is not None:
            result["levels"] = str(floors_min)

        # ─── Step 5: NLP fallback over description ─────────────────────────
        # E&V JSON gives strong structural data; NLP enriches extra_features
        # with amenities mentioned in description (sea_view, pool, balcony,
        # garden, fireplace, etc.) and fills year_built/levels when JSON
        # didn't have them. Never overwrites structural fields.
        self._apply_nlp_fallback(result)

        # ─── Step 7: Quality Gate (log-only) ───────────────────────────────
        if not self._passes_quality_gate(result.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate "
                f"for {url}"
            )

        return result