"""
Sithonia Rental & Sale Solutions scraper — Estate+ platform.

Estate+ (estateplus.gr / globalconcept.gr) is a Greek real-estate CMS used by
multiple agent sites in Greece. The HTML markup is the SAME Bootstrap template
that powers GLRealEstateScraper:
    `.kf_listing_outer_wrap`   — listing cards
    `.kf_property_detail_link` — info panels
    `.kf_property_detail_Essentail` — <ul><li>Label: Value</li>
    `.kf_property_detail_uptwon` — title + price + description block
    `a[data-imagelightbox="g"]` — lightbox links to HD photos
    `setView([lat, lng], zoom)` / `L.circle([lat, lng], radius)` — Leaflet map

So this scraper mirrors GLRealEstateScraper's structure 1:1. The key
differences are noted below.

Key differences from GLRealEstateScraper
========================================

1) Bilingual site — force ?lang=en
   The site defaults to Greek. Every URL is constructed with `&lang=en` to
   yield stable English labels. SOME values still leak through in Greek
   (notably category for non-residential, e.g. `Category: Αγροτεμάχιο`),
   so we keep a small Greek→English translation map for those.

2) Relative card hrefs
   Cards link to bare IDs: `<a href="104000">`. We construct the absolute
   detail URL `https://.../104000?lang=en` in collect_urls so that
   daily_sync._ingest_new_properties can call fetch_details(prop.url) directly.

3) Area/Subarea/Neighborhood semantics
   This platform uses three administrative levels:
     - Area:         prefecture (always 'Chalkidiki' here) — DROPPED, captured
                     by daily_sync via calc_prefecture.
     - Subarea:      municipality (Sithonia, Kassandra, ...) → Property.subarea
     - Neighborhood: neighborhood (Nikiti, Neos Marmaras, ...) → Property.area
                     (most specific = most useful for clustering)

4) "Area" label ambiguity
   The 'Area' label appears TWICE in the same panel with different meanings:
     - "Area: Chalkidiki" → prefecture (drop)
     - "Area: 155m²"      → size_sqm
   Disambiguation: if value contains digits → size_sqm; else drop.

5) No category filter
   All 49 listings ≥ €400k are ingested, including land/commercial. Property
   columns like bedrooms/bathrooms simply stay None for non-residential.

6) GPS = privacy circle (300m typical radius)
   The Leaflet map uses `L.circle([lat, lng], radius)` to obfuscate exact
   location to a circle ~300m. We extract:
     - lat/lng from setView() (which receives the circle center)
     - radius from L.circle() → extra_features.gps_radius_m
     - extra_features.gps_type = 'circle'
   300m precision still works for clustering (matches building-level).

7) Halkidiki whitelist auto-match
   The domain itself contains 'sithonia' → URL substring match satisfies the
   whitelist for every listing. We still emit a clean English location_raw
   ('Sithonia, Halkidiki') for downstream readability.

8) No LLM fallback
   The structured panels yield rich data — every regex-extractable field is
   already present in HTML. LLMExtractor is skipped (same decision as GL).
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.models.schemas import PropertyTemplate


# =============================================================
# Mappings — pure data, no logic
# =============================================================

# English labels (after ?lang=en) routed to first-class Property columns.
# Lowercase, no trailing colons. 'Area' is handled specially due to ambiguity.
_LABEL_TO_PROPERTY_COLUMN: Dict[str, str] = {
    "price":                "price",
    "bedrooms":             "bedrooms",
    "bathrooms":            "bathrooms",
    "year of construction": "year_built",
    "year built":           "year_built",
    "category":             "category",
    "subarea":              "subarea",      # municipality
    "neighborhood":         "area",          # specific area
    "levels":               "levels",
    "land area":            "land_size_sqm",
    "land":                 "land_size_sqm",
}

# Labels we explicitly DROP — always-same values or duplicates of other fields.
_DROP_LABELS: set = {
    "property id",   # already captured upstream as site_property_id from URL
    "type",          # always "For Sale" for our scope
}

# extra_features.<slug>_count for these integer fields.
_COUNT_LABELS: set = {
    "kitchens",
    "living rooms",
    "wc",
}

# Some values leak through in Greek even when ?lang=en is set (most often
# 'Category:' values for non-residential properties). This map normalises
# them to English for clean DB storage.
_CATEGORY_GREEK_TO_ENGLISH: Dict[str, str] = {
    "αγροτεμάχιο":  "Agricultural Land",
    "οικόπεδο":     "Land",
    "βίλα":         "Villa",
    "μεζονέτα":     "Maisonette",
    "μονοκατοικία": "Detached House",
    "διαμέρισμα":   "Apartment",
    "κατάστημα":    "Shop",
    "γραφείο":      "Office",
    "βιοτεχνία":    "Industrial",
    "studio":       "Studio",
}

# Yes/No normalisation. English-only (ASCII) — site is forced to lang=en.
_YES_VALUES = {"yes", "y", "true", "1"}
_NO_VALUES  = {"no",  "n", "false", "0"}

# Halkidiki bounding box. Used to sanity-check GPS coords — anything outside
# this box is treated as malformed (Leaflet defaults / wrong setView call).
_HALKIDIKI_LAT_RANGE = (39.0, 41.0)
_HALKIDIKI_LNG_RANGE = (22.0, 24.0)

# Maximum pages to walk. Real value is ~5; the cap protects against
# pagination bugs (infinite loops on broken HTML).
_PAGE_SAFETY_CAP = 30


# =============================================================
# Helpers — pure functions, no scraper state
# =============================================================

def _slug(label: str) -> str:
    """
    Convert a free-form English HTML label into a stable JSON key.

    Examples:
      "Floor type"          -> "floor_type"
      "Energy class"        -> "energy_class"
      "Distance from Sea"   -> "distance_from_sea"
      "Air condition"       -> "air_condition"
      "WC"                  -> "wc"
    """
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_int_euro(text: str) -> Optional[int]:
    """
    Parse a EU price string into integer euros.

    Acceptable inputs:
      "420.000€"           -> 420000
      "420.000,00€"        -> 420000  (cents dropped)
      "1.050.000€"         -> 1050000
      "Price: 420.000€"    -> 420000
      ""                   -> None

    Defensive output cap: >€200M is rejected as malformed (likely two prices
    concatenated by HTML quirk). INT32 max is 2.1B; over the cap won't fit
    in Property.price INTEGER column anyway.
    """
    if not text:
        return None
    cleaned = re.sub(r"[^\d.,]", "", text)
    if not cleaned:
        return None
    # If both . and , present, the rightmost separator is decimal — drop suffix.
    if "." in cleaned and "," in cleaned:
        last = max(cleaned.rfind("."), cleaned.rfind(","))
        cleaned = cleaned[:last]
    cleaned = re.sub(r"[.,]", "", cleaned)
    try:
        value = int(cleaned)
    except ValueError:
        return None

    if value > 200_000_000:
        return None
    return value


def _to_int_simple(text: str) -> Optional[int]:
    """Extract first integer from a string. '3', 'Bedrooms: 3' both → 3."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """
    Parse area-in-square-meters strings.

    "155m²"       -> 155.0
    "127 m²"      -> 127.0
    "Size: 200"   -> 200.0
    "10500 m²"    -> 10500.0
    "Under Construction" -> None
    """
    if not text:
        return None
    m = re.search(r"\d+(?:[.,]\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def _normalise_yes_no(text: str) -> Optional[bool]:
    """Normalise 'Yes'/'No' into bool. None for anything else."""
    if not text:
        return None
    s = text.strip().lower()
    if s in _YES_VALUES:
        return True
    if s in _NO_VALUES:
        return False
    return None


def _split_label_value(li_text: str) -> Tuple[str, Optional[str]]:
    """
    Split an <li> text into (label, value).

    "Bedrooms: 3"          -> ("Bedrooms", "3")
    "Type:: For Sale"      -> ("Type", "For Sale")  -- handles double-colon
    "Air condition"        -> ("Air condition", None)  -- boolean amenity
    "Price per m²: 3.088€" -> ("Price per m", "3.088€")  -- ² stripped by slug
    """
    text = li_text.strip()
    if ":" not in text:
        return text, None
    label, _, value = text.partition(":")
    # Handle 'Type::' double-colon — strip leading colon from value
    value = value.lstrip(":").strip()
    return label.strip(), (value if value else None)


def _translate_category(value: Optional[str]) -> Optional[str]:
    """
    Translate Greek category value to English. Returns input unchanged if
    already English or unknown.

    "Αγροτεμάχιο" -> "Agricultural Land"
    "Detached Home" -> "Detached Home"
    """
    if not value:
        return value
    lower = value.strip().lower()
    return _CATEGORY_GREEK_TO_ENGLISH.get(lower, value.strip())


def _extract_category_from_title(title: str) -> Optional[str]:
    """
    Card title contains category + size:

      "Villa 170 m²"        -> "Villa"
      "Detached Home 155 m²" -> "Detached Home"
      "Parcel 10500 m²"     -> "Parcel"
      "Maisonette 75 m²"    -> "Maisonette"

    Strategy: strip the trailing "<num> m²" pattern; what's left is the category.
    """
    if not title:
        return None
    m = re.match(r"^(.+?)\s+\d+", title.strip())
    if m:
        return m.group(1).strip()
    return None


def _extract_size_from_title(title: str) -> Optional[float]:
    """
    Card title contains size as last token:

      "Villa 170 m²"        -> 170.0
      "Parcel 10500 m²"     -> 10500.0
    """
    if not title:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*m²?", title)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None


def _build_location_raw(card_location_text: Optional[str]) -> str:
    """
    Build a clean location_raw string. ALWAYS appends " Halkidiki" to satisfy
    the global whitelist (HALKIDIKI_REGIONS_WHITELIST) downstream.

    Card text format: "Code 80500   Sithonia" (English) — strip "Code <id>"
    prefix and what's left is the locality.

    Returns:
      "Sithonia, Halkidiki" — when locality extracted
      "Halkidiki"           — fallback (still whitelist-safe)
    """
    if not card_location_text:
        return "Halkidiki"
    m = re.search(r"Code\s+\S+\s+(.+?)$", card_location_text.strip())
    if m:
        locality = m.group(1).strip()
        if locality:
            return f"{locality}, Halkidiki"
    return "Halkidiki"


# =============================================================
# Scraper
# =============================================================

class SithoniaRentalSalesScraper(EnrichmentMixin, BaseScraper):

    # Defensive override (same rationale as grekodom + halkidiki_estate):
    # detail page may not have explicit category in its structural panels,
    # so result["category"] = None when NLP runs. Without this override
    # NLP would fill "Land/Plot" → silent overwrite of clean card-derived
    # category on next daily_sync save. Card is the authoritative source.
    #
    # LLM fallback is INTENTIONALLY SKIPPED here (same decision as GL):
    # the structured panel + DataExtractor regex cover the field set
    # well enough; LLM cost not justified.
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )
    """
    Estate+ platform agent — Sithonia Rental & Sale Solutions, Halkidiki.

    At min_price=400000: ~49 listings across 5 pages of 10 cards each.
    Mixed categories: residential (Villa, Maisonette, Apartment, Detached
    Home), commercial (Shop, Office), and land (Parcel, Land).
    """

    BASE_URL = "https://www.sithoniarental-sales.gr"

    def __init__(self):
        super().__init__()
        self.source_domain = "sithoniarental-sales.gr"

    async def fetch_listings(self):
        """Backwards-compatible entry point for the dispatcher."""
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # URL builders
    # ---------------------------------------------------------------

    def _build_list_url(self, page: int, min_price: int) -> str:
        """Construct paginated listing URL with English language forced."""
        return (
            f"{self.BASE_URL}/listings"
            f"?price_min={min_price}"
            f"&search=Search"
            f"&page={page}"
            f"&lang=en"
        )

    def _construct_detail_url(self, card_href: str) -> str:
        """
        Card hrefs are relative (just the numeric ID, e.g. 'href="104000"').
        Build absolute URL with ?lang=en for stable English label parsing
        in fetch_details.
        """
        if not card_href:
            return ""
        if card_href.startswith("http"):
            sep = "&" if "?" in card_href else "?"
            return f"{card_href}{sep}lang=en"
        return f"{self.BASE_URL}/{card_href.lstrip('/')}?lang=en"

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs
    # ---------------------------------------------------------------

    async def collect_urls(self, min_price: int = 400000) -> List[PropertyTemplate]:
        """
        Walk paginated listing. Stop when:
          - .kf_listing_outer_wrap returns empty (post-last page or HTML break)
          - PAGE_SAFETY_CAP exceeded (defensive)

        Each card yields a seed PropertyTemplate with site_property_id, full
        detail URL (with ?lang=en), location_raw, price, size, bedrooms,
        bathrooms, category. fetch_details fills the rest.
        """
        all_properties: List[PropertyTemplate] = []
        page = 1

        while page <= _PAGE_SAFETY_CAP:
            url = self._build_list_url(page, min_price)
            logger.info(f"[{self.source_domain}] Парсинг страницы {page}...")

            try:
                response = await self.client.get(url)
                parser = LexborHTMLParser(response.text)
                cards = parser.css(".kf_listing_outer_wrap")

                if not cards:
                    logger.info(
                        f"[{self.source_domain}] нет карточек на странице {page} "
                        f"— конец пагинации"
                    )
                    break

                page_count = 0
                for card in cards:
                    try:
                        prop = self._parse_card(card)
                        if prop:
                            all_properties.append(prop)
                            page_count += 1
                    except Exception as e:
                        logger.error(
                            f"[{self.source_domain}] ошибка парсинга карточки: {e}"
                        )

                logger.info(
                    f"[{self.source_domain}] страница {page}: "
                    f"собрано {page_count} объектов"
                )

                await asyncio.sleep(2)
                page += 1

            except Exception as e:
                logger.error(
                    f"[{self.source_domain}] критическая ошибка на странице {page}: {e}"
                )
                break

        logger.info(
            f"[{self.source_domain}] Phase 1 завершён: "
            f"{len(all_properties)} URLs за {page - 1} стр."
        )
        return all_properties

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """Extract seed PropertyTemplate from one .kf_listing_outer_wrap card."""
        # Title link — h5 a inside .kf_property_caption
        title_link = card.css_first(".kf_property_caption h5 a")
        if not title_link:
            return None

        title = title_link.text(strip=True)
        href = title_link.attributes.get("href", "")
        if not href:
            return None

        full_url = self._construct_detail_url(href)

        # Site property ID = the relative href itself (just a number)
        site_id = href.split("/")[-1].split("?")[0].strip()
        if not site_id:
            return None

        # Location from <p>Code <strong>{id}</strong>... {Locality}</p>
        loc_p = card.css_first(".kf_property_caption p")
        loc_text = loc_p.text(separator=" ", strip=True) if loc_p else None
        location_raw = _build_location_raw(loc_text)

        # Price = the h5 that contains '€' (second h5 in the card; the first
        # is the title which has size suffix but no currency).
        price_text = None
        for h5 in card.css("h5"):
            text = h5.text(strip=True)
            if "€" in text:
                price_text = text
                break

        # Card-level extracts from title
        size_sqm = _extract_size_from_title(title)
        category = _translate_category(_extract_category_from_title(title))

        # Bedrooms / Bathrooms from "kf_property_dolar" lists.
        # Format: <li><i class="fa fa-bed"></i>Bedrooms: 3</li>
        # We accept both ":" and naked patterns ("Bedrooms 3").
        bedrooms = None
        bathrooms = None
        for li in card.css("ul.kf_property_dolar li"):
            text = li.text(separator=" ", strip=True).lower()
            if "bedroom" in text:
                m = re.search(r"bedrooms?\s*:?\s*(\d+)", text)
                if m:
                    bedrooms = int(m.group(1))
            elif "bathroom" in text:
                m = re.search(r"bathrooms?\s*:?\s*(\d+)", text)
                if m:
                    bathrooms = int(m.group(1))

        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=full_url,
            price=price_text,  # PropertyTemplate.clean_price parses string
            location_raw=location_raw,
            size_sqm=size_sqm,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            category=category,
        )

    # ---------------------------------------------------------------
    # PHASE 2 — fetch full details
    # ---------------------------------------------------------------

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Fetch detail page and extract:
          - Description (.kf_property_detail_uptwon <p> paragraphs)
          - Date listed (DD/MM/YYYY string)
          - Structured panels (Property information + MORE FEATURES)
          - GPS coords from Leaflet JS (setView + circle radius)
          - Photos (data-imagelightbox lightbox links)

        Returns dict matching daily_sync's `base_data.update(details)`
        expectation. None values are filtered at the end so card-level seed
        values are not clobbered when detail-page parsing yields nothing.
        """
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)
            raw_html = response.text

            data: Dict[str, Any] = {
                "description":       "",
                "price":             None,
                "size_sqm":          None,
                "land_size_sqm":     None,
                "bedrooms":          None,
                "bathrooms":         None,
                "year_built":        None,
                "area":              None,
                "subarea":           None,
                "category":          None,
                "levels":            None,
                "site_last_updated": None,
                "latitude":          None,
                "longitude":         None,
                "images":            [],
                "extra_features":    {},
            }

            # 1. Photos — clean lightbox links
            data["images"] = self._collect_image_urls(parser)

            # 2. Description from .kf_property_detail_uptwon
            data["description"] = self._parse_description(parser)

            # 3. Date listed (calendar icon adjacent)
            data["site_last_updated"] = self._parse_listed_date(parser)

            # 4. Structured panels — main source of truth
            self._parse_structured_blocks(parser, data)

            # 5. Coordinates from Leaflet JS
            self._parse_coordinates(raw_html, data)

            # 6. NLP fallback over description (EnrichmentMixin step 5).
            # Fills missing size/year/bedrooms/etc + adds amenity features
            # (sea_view, pool, parking, garden, balcony, ...) detected in
            # description text. Never overwrites structural data.
            self._apply_nlp_fallback(data)

            # 7. Quality Gate — log-only; daily_sync decides retry policy.
            if not self._passes_quality_gate(data.get("description")):
                logger.warning(
                    f"[{self.source_domain}] description below quality gate "
                    f"for {url}"
                )

            # 8. Drop None values so card seed isn't clobbered. Empty list/dict/
            #    string are KEPT (signal "successfully parsed but empty").
            return {k: v for k, v in data.items() if v is not None}

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] ошибка fetch_details для {url}: {e}"
            )
            return {}

    # ---------------------------------------------------------------
    # Phase 2 helpers
    # ---------------------------------------------------------------

    def _collect_image_urls(self, parser: LexborHTMLParser) -> List[str]:
        """
        Photos via a[data-imagelightbox="g"] lightbox links.

        Site renders the bxSlider gallery with clean <a> wrappers around each
        original-resolution image at socratis.estateplus.gr/uploads/{hash}.
        This selector excludes thumbnails, the logo, and bx-clone duplicates.
        """
        photos: List[str] = []
        for a in parser.css('a[data-imagelightbox="g"]'):
            href = a.attributes.get("href", "")
            if href and "uploads" in href.lower() and href not in photos:
                photos.append(href)
        return photos

    def _parse_description(self, parser: LexborHTMLParser) -> str:
        """
        Description = <p> children of .kf_property_detail_uptwon, joined.

        The block contains:
          - <h3>{title}</h3>
          - <h3>{price}€</h3>
          - <ul><li>{location}</li><li>{date}</li></ul>
          - <p>...description paragraphs...</p>
          - <p>...</p>

        We collect all <p> ≥ 30 chars. Fallback: og:description meta.
        """
        upton = parser.css_first(".kf_property_detail_uptwon")
        if not upton:
            og = parser.css_first('meta[property="og:description"]')
            if og:
                return (og.attributes.get("content", "") or "").strip()
            return ""

        paragraphs: List[str] = []
        for p in upton.css("p"):
            txt = p.text(separator=" ", strip=True)
            if txt and len(txt) >= 30 and txt not in paragraphs:
                paragraphs.append(txt)

        if paragraphs:
            return "\n\n".join(paragraphs)

        og = parser.css_first('meta[property="og:description"]')
        if og:
            return (og.attributes.get("content", "") or "").strip()
        return ""

    def _parse_listed_date(self, parser: LexborHTMLParser) -> Optional[str]:
        """
        Date listed appears next to the calendar icon:
          <li><i class="fa fa-calendar"></i><a href="#">21/01/2026</a></li>

        Format DD/MM/YYYY. Stored as-is in PropertyTemplate.site_last_updated
        (which is Optional[str]). Returns None if missing or malformed.
        """
        calendar_icon = parser.css_first(".kf_property_detail_uptwon .fa-calendar")
        if not calendar_icon:
            return None

        parent_li = calendar_icon.parent
        if not parent_li:
            return None

        a_tag = parent_li.css_first("a")
        if not a_tag:
            return None

        date_text = a_tag.text(strip=True)
        if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", date_text):
            return date_text
        return None

    def _parse_coordinates(self, raw_html: str, data: Dict[str, Any]) -> None:
        """
        Estate+ uses Leaflet. Two relevant JS patterns:

          1. var mymap = L.map('map').setView([lat, lng], 15);
          2. L.circle([lat, lng], radius, { ... }).addTo(mymap)...

        Both patterns appear together (the circle is centered on the setView
        location). We extract:

          - latitude/longitude from setView() — center of the obfuscation
            circle
          - extra_features.gps_type = 'circle' — flag that this is NOT exact
          - extra_features.gps_radius_m = radius — 300m typical, used by
            cluster matchers to weight geo similarity properly

        Sanity: coords outside the Halkidiki bbox are rejected (defensive
        against Leaflet defaults like setView([0,0]) on broken pages).
        """
        # 1. setView pattern — primary source of lat/lng
        m_setview = re.search(
            r'setView\(\[\s*([0-9.\-]+)\s*,\s*([0-9.\-]+)\s*\]',
            raw_html,
        )
        if m_setview:
            try:
                lat = float(m_setview.group(1))
                lng = float(m_setview.group(2))
                if (_HALKIDIKI_LAT_RANGE[0] <= lat <= _HALKIDIKI_LAT_RANGE[1]
                        and _HALKIDIKI_LNG_RANGE[0] <= lng <= _HALKIDIKI_LNG_RANGE[1]):
                    data["latitude"] = lat
                    data["longitude"] = lng
            except ValueError:
                pass

        # 2. L.circle(...) radius — privacy obfuscation indicator
        m_circle = re.search(
            r'L\.circle\(\s*\[\s*[0-9.\-]+\s*,\s*[0-9.\-]+\s*\]\s*,\s*(\d+)',
            raw_html,
        )
        if m_circle and data.get("latitude") is not None:
            try:
                radius = int(m_circle.group(1))
                data["extra_features"]["gps_type"] = "circle"
                data["extra_features"]["gps_radius_m"] = radius
            except ValueError:
                pass

    # ---------------------------------------------------------------
    # Structured panels — Property information / MORE FEATURES
    # ---------------------------------------------------------------

    def _parse_structured_blocks(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        Walk every .kf_property_detail_link panel. Each panel has:
          <h5>Property information</h5>     -- or "MORE FEATURES"
          <div class="kf_property_detail_Essentail">
            <ul><li>Label: Value</li> ... </ul>
          </div>

        Panel headings don't matter — both panels share the same <li>label:
        value</li> shape. We process every <li> through _route_field.
        """
        panels = parser.css(".kf_property_detail_link")
        for panel in panels:
            # Defensive: skip wrappers without the Essentail content block
            if not panel.css_first(".kf_property_detail_Essentail"):
                continue

            for li in panel.css(".kf_property_detail_Essentail ul li"):
                raw_text = self._li_visible_text(li)
                if not raw_text:
                    continue
                self._route_field(raw_text, data)

    @staticmethod
    def _li_visible_text(li: LexborNode) -> str:
        """
        Extract the immediate label/value text of one <li>, normalising
        HTML quirks (same logic as GLRealEstate._li_visible_text).

        Issues handled:
        1. Discounted prices: <s><small>1.550.000€</small></s> 1.500.000€
           The <s> (strikethrough) wraps the OLD price. We strip <s>...</s>
           entirely so only the active price remains.
        2. Nested <li> blocks (lexbor recovery quirk on malformed HTML).
        3. HTML entities (&nbsp;, &euro;, etc.).

        Other inline tags (<sup>, <small>, <i>, <a>) are stripped of markup
        but their text content is preserved (so 'm²' becomes 'm2', etc.).
        """
        target = li.css_first("a") or li
        inner = target.html or ""

        # 1. Strike-through (old price) — drop entirely
        inner = re.sub(
            r"<s\b[^>]*>.*?</s>",
            "",
            inner,
            flags=re.DOTALL | re.IGNORECASE,
        )

        # 2. Nested <li> (defensive)
        inner = re.sub(
            r"<li\b[^>]*>.*?</li>",
            "",
            inner,
            flags=re.DOTALL | re.IGNORECASE,
        )

        # 3. Strip remaining tags
        cleaned = re.sub(r"<[^>]+>", " ", inner)

        # 4. Decode common entities
        cleaned = (cleaned
                .replace("&nbsp;", " ")
                .replace("&amp;", "&")
                .replace("&euro;", "€")
                .replace("&#8364;", "€"))
        return re.sub(r"\s+", " ", cleaned).strip()

    def _route_field(self, raw_text: str, data: Dict[str, Any]) -> None:
        """
        Decide where one <li> goes:
          * dropped (Property id, Type::, etc.)
          * Property column (price, size_sqm, bedrooms, ...) — typed write
          * extra_features (everything else) — typed when possible

        'Area' label is special-cased due to value-dependent semantics
        (prefecture text vs sqm number).
        """
        label, value = _split_label_value(raw_text)
        if not label:
            return

        slug = _slug(label)
        if not slug:
            return

        label_lower = label.strip().lower()

        # --- Explicit drops ----------------------------------------
        if label_lower in _DROP_LABELS:
            return

        # --- "Area" label — disambiguate by value pattern ----------
        if label_lower == "area":
            self._route_area_label(value, data)
            return

        # --- Property column ---------------------------------------
        column = _LABEL_TO_PROPERTY_COLUMN.get(label_lower)
        if column is not None:
            self._write_column(column, value, data)
            return

        # --- extra_features ----------------------------------------
        # Boolean amenity (no colon, no value)
        if value is None:
            data["extra_features"][slug] = True
            return

        # Count fields ("Kitchens: 1", "WC: 1") → <slug>_count
        # MUST be checked BEFORE yes/no, because '1' is in _YES_VALUES
        # and would otherwise hijack count=1 → True.
        if label_lower in _COUNT_LABELS:
            n = _to_int_simple(value)
            if n is not None:
                data["extra_features"][f"{slug}_count"] = n
                return

        # "Price per m²: 3.088€" — euros
        if "price" in slug and "€" in value:
            cents = _to_int_euro(value)
            if cents is not None:
                data["extra_features"][slug] = cents
                return

        # Yes/No → bool
        yn = _normalise_yes_no(value)
        if yn is not None:
            data["extra_features"][slug] = yn
            return

        # Anything else → string (energy_class, orientation, view, ...)
        data["extra_features"][slug] = value

    def _route_area_label(
        self,
        value: Optional[str],
        data: Dict[str, Any],
    ) -> None:
        """
        Disambiguate the overloaded 'Area' label:
          'Area: Chalkidiki' (text)   → DROP (prefecture, redundant with
                                        calc_prefecture computed downstream)
          'Area: 155m²'      (digits) → size_sqm

        Strategy: if the value contains a digit, treat as area-in-sqm;
        otherwise treat as prefecture and silently drop. Defensive: only
        write size_sqm if not already set (don't overwrite from later in
        the same panel).
        """
        if not value:
            return
        if re.search(r"\d", value):
            v = _to_float_sqm(value)
            if v is not None and data.get("size_sqm") is None:
                data["size_sqm"] = v
        # else: drop silently — prefecture is always 'Chalkidiki' for our scope

    def _write_column(
        self,
        column: str,
        value: Optional[str],
        data: Dict[str, Any],
    ) -> None:
        """
        Type-coerce a value into the named Property column. Ignores empty
        values (so the structured panel never overwrites with garbage like
        '' or whitespace).

        year_built is bounded to [1901, 2099] — rejects placeholders like
        'Under Construction' (returns None from _to_int_simple anyway).
        """
        if value is None or value == "":
            return

        if column == "price":
            v = _to_int_euro(value)
            if v is not None:
                data["price"] = v
        elif column == "size_sqm":
            v = _to_float_sqm(value)
            if v is not None:
                data["size_sqm"] = v
        elif column == "land_size_sqm":
            v = _to_float_sqm(value)
            if v is not None:
                data["land_size_sqm"] = v
        elif column in {"bedrooms", "bathrooms"}:
            v = _to_int_simple(value)
            if v is not None:
                data[column] = v
        elif column == "year_built":
            # Accept only plausible build years. "Under Construction" → None.
            v = _to_int_simple(value)
            if v is not None and 1900 < v < 2100:
                data["year_built"] = v
        elif column == "levels":
            data["levels"] = value.strip()
        elif column == "category":
            data["category"] = _translate_category(value)
        elif column in {"area", "subarea"}:
            data[column] = value.strip()