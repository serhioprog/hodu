"""
Popi Kalogirou Real Estate (popikalogirou-realestate.gr).

Joomla site using the "Estate Agent" component (eaimproved.eu, v3.3.2).
Single search-results page returns all For-Sale properties (~11 at the
time of writing). No anti-bot protection — direct HTTP works.

Extraction pipeline mirrors the canonical pattern of GL Real Estate +
Greek Exclusive Properties:
  1. PHASE 1 — listing-card parse. Joomla EA cards are RICH on this site
     (Reference Nr, category badge, EU-formatted price, m², rooms,
     distance-from-sea, thumbnail). Most fields populated here.
  2. PHASE 2 — fetch_details:
     a) Description selector chain (.ea_object_description → og:description)
     b) Image gallery (Joomla EA paths: /media/com_estateagent/pictures/)
     c) Coordinates (3-tier: data-attrs → JS setView → Halkidiki bbox regex)
     d) Structured detail panel (label: value rows; Joomla EA convention)
     e) NLP fallback via DataExtractor.analyze_full_text — fills blanks
        only (structured-source values always win on overlap)
     f) calc_municipality routing (Kassandra / Sithonia / Nea Propontida /
        Aristotelis / Polygyros) based on detail-page title text
     g) Quality Gate — description >= 50 chars (log-only)

Field mapping:
  * site_property_id = Reference Nr (customer-facing) NOT the Joomla URL ID.
    URL ID (5327) is internal; Reference (2743) is the persistent customer-
    facing identifier.
  * Sites/Parcels categories → m² goes to land_size_sqm, NOT size_sqm.
  * "Rooms" in card → bedrooms approximation. Detail panel "Bedrooms"
    field, when present, takes priority.
  * EU number format (1.500.000 = one and a half million) handled by
    _to_int_euro / _to_float_sqm helpers.

Server filtering: the site's search form caps `minvalue` at €300k, which
is below our €400k threshold — we GET the default listing (all sale items)
and filter client-side. `?limit=0` is appended defensively so the scraper
stays correct if the agency adds more inventory beyond the default page size.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.models.schemas import PropertyTemplate


# =============================================================
# Mappings — module-level constants
# =============================================================

# Category badge → Property.category. Both singular and plural forms
# accepted (the site sometimes pluralises the badge text).
_CATEGORY_MAP: Dict[str, str] = {
    "apartment":           "Apartment",
    "apartments":          "Apartment",
    "maisonette":          "Maisonette",
    "maisonettes":         "Maisonette",
    "detached house":      "Detached House",
    "detached houses":     "Detached House",
    "villa":               "Villa",
    "villas":              "Villa",
    "site":                "Plot",
    "sites":               "Plot",
    "parcel":              "Parcel",
    "parcels":             "Parcel",
    "block of flats":      "Apartment complex",
    "blocks of flats":     "Apartment complex",
    "investment property": "Commercial",
}

# Neighborhood (lowercase) → (English canonical, Municipality).
# Built from the agency's town dropdown; municipality routing follows the
# Halkidiki administrative map. Used both for location_raw construction
# and calc_municipality routing.
_NEIGHBORHOOD_MAP: Dict[str, Tuple[str, str]] = {
    # Nea Propontida
    "nea iraklia":         ("Nea Iraklia", "Nea Propontida"),
    "nea kallikratia":     ("Nea Kallikrateia", "Nea Propontida"),
    "nea kalikratia":      ("Nea Kallikrateia", "Nea Propontida"),
    "kallikrateia":        ("Nea Kallikrateia", "Nea Propontida"),
    "kallikratia":         ("Nea Kallikrateia", "Nea Propontida"),
    "moudania":            ("Moudania", "Nea Propontida"),
    "nea moudania":        ("Moudania", "Nea Propontida"),
    "triglia":             ("Triglia", "Nea Propontida"),
    "triglia beach":       ("Triglia Beach", "Nea Propontida"),
    "paralia triglias":    ("Paralia Triglias", "Nea Propontida"),
    "flogita":             ("Flogita", "Nea Propontida"),
    "nea plagia":          ("Nea Plagia", "Nea Propontida"),
    "potidaia":            ("Potidaia", "Nea Propontida"),
    "nea silata":          ("Nea Silata", "Nea Propontida"),
    "sozopoli":            ("Sozopoli", "Nea Propontida"),
    "paralia dionisiou":   ("Paralia Dionisiou", "Nea Propontida"),
    "paralia dionysiou":   ("Paralia Dionysiou", "Nea Propontida"),
    "dionisiou beach":     ("Dionisiou Beach", "Nea Propontida"),
    # Kassandra
    "nea fokaia":          ("Nea Fokaia", "Kassandra"),
    "nea gonia":           ("Nea Gonia", "Kassandra"),
    "afytos":              ("Afytos", "Kassandra"),
    "fourka":              ("Fourka", "Kassandra"),
    "skala fourkas":       ("Skala Fourkas", "Kassandra"),
    "polychrono":          ("Polychrono", "Kassandra"),
    "pefkochori":          ("Pefkochori", "Kassandra"),
    "chanioti":            ("Chanioti", "Kassandra"),
    "kassandra":           ("Kassandra", "Kassandra"),
    "kryopigi":            ("Kryopigi", "Kassandra"),
    "kallithea":           ("Kallithea", "Kassandra"),
    "elani":               ("Elani", "Kassandra"),
    # Polygyros
    "gerakini":            ("Gerakini", "Polygyros"),
    "ormos panagias":      ("Ormos Panagias", "Polygyros"),
    "metamorfosi":         ("Metamorfosi", "Polygyros"),
    "agios pavlos":        ("Agios Pavlos", "Polygyros"),
    # Aristotelis
    "nea roda":            ("Nea Roda", "Aristotelis"),
    "νέα ηράκλεια":        ("Nea Iraklia", "Nea Propontida"),
    "νέα καλλικράτεια":    ("Nea Kallikrateia", "Nea Propontida"),
    "νέα κασσάνδρα":       ("Kassandra", "Kassandra"),
    "νέα σίλατα":          ("Nea Silata", "Nea Propontida"),
    "νέα φλογητά":         ("Flogita", "Nea Propontida"),
    "νέα γωνία":           ("Nea Gonia", "Kassandra"),
    "νέα πλάγια":          ("Nea Plagia", "Nea Propontida"),
    "σκάλα φούρκας":       ("Skala Fourkas", "Kassandra"),
    "νέα μουδανιά":        ("Moudania", "Nea Propontida"),
    "παραλία διονυσίου":   ("Paralia Dionisiou", "Nea Propontida"),
    "νέα ποτείδαια":       ("Potidaia", "Nea Propontida"),
    "νέα τρίγλια":         ("Triglia", "Nea Propontida"),
}

# NLP smart_data routing — same convention as Greek Exclusive scraper.
# Top-level keys land either in Property columns (if not yet filled) or
# in extra_features as integer counts. Anything not in these sets stays
# in details["extra_features"] as a string.
_COLUMN_KEYS = {
    "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
    "year_built", "levels", "category", "site_property_id",
}
_EXTRA_NUMERIC_KEYS = {
    "pool_size_sqm", "parking_count", "elevator_count",
    "buildings_count", "rooms_count", "beds_count",
    "renovation_year", "distance_to_sea",
    "living_rooms_count", "kitchens_count",
}

# Maps NLP canonical key → set of structural slugs that mean the same thing.
# If ANY of the related slugs is already in extra_features (from the panel
# or amenities parse), the NLP key is dropped to avoid visual duplicates.
_NLP_TO_STRUCTURAL = {
    "alarm_system":     {"alarm", "security_door", "security_system"},
    "storage_room":     {"storage"},
    "parking":          {"outdoor_garage", "garage", "parking_lot",
                         "covered_parking",
                         "1_parking_spot", "2_parking_spots",
                         "3_parking_spots", "4_parking_spots"},
    "swimming_pool":    {"pool", "indoor_outdoor_pool",
                         "indoor_pool", "outdoor_pool"},
    "sea_view":         {"view", "distance_from_sea", "distance_from_the_sea"},
    "distance_to_sea":  {"distance_from_sea", "distance_from_the_sea"},
    "air_conditioning": {"air_condition"},
    "bbq":              {"barbeque", "barbecue"},
    "renovated":        {"rennovated"},  # site has misspelling in their HTML
    "heating":          {"oil_heating", "central_heating", "boiler",
                         "underfloor_heating", "electric_heating"},
    "landscape_design": {"landscaping"},
    "water_well":       {"irrigation_system", "automatic_irrigation"},
}

# Halkidiki bounding box for coordinate sanity check.
_HALKIDIKI_LAT_RANGE = (39.0, 41.0)
_HALKIDIKI_LNG_RANGE = (22.0, 24.0)


# =============================================================
# Pure helpers — same shape as GL/Greek Exclusive
# =============================================================

def _to_int_euro(text: str) -> Optional[int]:
    """
    Parse '300.000 €' / '1.500.000,00 €' / 'Price: 420.000€' → integer euros.
    EU decimal/grouping handled. Sanity cap: reject > €200M.
    """
    if not text:
        return None
    cleaned = re.sub(r"[^\d.,]", "", text)
    if not cleaned:
        return None
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


def _to_float_sqm(text: str) -> Optional[float]:
    """
    Parse EU-format m² values.

    EU number convention is the REVERSE of US:
      "3.000,00 m²" → 3000.0   (. = thousands grouping, , = decimal)
      "205,00 m²"   → 205.0    (, = decimal)
      "205 m²"      → 205.0    (no separator)
      "1.500.000 m²" → 1500000.0 (multiple grouping periods)
      "0,00 m²"     → None     (template's 'not specified' value)

    Disambiguation rule for single-separator strings:
      A separator followed by EXACTLY 3 digits is treated as a thousands
      grouping ("3.000" = 3000). Anything else is decimal ("205,00" = 205.00,
      "3.5" = 3.5). Multiple periods are always thousands ("1.500.000").
    """
    if not text:
        return None
    cleaned = re.sub(r"[^\d.,]", "", text)
    if not cleaned:
        return None

    if "." in cleaned and "," in cleaned:
        # Both present — the LAST one is the decimal separator (EU rule).
        last = max(cleaned.rfind("."), cleaned.rfind(","))
        integer_part = re.sub(r"[.,]", "", cleaned[:last])
        decimal_part = cleaned[last + 1:]
        num = f"{integer_part}.{decimal_part}"
    elif "." in cleaned:
        parts = cleaned.split(".")
        if len(parts) > 2 or (len(parts) == 2 and len(parts[1]) == 3):
            num = "".join(parts)     # thousands separator
        else:
            num = cleaned            # decimal point
    elif "," in cleaned:
        parts = cleaned.split(",")
        if len(parts) > 2 or (len(parts) == 2 and len(parts[1]) == 3):
            num = "".join(parts)     # thousands separator (rare)
        else:
            num = cleaned.replace(",", ".")  # decimal comma (common EU)
    else:
        num = cleaned

    try:
        value = float(num)
        return value if value > 0 else None
    except ValueError:
        return None


def _to_int_simple(text: str) -> Optional[int]:
    """First integer in a string. '3', 'Bedrooms: 3', '3 ' all → 3."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _slug(label: str) -> str:
    """
    Convert HTML label → stable JSON key for extra_features.
    'Distance from the Sea' → 'distance_from_the_sea'
    'Energy class'          → 'energy_class'
    """
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _extract_neighborhood_from_title(
    title: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract location from title text. Examples:
      "For sale two-story house in Nea Iraklia, Halkidiki."
        → ("nea iraklia", "Nea Iraklia", "Nea Propontida")
      "...on the beach of Triglia, Halkidiki."
        → ("triglia", "Triglia", "Nea Propontida")

    Returns (raw_key, mapped_english, municipality). Longest-match-wins
    is enforced so that 'nea iraklia' beats a bare 'iraklia' if both
    were ever in the map.
    """
    if not title:
        return None, None, None

    title_lower = title.lower()

    # Collect all matching keys with their positions in the title.
    # The FIRST-MENTIONED neighborhood wins (left-to-right) — this is
    # how Greek agency titles work: "in Nea Gonia, Kallikrateia" means
    # the property is in Nea Gonia (Kallikrateia is just a nearby
    # reference point). At identical positions, prefer the LONGER match
    # so 'nea iraklia' beats a bare 'iraklia' inside "Nea Iraklia".
    matches = []  # (position, key, english, municipality)
    for key, (english, muni) in _NEIGHBORHOOD_MAP.items():
        pos = title_lower.find(key)
        if pos != -1:
            matches.append((pos, key, english, muni))

    if matches:
        matches.sort(key=lambda m: (m[0], -len(m[1])))
        _, key, english, muni = matches[0]
        return key, english, muni

    # Regex fallback for unmapped neighborhoods (defensive)
    patterns = [
        r"(?:in|of|at|on the beach of|in the area of)\s+([\w\s]+?)[,\.\s]+(?:Halkidiki|Chalkidiki)",
        r"area of (.+?)[\.,]",
    ]
    for pattern in patterns:
        m = re.search(pattern, title, flags=re.IGNORECASE)
        if m:
            raw = m.group(1).strip().rstrip(",.")
            return raw, raw, None

    return None, None, None


def _coords_in_halkidiki(lat: float, lng: float) -> bool:
    return (
        _HALKIDIKI_LAT_RANGE[0] <= lat <= _HALKIDIKI_LAT_RANGE[1]
        and _HALKIDIKI_LNG_RANGE[0] <= lng <= _HALKIDIKI_LNG_RANGE[1]
    )


# =============================================================
# Scraper
# =============================================================

class KalogirouRealEstateScraper(EnrichmentMixin, BaseScraper):
    """
    Popi Kalogirou Real Estate — Joomla EstateAgent component.

    Mirrors the GL Real Estate pipeline (EnrichmentMixin + structured-first
    extraction + description-only NLP fallback) with Greek Exclusive's
    image/coordinate fallback chains and calc_municipality routing.
    """

    BASE_URL = "http://www.popikalogirou-realestate.gr"
    # ?limit=0 forces 'All' results on one page — future-proof against
    # the agency adding more inventory beyond the default 15.
    LISTING_URL = "http://www.popikalogirou-realestate.gr/en/real-estate/result"

    # EnrichmentMixin uses this set to know which Property columns NLP
    # is allowed to fill when the structured pass leaves them blank.
    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )

    def __init__(self):
        super().__init__()
        self.source_domain = "popikalogirou-realestate.gr"

    async def fetch_listings(self):
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs (single GET, client-side price filter)
    # ---------------------------------------------------------------
    async def collect_urls(self, min_price: int = 400000) -> List[PropertyTemplate]:
        """
        GET the search-results page (default state lists all For-Sale items)
        and filter by price client-side. The server form's minvalue dropdown
        caps at €300k, below our €400k floor, so server-side filtering is
        unreliable for our threshold.
        """
        # Joomla EstateAgent requires POST with isnewsearch=1 to return results.
        # GET to the same URL returns only the search form, no listings.
        # The form's minvalue dropdown caps at €300k so server-side price
        # filtering is unreliable for our €400k floor — we send empty min/max
        # and filter price client-side below.
        form_data = {
            "type":         "6",   # For Sale
            "src_cat":      "",
            "src_town":     "no",
            "space_min":    "-1",
            "space_max":    "-1",
            "rooms_max":    "-1",
            "rooms_min":    "-1",
            "minvalue":     "",
            "maxvalue":     "",
            "searchstring": "",
            "isnewsearch":  "1",
            "limit":        "0",
        }
        logger.info(f"[{self.source_domain}] POST {self.LISTING_URL}")

        try:
            response = await self.client.post(self.LISTING_URL, data=form_data)
            parser = LexborHTMLParser(response.text)
            cards = parser.css(".property-box-simple")
            logger.info(
                f"[{self.source_domain}] received {len(cards)} cards from listing"
            )
        except Exception as e:
            logger.error(f"[{self.source_domain}] listing POST failed: {e}")
            return []

        all_properties: List[PropertyTemplate] = []
        rejected_by_price = 0

        for card in cards:
            try:
                prop = self._parse_card(card)
                if not prop:
                    continue
                if prop.price is not None and prop.price < min_price:
                    rejected_by_price += 1
                    continue
                all_properties.append(prop)
            except Exception as e:
                logger.error(f"[{self.source_domain}] card parse err: {e}")

        logger.info(
            f"[{self.source_domain}] Phase 1 done: "
            f"{len(all_properties)} URLs (rejected {rejected_by_price} <{min_price}€)"
        )
        return all_properties

    def _parse_card(self, card: LexborNode) -> Optional[PropertyTemplate]:
        """Extract a PropertyTemplate seed from one .property-box-simple card."""

        # 1. Detail URL + title
        title_link = card.css_first(".property-box-simple-header h2 a")
        if not title_link:
            return None
        href = title_link.attributes.get("href", "")
        title = title_link.text(strip=True)
        if not href or not title:
            return None
        if href.startswith("/"):
            href = f"{self.BASE_URL}{href}"

        # 2. site_property_id = Reference Nr (customer-facing), NOT URL ID
        site_id: Optional[str] = None
        for li in card.css(".property-box-simple-meta ul li"):
            span = li.css_first("span")
            strong = li.css_first("strong")
            if not (span and strong):
                continue
            if "reference" in span.text(strip=True).lower():
                site_id = strong.text(strip=True)
                break
        if not site_id:
            # Fallback: Joomla URL ID (e.g. /property/5327/...)
            m = re.search(r"/property/(\d+)/", href)
            if m:
                site_id = m.group(1)
        if not site_id:
            return None

        # 3. Category badge
        category: Optional[str] = None
        cat_link = card.css_first(".for-sale a")
        if cat_link:
            category = _CATEGORY_MAP.get(cat_link.text(strip=True).lower())

        # 4. Price (nested .ea_price wrapping)
        price: Optional[int] = None
        price_el = card.css_first(".sale-prize .ea_price")
        if price_el:
            price = _to_int_euro(price_el.text(strip=True))

        # 5. Meta — size + rooms
        size_sqm: Optional[float] = None
        rooms: Optional[int] = None
        for li in card.css(".property-box-simple-meta ul li"):
            span = li.css_first("span")
            strong = li.css_first("strong")
            if not (span and strong):
                continue
            label = span.text(strip=True).lower()
            value = strong.text(strip=True)
            if "square meter" in label:
                size_sqm = _to_float_sqm(value)
            elif label == "rooms":
                rooms = _to_int_simple(value)

        # 6. Location from title
        _, english_nb, municipality = _extract_neighborhood_from_title(title)

        # 7. Size routing — Sites/Parcels carry land area, not building
        is_land = category in {"Plot", "Parcel"}
        if is_land:
            seed_size_sqm = None
            seed_land_size_sqm = size_sqm
        else:
            seed_size_sqm = size_sqm
            seed_land_size_sqm = None

        # 8. location_raw — Greece-ends-with-Halkidiki keeps whitelist happy
        if english_nb and municipality:
            location_raw = f"{english_nb}, {municipality}, Halkidiki"
        elif english_nb:
            location_raw = f"{english_nb}, Halkidiki"
        else:
            location_raw = "Halkidiki"

        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=href,
            price=price,
            location_raw=location_raw,
            size_sqm=seed_size_sqm,
            land_size_sqm=seed_land_size_sqm,
            bedrooms=rooms,  # rooms-as-bedrooms approx; detail panel may override
            category=category,
        )

    # ---------------------------------------------------------------
    # PHASE 2 — fetch full details
    # ---------------------------------------------------------------
    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Hybrid pipeline:
          1. Description (selector chain → og:description fallback)
          2. Images (Joomla EA media paths + og:image prepend)
          3. Coordinates (3-tier: data-attrs → JS setView → bbox regex)
          4. Structured detail panel (label: value rows)
          5. calc_municipality routing from page title
          6. NLP fallback (description-only) — fills blanks
          7. Quality Gate (log-only)
        """
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)

            details: Dict[str, Any] = {
                "description": "",
                "price": None,
                "size_sqm": None, "land_size_sqm": None,
                "bedrooms": None, "bathrooms": None, "year_built": None,
                "area": None, "subarea": None, "category": None, "levels": None,
                "site_last_updated": None,
                "latitude": None, "longitude": None,
                "images": [],
                "extra_features": {},
            }

            # ---- 1. Description
            details["description"] = self._extract_description(parser)

            # ---- 2. Images
            details["images"] = self._collect_image_urls(parser)

            # ---- 3. Coordinates
            self._parse_coordinates(parser, response.text, details)

            # ---- 4. Structured detail panel (<dl><dt>/<dd>)
            self._parse_structured_panel(parser, details)

            # ---- 5. Amenities (.property-amenities ul.amentul li)
            self._parse_amenities(parser, details)

            # ---- 6. calc_municipality — route from page title
            self._route_municipality(parser, details)

            # ---- 7. NLP fallback on description
            self._apply_nlp_fallback(details)

            # ---- 8. Quality Gate
            if not self._passes_quality_gate(details.get("description")):
                logger.warning(
                    f"[{self.source_domain}] description below quality gate for {url}"
                )

            return details

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] fetch_details error for {url}: {e}"
            )
            return {}

    # ---------------------------------------------------------------
    # Phase 2 helpers
    # ---------------------------------------------------------------
    def _extract_description(self, parser: LexborHTMLParser) -> str:
        """
        Description selector chain. The Kalogirou template puts the body
        text in .property-detail-description as a sequence of <p> tags;
        empty <p></p> tags surround the real content and must be skipped.
        Generic selectors kept as fallbacks for forward compatibility.
        """
        for sel in [
            ".property-detail-description",  # canonical for this template
            ".ea_object_description",
            "#object_description",
            ".property-description",
        ]:
            el = parser.css_first(sel)
            if not el:
                continue
            # Prefer <p> children — strips empty paragraph wrappers cleanly
            paragraphs = []
            for p in el.css("p"):
                txt = p.text(strip=True)
                if txt:
                    paragraphs.append(txt)
            if paragraphs:
                joined = "\n\n".join(paragraphs)
                if len(joined) >= 50:
                    return joined
            # Fallback: raw text of the container
            txt = el.text(separator="\n", strip=True)
            if len(txt) >= 50:
                return txt

        og = parser.css_first('meta[property="og:description"]')
        if og:
            return (og.attributes.get("content") or "").strip()
        return ""

    def _collect_image_urls(self, parser: LexborHTMLParser) -> List[str]:
        """
        Collect main-property gallery images only.

        Critical scoping note: detail pages embed a "Recommended Properties"
        section near the bottom with full property cards from the same site.
        Those cards reference the same /media/com_estateagent/ image folder
        as the actual gallery — a naive page-wide image sweep would pollute
        the gallery with recommendations.

        Solution: scope extraction to #eagalleria (or .property-gallery).
        The Galleria.js widget lists every property image as a thumbnail
        in /pictures/tea_*.jpg. The full-resolution counterparts live at
        /pictures/ea_*.jpg — same filename minus the leading 't'. We do
        the tea→ea swap so what we save is high-quality, not the 71×40 thumb.
        """
        out: List[str] = []

        gallery = (
            parser.css_first("#eagalleria")
            or parser.css_first(".property-gallery")
        )
        if gallery:
            for img in gallery.css("img"):
                src = img.attributes.get("src", "")
                if not src or "/media/com_estateagent/" not in src:
                    continue
                # tea_X.jpg → ea_X.jpg (full-size)
                full_src = re.sub(r"/pictures/tea_", "/pictures/ea_", src)
                if full_src not in out:
                    out.append(full_src)

        # Defensive fallback: if the gallery wrapper changes, fall back to
        # a page-wide sweep — but only if the gallery yielded nothing.
        if not out:
            for img in parser.css("img"):
                src = img.attributes.get("src", "")
                if src and "/media/com_estateagent/" in src:
                    full_src = re.sub(r"/pictures/tea_", "/pictures/ea_", src)
                    if full_src not in out:
                        out.append(full_src)

        # Normalise //protocol-relative + skip SVG
        normalised: List[str] = []
        for src in out:
            if src.startswith("//"):
                src = "https:" + src
            if src and src not in normalised and not src.lower().endswith(".svg"):
                normalised.append(src)
        return normalised

    def _parse_coordinates(
        self,
        parser: LexborHTMLParser,
        raw_html: str,
        details: Dict[str, Any],
    ) -> None:
        """
        Three-tier coordinate fallback (same as Greek Exclusive):
          1. data-lat / data-lng attributes
          2. JS setView([lat, lng], ...) regex
          3. Raw text regex against Halkidiki bbox (39-41, 22-24)
        Every candidate is sanity-checked against the bbox before commit.
        """
        # 1. data-attributes — try multiple containers
        for sel in [
            "#ea_object_map",
            "#property-map",
            "[data-lat][data-lng]",
            "[data-latitude][data-longitude]",
        ]:
            node = parser.css_first(sel)
            if not node:
                continue
            lat = (
                node.attributes.get("data-lat")
                or node.attributes.get("data-latitude")
            )
            lng = (
                node.attributes.get("data-lng")
                or node.attributes.get("data-longitude")
            )
            if not (lat and lng):
                continue
            try:
                lat_f, lng_f = float(lat), float(lng)
            except ValueError:
                continue
            if _coords_in_halkidiki(lat_f, lng_f):
                details["latitude"] = lat_f
                details["longitude"] = lng_f
                return

        # 2. JS setView
        m = re.search(
            r'setView\(\[\s*([0-9.\-]+)\s*,\s*([0-9.\-]+)\s*\]',
            raw_html,
        )
        if m:
            try:
                lat_f, lng_f = float(m.group(1)), float(m.group(2))
                if _coords_in_halkidiki(lat_f, lng_f):
                    details["latitude"] = lat_f
                    details["longitude"] = lng_f
                    return
            except ValueError:
                pass

        # 3. Bbox-targeted regex on full HTML text
        m = re.search(
            r'((?:39|40|41)\.\d{4,})\s*[,|]\s*((?:22|23|24)\.\d{4,})',
            raw_html,
        )
        if m:
            try:
                lat_f, lng_f = float(m.group(1)), float(m.group(2))
                if _coords_in_halkidiki(lat_f, lng_f):
                    details["latitude"] = lat_f
                    details["longitude"] = lng_f
            except ValueError:
                pass

    def _parse_structured_panel(
        self,
        parser: LexborHTMLParser,
        details: Dict[str, Any],
    ) -> None:
        """
        Joomla EstateAgent v3.3.2 uses a definition list for the property
        info panel, NOT an unordered list:

            <dl>
              <dt>Price</dt>            <dd>650.000 €</dd>
              <dt>Code</dt>             <dd>3138</dd>
              <dt>Category</dt>         <dd><a>Villas</a></dd>
              <dt>Type</dt>             <dd>For Sale</dd>
              <dt>Location</dt>         <dd>Nea Iraklia</dd>
              <dt>Living area</dt>      <dd>205,00 m²</dd>
              <dt>Land area</dt>        <dd>3.000,00 m²</dd>
              <dt>Bedrooms</dt>         <dd>6</dd>
              <dt>Bathroom</dt>         <dd>3</dd>
              <dt>Distance from the Sea</dt> <dd>650m</dd>
            </dl>

        We pair <dt>/<dd> by index — the template emits them in lockstep
        and the css selectors return them in document order. The .property-list
        scope avoids any unrelated <dl>s elsewhere on the page.
        """
        dl = (
            parser.css_first(".property-list dl")
            or parser.css_first(".objrel dl")
            or parser.css_first("dl")
        )
        if not dl:
            return

        dts = dl.css("dt")
        dds = dl.css("dd")
        for dt, dd in zip(dts, dds):
            label = dt.text(strip=True).lower().rstrip(":").strip()
            value = dd.text(strip=True)
            if not label or not value:
                continue
            self._route_dl_row(label, value, details)

    def _route_dl_row(
        self,
        label: str,
        value: str,
        details: Dict[str, Any],
    ) -> None:
        """
        Route one <dt>/<dd> pair. Known labels → Property columns (only if
        the column is still blank — structured pass never overrides). Other
        labels → extra_features as slugged keys.
        """
        # 'Code' is the Reference Nr / site_property_id — overrides URL fallback
        if label in {"code", "reference", "reference nr"}:
            if not details.get("site_property_id"):
                details["site_property_id"] = value
            return

        # Always-'For Sale' in our scope — drop
        if label == "type":
            return

        if label == "price" and not details.get("price"):
            v = _to_int_euro(value)
            if v is not None:
                details["price"] = v
            return

        if label == "category" and not details.get("category"):
            # Normalise "Villas" → "Villa", "Maisonettes" → "Maisonette", etc.
            details["category"] = _CATEGORY_MAP.get(value.lower(), value)
            return

        if label == "location" and not details.get("area"):
            # Normalize via neighborhood map — the dl 'Location' field
            # carries inconsistent spellings ("Nea Kalikratia" vs "Nea
            # Kallikrateia") and sometimes Greek script ("Νέα Καλλικράτεια").
            # The map collapses all variants to a single English canonical
            # so downstream consumers see one consistent town name.
            key = value.lower()
            if key in _NEIGHBORHOOD_MAP:
                details["area"] = _NEIGHBORHOOD_MAP[key][0]
            else:
                details["area"] = value
            return

        if label in {"living area", "living space", "size"} \
                and not details.get("size_sqm"):
            v = _to_float_sqm(value)
            if v is not None:
                details["size_sqm"] = v
            return

        if label in {"land area", "land", "plot", "lot size"} \
                and not details.get("land_size_sqm"):
            v = _to_float_sqm(value)
            if v is not None:
                details["land_size_sqm"] = v
            return

        if "bedroom" in label:
            # Bedrooms is the authoritative field — always override.
            # The dl emits 'Rooms' BEFORE 'Bedrooms' in document order, so
            # without the override the generic Rooms count wins by accident.
            n = _to_int_simple(value)
            if n is not None:
                details["bedrooms"] = n
            return

        if "bathroom" in label:
            # Bathrooms is the authoritative field — always override.
            n = _to_int_simple(value)
            if n is not None:
                details["bathrooms"] = n
            return

        # 'Rooms' is the generic total-room count (e.g. 6 bedrooms + 2 living
        # rooms = 8 rooms). We always preserve it as extra_features.rooms_count
        # AND use it as a bedrooms fallback for properties where the dedicated
        # Bedrooms field is absent. If a Bedrooms field appears later in the
        # dl, it will override (see _bedroom_ block above).
        if label == "rooms":
            n = _to_int_simple(value)
            if n is not None:
                details["extra_features"]["rooms_count"] = n
                if not details.get("bedrooms"):
                    details["bedrooms"] = n
            return

        if "year" in label and ("built" in label or "construction" in label):
            if not details.get("year_built"):
                n = _to_int_simple(value)
                if n is not None:
                    details["year_built"] = n
            return

        if ("floor" in label or "level" in label) and not details.get("levels"):
            details["levels"] = value
            return

        # Distance from the Sea — typed int into extra_features
        if "distance" in label and "sea" in label:
            n = _to_int_simple(value)
            if n is not None:
                details["extra_features"]["distance_from_sea"] = n
            return

        # Anything else → extra_features (slugged)
        slug = _slug(label)
        if slug and slug not in details["extra_features"]:
            details["extra_features"][slug] = value

    def _route_panel_row(
        self,
        row: LexborNode,
        details: Dict[str, Any],
    ) -> None:
        """
        One <li><span>Label</span><strong>Value</strong></li> row →
        either a Property column (only if blank) or extra_features slot.
        """
        span = row.css_first("span")
        strong = row.css_first("strong")
        if not (span and strong):
            return
        label = span.text(strip=True).lower()
        value = strong.text(strip=True)
        if not label or not value:
            return

        # site_property_id from Reference (overrides URL-ID fallback)
        if "reference" in label:
            if not details.get("site_property_id"):
                details["site_property_id"] = value
            return

        # Skip card-already-captured fields
        if label in {"price", "square meter", "square meters", "rooms"}:
            return

        if "bedroom" in label and not details.get("bedrooms"):
            n = _to_int_simple(value)
            if n is not None:
                details["bedrooms"] = n
            return

        if "bathroom" in label and not details.get("bathrooms"):
            n = _to_int_simple(value)
            if n is not None:
                details["bathrooms"] = n
            return

        if "year" in label and ("built" in label or "construction" in label):
            if not details.get("year_built"):
                n = _to_int_simple(value)
                if n is not None:
                    details["year_built"] = n
            return

        if ("floor" in label or "level" in label) and not details.get("levels"):
            details["levels"] = value
            return

        # Everything else → extra_features (slugged key)
        slug = _slug(label)
        if not slug or slug in details["extra_features"]:
            return
        # Coerce common numeric labels
        if "distance" in slug or slug.endswith("_count"):
            n = _to_int_simple(value)
            if n is not None:
                details["extra_features"][slug] = n
                return
        details["extra_features"][slug] = value

    def _parse_amenities(
        self,
        parser: LexborHTMLParser,
        details: Dict[str, Any],
    ) -> None:
        """
        Amenities are rendered as a flat <ul class="amentul"> inside
        .property-amenities. The template only emits features the property
        HAS — there's no "no" state — so every <li> = True boolean amenity.

        Example:
            <ul class="amentul">
              <li class="yes">Air Condition</li>
              <li class="yes">Indoor/Outdoor Pool</li>
              <li class="yes">Parking Lot</li>
              ...
            </ul>

        Slugged keys: 'Air Condition' → 'air_condition',
        'Indoor/Outdoor Pool' → 'indoor_outdoor_pool', etc.
        """
        for sel in [
            ".property-amenities ul.amentul li",
            ".property-amenities ul li",
            "ul.amentul li",
        ]:
            rows = parser.css(sel)
            if not rows:
                continue
            for li in rows:
                text = li.text(strip=True)
                if not text:
                    continue
                slug = _slug(text)
                if slug and slug not in details["extra_features"]:
                    details["extra_features"][slug] = True
            break  # first matching selector wins

    def _route_municipality(
        self,
        parser: LexborHTMLParser,
        details: Dict[str, Any],
    ) -> None:
        """
        Set calc_municipality based on text found in the detail page title
        or main heading. Mirrors Greek Exclusive's pattern but uses our
        _NEIGHBORHOOD_MAP for routing.
        """
        # Prefer the largest source of location text on the detail page
        for sel in ["h1", "title", ".property-box-simple-header h2"]:
            el = parser.css_first(sel)
            if not el:
                continue
            txt = el.text(strip=True)
            _, _, municipality = _extract_neighborhood_from_title(txt)
            if municipality:
                details["calc_municipality"] = municipality
                return

    # ---------------------------------------------------------------
    # NLP fallback — same shape as GL Real Estate
    # ---------------------------------------------------------------
    def _apply_nlp_fallback(self, data: Dict[str, Any]) -> None:
        """
        Run the regex DataExtractor on description text only.

        Crucial design points (copied from GL):
          * Description-only — never feed greedy DOM text. The agency
            embeds boilerplate (footer, contact, subscription) elsewhere
            and we don't want false-positive amenities from that chrome.
          * Structured-source values WIN on overlap. NLP fills blanks
            only — never overrides existing data.
          * extra_features merge is dedup-aware via _NLP_TO_STRUCTURAL:
            if a structural slug semantically equal to the NLP key is
            already present, the NLP key is dropped.
          * COLUMN_KEYS / EXTRA_NUMERIC_KEYS routing for top-level
            smart_data keys (same as Greek Exclusive).
        """
        description = data.get("description") or ""
        if not description:
            return

        try:
            smart = self.extractor.analyze_full_text(description)
        except Exception as e:
            logger.warning(f"[{self.source_domain}] NLP fallback failed: {e}")
            return

        existing_keys = set(data["extra_features"].keys())

        for key, value in smart.items():
            if value is None:
                continue
            if key == "extra_features":
                for k, v in value.items():
                    if k in existing_keys:
                        continue
                    # Defensive: the DataExtractor sometimes mirrors column-
                    # equivalent keys into extra_features (e.g. it returns
                    # both smart_data["bedrooms"] and
                    # smart_data["extra_features"]["bedrooms"]). The column
                    # mirror is redundant — Property already has a dedicated
                    # column for it. Drop to avoid 'bedrooms: 6' showing up
                    # alongside the actual bedrooms column.
                    if k in _COLUMN_KEYS:
                        continue
                    # Skip if a structurally-equivalent slug already exists
                    related = _NLP_TO_STRUCTURAL.get(k, set())
                    if related & existing_keys:
                        continue
                    data["extra_features"][k] = v
            elif key in _EXTRA_NUMERIC_KEYS:
                try:
                    data["extra_features"][key] = int(float(value))
                except (TypeError, ValueError):
                    pass
            elif key in _COLUMN_KEYS and not data.get(key):
                # Defensive: regex sometimes captures land area as size_sqm
                # when description says "covers 3500 sqm of land". Reject
                # the duplicate.
                if key == "size_sqm" and value == data.get("land_size_sqm"):
                    continue
                data[key] = value