"""
GL Real Estate scraper — hybrid extraction pipeline.

Strategy (in order of trust):
  1. Structured blocks: 'Property information' and 'MORE FEATURES' panels
     consist of <li>Label: Value</li> rows. Reading them directly via CSS
     gives 100% accuracy for known fields.
  2. Description: extracted only from .kf_property_detail_uptwon paragraphs.
     The og:description meta tag is a fallback when the main text is empty.
  3. NLP fallback: the regex-based DataExtractor runs on (description + raw
     greedy text) ONLY for fields that the structured pass didn't fill.
     Never overrides anything already populated.

Why hybrid:
  * GL pages reliably have the structured panels — that's where Type, Price,
    Bedrooms, Year, Energy class, etc. live in <label: value> form.
  * The legacy regex pipeline produced two failure modes we want to avoid:
    (a) it missed half the panel fields (no patterns for Energy class,
        Orientation, Distance from Sea, etc.),
    (b) it leaked false positives into extra_features by matching keywords
        in free description text ("garden" anywhere → garden=True), creating
        noisy outbound payloads.
  * The structured-first approach fixes both: known fields come from HTML
    structure, and free-text NLP only fills the gaps.

Mapping decisions (from project plan):
  * Structural columns are filled in Property: price, size_sqm, land_size_sqm,
    bedrooms, bathrooms, year_built, area, subarea, category, levels,
    description, latitude, longitude.
  * Everything else from the panels (heating, energy class, distance from
    sea, orientation, floor, kitchens count, view, etc.) goes into
    extra_features as a JSONB blob — typed values when available, slugged
    keys (lowercase, underscore-separated).
  * <li>Air condition</li>, <li>Alarm</li>, <li>Garden</li> (no colon) →
    extra_features.<slug> = True (boolean amenities).
  * 'Type:: For Sale' is intentionally dropped — it's noise (always 'For Sale'
    for our scope; the actual category lives in 'Category:' line).
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.scrapers.base import BaseScraper
from src.models.schemas import PropertyTemplate


# =============================================================
# Helpers — pure functions, no scraper state
# =============================================================

# These are HTML labels the panel parser maps to first-class Property columns.
# Anything NOT in this set goes to extra_features. Lowercase keys, no colons.
_LABEL_TO_PROPERTY_COLUMN: Dict[str, str] = {
    "price":                "price",
    "size":                 "size_sqm",
    "land area":            "land_size_sqm",
    "land":                 "land_size_sqm",
    "bedrooms":             "bedrooms",
    "bathrooms":            "bathrooms",
    "year of construction": "year_built",
    "year built":           "year_built",
    "category":             "category",
    "area":                 "area",
    "subarea":              "subarea",
    "levels":               "levels",
}

_DROP_SLUGS = {
    "type",         # "Type:: For Sale" — always for-sale in our scope
    "property_id",  # already captured as site_property_id upstream
    "location",     # already captured via area / subarea / calc_municipality
}


# Yes/no normalisation — accepts both English and Greek site dialects.
_YES_VALUES = {"yes", "ναι", "y", "true", "1"}
_NO_VALUES  = {"no",  "όχι", "οχι", "n", "false", "0"}


def _slug(label: str) -> str:
    """
    Convert a free-form HTML label into a stable JSON key.

    Examples:
      "Floor type"          -> "floor_type"
      "Energy class"        -> "energy_class"
      "Distance from Sea"   -> "distance_from_sea"
      "Air condition"       -> "air_condition"
      "WC"                  -> "wc"
    """
    s = label.strip().lower()
    # collapse non-alphanumeric runs to single underscore
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _to_int_euro(text: str) -> Optional[int]:
    """
    Parse a Greek/EU price string into integer euros.

    Acceptable inputs:
      "420.000€"           -> 420000
      "420.000,00€"        -> 420000  (cents dropped)
      "1,350,000 €"        -> 1350000
      "Price: 420.000€"    -> 420000
      "—" / ""             -> None

    Defensive output cap:
      Real-estate prices in Halkidiki are in [€10,000 .. €100,000,000].
      If we parse something > 200,000,000, the input was almost certainly
      malformed (e.g. two adjacent prices got concatenated by an HTML quirk).
      Reject it — better None than a value that crashes the database.
      INT32 max is 2,147,483,647; any value above that won't fit in
      Property.price (Integer column) anyway.
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

    # Sanity cap — reject implausibly large values (concatenated prices, etc.)
    if value > 200_000_000:
        return None
    return value


def _to_int_simple(text: str) -> Optional[int]:
    """Extract first integer from a string. '3', 'Bedrooms: 3', '3 ' all → 3."""
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    """
    Parse area-in-square-meters style strings.

    "136m²"     -> 136.0
    "127 m²"    -> 127.0
    "Size: 200" -> 200.0
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
    """Normalise 'Yes'/'No' (en+gr) into bool. Returns None for anything else."""
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

    Inputs:
      "Bedrooms: 3"       -> ("Bedrooms", "3")
      "Type:: For Sale"   -> ("Type", "For Sale")        -- handles double-colon
      "Air condition"     -> ("Air condition", None)     -- boolean amenity
      "Price per m²: 3.088€" -> ("Price per m", "3.088€") -- ² gets dropped by slug

    Returns label as raw (caller slugs it), value as raw stripped string or None.
    """
    text = li_text.strip()
    if ":" not in text:
        return text, None
    label, _, value = text.partition(":")
    # handle 'Type::' double-colon — strip leading colon from value
    value = value.lstrip(":").strip()
    return label.strip(), (value if value else None)


# =============================================================
# Scraper
# =============================================================

class GLRealEstateScraper(BaseScraper):

    def __init__(self):
        super().__init__()
        self.source_domain = "glrealestate.gr"
        self.base_url = "https://glrealestate.gr/listings"

    async def fetch_listings(self):
        """Backwards-compatible entry point for the dispatcher."""
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs
    # ---------------------------------------------------------------
    async def collect_urls(self, min_price: int = 400000):
        page = 1
        all_properties: List[PropertyTemplate] = []

        while True:
            params = {
                "category": "residential",
                "price_min": min_price,
                "page": page,
                "sort": "id",
                "order": "DESC",
            }

            logger.info(f"Парсинг страницы {page}...")
            try:
                response = await self.client.get(self.base_url, params=params)
                parser = LexborHTMLParser(response.text)
                cards = parser.css(".kf_listing_outer_wrap")

                if not cards:
                    logger.info("Карточки не найдены. Пагинация завершена.")
                    break

                for card in cards:
                    try:
                        url_node = card.css_first("h5 a")
                        if not url_node:
                            continue

                        # Опportunistic price grab from the listing card.
                        # Detail page always has the authoritative price; this
                        # is for fast filtering during PHASE 1 only.
                        price_val = None
                        for el in card.css("h5, h6, span, div, strong"):
                            text = el.text() or ""
                            text_lower = text.lower()
                            if "€" in text_lower or (
                                "price" in text_lower
                                and any(c.isdigit() for c in text_lower)
                            ):
                                price_val = text.strip()
                                break

                        location_node = card.css_first(".fa-map-marker")
                        id_node = card.css_first("span")
                        id_text = id_node.text() if id_node else "0"
                        clean_id = id_text.split("#")[-1].strip()

                        prop_data = PropertyTemplate(
                            site_property_id=clean_id,
                            source_domain=self.source_domain,
                            url=url_node.attributes.get("href"),
                            price=price_val,
                            location_raw=location_node.parent.text().strip()
                                if location_node and location_node.parent else None,
                        )
                        all_properties.append(prop_data)

                    except Exception as e:
                        logger.error(f"Ошибка парсинга отдельной карточки: {e}")

                logger.info(f"Успешно собрано {len(cards)} объектов со страницы {page}")
                await asyncio.sleep(2)
                page += 1

            except Exception as e:
                logger.error(f"Критическая ошибка на странице {page}: {e}")
                break

        return all_properties

    # ---------------------------------------------------------------
    # PHASE 2 — fetch full details
    # ---------------------------------------------------------------
    async def fetch_details(self, url: str) -> dict:
        """
        Hybrid extraction:
          1. Structured panel blocks (high-confidence)
          2. Description (high-confidence, .kf_property_detail_uptwon)
          3. NLP fallback (low-confidence, fills only what's missing)
        """
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)

            data: Dict[str, Any] = {
                "description": "", "price": None,
                "size_sqm": None, "land_size_sqm": None,
                "bedrooms": None, "bathrooms": None, "year_built": None,
                "area": None, "subarea": None, "category": None, "levels": None,
                "site_last_updated": None,
                "latitude": None, "longitude": None,
                "images": [], "extra_features": {},
            }

            # ----- 1. Photos --------------------------------------
            data["images"] = self._collect_image_urls(parser)

            # ----- 2. Structured panel blocks ---------------------
            #   This is the primary source of truth for fields like
            #   bedrooms, bathrooms, year_built, energy class, etc.
            self._parse_structured_blocks(parser, data)

            # ----- 3. Description ---------------------------------
            data["description"] = self._parse_description(parser)

            # ----- 4. Coordinates ---------------------------------
            self._parse_coordinates(parser, response.text, data)

            # ----- 5. NLP fallback for unfilled fields ------------
            #   Runs only over description (not greedy DOM text) to avoid
            #   leaking into extra_features from random page chrome.
            self._apply_nlp_fallback(data)

            return data

        except Exception as e:
            logger.error(f"Ошибка при глубоком парсинге {url}: {e}")
            return {}

    # ---------------------------------------------------------------
    # Phase 2 helpers
    # ---------------------------------------------------------------
    def _collect_image_urls(self, parser: LexborHTMLParser) -> List[str]:
        """All <img> nodes whose src points to /uploads/ and is not a logo."""
        out: List[str] = []
        for img_node in parser.css("img"):
            src = img_node.attributes.get("src", "") or ""
            sl = src.lower()
            if not sl or "/uploads/" not in sl or "logo" in sl:
                continue
            if src.startswith("//"):
                src = "https:" + src
            if src not in out:
                out.append(src)
        return out

    def _parse_description(self, parser: LexborHTMLParser) -> str:
        """
        Description is the <p> children of .kf_property_detail_uptwon.

        That block also contains <h3> price/title and <ul> meta — those are
        skipped (we collect <p> only). If we find nothing, fall back to the
        og:description meta tag (typically 1-2 sentences; better than empty).
        """
        paragraphs: List[str] = []
        for sel in [".kf_property_detail_uptwon p", ".property-description"]:
            for node in parser.css(sel):
                txt = node.text(strip=True)
                if txt and txt not in paragraphs:
                    paragraphs.append(txt)

        if paragraphs:
            return "\n\n".join(paragraphs)

        # Fallback — short but better than empty. Quality Gate (>= 50 chars)
        # will still drop genuinely empty pages.
        og = parser.css_first('meta[property="og:description"]')
        if og:
            content = og.attributes.get("content", "") or ""
            return content.strip()

        return ""

    def _parse_coordinates(
        self,
        parser: LexborHTMLParser,
        raw_html: str,
        data: Dict[str, Any],
    ) -> None:
        """
        GL embeds Leaflet coords in two places:
          (a) data-lat / data-lng on a #property-map div (preferred — clean)
          (b) JS string  L.map(...).setView([lat, lng], ...)   (fallback)
        """
        # data-attribute path (clean)
        map_node = parser.css_first("#property-map")
        if map_node:
            lat = map_node.attributes.get("data-lat")
            lng = map_node.attributes.get("data-lng")
            if lat and lng:
                try:
                    data["latitude"] = float(lat)
                    data["longitude"] = float(lng)
                    return
                except ValueError:
                    pass

        # JS fallback
        m = re.search(r'setView\(\[\s*([0-9.\-]+)\s*,\s*([0-9.\-]+)\s*\]', raw_html)
        if m:
            try:
                data["latitude"] = float(m.group(1))
                data["longitude"] = float(m.group(2))
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
          <h5>Property information</h5>     <- or "MORE FEATURES"
          <ul><li>Label: Value</li> ... </ul>

        Panel headings don't matter for parsing — both panels follow the same
        <li>label: value</li> shape. We process all <li> nodes inside any
        such panel and route each one through _route_field.
        """
        panels = parser.css(".kf_property_detail_link")
        for panel in panels:
            # Skip if it's not a "Essentail" content panel (defensive — there
            # might be similar wrappers used elsewhere on GL pages)
            if not panel.css_first(".kf_property_detail_Essentail"):
                continue

            for li in panel.css(".kf_property_detail_Essentail ul li"):
                # The <li> often wraps content inside <a><i></i>Text</a>.
                # We want the visible text, with the icon stripped.
                raw_text = self._li_visible_text(li)
                if not raw_text:
                    continue
                self._route_field(raw_text, data)

    @staticmethod
    def _li_visible_text(li: LexborNode) -> str:
        """
        Extract the immediate label/value text of one <li>, normalising HTML quirks.

        Two issues this method handles:
        1. Discounted prices: GL renders sale prices as
                <li>Price: <s><small>1.550.000€</small></s> 1.500.000€</li>
            The <s> (strikethrough) wraps the OLD price. We strip <s>...</s>
            entirely so only the active price remains. Without this, naive
            text() returns "Price: 1.550.000€ 1.500.000€" and downstream
            parsers concatenate the digits → 15500001500000 → DataError.
        2. Defensive: nested <li> blocks (rare lexbor quirk on malformed HTML)
            get stripped too, just in case.

        Other inline tags (<sup>, <small>, <i>, <a>) are stripped of their
        markup but their text content is preserved.
        """
        # Prefer the <a> wrapper, which is the canonical GL structure
        target = li.css_first("a") or li
        inner = target.html or ""

        # 1. Strike-through content (<s>...</s>) is a discontinued/old value.
        #    Drop it entirely before any other processing.
        inner = re.sub(
            r"<s\b[^>]*>.*?</s>",
            "",
            inner,
            flags=re.DOTALL | re.IGNORECASE,
        )

        # 2. Defensive: drop nested <li> blocks (lexbor recovery quirk).
        inner = re.sub(
            r"<li\b[^>]*>.*?</li>",
            "",
            inner,
            flags=re.DOTALL | re.IGNORECASE,
        )

        # 3. Strip remaining HTML tags
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
          * dropped (Type::, Property id) — explicit drops
          * Property column (price, size_sqm, bedrooms, ...) — typed write
          * extra_features (everything else) — typed when possible
        """
        label, value = _split_label_value(raw_text)
        slug = _slug(label)
        if not slug:
            return

        # --- explicit drops ---------------------------------------
        if slug in _DROP_SLUGS:
            return

        # --- Property columns -------------------------------------
        # We check by lowercase label (with optional matching variants
        # in _LABEL_TO_PROPERTY_COLUMN). Some labels need fuzzy matching:
        # "Land area" vs "Land", "Year of Construction" vs "Year built".
        column = _LABEL_TO_PROPERTY_COLUMN.get(label.strip().lower())
        if column is not None:
            self._write_column(column, value, data)
            return

        # --- extra_features ---------------------------------------
        # Boolean amenity (no colon, no value)
        if value is None:
            data["extra_features"][slug] = True
            return

        # Yes/No → bool
        yn = _normalise_yes_no(value)
        if yn is not None:
            data["extra_features"][slug] = yn
            return

        # "Price per m²: 3.088€" — euros
        if "price" in slug and "€" in value:
            cents = _to_int_euro(value)
            if cents is not None:
                data["extra_features"][slug] = cents
                return

        # Numeric fields with explicit name pattern
        if slug in {"wc", "kitchens", "living_rooms"}:
            n = _to_int_simple(value)
            if n is not None:
                data["extra_features"][f"{slug}_count"] = n
                return

        # Anything else stays as a string (energy_class, orientation, distance_from_sea, ...)
        data["extra_features"][slug] = value

    def _write_column(
        self,
        column: str,
        value: Optional[str],
        data: Dict[str, Any],
    ) -> None:
        """
        Type-coerce a value into the named Property column.

        Ignores empties (so structured panel never overwrites with garbage).
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
        elif column in {"bedrooms", "bathrooms", "year_built"}:
            v = _to_int_simple(value)
            if v is not None:
                data[column] = v
        elif column == "levels":
            # Model column is String — keep verbatim (might be "0", "Two-level", etc)
            data["levels"] = value.strip()
        elif column in {"area", "subarea", "category"}:
            data[column] = value.strip()

    # ---------------------------------------------------------------
    # NLP fallback (regex-based, safe — fills only blanks)
    # ---------------------------------------------------------------
    def _apply_nlp_fallback(self, data: Dict[str, Any]) -> None:
        """
        Run the legacy DataExtractor on the description text only.

        Crucially:
          * Only applied to description (not greedy DOM text). This kills
            the false-positive amenity matches from page chrome.
          * Never overrides a value the structured pass already filled.
          * Extra_features from NLP are merged (additive), not replaced.
        """
        description = data.get("description") or ""
        if not description:
            return

        try:
            smart = self.extractor.analyze_full_text(description)
        except Exception as e:
            logger.warning(f"[GL] NLP fallback failed: {e}")
            return

        # Fill missing structural fields only.
        for key in ["size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
                    "year_built", "category", "levels"]:
            if data.get(key) is None and smart.get(key) is not None:
                data[key] = smart[key]

        # Merge extra_features additively. Structured-source values win;
        # NLP fills only keys that aren't already set, AND only if no
        # semantically-related key is already present.
        #
        # The NLP regex dictionary uses canonical names like 'alarm_system'
        # or 'storage_room', while the structured panel often uses shorter
        # variants ('alarm', 'storage'). Without dedup, both end up in
        # extra_features as visual duplicates ("Alarm • Alarm System").
        # We treat them as the same feature and let the structured value win.
        nlp_features = smart.get("extra_features") or {}
        existing_keys = set(data["extra_features"].keys())

        # Map NLP canonical key -> set of structural slugs that mean the same.
        # If ANY of the related slugs is already in extra_features, skip NLP.
        _NLP_TO_STRUCTURAL = {
            "alarm_system":   {"alarm"},
            "storage_room":   {"storage"},
            "parking":        {"outdoor_garage", "garage", "4_parking_spots",
                               "3_parking_spots", "2_parking_spots", "1_parking_spot"},
            "swimming_pool":  {"pool"},
            "sea_view":       {"view"},  # if 'view': 'Sea', NLP duplicate is redundant
        }

        for k, v in nlp_features.items():
            if k in existing_keys:
                continue
            # Skip if semantically equivalent slug already exists
            related = _NLP_TO_STRUCTURAL.get(k, set())
            if related & existing_keys:
                continue
            data["extra_features"][k] = v