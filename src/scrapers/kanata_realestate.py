"""
Kanata Real Estate scraper — Greek-listing-only mode (PerimeterX bypass).

After diagnosing that PerimeterX blocks ALL detail pages (both
`/en/propertyDetails/{id}` AND `/el/akinito/{id}` return the "Pardon Our
Interruption" challenge page), we extract everything possible from the
Greek search listing endpoint `/el/akinita/anazitisi` which is the ONLY
unprotected route.

Architecture
============
collect_urls walks Greek listings for 3 categories. EACH card is parsed
fully (not just seed fields) — title (often descriptive in Greek), type,
neighborhood, price, size, coords, thumbnail, short description if any.
All data is cached on the instance under site_property_id.

fetch_details just reads from cache. No HTTP request — detail pages are
PX-blocked anyway and we'd waste 500ms per property re-confirming that.

What we keep (~80% of useful data)
==================================
  ✓ site_property_id, category, type, neighborhood, municipality (mapped)
  ✓ price (€), size_sqm OR land_size_sqm (by category)
  ✓ €/m², coordinates (Halkidiki bbox-checked)
  ✓ thumbnail image (300x220 from spitogatos CDN)
  ✓ synthetic English description from card data + transliterated Greek title

What we lose
============
  ✗ Full-res image gallery (have only 300x220 thumb per property)
  ✗ Property features panel (Facade count, year built, etc.)
  ✗ Amenities list (sometimes recovered by NLP from transliterated title)

Greek text handling
===================
- Property TYPE — keyword search (αγροτεμάχιο → Farm parcel, βίλα → Villa, etc.)
- Neighborhood — explicit map (βουρβουρού → Vourvourou + Sithonia) +
  transliteration fallback for unknown locations
- Municipality — derived from neighborhood map (Greek neighborhood → known
  Halkidiki municipality)
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
# Constants & mappings
# =============================================================

# Three category walks. URL param value stays English even on Greek route.
_CATEGORIES: List[Tuple[str, str]] = [
    ("land",        "Land"),
    ("commercial",  "Commercial"),
    ("residential", "Residential"),
]

_HALKIDIKI_LAT_RANGE = (39.0, 41.0)
_HALKIDIKI_LNG_RANGE = (22.0, 24.0)

_PAGE_SAFETY_CAP = 20


# Greek property TYPE keywords. Order matters — more specific first to
# avoid e.g. "οικόπεδο" matching inside compounds. Lookup is case- and
# accent-insensitive (we normalise before checking).
_GREEK_TYPE_KEYWORDS: List[Tuple[str, str]] = [
    # Land
    ("αγροτεμαχιο",       "Farm parcel"),
    ("οικοπεδο",          "Plot"),
    ("νησι",              "Island"),
    # Residential
    ("μονοκατοικια",      "Detached House"),
    ("μεζονετα",          "Maisonette"),
    ("διαμερισμα",        "Apartment"),
    ("στουντιο",          "Studio"),
    ("βιλα",              "Villa"),
    ("κατοικια",          "Residence"),
    # Commercial
    ("βιοτεχνικος χωρος", "Industrial space"),
    ("βιοτεχνια",         "Industrial"),
    ("ξενοδοχειο",        "Hotel"),
    ("καταστημα",         "Shop"),
    ("γραφειο",           "Office"),
    ("αποθηκη",           "Warehouse"),
    ("επιχειρηση",        "Business"),
]


# Greek neighborhood (accent-stripped lowercase) → (English, Municipality).
# Built from the Halkidiki master list. Covers ~30 most common locations.
_GREEK_NEIGHBORHOOD_MAP: Dict[str, Tuple[str, str]] = {
    # Sithonia
    "βουρβουρου":     ("Vourvourou", "Sithonia"),
    "ελια":           ("Elia", "Sithonia"),
    "διαπορος":       ("Diaporos", "Sithonia"),
    "καρυδι":         ("Karidi", "Sithonia"),
    "νικητη":         ("Nikiti", "Sithonia"),
    "λαγομανδρα":     ("Lagomandra", "Sithonia"),
    "αγιος νικολαος": ("Agios Nikolaos", "Sithonia"),
    "νεος μαρμαρας":  ("Neos Marmaras", "Sithonia"),
    "σαρτη":          ("Sarti", "Sithonia"),
    "τορωνη":         ("Toroni", "Sithonia"),
    "καλαμιτσι":      ("Kalamitsi", "Sithonia"),
    "πορτο κουφο":    ("Porto Koufo", "Sithonia"),
    # Kassandra
    "χανιωτη":        ("Chanioti", "Kassandra"),
    "πευκοχωρι":      ("Pefkochori", "Kassandra"),
    "πολυχρονο":      ("Polychrono", "Kassandra"),
    "καλλιθεα":       ("Kallithea", "Kassandra"),
    "αφυτος":         ("Afytos", "Kassandra"),
    "σανη":           ("Sani", "Kassandra"),
    "φουρκα":         ("Fourka", "Kassandra"),
    "σκιωνη":         ("Skioni", "Kassandra"),
    "παλιουρι":       ("Paliouri", "Kassandra"),
    "καλανδρα":       ("Kalandra", "Kassandra"),
    "σιβιρι":         ("Siviri", "Kassandra"),
    # Nea Propontida
    "μουδανια":       ("Moudania", "Nea Propontida"),
    "φλογητα":        ("Flogita", "Nea Propontida"),
    "τριγλια":        ("Triglia", "Nea Propontida"),
    "καλλικρατεια":   ("Kallikrateia", "Nea Propontida"),
    # Aristotelis
    "ιερισσος":       ("Ierissos", "Aristotelis"),
    "ουρανουπολη":    ("Ouranoupoli", "Aristotelis"),
    "νεα ροδα":       ("Nea Roda", "Aristotelis"),
    "αμμουλιανη":     ("Amouliani", "Aristotelis"),
    "ολυμπιαδα":      ("Olympiada", "Aristotelis"),
    # Polygyros
    "πολυγυρος":      ("Polygyros", "Polygyros"),
    "γερακινη":       ("Gerakini", "Polygyros"),
    "ψακουδια":       ("Psakoudia", "Polygyros"),
    "ορμυλια":        ("Ormylia", "Polygyros"),
}


# Greek → Latin character map for transliteration fallback (unknown words).
_GREEK_CHAR_MAP: Dict[str, str] = {
    "α": "a", "β": "v", "γ": "g", "δ": "d", "ε": "e", "ζ": "z",
    "η": "i", "θ": "th", "ι": "i", "κ": "k", "λ": "l", "μ": "m",
    "ν": "n", "ξ": "x", "ο": "o", "π": "p", "ρ": "r", "σ": "s",
    "ς": "s", "τ": "t", "υ": "y", "φ": "f", "χ": "ch", "ψ": "ps",
    "ω": "o",
}


# =============================================================
# Pure helpers
# =============================================================

def _strip_greek_accents(text: str) -> str:
    """
    Strip Greek accent marks to make lookups robust across slightly varied
    spellings (Βουρβουρού vs Βουρβουρου, etc).
    """
    accent_map = {
        "ά": "α", "έ": "ε", "ή": "η", "ί": "ι", "ό": "ο", "ύ": "υ", "ώ": "ω",
        "ϊ": "ι", "ϋ": "υ", "ΐ": "ι", "ΰ": "υ",
    }
    return "".join(accent_map.get(c, c) for c in text)


def _transliterate_greek(text: str) -> str:
    """
    Naive Greek → Latin transliteration. Used for unknown neighborhood
    names and for the descriptive part of the synthetic description.

    Doesn't handle digraphs (ου → ou) optimally — gives "oy" — but is
    good enough for English search / display.
    """
    out: List[str] = []
    stripped = _strip_greek_accents(text)
    for ch in stripped:
        lower = ch.lower()
        mapped = _GREEK_CHAR_MAP.get(lower)
        if mapped is None:
            out.append(ch)
        elif ch.isupper():
            out.append(mapped.capitalize() if len(mapped) == 1 else mapped[0].upper() + mapped[1:])
        else:
            out.append(mapped)
    return "".join(out)


def _extract_type_from_greek_title(title: str) -> Optional[str]:
    """Search Greek title for known type keyword. Returns English name."""
    if not title:
        return None
    normalised = _strip_greek_accents(title).lower()
    for greek_kw, english in _GREEK_TYPE_KEYWORDS:
        if greek_kw in normalised:
            return english
    return None


def _extract_neighborhood_from_greek_title(
    title: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract neighborhood. Returns (greek_original, english_name, municipality).

    Strategy:
      1. Take first chunk before comma → lookup in known map
      2. If miss, scan entire title for any known neighborhood key
      3. If still miss, transliterate first chunk as the english_name and
         return None as municipality (cluster matching will rely on coords)
    """
    if not title:
        return None, None, None

    first_chunk = title.split(",", 1)[0].strip().rstrip(".")

    # Strategy A: first chunk lookup
    if first_chunk:
        key = _strip_greek_accents(first_chunk).lower()
        if key in _GREEK_NEIGHBORHOOD_MAP:
            english, muni = _GREEK_NEIGHBORHOOD_MAP[key]
            return first_chunk, english, muni

    # Strategy B: scan whole title for any known key
    normalised = _strip_greek_accents(title).lower()
    for key, (english, muni) in _GREEK_NEIGHBORHOOD_MAP.items():
        if key in normalised:
            return key, english, muni

    # Strategy C: transliterate first chunk as fallback
    if first_chunk:
        return first_chunk, _transliterate_greek(first_chunk), None

    return None, None, None


def _to_int_euro(text: str) -> Optional[int]:
    """Parse EU/EN price string into integer euros."""
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
    """Parse area as float. Both EU (dot thousands) and EN (comma thousands) accepted."""
    if not text:
        return None
    cleaned = re.sub(r"[,.]", "", text)
    m = re.search(r"\d+", cleaned)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _extract_thumb_from_style(style: str) -> Optional[str]:
    """Extract URL from inline CSS background-image."""
    if not style:
        return None
    m = re.search(r"url\(['\"]?([^)'\"]+)['\"]?\)", style)
    return m.group(1) if m else None


def _coords_in_halkidiki(lat: float, lng: float) -> bool:
    return (
        _HALKIDIKI_LAT_RANGE[0] <= lat <= _HALKIDIKI_LAT_RANGE[1]
        and _HALKIDIKI_LNG_RANGE[0] <= lng <= _HALKIDIKI_LNG_RANGE[1]
    )


def _build_synthetic_description(
    greek_title: str,
    english_neighborhood: Optional[str],
    municipality: Optional[str],
    english_type: Optional[str],
    category: str,
    size_sqm: Optional[float],
    land_size_sqm: Optional[float],
    price: Optional[int],
    price_per_sqm: Optional[int],
) -> str:
    """
    Build a synthetic description from structured card data + transliterated
    Greek title.

    Greek titles are often descriptive (e.g. "Βουρβουρού, πωλείται αγροτεμάχιο
    με 2 κατοικίες πρώτο στην θάλασσα" = "Vourvourou, agricultural land
    with 2 residences, first by the sea, for sale"), so transliterating
    them adds real descriptive material that NLP can scan.
    """
    parts: List[str] = []

    # 1. Opening — type + sale + location
    if english_type and english_neighborhood:
        if municipality:
            parts.append(
                f"{english_type} for sale in {english_neighborhood} "
                f"({municipality}), Halkidiki."
            )
        else:
            parts.append(
                f"{english_type} for sale in {english_neighborhood}, Halkidiki."
            )
    elif english_neighborhood:
        parts.append(f"Property for sale in {english_neighborhood}, Halkidiki.")
    else:
        parts.append("Property for sale in Halkidiki.")

    # 2. Size sentence
    if category == "Land" and land_size_sqm:
        parts.append(f"Land area {int(land_size_sqm):,} m².")
    elif size_sqm:
        parts.append(f"Total area {int(size_sqm):,} m².")

    # 3. Price + €/m²
    if price and price_per_sqm:
        parts.append(f"Price €{price:,} (€{price_per_sqm:,}/m²).")
    elif price:
        parts.append(f"Price €{price:,}.")

    # 4. Transliterated Greek title — adds descriptive content for NLP
    if greek_title and len(greek_title) > 20:
        transliterated = _transliterate_greek(greek_title).strip()
        if transliterated:
            parts.append(f"Listing details: {transliterated}")

    return " ".join(parts)


# =============================================================
# Scraper
# =============================================================

class KanataRealEstateScraper(EnrichmentMixin, BaseScraper):
    """
    Kanata Real Estate (kanatarealestate.gr) — Greek-listing-only mode.

    Detail pages PX-blocked; we extract everything from the listing card
    on the Greek search endpoint (which is the one route NOT under PX).
    fetch_details reads from cache — no extra HTTP per property.
    """

    BASE_URL = "https://www.kanatarealestate.gr"

    # NLP can fill these gaps from the (partially-transliterated)
    # description. Primary fields are already filled from card data.
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
        self.source_domain = "kanatarealestate.gr"

        # Rich card-data cache. Populated by collect_urls, read by
        # fetch_details. Survives across calls within the same scraper
        # instance (which daily_sync uses for both phases of one run).
        self._card_cache: Dict[str, Dict[str, Any]] = {}

    async def fetch_listings(self):
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # URL builders
    # ---------------------------------------------------------------

    def _build_list_url(self, url_category: str, page: int, min_price: int) -> str:
        """
        Greek listing URL — proven to bypass PerimeterX. Note that
        `category` param keeps English values ("land"/"commercial"/
        "residential") even though the path is in Greek.
        """
        base = (
            f"{self.BASE_URL}/el/akinita/anazitisi"
            f"?listingType=sale"
            f"&category={url_category}"
            f"&region=196"
            f"&roomsLow=nd"
            f"&priceLow={min_price}"
            f"&priceHigh="
            f"&livingAreaLow="
            f"&livingAreaHigh="
            f"&myCode="
        )
        if page > 1:
            base += f"&page={page}"
        return base

    def _detail_url_en(self, site_id: str) -> str:
        """English detail URL — stored as canonical URL for reviewers.
        Will return PX block when fetched programmatically but works in
        a real browser session with cookies."""
        return f"{self.BASE_URL}/en/propertyDetails/{site_id}"

    # ---------------------------------------------------------------
    # PHASE 1 — collect_urls (parses cards FULLY, caches rich data)
    # ---------------------------------------------------------------

    async def collect_urls(self, min_price: int = 400000) -> List[PropertyTemplate]:
        """
        Walk 3 categories on the Greek endpoint. Each card is parsed
        completely (all fields, not just seed) and the rich result is
        cached under site_property_id. fetch_details later reads from
        this cache — no per-property HTTP.
        """
        all_properties: List[PropertyTemplate] = []
        self._card_cache.clear()

        for url_category, our_category in _CATEGORIES:
            logger.info(
                f"[{self.source_domain}] === Category {our_category} ==="
            )
            page = 1

            while page <= _PAGE_SAFETY_CAP:
                url = self._build_list_url(url_category, page, min_price)
                logger.info(
                    f"[{self.source_domain}] {our_category} стр.{page}..."
                )

                try:
                    response = await self.client.get(url)
                    parser = LexborHTMLParser(response.text)
                    cards = parser.css(".listing-item")

                    if not cards:
                        logger.info(
                            f"[{self.source_domain}] нет карточек "
                            f"{our_category} стр.{page}"
                        )
                        break

                    # PX-protection sanity check (defensive — if Greek
                    # route ever starts returning PX block, we want a
                    # loud signal not silent failure).
                    if "Pardon Our Interruption" in response.text:
                        logger.error(
                            f"[{self.source_domain}] PX BLOCK on Greek route! "
                            f"({our_category} стр.{page}) — protection extended"
                        )
                        break

                    page_count = 0
                    for card in cards:
                        try:
                            prop = self._parse_card_rich(card, our_category)
                            if prop:
                                all_properties.append(prop)
                                page_count += 1
                        except Exception as e:
                            logger.error(
                                f"[{self.source_domain}] card err: {e}"
                            )

                    logger.info(
                        f"[{self.source_domain}] {our_category} стр.{page}: "
                        f"+{page_count}"
                    )

                    # Next-page sentinel: spitogatos drops `enabled` class
                    # on the last page's next link.
                    next_link = parser.css_first(
                        "#pagination-list li.next.enabled a"
                    )
                    if not next_link:
                        break

                    await asyncio.sleep(2)
                    page += 1

                except Exception as e:
                    logger.error(
                        f"[{self.source_domain}] err {our_category} стр.{page}: {e}"
                    )
                    break

        logger.info(
            f"[{self.source_domain}] Phase 1 done: "
            f"{len(all_properties)} properties (cached for fetch_details)"
        )
        return all_properties

    def _parse_card_rich(
        self,
        card: LexborNode,
        category: str,
    ) -> Optional[PropertyTemplate]:
        """Parse one card into a seed + cache the rich data."""

        # 1. site_property_id from href (/el/akinito/{id} or /en/propertyDetails/{id})
        #    or fallback to "Κωδ. {id}" text in .listing-item-code
        href_el = card.css_first("h4 a") or card.css_first(".listing-image")
        href = href_el.attributes.get("href", "") if href_el else ""

        site_id: Optional[str] = None
        m = re.search(
            r"/(akinito|propertyDetails|emlakDetaylar|propiedad)/(\d+)",
            href,
        )
        if m:
            site_id = m.group(2)
        else:
            code_el = card.css_first(".listing-item-code")
            if code_el:
                m2 = re.search(r"(\d+)", code_el.text(strip=True))
                if m2:
                    site_id = m2.group(1)
        if not site_id:
            return None

        # 2. Greek title — often a mini-description
        h4 = card.css_first("h4 a")
        greek_title = h4.text(strip=True) if h4 else ""

        # 3. Parse Greek title — type + neighborhood + municipality
        english_type = _extract_type_from_greek_title(greek_title)
        greek_nb, english_nb, municipality = _extract_neighborhood_from_greek_title(
            greek_title
        )

        # 4. Price + size from "€ 2.500.000 2.741 τ.μ." pattern
        price: Optional[int] = None
        size_value: Optional[float] = None

        price_b = card.css_first("p b")
        if price_b:
            text = price_b.text(separator=" ", strip=True)
            # Match "X τ.μ." (Greek) or "X m²" (English) — defensive
            size_match = re.search(r"([\d.,]+)\s*(?:τ\.?\s*μ\.?|m²)", text)
            if size_match:
                size_value = _to_float_sqm(size_match.group(1))
                price_part = text[: size_match.start()].strip()
                price = _to_int_euro(price_part)
            else:
                price = _to_int_euro(text)

        # Route size into size_sqm vs land_size_sqm by category
        if category == "Land":
            seed_size_sqm = None
            seed_land_size_sqm = size_value
        else:
            seed_size_sqm = size_value
            seed_land_size_sqm = None

        # 5. €/m² from <ul><li>912 €/τ.μ.</li></ul>
        price_per_sqm: Optional[int] = None
        for li in card.css("ul li"):
            li_text = li.text(strip=True)
            m_eur = re.search(r"([\d.,]+)\s*€\s*/", li_text)
            if m_eur:
                price_per_sqm = _to_int_euro(m_eur.group(1))
                break

        # 6. Coordinates — data-lat / data-lon on popup-modal link
        latitude: Optional[float] = None
        longitude: Optional[float] = None
        coord_el = card.css_first("a.popup-modal[data-lat]")
        if coord_el:
            lat_raw = coord_el.attributes.get("data-lat")
            lon_raw = coord_el.attributes.get("data-lon")
            if lat_raw and lon_raw:
                try:
                    lat_f = float(lat_raw)
                    lon_f = float(lon_raw)
                    if _coords_in_halkidiki(lat_f, lon_f):
                        latitude = lat_f
                        longitude = lon_f
                except ValueError:
                    pass

        # 7. Thumbnail from background-image style OR fallback to <img>
        thumb_url: Optional[str] = None
        cover = card.css_first(".cover.bg-image")
        if cover:
            thumb_url = _extract_thumb_from_style(
                cover.attributes.get("style", "")
            )
        if not thumb_url:
            img_el = card.css_first("img")
            if img_el:
                src = img_el.attributes.get("src", "")
                if src and "spitogatos" in src:
                    thumb_url = src

        # 8. Short description from any <p> that has length > 60 chars and
        #    isn't the price/code text (some properties without coords
        #    have a short text description here instead of a map link).
        short_descr: Optional[str] = None
        for p in card.css("p"):
            p_text = p.text(strip=True)
            if (len(p_text) > 60
                    and "€" not in p_text
                    and "τ.μ." not in p_text
                    and "m²" not in p_text
                    and not p_text.startswith("Κωδ.")
                    and not p_text.startswith("Code")
                    and "popup-modal" not in (p.html or "")):
                short_descr = p_text
                break

        # 9. Build location_raw with Halkidiki suffix (whitelist downstream)
        if english_nb and municipality:
            location_raw = f"{english_nb}, {municipality}, Halkidiki"
        elif english_nb:
            location_raw = f"{english_nb}, Halkidiki"
        else:
            location_raw = "Halkidiki"

        # 10. Build synthetic description (English + transliterated Greek)
        synth_descr = _build_synthetic_description(
            greek_title=greek_title,
            english_neighborhood=english_nb,
            municipality=municipality,
            english_type=english_type,
            category=category,
            size_sqm=seed_size_sqm,
            land_size_sqm=seed_land_size_sqm,
            price=price,
            price_per_sqm=price_per_sqm,
        )
        if short_descr:
            # Prepend the card's own short description (transliterated)
            transliterated_short = _transliterate_greek(short_descr)
            synth_descr = f"{transliterated_short} {synth_descr}"

        # 11. Cache rich data for fetch_details
        rich_data: Dict[str, Any] = {
            "description":       synth_descr,
            "price":             price,
            "size_sqm":          seed_size_sqm,
            "land_size_sqm":     seed_land_size_sqm,
            "bedrooms":          None,
            "bathrooms":         None,
            "year_built":        None,
            "area":              english_nb,
            "subarea":           municipality,
            "category":          category,
            "levels":            None,
            "site_last_updated": None,
            "latitude":          latitude,
            "longitude":         longitude,
            "images":            [thumb_url] if thumb_url else [],
            "extra_features":    {},
        }

        # Traceability — store originals so we can debug + future-improve
        ef = rich_data["extra_features"]
        if greek_title:
            ef["greek_title"] = greek_title
        if greek_nb:
            ef["greek_neighborhood"] = greek_nb
        if english_type:
            ef["property_subtype"] = english_type
        if price_per_sqm:
            ef["price_per_sqm"] = price_per_sqm
        # Flag PX status for future reference (and for filtering in UI
        # — reviewers can know that detail-page link won't auto-load)
        ef["px_protected"] = True
        ef["data_source"] = "greek_listing_only"

        self._card_cache[site_id] = rich_data

        # 12. Return seed PropertyTemplate for daily_sync
        return PropertyTemplate(
            site_property_id=site_id,
            source_domain=self.source_domain,
            url=self._detail_url_en(site_id),
            price=price,
            location_raw=location_raw,
            size_sqm=seed_size_sqm,
            land_size_sqm=seed_land_size_sqm,
            category=category,
        )

    # ---------------------------------------------------------------
    # PHASE 2 — fetch_details (cache read, NO HTTP)
    # ---------------------------------------------------------------

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Return cached rich data. No HTTP request — detail pages are
        PX-blocked, and we've already extracted everything from the
        listing card during collect_urls.

        Cache miss should be rare (only possible if fetch_details is
        called without a preceding collect_urls in the same instance).
        In that case we return {} so daily_sync falls back to seed data.
        """
        m = re.search(
            r"/(akinito|propertyDetails|emlakDetaylar|propiedad)/(\d+)",
            url,
        )
        if not m:
            logger.warning(
                f"[{self.source_domain}] cannot extract id from {url}"
            )
            return {}

        site_id = m.group(2)
        cached = self._card_cache.get(site_id)
        if not cached:
            logger.warning(
                f"[{self.source_domain}] cache miss for {site_id}"
            )
            return {}

        # Shallow-copy so NLP doesn't mutate the cache (in case
        # fetch_details is called multiple times for the same URL)
        data = dict(cached)
        data["extra_features"] = dict(cached.get("extra_features", {}))

        # NLP fallback — extracts amenities from the synthetic description.
        # Some Greek words bleed through transliteration in a way NLP can
        # still detect (e.g. "thalassa" doesn't match "sea", but
        # "swimming pool" if present will be caught).
        try:
            self._apply_nlp_fallback(data)
        except Exception as e:
            logger.warning(
                f"[{self.source_domain}] NLP fallback failed for {site_id}: {e}"
            )

        # Quality gate (log only)
        if not self._passes_quality_gate(data.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate "
                f"for {site_id}"
            )

        # Drop None so daily_sync doesn't overwrite seed values with None
        return {k: v for k, v in data.items() if v is not None}