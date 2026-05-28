"""
~/hodu/src/scrapers/kw_greece.py

KW Greece (kwgreece.gr) — Keller Williams Greece, powered by ilist.gr CRM
template ("e-agents workspace").

Scale: 59 residential listings ≥ €400k in Halkidiki at min_price=400000
(area=196 = Chalkidiki on the ilist taxonomy, same numeric ID as Spitogatos
uses but with a different backend).

URL patterns
------------
List:
    /results?page={N}&aim=1&area=196&priceFrom={min_price}
        aim=1   for-sale
        aim=2   for-rent
        area=196 Chalkidiki
Detail:
    /property/{numeric_id}      e.g. /property/2326636

Architecture (hybrid funnel — same pattern as halkidiki_estate)
---------------------------------------------------------------
Phase 1: Stage 1 (Playwright) — REQUIRED. The listing page is rendered
client-side via the ilist CRM JS; static HTML returns only chrome + filter
UI + footer with NO property cards visible. Setting
`scraper_routing.preferred_stage = 1` for this domain forces funnel to
Stage 1 for all requests; the ~2x latency penalty on detail pages is
acceptable for the consistency benefit.

Phase 2: Stage 0 would work for detail pages (verified — static server-
rendered HTML with full property data), but we let the per-domain
preferred_stage drive everything. If cost becomes an issue, the
optimization is to bypass the funnel and call curl_cffi directly for
/property/{id} URLs.

Language handling
-----------------
The site supports English + Greek (toggle in header). Language is
session/cookie-bound, NOT URL-prefixed (`/property/2326636` works in
both languages). To get the English version (which makes parsing
easier — labels match the standard hodu vocabulary) we send
`Accept-Language: en-US,en;q=0.9` on every request.

For robustness we parse BOTH English and Greek labels. The site
occasionally falls back to Greek for users without an English cookie
on first visit; the bilingual map keeps parsing reliable regardless.

Detail-page anatomy
-------------------
1. Title section:
       <h1>Πολυκατοικία/Κτίριο, προς πώληση</h1>   ← "Apartment Building, for sale"
       <h2 or sub>Νικήτη, Σιθωνία</h2>             ← "{Area}, {Region}"
   English form:
       <h1>Apartment Building, For Sale</h1>
       <h2>Nikiti, Sithonia</h2>

2. Price block: standalone "840.000 €" displayed prominently.

3. Code: "Κωδικός: 2326636" / "Code: 2326636" — matches URL ID.

4. STRUCTURED TABLE — "Βασικά Χαρακτηριστικά" (Basic Characteristics)
   Layout: definition-list–style key→value pairs. Each row is one of:
       Κατηγορία         (Category)
       Τύπος Ακινήτου    (Property Type)
       Τιμή              (Price)
       Εμβαδόν           (Area, in sqm)
       Τιμή ανά τ.μ      (Price per sqm)
       Κωδικός           (Code)
       Υπνοδωμάτια       (Bedrooms)
       Μπάνια            (Bathrooms)
       WC                (Half-bath count)
       Όροφος            (Floor)
       Έτος Κατασκευής   (Construction Year)
       Ενεργειακή Κλάση  (Energy Class — Α to Z)
       Απόσταση από      (Distance from — sub-keys: Sea, Center, Airport)

5. EXTRA TABLE — "Επιπλέον Χαρακτηριστικά" (Additional Characteristics)
   Same definition-list layout but with optional fields, examples:
       Σαλόνια           (Living rooms — count)
       Μπαλκόνια         (Balconies — count)
       Κήπος             (Garden — Yes/No)
       Εσωτερική Σκάλα   (Internal Stairs — Yes/No)
       Σοφίτα            (Loft — Yes/No)
       Κουζίνες          (Kitchens — count)
       Πρόσβαση από      (Access from — string: "Asphalt" / "Άσφαλτο")
       Ζώνη/Είδος        (Zone/Type — "Residential" / "Κατοικία")
       Πλεονεκτήματα     (Advantages — list: "BBQ, ...")
       Ιδιαίτερα Χαρακτηριστικά (Special Features — list)
       Προσανατολισμός   (Orientation — Δυτικό/East/West)
       Με Θέα            (With View — Sea/Mountain/etc.)
       Κατάσταση         (Condition — Very Good / Excellent / Average)
       Κοντά σε          (Near — list)
       Επιπρόσθετα       (Additional — A/C, Boiler, Mosquito nets)
       Heating medium    (Electricity, Gas, Oil)
       Heating type      (Individual, Central)
       Δάπεδα            (Floors — Tiles, Wood, Marble)
       Τζάμια            (Glass — Single, Double)
       Κουφώματα         (Frames — Aluminum, Wood, PVC)

6. Description: long Greek (or English) prose block. ~500-3000 chars.

7. Video: `<iframe src="https://www.youtube.com/embed/{id}">` (optional).

8. Map: lat/lng coordinates. Several possible storage forms — we attempt
   data-attrs, JS variables (`setView`, `lat=`), and OG geo meta. Halkidiki
   bbox sanity check filters wrong coords (same as halkidiki_agency).

9. Images: CDN-hosted, ~30-50 per listing on premium properties.
   URL pattern: `https://ilist-cdn.e-agents.cloud/appFol/appDetails/
   estatePhotos/fol{property_id}/{hash}.jpg`
   The property_id is embedded in the path, useful for site_id verification.

10. og:title format: " {Type} Προς Πώληση, {Region}, {Size} τ.μ., €{Price}"
    Used as a stable fallback when h1 parsing fails.

Anti-bot
--------
None detected. Detail pages return full HTML to curl_cffi. Listing pages
are JS-rendered, not bot-blocked.

Greek-to-canonical mappings
---------------------------
Property type → hodu category:
  Πολυκατοικία/Κτίριο   → "Maisonette"     (typically a multi-unit complex
                                             where each unit is a maisonette —
                                             see property 2326636 for the
                                             archetypal "6 autonomous mezzonettes"
                                             case)
  Μονοκατοικία         → "House"
  Διαμέρισμα           → "Apartment"
  Μεζονέτα             → "Maisonette"
  Βίλα                 → "House (Villa)"   (hodu convention; "Villa" alone
                                             collides with EllasEstate)
  Οικόπεδο             → "Land"
  Αγρόκτημα            → "Land"            (rural plot — same category)
  Επαγγελματικό        → "Business"

NLP fallback
------------
Disabled. The structured tables provide everything; NLP regex is English-
tuned and wouldn't help on Greek descriptions anyway. og:description is
used as fallback only when the main body description is empty (rare).
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from selectolax.lexbor import LexborHTMLParser, LexborNode

from src.models.schemas import PropertyTemplate
from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin


# =============================================================================
# Constants
# =============================================================================

_BASE_URL = "https://www.kwgreece.gr"
_LISTING_PATH = "/results"
_DETAIL_PATH_TPL = "/property/{id}"
_SOURCE_DOMAIN = "kwgreece.gr"

# ilist CRM uses the same Spitogatos region taxonomy: 196 = Chalkidiki.
# This is coincidental — the platforms aren't related, both just adopted
# the same Greek admin codes.
_AREA_ID_CHALKIDIKI = 196

# Discovered empirically: ~59 listings at min_price=400000 in Chalkidiki,
# typically 10-20 cards per page. Cap at 30 pages of safety headroom.
_MAX_PAGES = 30

# Polite pause between listing pages — Playwright has its own resource
# cost; 2s gives the page time to settle.
_INTER_PAGE_SLEEP_SEC = 2.0

# Empirical site_id range — observed IDs ~1.2M-2.5M (numeric, monotonic).
# We use this for sanity validation in URL extraction.
_SITE_ID_MIN = 100_000
_SITE_ID_MAX = 99_999_999


# =============================================================================
# Title / Property-type parsing
# =============================================================================
#
# Detail-page <h1> follows two formats depending on language session:
#   "Πολυκατοικία/Κτίριο, προς πώληση"   (Greek default)
#   "Apartment Building, For Sale"        (English with Accept-Language)
#
# We map the type-word (left of comma) to hodu's canonical categories.
# Both languages mapped → robust to language flips mid-session.

# Bilingual property-type → canonical category map. Keys are the type-word
# from the h1, lower-cased and trimmed.
_TYPE_TO_CATEGORY: Dict[str, str] = {
    # Greek
    "πολυκατοικία/κτίριο":   "Maisonette",
    "πολυκατοικία":           "Maisonette",
    "κτίριο":                 "Maisonette",
    "μονοκατοικία":           "House",
    "διαμέρισμα":             "Apartment",
    "μεζονέτα":               "Maisonette",
    "μεζονετα":               "Maisonette",
    "βίλα":                   "House (Villa)",
    "βιλα":                   "House (Villa)",
    "οικόπεδο":               "Land",
    "αγρόκτημα":              "Land",
    "αγροτεμάχιο":            "Land",
    "επαγγελματικό":          "Business",
    "επαγγελματικός χώρος":   "Business",
    "κατάστημα":              "Business",
    "γραφείο":                "Business",
    "αποθήκη":                "Business",
    "ξενοδοχείο":             "Business",
    "γκαρσονιέρα":            "Apartment",
    "οροφοδιαμέρισμα":        "Apartment",
    "βιοτεχνικός χώρος":      "Business",
    "βιομηχανικός χώρος":     "Business",
    # English (Accept-Language: en-US session)
    "apartment building":     "Maisonette",
    "residential building":   "Maisonette",
    "building":               "Maisonette",
    "residential":            "Maisonette",
    "detached house":         "House",
    "house":                  "House",
    "apartment":              "Apartment",
    "maisonette":             "Maisonette",
    "villa":                  "House (Villa)",
    "land":                   "Land",
    "plot":                   "Land",
    "plot of land":           "Land",
    "parcel":                 "Land",
    "land area":              "Land",
    "farm":                   "Land",
    "residential complex":    "Maisonette",
    "business":               "Business",
    "office":                 "Business",
    "store":                  "Business",
    "warehouse":              "Business",
    "hotel":                  "Business",
    "studio":                 "Apartment",
    "loft":                   "Apartment",
    "industrial":             "Business",
    "industrial space":       "Business",
    "industrial property":    "Business",
    "professional space":     "Business",
}


# =============================================================================
# Structured-table label routing
# =============================================================================
#
# Keys are labels (lower-cased, trimmed) from the "Βασικά Χαρακτηριστικά"
# (Basic) and "Επιπλέον Χαρακτηριστικά" (Extra) panels. Values:
#   "_skip"        → ignore (already obtained elsewhere)
#   "<column>"     → set top-level PropertyTemplate.<column>
#   "_extra:<key>" → put parsed value into extra_features[<key>]
#
# Bilingual — both Greek and English variants populated for the common
# fields. Less-common labels covered in one language are matched verbatim
# via _slug() fallback.
_LABEL_TO_FIELD: Dict[str, str] = {
    # ─── Basic Characteristics ──────────────────────────────────────────
    # Greek
    "κατηγορία":             "_skip",   # we get category from h1 type-word
    "τύπος ακινήτου":        "_skip",   # ditto
    "τιμή":                  "price",
    "εμβαδόν":               "size_sqm",
    "τιμή ανά τ.μ":          "_extra:price_per_sqm",
    "τιμή ανά τ.μ.":         "_extra:price_per_sqm",
    "κωδικός":               "_extra:site_id_authoritative",
    "υπνοδωμάτια":           "bedrooms",
    "μπάνια":                "bathrooms",
    "wc":                    "_extra:wc_count",
    "όροφος":                "_extra:floor",
    "έτος κατασκευής":       "year_built",
    "ενεργειακή κλάση":      "_extra:energy_class",
    "απόσταση από":          "_skip",  # this is a label for a sub-block
    # English
    "category":              "_skip",
    "property type":         "_skip",
    "price":                 "price",
    "area":                  "size_sqm",
    "price per sqm":         "_extra:price_per_sqm",
    "price per sq.m":        "_extra:price_per_sqm",
    "code":                  "_extra:site_id_authoritative",
    "bedrooms":              "bedrooms",
    "bathrooms":             "bathrooms",
    "floor":                 "_extra:floor",
    "construction year":     "year_built",
    "year of construction":  "year_built",
    "year built":            "year_built",
    "energy class":          "_extra:energy_class",
    "distance from":         "_skip",
    # ─── Extra Characteristics ──────────────────────────────────────────
    # Greek
    "σαλόνια":               "_extra:living_rooms_count",
    "μπαλκόνια":             "_extra:balconies_count",
    "κήπος":                 "_extra:garden",
    "εσωτερική σκάλα":       "_extra:internal_stairs",
    "σοφίτα":                "_extra:loft",
    "κουζίνες":              "_extra:kitchens_count",
    "πρόσβαση από":          "_extra:access_from",
    "ζώνη/είδος":            "_extra:zone",
    "πλεονεκτήματα":         "_extra:advantages",
    "ιδιαίτερα χαρακτηριστικά": "_extra:special_features",
    "προσανατολισμός":       "_extra:orientation",
    "με θέα":                "_extra:view",
    "κατάσταση":             "_extra:condition",
    "κοντά σε":              "_extra:nearby",
    "επιπρόσθετα":           "_extra:additional",
    "δάπεδα":                "_extra:floors_type",
    "τζάμια":                "_extra:glass_type",
    "κουφώματα":             "_extra:frames_type",
    # English (some labels are NOT translated by the site — e.g. "Heating medium"
    # appears verbatim in both Greek and English views — so we cover them both
    # under their displayed form)
    "living rooms":          "_extra:living_rooms_count",
    "balconies":             "_extra:balconies_count",
    "garden":                "_extra:garden",
    "internal stairs":       "_extra:internal_stairs",
    "internal staircase":    "_extra:internal_stairs",
    "loft":                  "_extra:loft",
    "attic":                 "_extra:loft",
    "kitchens":              "_extra:kitchens_count",
    "access from":           "_extra:access_from",
    "zone":                  "_extra:zone",
    "zone/type":             "_extra:zone",
    "zone/sort":             "_extra:zone",
    "advantages":            "_extra:advantages",
    "special features":      "_extra:special_features",
    "orientation":           "_extra:orientation",
    "view":                  "_extra:view",
    "with view":             "_extra:view",
    "condition":             "_extra:condition",
    "near":                  "_extra:nearby",
    "nearby":                "_extra:nearby",
    "near to":               "_extra:nearby",
    "additional":            "_extra:additional",
    "heating medium":        "_extra:heating_medium",
    "heating type":          "_extra:heating_type",
    "floors":                "_extra:floors_type",
    "floor type":            "_extra:floors_type",
    "floors type":           "_extra:floors_type",
    "glass":                 "_extra:glass_type",
    "glazed windows":        "_extra:glass_type",
    "frames":                "_extra:frames_type",
    "swimming pool":     "_extra:swimming_pool",
    "parking":           "_extra:parking",
    "fireplaces":        "_extra:fireplaces_count",
    "storage":           "_extra:storage",
    "suitable for":      "_extra:suitable_for",
    "unique features":   "_extra:unique_features",
    "balconies (sq.m.)": "_extra:balconies_sqm",
}


# =============================================================================
# Greek "Yes/No" + boolean detection
# =============================================================================

_YES_TOKENS = {"ναι", "yes", "y", "true", "1"}
_NO_TOKENS = {"όχι", "οχι", "no", "n", "false", "0"}


# =============================================================================
# Halkidiki area → municipality routing
# =============================================================================
#
# Used for the optional `municipality` field on extras. Subset of the
# dionisiou_realestate map, kept minimal — KW Greece's area names are
# the standard Greek transliterations or English variants.
_AREA_TO_MUNICIPALITY: Dict[str, str] = {
    # Kassandra peninsula
    "kassandra":       "Kassandra",
    "kassandreia":     "Kassandra",
    "kassandria":      "Kassandra",
    "kallithea":       "Kassandra",
    "afytos":          "Kassandra",
    "kriopigi":        "Kassandra",
    "polychrono":      "Kassandra",
    "hanioti":         "Kassandra",
    "chanioti":        "Kassandra",
    "pefkohori":       "Kassandra",
    "pefkochori":      "Kassandra",
    "paliouri":        "Kassandra",
    "nea skioni":      "Kassandra",
    "sani":            "Kassandra",
    "fourka":          "Kassandra",
    "siviri":          "Kassandra",
    "possidi":         "Kassandra",
    "kalandra":        "Kassandra",
    "nea fokea":       "Kassandra",
    "elani":           "Kassandra",
    # Sithonia
    "sithonia":        "Sithonia",
    "nikiti":          "Sithonia",
    "neos marmaras":   "Sithonia",
    "marmaras":        "Sithonia",
    "sarti":           "Sithonia",
    "toroni":          "Sithonia",
    "sikia":           "Sithonia",
    "vourvourou":      "Sithonia",
    "porto koufo":     "Sithonia",
    "ormos panagias":  "Sithonia",
    "metamorfosi":     "Sithonia",
    "psakoudia":       "Sithonia",
    "agios nikolaos":  "Sithonia",
    # Aristotelis (Athos foothills)
    "ouranoupoli":     "Aristotelis",
    "ierissos":        "Aristotelis",
    "ammouliani":      "Aristotelis",
    "nea roda":        "Aristotelis",
    "stratoni":        "Aristotelis",
    "olympiada":       "Aristotelis",
    # Polygyros (mainland)
    "polygyros":       "Polygyros",
    "gerakini":        "Polygyros",
    "ormylia":         "Polygyros",
    "agios mamas":     "Polygyros",
    # Nea Propontida
    "moudania":        "Nea Propontida",
    "nea moudania":    "Nea Propontida",
    "kallikrateia":    "Nea Propontida",
    "kallikratia":     "Nea Propontida",
    "flogita":         "Nea Propontida",
    "sozopoli":        "Nea Propontida",
    "nea triglia":     "Nea Propontida",
    "nea iraklia":     "Nea Propontida",
    "nea plagia":      "Nea Propontida",
    "nea potidea":     "Nea Propontida",
}


# =============================================================================
# Regexes
# =============================================================================

# Detail URL pattern: extract numeric ID. Tolerant of trailing slash + query.
_PROPERTY_URL_RE = re.compile(r"/property/(\d+)(?:[/?#]|$)")

# og:title format: " {Type} Προς Πώληση, {Region}, {Size} τ.μ., €{Price}"
# or English equivalent. We use it as a fallback when h1 parsing fails.
# Captures the FIRST capitalized phrase (the type-word).
_OG_TITLE_TYPE_RE = re.compile(
    r"^\s*([\w\u0370-\u03FF/\s.()-]+?)\s*(?:,|προς|for sale|for rent)",
    re.IGNORECASE,
)

# Price patterns. The site uses Greek convention (`.` thousand sep, `,` decimal)
# but our parsers handle both.
_PRICE_DIGITS_RE = re.compile(r"[\d.,]+")

# Numeric extraction from labels like "315 τ.μ" / "315 m²" / "315 sqm"
_SQM_VALUE_RE = re.compile(
    r"(\d{1,6}(?:[.,]\d+)?)\s*(?:τ\.?μ\.?|m²|m2|sqm|sq\.?\s*m\.?)",
    re.IGNORECASE,
)

# Distance values: "100 Μέτρα" / "100 m" / "65 km"
_DISTANCE_M_RE = re.compile(
    r"(\d{1,6}(?:[.,]\d+)?)\s*(?:μέτρα|μ\.?|m\b|meters?)",
    re.IGNORECASE,
)
_DISTANCE_KM_RE = re.compile(
    r"(\d{1,6}(?:[.,]\d+)?)\s*(?:χιλιόμετρα|χλμ\.?|km\b|kilom)",
    re.IGNORECASE,
)

# YouTube embed URL extraction from iframe src
_YOUTUBE_EMBED_RE = re.compile(
    r"https?://(?:www\.)?youtube\.com/embed/([\w-]{6,})",
    re.IGNORECASE,
)

# Coordinate extraction patterns — many possible formats, try them all
_COORDS_LATLNG_RE = re.compile(
    r"lat[a-z]*\s*[:=]\s*['\"]?(-?\d+\.\d+)['\"]?\s*,?\s*"
    r"l[no]ng?[a-z]*\s*[:=]\s*['\"]?(-?\d+\.\d+)['\"]?",
    re.IGNORECASE,
)
_COORDS_SETVIEW_RE = re.compile(
    r"setView\s*\(\s*\[\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\s*\]",
    re.IGNORECASE,
)
_COORDS_LATLNG_PAIR_RE = re.compile(
    r"((?:39|40|41)\.\d{4,})\s*[,|]\s*((?:22|23|24)\.\d{4,})"
)

# Separate lat / lng regexes — for cases where assignments are on
# different lines or interspersed with other JS code.
#   var lat = 40.21249970745811;
#   var lon = 23.674292337607223;
# Strict word-boundary anchored so it doesn't fire on substring matches.
_COORDS_LAT_ONLY_RE = re.compile(
    r"\b(?:lat|latitude)\b\s*[:=]\s*['\"]?(-?\d+\.\d+)['\"]?",
    re.IGNORECASE,
)
_COORDS_LNG_ONLY_RE = re.compile(
    r"\b(?:lng|lon|longitude)\b\s*[:=]\s*['\"]?(-?\d+\.\d+)['\"]?",
    re.IGNORECASE,
)

# CDN image URL pattern (so we can filter out logos/chrome images)
_CDN_IMAGE_RE = re.compile(
    r"https?://ilist-cdn\.e-agents\.cloud/[^\"'\s)]+\.(?:jpg|jpeg|png|webp)",
    re.IGNORECASE,
)

# Property-id extraction from CDN URLs:
# .../fol{id}/{hash}.jpg → id
_CDN_PROPERTY_ID_RE = re.compile(r"/fol(\d+)/")


# =============================================================================
# Helpers — pure functions
# =============================================================================

def _normalize_text(s: Optional[str]) -> str:
    """Collapse whitespace, decode &nbsp; and narrow-NBSP, strip."""
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("\u202f", " ")
    return re.sub(r"\s+", " ", s).strip()


def _slug(label: str) -> str:
    """Free-form label → stable snake_case key."""
    s = label.strip().lower()
    s = re.sub(r"[^a-z0-9\u0370-\u03FF]+", "_", s)
    return s.strip("_")


def _to_int_simple(text: str) -> Optional[int]:
    """First integer in string. '9 ', 'Floor: 2', '315' → int."""
    if not text:
        return None
    m = re.search(r"\d+", text.replace(".", "").replace(",", ""))
    return int(m.group(0)) if m else None


def _to_float_value(text: str) -> Optional[float]:
    """
    Parse a single numeric value from text. Handles Greek locale:
      '315 τ.μ' / '315 m²' / '315'  → 315.0
      '2.666,67 €' / '€2,666.67'    → 2666.67
      '840.000 €' / '1.500.000 €'   → 840000.0 / 1500000.0
      '40.21249970745811'           → 40.21249970745811  (preserved as decimal)

    Approach: extract numeric blob (digits + dots + commas), then
    disambiguate dots and commas using positional heuristics:
      - both dot+comma → European: dot=thousand, comma=decimal
      - comma only → 1-2 trailing digits = decimal; else thousand
      - dot only → 3 trailing digits = thousand sep; else decimal
      - neither → integer
    """
    if not text:
        return None
    # Extract first numeric blob (allows leading minus). Stops at the
    # first non-numeric/non-separator char so units, euro sign, end-of-
    # string boundary issues don't interfere.
    m = re.search(r"-?[\d.,]+", text)
    if not m:
        return None
    cleaned = m.group(0).strip(",.")
    if not cleaned:
        return None

    if "." in cleaned and "," in cleaned:
        # European format: dot=thousand, comma=decimal ("2.666,67")
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        # `,` could be either thousand sep or decimal — disambiguate.
        # 1-2 trailing digits after comma → decimal ("2,5")
        m_dec = re.search(r",\d{1,2}$", cleaned)
        if m_dec:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "." in cleaned:
        # Dot-only case: Greek/European convention often uses `.` as
        # thousand sep (e.g. "840.000"). Heuristic:
        #   - Multiple dots → thousand sep ("1.500.000")
        #   - Single dot followed by exactly 3 digits → thousand sep
        #     ("840.000", "1.234")
        #   - else → decimal ("315.5", "40.2125", "40.21")
        parts = cleaned.split(".")
        if len(parts) > 2:
            cleaned = cleaned.replace(".", "")
        elif len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
            cleaned = cleaned.replace(".", "")
        # else: keep as decimal

    try:
        return float(cleaned)
    except ValueError:
        return None


def _to_int_euro(text: str) -> Optional[int]:
    """
    Parse euro price strings (Greek locale).

      '840.000 €'    → 840000
      '€ 840.000'    → 840000
      '1.500.000 €'  → 1500000
      '2.666,67 €'   → 2666 (price-per-sqm; cents truncated)
      'On request'   → None
    """
    if not text:
        return None
    cleaned = text.strip()
    lowered = cleaned.lower()
    if "request" in lowered or "κατόπιν" in lowered or "poa" in lowered:
        return None
    v = _to_float_value(cleaned)
    if v is None:
        return None
    value = int(v)
    if value > EnrichmentMixin._PRICE_SANITY_CAP:
        return None
    if value <= 0:
        return None
    return value


def _to_float_sqm(text: str) -> Optional[float]:
    """Parse area in sqm: '315 τ.μ' / '315 m²' / '315' → 315.0."""
    if not text:
        return None
    m = _SQM_VALUE_RE.search(text)
    if m:
        return _to_float_value(m.group(1))
    return _to_float_value(text)


def _interpret_bool(text: str) -> Optional[bool]:
    """Greek/English 'Ναι/Yes/Όχι/No' → bool. Anything else → None."""
    if not text:
        return None
    v = text.strip().lower()
    if v in _YES_TOKENS:
        return True
    if v in _NO_TOKENS:
        return False
    return None


def _extract_id_from_url(url: str) -> Optional[str]:
    """`/property/2326636` → `'2326636'`."""
    if not url:
        return None
    m = _PROPERTY_URL_RE.search(url)
    if not m:
        return None
    raw_id = m.group(1)
    # Sanity check — site IDs are 6-8 digit integers
    try:
        id_int = int(raw_id)
    except ValueError:
        return None
    if not (_SITE_ID_MIN <= id_int <= _SITE_ID_MAX):
        return None
    return raw_id


def _extract_distance_m(text: str) -> Optional[int]:
    """
    Parse a distance value, returning meters as int.

    Handles:
      '100 Μέτρα'  → 100
      '100 m'      → 100
      '0,5 χλμ'    → 500
      '2 km'       → 2000
    """
    if not text:
        return None
    # km first (longer suffix)
    m_km = _DISTANCE_KM_RE.search(text)
    if m_km:
        v = _to_float_value(m_km.group(1))
        if v is not None:
            return int(v * 1000)
    m_m = _DISTANCE_M_RE.search(text)
    if m_m:
        v = _to_float_value(m_m.group(1))
        if v is not None:
            return int(v)
    # Plain number fallback
    v = _to_float_value(text)
    return int(v) if v is not None else None


# =============================================================================
# Scraper
# =============================================================================

class KWGreeceScraper(EnrichmentMixin, BaseScraper):
    """
    kwgreece.gr — Keller Williams Greece, ilist.gr CRM template.

    Two-phase canonical pattern with hybrid stage routing:
      Phase 1: collect_urls(min_price)  → List[PropertyTemplate] seeds
      Phase 2: fetch_details(url)        → Dict[str, Any] for one property

    Phase 1 REQUIRES Stage 1 (Playwright) — set `scraper_routing.preferred_stage = 1`
    for `kwgreece.gr` in the DB before first run. Phase 2 detail pages
    work fine on Stage 0 but inherit the same preferred_stage for
    consistency (acceptable ~2x latency cost on 59 properties).

    Inherits canonical enrichment helpers:
      * _og_description_fallback()  used when site description thin
      * _og_image_fallback()        used when CDN gallery empty
      * _passes_quality_gate()      log-only desc-length check

    NOT used:
      * _apply_nlp_fallback()  — English-tuned regex won't help on Greek
                                  descriptions; structured tables already
                                  cover all signal-rich fields.
      * LLM fallback            — same reason; cost not justified here.
    """

    # ── Mixin overrides ──────────────────────────────────────────────────

    # The structured table is authoritative for everything; nothing
    # should be NLP-filled. Empty tuple disables structural→NLP merge.
    _NLP_FILLABLE_COLUMNS: Tuple[str, ...] = ()

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    # ── URL builders ─────────────────────────────────────────────────────

    def _construct_listing_url(self, page: int, min_price: int) -> str:
        """Build a paginated /results search URL."""
        params = [
            f"page={page}",
            "aim=1",
            f"area={_AREA_ID_CHALKIDIKI}",
            f"priceFrom={min_price}",
        ]
        return f"{_BASE_URL}{_LISTING_PATH}?{'&'.join(params)}"

    def _construct_detail_url(self, site_id: str) -> str:
        return f"{_BASE_URL}{_DETAIL_PATH_TPL.format(id=site_id)}"

    # ── Phase 1: collect_urls ────────────────────────────────────────────

    async def collect_urls(
        self,
        min_price: int = 400_000,
        max_pages: int = _MAX_PAGES,
    ) -> List[PropertyTemplate]:
        """
        Walk paginated /results pages, extract /property/{id} links from
        rendered HTML (Playwright Stage 1). Returns deduplicated seeds.

        End-of-results detection: a page with fewer NEW property links
        than the previous page (i.e. duplicates only) means we've hit
        the last page or the site stopped paginating beyond results.
        """
        seeds: Dict[str, PropertyTemplate] = {}
        last_page_processed = 0
        consecutive_empty = 0

        for page in range(1, max_pages + 1):
            url = self._construct_listing_url(page=page, min_price=min_price)
            logger.info(
                f"[{self.source_domain}] Phase 1: Парсинг страницы {page} - {url}"
            )

            try:
                html_text = await self._fetch_html_direct(url, wait_for_cards=True)
            except Exception as exc:
                logger.error(
                    f"[{self.source_domain}] page {page} fetch failed: {exc!r}"
                )
                break

            if not html_text:
                logger.warning(
                    f"[{self.source_domain}] page {page} empty response — stopping"
                )
                break

            parser = LexborHTMLParser(html_text)
            new_links = self._extract_property_links(parser, html_text)

            page_added = 0
            for site_id, href in new_links.items():
                if site_id in seeds:
                    continue
                seeds[site_id] = PropertyTemplate(
                    site_property_id=site_id,
                    url=href,
                    source_domain=self.source_domain,
                )
                page_added += 1

            logger.info(
                f"[{self.source_domain}] страница {page}: "
                f"{len(new_links)} ссылок (+{page_added} новых, всего: {len(seeds)})"
            )

            # End-of-results detection: 2 consecutive pages with no new links
            if page_added == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    logger.info(
                        f"[{self.source_domain}] {consecutive_empty} consecutive "
                        f"empty pages — assuming end of results"
                    )
                    break
            else:
                consecutive_empty = 0

            last_page_processed = page
            await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)

        logger.info(
            f"[{self.source_domain}] Phase 1 завершён: "
            f"{len(seeds)} URLs за {last_page_processed} страниц"
        )
        return list(seeds.values())

    def _extract_property_links(
        self,
        parser: LexborHTMLParser,
        html_text: str,
    ) -> Dict[str, str]:
        """
        From a rendered listing page, extract all /property/{id} URLs.

        Strategy is selector-agnostic: scan ALL <a href> attributes for
        the pattern. The ilist CRM may change card-wrapper class names
        between deploys, but the href pattern is stable. We dedupe by
        site_id and absolutize relative URLs.

        Returns: {site_id: absolute_url}.
        """
        links: Dict[str, str] = {}

        # Primary path — selector
        for a in parser.css("a[href]"):
            href = (a.attributes.get("href") or "").strip()
            if "/property/" not in href:
                continue
            site_id = _extract_id_from_url(href)
            if not site_id:
                continue
            # Absolutize relative hrefs
            if href.startswith("/"):
                href = f"{_BASE_URL}{href}"
            elif not href.startswith("http"):
                continue
            links.setdefault(site_id, href)

        # Backstop — regex over raw HTML, catches links hidden in JS
        # blobs or non-<a> elements (defensive against template tweaks).
        if not links:
            for m in re.finditer(r'/property/(\d+)', html_text):
                site_id = m.group(1)
                if not (_SITE_ID_MIN <= int(site_id) <= _SITE_ID_MAX):
                    continue
                href = f"{_BASE_URL}/property/{site_id}"
                links.setdefault(site_id, href)

        return links

    # ── Direct Playwright helper (bypass funnel for language cookie control) ──
    async def _fetch_html_curl(self, url: str) -> str:
        """Detail pages are server-rendered — fetch via curl_cffi with the
        English cookie (2x faster than Playwright, no browser dependency).
        Only listing pages (JS-rendered) need Stage 1 Playwright."""
        from curl_cffi import requests as _cffi
        def _get() -> str:
            r = _cffi.get(
                url, impersonate="chrome",
                cookies={".AspNetCore.Culture": "c%3Den-US%7Cuic%3Den-US"},
                headers={"Accept-Language": "en-US,en;q=0.9"},
                timeout=20,
            )
            return r.text if r.status_code == 200 else ""
        return await asyncio.to_thread(_get)

    async def _fetch_html_direct(
        self,
        url: str,
        *,
        wait_for_cards: bool = False,
    ) -> str:
        """
        Fetch a kwgreece.gr URL via direct Playwright with the English
        language cookie injected.

        kwgreece.gr is rendered server-side based on the
        `.AspNetCore.Culture` cookie. Without this cookie the site defaults
        to Greek; with `c=en-US|uic=en-US` (URL-encoded) it returns
        English. We inject this cookie into the Playwright context BEFORE
        navigation so the very first server response is in English.

        For listing pages (`wait_for_cards=True`), we additionally wait
        for `a[href*="/property/"]` to appear, because the property cards
        are populated client-side via AJAX after `domcontentloaded`. For
        detail pages, content is server-rendered so no extra wait needed.

        This bypasses the funnel entirely — kwgreece has no anti-bot
        challenges (verified empirically), so the funnel's escalation
        logic isn't useful here. The trade-off is full control over
        cookies (esp. language) at the cost of losing funnel features.
        """
        from src.scrapers.fetchers.browser_pool import browser_pool

        if not browser_pool.available:
            raise RuntimeError("Stage 1 Playwright unavailable")

        async with browser_pool.acquire() as browser:
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="Europe/Athens",
                java_script_enabled=True,
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            # Inject English-locale cookie. The site reads this server-
            # side to choose language for both rendered HTML and the JS
            # bundle path (/js/site-scripts/en-US/ vs el-GR/).
            await context.add_cookies([{
                "name": ".AspNetCore.Culture",
                "value": "c%3Den-US%7Cuic%3Den-US",
                "domain": "www.kwgreece.gr",
                "path": "/",
            }])
            try:
                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

                if wait_for_cards:
                    # Listing page: wait for AJAX-rendered property cards
                    try:
                        await page.wait_for_selector(
                            'a[href*="/property/"]', timeout=15_000,
                        )
                    except Exception:
                        # No cards in 15s — either empty result page or
                        # site changed markup. Try networkidle as fallback.
                        try:
                            await page.wait_for_load_state(
                                "networkidle", timeout=10_000,
                            )
                        except Exception:
                            pass

                html = await page.content()
            finally:
                await context.close()

        return html

    # ── Phase 2: fetch_details ───────────────────────────────────────────

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Fetch one detail page and assemble the full data dict.

        Canonical 7-step pattern:
          1. Title block → category, area_label
          2. Structured "Basic Characteristics" table parse
          3. Structured "Extra Characteristics" table parse
          4. Description → og:description fallback
          5. Coordinates (multiple fallback strategies)
          6. Images (CDN URLs)
          7. Video + meta (optional)
          (NLP fallback skipped — see class docstring)
          (Quality gate logged)
        """
        try:
            html_text = await self._fetch_html_curl(url)
        except Exception as exc:
            logger.error(
                f"[{self.source_domain}] detail fetch failed for {url}: {exc!r}"
            )
            return {}

        if not html_text:
            return {}

        parser = LexborHTMLParser(html_text)
        result: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        # ─── 1. Title (h1 + city/region line) ──────────────────────────
        self._parse_title_block(parser, result, extra)

        # ─── 2/3. Structured tables (Basic + Extra) ────────────────────
        self._parse_structured_panels(parser, result, extra)

        # ─── 4. Description ────────────────────────────────────────────
        description = self._extract_description(parser)
        if not description:
            description = self._og_description_fallback(parser)
        if description:
            result["description"] = description

        # ─── 5. Coordinates ────────────────────────────────────────────
        lat, lng = self._extract_coordinates(parser, html_text)
        if lat is not None and lng is not None:
            result["latitude"] = lat
            result["longitude"] = lng
            extra["gps_type"] = "approximate"  # ilist doesn't publish gps_type;
                                                # assume approximate by default

        # ─── 6. Images ─────────────────────────────────────────────────
        images = self._extract_images(parser, html_text)
        if not images:
            og = self._og_image_fallback(parser)
            if og:
                images = [og]
        if images:
            result["images"] = images

        # ─── 7. Video ──────────────────────────────────────────────────
        video_url = self._extract_video_url(parser, html_text)
        if video_url:
            extra["virtual_tour_url"] = video_url

        # ─── Area → municipality routing ───────────────────────────────
        area_value = result.get("area")
        if area_value:
            slug = area_value.strip().lower()
            muni = _AREA_TO_MUNICIPALITY.get(slug)
            if muni:
                extra["municipality"] = muni

        # Merge extras
        if extra:
            result["extra_features"] = extra

        # ─── Quality Gate (log-only) ───────────────────────────────────
        if not self._passes_quality_gate(result.get("description")):
            logger.warning(
                f"[{self.source_domain}] description below quality gate "
                f"for {url}"
            )

        return result

    # ── fetch_details helpers ────────────────────────────────────────────

    def _parse_title_block(
        self,
        parser: LexborHTMLParser,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Parse the title block at the top of the detail page.

        Expected structure (bilingual):
            <h1>Πολυκατοικία/Κτίριο, προς πώληση</h1>
            <h2 or similar>Νικήτη, Σιθωνία</h2>
        English form:
            <h1>Apartment Building, For Sale</h1>
            <h2>Nikiti, Sithonia</h2>

        Sets:
          result['category']    via _TYPE_TO_CATEGORY[type-word]
          result['area']        from h2/city line (first comma-separated part)
          result['subarea']     from h2/city line (second part if present)
          extra['listing_title'] verbatim h1 text
        """
        h1 = parser.css_first("h1")
        if h1:
            title_raw = _normalize_text(h1.text(strip=False))
            if title_raw:
                extra["listing_title"] = title_raw
                # Type-word is everything before the first comma
                if "," in title_raw:
                    type_word = title_raw.split(",", 1)[0].strip().lower()
                else:
                    type_word = title_raw.lower()
                cat = _TYPE_TO_CATEGORY.get(type_word)
                if cat:
                    result["category"] = cat
                else:
                    logger.debug(
                        f"[{self.source_domain}] unknown type-word "
                        f"{type_word!r} in h1 — category not set "
                        f"(will use og:title fallback if available)"
                    )

        # City/region line — the area block uses .geodir-category-location
        # which contains TWO <span> children: "Νικήτη," and "Σιθωνία".
        # Other ilist template layouts may use h2 directly after h1, so we
        # also try those as fallbacks.
        city_node = None
        for selector in (
            ".geodir-category-location",
            "h1 + h2",
            "h1 ~ h2",
            ".property-location",
            ".location",
            ".area-name",
        ):
            city_node = parser.css_first(selector)
            if city_node:
                break

        # Fallback: find any h2 that looks like a location (has a comma
        # and short text)
        if not city_node:
            for h2 in parser.css("h2"):
                t = _normalize_text(h2.text(strip=False))
                if t and "," in t and len(t) < 80:
                    city_node = h2
                    break

        if city_node:
            # If the location block contains span children, prefer those
            # (they keep "Νικήτη" and "Σιθωνία" cleanly separated).
            spans = city_node.css("span")
            parts: List[str] = []
            if spans:
                for sp in spans:
                    t = _normalize_text(sp.text(strip=False)).rstrip(",").strip()
                    if t:
                        parts.append(t)

            if not parts:
                # Fall back to full text + comma split
                loc_text = _normalize_text(city_node.text(strip=False))
                if loc_text:
                    parts = [p.strip() for p in loc_text.split(",") if p.strip()]

            if parts:
                result["area"] = parts[0]
                if len(parts) >= 2:
                    result["subarea"] = parts[1]
                result["location_raw"] = ", ".join(parts)

        # Fallback to og:title parsing if h1 didn't yield a category
        if "category" not in result:
            og_title_node = parser.css_first('meta[property="og:title"]')
            if og_title_node:
                og_title = og_title_node.attributes.get("content") or ""
                og_title = _normalize_text(og_title)
                m = _OG_TITLE_TYPE_RE.match(og_title)
                if m:
                    type_word = m.group(1).strip().lower()
                    cat = _TYPE_TO_CATEGORY.get(type_word)
                    if cat:
                        result["category"] = cat

    def _parse_structured_panels(
        self,
        parser: LexborHTMLParser,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """
        Parse the Basic + Extra characteristics panels.

        The ilist template renders these as `<h3>{Section Title}</h3>`
        followed by a definition-list–style block. Each row is structured
        either as:

          - <li><strong>{Label}</strong>{Value}</li>          (bullet form)
          - <p><strong>{Label}</strong><br>{Value}</p>        (paragraph form)
          - <div class="key">{Label}</div><div class="val">{Value}</div>
          - dt/dd pairs

        We use a tolerant strategy: find <strong> (or <b>, <dt>) elements
        and treat their text as the LABEL; the value is the following text
        node, sibling, or specific container — whichever yields content.

        Restricting search to elements after the section heading would be
        ideal but DOM ordering varies; instead we use a label-match
        approach: any <strong> whose text matches a known label is treated
        as a row whose value is the surrounding container's remaining text.
        """
        # Candidate label-bearing elements (strong/b/dt/.label). The ilist
        # template uses <b> in BOTH Basic and Extra panels, with slightly
        # different inner-row markup:
        #   Basic: <div class="textright"><b>Label</b><br>Value</div>
        #   Extra: <b>Label</b>: Value  (inline text node)
        # The _extract_label_value chain handles both layouts.
        for label_node in parser.css("strong, b, dt, .label, .meta-label"):
            label_raw = _normalize_text(label_node.text(strip=False))
            if not label_raw:
                continue

            # Strip trailing colons and dots
            label = label_raw.rstrip(":：.").strip()
            label_key = label.lower()
            if not label_key:
                continue

            # Find the routing target — try exact, then trimmed
            target = _LABEL_TO_FIELD.get(label_key)
            if target is None:
                # Unknown label; skip (don't pollute extras with FA icon
                # labels or other irrelevant <b> tags)
                continue

            if target == "_skip":
                continue

            # Extract the value (parent.text minus label, then sibling fallback)
            value_text = self._extract_label_value(label_node, label_raw)
            if not value_text:
                continue

            self._route_value(target, label, value_text, result, extra)

        # ─── Distance sub-block ────────────────────────────────────────
        # The "Απόσταση από" / "Distance from" label is followed by a sub-list:
        #     <strong>Distance from</strong>
        #       Sea: 100 Meters
        #       Center: 0,5 km
        # We need a separate pass to capture this because each sub-item
        # doesn't have its own label-routed entry in _LABEL_TO_FIELD.
        self._parse_distance_block(parser, extra)

    def _extract_label_value(
        self,
        label_node: LexborNode,
        label_text: str,
    ) -> str:
        """
        Given a label-bearing node (strong/b/dt), extract the associated
        value as text.

        Strategy chain:
          1. Next sibling text: <strong>Label</strong>: <value>
          2. Parent container's text minus the label text
          3. Next sibling element's text (if value is in a sibling)
        """
        # Strategy 1: parent.text() minus label
        parent = label_node.parent
        if parent is not None:
            parent_text = _normalize_text(parent.text(strip=False))
            if parent_text and parent_text != label_text:
                # Remove the label text once (label appears first in parent)
                idx = parent_text.find(label_text)
                if idx >= 0:
                    value = parent_text[idx + len(label_text):].strip()
                    # Strip leading separators
                    value = re.sub(r"^[:：.,\s-]+", "", value).strip()
                    if value:
                        return value

        # Strategy 2: next sibling text
        sibling = label_node.next
        while sibling is not None:
            if sibling.tag == "-text":
                t = _normalize_text(sibling.text())
                if t:
                    return t.lstrip(":：.,- \t")
            elif sibling.tag in ("br",):
                pass  # skip whitespace tags
            elif sibling.tag is not None:
                t = _normalize_text(sibling.text(strip=False))
                if t:
                    return t
            sibling = sibling.next

        return ""

    def _parse_distance_block(
        self,
        parser: LexborHTMLParser,
        extra: Dict[str, Any],
    ) -> None:
        """
        Parse the "Distance from" / "Απόσταση από" sub-block.

        Looks for sub-labels (Sea, Center, Airport, etc.) anywhere on
        the page after the parent label. We do a text-pattern scan
        because the markup varies between properties.
        """
        # Find the heading text
        body_text = _normalize_text(parser.text())
        if not body_text:
            return

        # Pattern: "{thing}: {N} {unit}" where thing is a distance target
        for sub_label, key in [
            ("sea", "distance_to_sea_m"),
            ("θάλασσα", "distance_to_sea_m"),
            ("beach", "distance_to_beach_m"),
            ("παραλία", "distance_to_beach_m"),
            ("airport", "distance_to_airport_m"),
            ("αεροδρόμιο", "distance_to_airport_m"),
            ("center", "distance_to_center_m"),
            ("κέντρο", "distance_to_center_m"),
            ("city", "distance_to_city_m"),
            ("πόλη", "distance_to_city_m"),
        ]:
            # Find "{sub_label}: {number} {unit}" with case-insensitive sub_label
            pattern = re.compile(
                rf"\b{re.escape(sub_label)}\s*:\s*([^,;\n]+)",
                re.IGNORECASE,
            )
            m = pattern.search(body_text)
            if m and key not in extra:
                dist_m = _extract_distance_m(m.group(1))
                if dist_m is not None and 0 < dist_m < 500_000:
                    extra[key] = dist_m

    def _route_value(
        self,
        target: str,
        label: str,
        value_raw: str,
        result: Dict[str, Any],
        extra: Dict[str, Any],
    ) -> None:
        """Type-coerce one structured-table value into result or extras."""
        if not value_raw:
            return

        if target.startswith("_extra:"):
            key = target[len("_extra:"):]
            # Bool first
            b = _interpret_bool(value_raw)
            if b is not None:
                extra[key] = b
                return
            # Numeric counters
            if key.endswith("_count"):
                n = _to_int_simple(value_raw)
                if n is not None:
                    extra[key] = n
                    return
            # Distance-suffixed
            if key.endswith("_m") or key.endswith("_km"):
                d = _extract_distance_m(value_raw)
                if d is not None:
                    extra[key] = d
                    return
            # Price-per-sqm
            if key == "price_per_sqm":
                v = _to_int_euro(value_raw)
                if v is not None:
                    extra[key] = v
                    return
            # Default: string verbatim. If the same label appears multiple
            # times (e.g. "Additional:" maps to both A/C and Window Screens),
            # concatenate values with ", " instead of overwriting.
            existing = extra.get(key)
            if isinstance(existing, str) and existing and existing != value_raw:
                # Avoid duplicating if the new value is already in the existing
                parts = [p.strip() for p in existing.split(",") if p.strip()]
                if value_raw not in parts:
                    extra[key] = f"{existing}, {value_raw}"
            else:
                extra[key] = value_raw
            return

        # Top-level columns
        if target == "price":
            v = _to_int_euro(value_raw)
            if v is not None:
                result["price"] = v
        elif target == "size_sqm":
            v = _to_float_sqm(value_raw)
            if v is not None:
                result["size_sqm"] = v
        elif target == "year_built":
            v = _to_int_simple(value_raw)
            if v is not None and 1800 <= v <= 2100:
                result["year_built"] = v
        elif target in {"bedrooms", "bathrooms"}:
            v = _to_int_simple(value_raw)
            if v is not None:
                result[target] = v
        else:
            result[target] = value_raw

    def _extract_description(
        self,
        parser: LexborHTMLParser,
    ) -> Optional[str]:
        """
        Pull the long description text.

        The ilist template typically uses one of:
          - <section id="sec3"> (the "Description" anchor section)
          - <div class="description">
          - <h3>Περιγραφή</h3> followed by a <div> or <p>

        We try each in order. The CTA paragraphs ("Email: ...", "Tel: ...",
        "Επικοινωνία: ...") are filtered out — they're contact info, not
        property description.
        """
        for selector in (
            ".list-single-main-item_content",   # ← clean description body (verified)
            "section#sec3",
            ".description",
            "#sec3",
            ".property-description",
            ".content-description",
        ):
            node = parser.css_first(selector)
            if not node:
                continue
            text = _normalize_text(node.text(separator="\n", strip=True))
            if text and len(text) >= 50:
                # Drop the section heading itself if it leaks
                text = re.sub(r"^(?:Description|Περιγραφή)\s*:?\s*", "", text, flags=re.IGNORECASE)
                # Filter out trailing CTA lines
                lines = text.split("\n")
                clean_lines = []
                for line in lines:
                    line_low = line.strip().lower()
                    if (
                        line_low.startswith(("email:", "τηλ:", "tel:",
                                             "phone:", "επικοινωνία:", "contact:"))
                    ):
                        continue
                    clean_lines.append(line)
                cleaned = "\n".join(clean_lines).strip()
                if cleaned and len(cleaned) >= 50:
                    return cleaned

        # Fallback: og:description (always available, ~200 chars)
        return None

    def _extract_coordinates(
        self,
        parser: LexborHTMLParser,
        html_text: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Try multiple coordinate sources, falling back through:
          1. <div id="map" data-lat="..." data-lng="...">
          2. <iframe src="...?q=lat,lng">
          3. Leaflet setView(...) JS pattern
          4. lat:..., lng:... JS object literal
          5. Bare 40.NNNNN, 23.NNNNN pair (Halkidiki bbox)
          6. og:geo meta tags

        All results sanity-checked against Halkidiki bbox.
        """
        # 1. data-lat / data-lng attributes
        for selector in (
            "#map[data-lat]",
            "[data-lat][data-lng]",
            ".map-canvas[data-lat]",
        ):
            node = parser.css_first(selector)
            if not node:
                continue
            try:
                lat = float(node.attributes.get("data-lat") or "")
                lng = float(node.attributes.get("data-lng") or "")
            except (ValueError, TypeError):
                continue
            if self._coords_in_bbox(lat, lng):
                return lat, lng

        # 2. Google Maps iframe
        iframe = parser.css_first("iframe[src*='maps']")
        if iframe:
            src = iframe.attributes.get("src") or ""
            m = re.search(r"q=(-?\d+\.\d+)[,%2C]+(-?\d+\.\d+)", src)
            if m:
                try:
                    lat = float(m.group(1))
                    lng = float(m.group(2))
                    if self._coords_in_bbox(lat, lng):
                        return lat, lng
                except ValueError:
                    pass

        # 3-5. Pattern scan of full HTML
        for pat in (_COORDS_SETVIEW_RE, _COORDS_LATLNG_RE, _COORDS_LATLNG_PAIR_RE):
            m = pat.search(html_text)
            if not m:
                continue
            try:
                lat = float(m.group(1))
                lng = float(m.group(2))
            except ValueError:
                continue
            if self._coords_in_bbox(lat, lng):
                return lat, lng

        # 5b. Lat / lng on separate lines (kwgreece.gr's ilist template
        # assigns them as `var lat = 40.21...; var lon = 23.67...;` —
        # the dual-pattern regexes above expect them on the same line).
        lat_m = _COORDS_LAT_ONLY_RE.search(html_text)
        lng_m = _COORDS_LNG_ONLY_RE.search(html_text)
        if lat_m and lng_m:
            try:
                lat = float(lat_m.group(1))
                lng = float(lng_m.group(1))
            except ValueError:
                lat = lng = None
            if lat is not None and lng is not None and self._coords_in_bbox(lat, lng):
                return lat, lng

        # 6. og:geo meta tags
        og_lat_node = parser.css_first('meta[property="place:location:latitude"]')
        og_lng_node = parser.css_first('meta[property="place:location:longitude"]')
        if og_lat_node and og_lng_node:
            try:
                lat = float(og_lat_node.attributes.get("content") or "")
                lng = float(og_lng_node.attributes.get("content") or "")
                if self._coords_in_bbox(lat, lng):
                    return lat, lng
            except (ValueError, TypeError):
                pass

        return None, None

    def _coords_in_bbox(self, lat: float, lng: float) -> bool:
        """Halkidiki bbox sanity check (matches halkidiki_agency)."""
        return 39.0 <= lat <= 41.5 and 22.0 <= lng <= 25.0

    def _extract_images(
        self,
        parser: LexborHTMLParser,
        html_text: str,
    ) -> List[str]:
        """
        Collect property images from the ilist CDN.

        The page renders thumbnails first, then full-size images linked
        via <a href="cdn-url">. We use a regex scan over the full HTML
        rather than DOM traversal because the gallery markup is
        non-standard (table-based) and varies between properties.

        All images come from a single CDN host pattern. The site's chrome
        images (logo, icons) come from `kwgreece.gr/images/` — easy to
        distinguish.
        """
        seen: set = set()
        images: List[str] = []

        for m in _CDN_IMAGE_RE.finditer(html_text):
            url_full = m.group(0)
            if url_full in seen:
                continue
            seen.add(url_full)
            images.append(url_full)

        return images

    def _extract_video_url(
        self,
        parser: LexborHTMLParser,
        html_text: str,
    ) -> Optional[str]:
        """
        Pull a YouTube video URL from any embedded iframe.

        The site uses /embed/{id} format which we convert to /watch?v={id}
        for storage (more share-able and a stable canonical form).
        """
        # 1. Direct iframe
        for iframe in parser.css("iframe[src]"):
            src = iframe.attributes.get("src") or ""
            m = _YOUTUBE_EMBED_RE.search(src)
            if m:
                return f"https://www.youtube.com/watch?v={m.group(1)}"

        # 2. Regex over HTML (catches lazy-loaded embeds)
        m = _YOUTUBE_EMBED_RE.search(html_text)
        if m:
            return f"https://www.youtube.com/watch?v={m.group(1)}"

        return None
