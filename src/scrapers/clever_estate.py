"""
Clever Estate scraper — clever-estate.gr.

Custom WordPress theme (wp_oikia5-v1.2), Greek-language Chalkidiki real estate
site. Tiny inventory (~132 listings across 11 pages of 12 cards), most under
our €400k threshold — production target ~5-15 properties per sync.

URL structure
=============
  Listing: /property-listing/                       (page 1)
           /property-listing/page/N/                (page 2..11)
  Detail:  /property/<greek-percent-encoded-slug>/  (canonical)
  Short:   /?p=<post_id>                            (302 → slug)

No anti-bot: curl_cffi at Stage 0 returns full HTML.
No sitemap, no REST API exposed — pagination walk is mandatory.
No min_price filter on site form — we pre-filter on card before fetch_details.

Card structure (one .item.cpt_property per property on listing)
============================================================
  <div class="item item-media post-NNNN cpt_property
              property_type-XXX property_location-XXX
              property_status-XXX [featured-property]">
      <figure class="item-thumb">
          <a href="/property/<slug>/"><img.../></a>
      </figure>
      <div class="item-content">
          <p class="item-title"><a href="/property/<slug>/">Greek title</a></p>
          <div class="item-meta">…</div>
          <div class="item-excerpt"><p>Greek 1-paragraph teaser…</p></div>
          <a class="item-more property-price">450.000€</a>   ← only when price set
      </div>
  </div>

Three flag fields are in the CSS class string — we slice them out:
  property_type-{slug}      → hodu category (Land/Apartment/House/…)
  property_location-{slug}  → Chalkidiki sub-area (Gerakini/Polygyros/…)
  property_status-{slug}    → pros-pwlhsh (sale, KEEP)
                              pros-enoikiasi (rent, SKIP)

post-NNNN → site_property_id (matches ?p=NNNN canonical short link).

Detail page structure
=====================
  <body class="single single-cpt_property postid-NNN …">
      <h1 class="page-title">Greek title</h1>
      <span class="property-price">400.000€</span>   ← primary price source
      <article class="post-NNN cpt_property property_type-XXX …">
          <figure>
              <ul class="slides">
                  <li><a href="full-1024x768.jpg" class="ci-lightbox">
                      <img src="850x530.jpg"/></a></li>
                  …
              </ul>
          </figure>
          <table class="property-overview">
              <tr><th>Κωδικός:</th><td>10009</td></tr>
              <tr><th>Εμβαδό:</th><td>2500m²</td></tr>
              <!-- other rows when populated: rooms / floor / year / heating -->
          </table>
          <div class="entry-content">
              <p>Greek description… 1-3 sentences typically.</p>
          </div>
          <div id="property_map" data-lat="40.27" data-lng="23.43"
               data-approximate="1"></div>
      </article>

Key decisions
=============
1) site_property_id from .item class "post-NNNN" — clean & stable.
2) Pre-filter price ON CARD: skip if .item-more text < €400k AND not None.
   Saves ~85% of Phase 2 fetches.
3) Category from property_type-XXX slug; some slugs are numeric (`70`,`68`)
   when the WP taxonomy term has no proper slug — we map them explicitly.
4) Location slug whitelist (18 known Chalkidiki sub-areas) → human name.
   Unknown slug → still safe; geo_matcher gets "Halkidiki" fallback.
5) Map data-approximate="1" → store gps_type="approximate" in extras
   (privacy hint, ~200m precision).
6) Greek description → handled by EnrichmentMixin NLP fallback (already
   supports Greek via shared regex pack used by kwgreece, halkidikiestate).
7) "Εμβαδό" overview row → size_sqm for buildings; for Land properties
   the same row holds plot area → routed to land_size_sqm via category check
   (same Land disambiguation pattern as mproperties).
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


# =============================================================================
# Constants
# =============================================================================

_BASE_URL = "https://clever-estate.gr"
_LISTING_PATH = "/property-listing/"
_SOURCE_DOMAIN = "clever-estate.gr"

_MAX_PAGES = 30                  # site has 11 pages today; cap for headroom
_INTER_PAGE_SLEEP_SEC = 1.5


# property_type-XXX slug → hodu canonical category.
# Numeric slugs (70, 68, 44) appear when WP term has no nice slug; we lock them
# from the form dropdown observed in the sidebar widget:
#   40 → Οικόπεδα      / oikopeda      → Land
#   42 → Αγροτεμάχια   / agrotemaxia   → Land
#   44 → Επαγγελματικοί χώροι           → Business
#   46 → Διαμερίσματα  / diamerismata  → Apartment
#   48 → Μονοκατοικίες / monokatoikies → House
#   68 → Ενοικιαζόμενα δωμάτια          → (rent-only, skipped)
#   70 → Μεζονέτες                       → Maisonette
_TYPE_SLUG_TO_CATEGORY: Dict[str, Optional[str]] = {
    "oikopeda":      "Land",
    "agrotemaxia":   "Land",
    "diamerismata":  "Apartment",
    "monokatoikies": "House",
    "70":            "Maisonette",
    "44":            "Business",
    "68":            None,            # rooms-for-rent; we skip rent statuses anyway
}

# property_location-XXX slug → human-readable area name. Whitelist of 18
# Chalkidiki sub-areas observed in the form dropdown.
_LOCATION_SLUG_TO_NAME: Dict[str, str] = {
    "gerakinh":        "Gerakini",
    "polygyros":       "Polygyros",
    "taxiarchis":      "Taxiarchis",
    "vrastama":        "Vrastama",
    "kalives":         "Kalives",
    "palaiokastro":    "Palaiokastro",
    "potidea":         "Potidea",
    "porto-koufo":     "Porto Koufo",
    "metamorfosi":     "Metamorfosi",
    "ag-nikolaos":     "Agios Nikolaos",
    "ormylia":         "Ormylia",
    "peukochori":      "Pefkochori",
    "n-moudania":      "Nea Moudania",
    "akti-elias":      "Akti Elias",
    "trikorfo":        "Trikorfo",
    "pyrgadikia":      "Pyrgadikia",
    "paralia-fourkas": "Paralia Fourkas",
    "adam":            "Adam",
}

# property_location-XXX slug → Chalkidiki municipality (calc_municipality).
# Same pattern as halkidiki_estate/greek_exclusive: pre-route at the scraper
# so geo_matcher doesn't have to disambiguate; "Gerakini" alone is ambiguous
# without the surrounding municipality.
#
# Chalkidiki municipalities and their constituent sub-areas:
#   Kassandra      — Pefkochori, Akti Elias, Paralia Fourkas (+ Kallithea,
#                    Sani, Afytos, Skioni, Paliouri etc. not on this site)
#   Sithonia       — Porto Koufo (+ Nikiti, Marmaras, Vourvourou, Sarti
#                    not on this site)
#   Nea Propontida — Nea Moudania, Potidea, Palaiokastro
#   Polygyros      — Polygyros, Gerakini, Ormylia, Taxiarchis, Vrastama,
#                    Agios Nikolaos, Pyrgadikia, Trikorfo, Metamorfosi,
#                    Kalives
#   Aristotelis    — Adam (Ouranoupoli area)
_LOCATION_SLUG_TO_MUNICIPALITY: Dict[str, str] = {
    "gerakinh":        "Polygyros",
    "polygyros":       "Polygyros",
    "taxiarchis":      "Polygyros",
    "vrastama":        "Polygyros",
    "kalives":         "Polygyros",
    "ormylia":         "Polygyros",
    "ag-nikolaos":     "Polygyros",
    "pyrgadikia":      "Polygyros",
    "trikorfo":        "Polygyros",
    "metamorfosi":     "Polygyros",
    "palaiokastro":    "Nea Propontida",
    "potidea":         "Nea Propontida",
    "n-moudania":      "Nea Propontida",
    "porto-koufo":     "Sithonia",
    "peukochori":      "Kassandra",
    "akti-elias":      "Kassandra",
    "paralia-fourkas": "Kassandra",
    "adam":            "Aristotelis",
}

# Greek labels in <table class="property-overview"> → field routing.
# Stripped of trailing colon, lowercased. Extras: most rows except size are
# usually empty/hidden in current site state, but we route what we see.
_OVERVIEW_LABEL_TO_FIELD: Dict[str, str] = {
    "κωδικός":           "site_code",       # property code shown on site (e.g. 10009)
    "εμβαδό":            "size_sqm",        # total area in m² (re-routed to land for Land category)
    "οικόπεδο":          "land_size_sqm",   # plot area in m²
    "δωμάτια":           "bedrooms",
    "υπνοδωμάτια":       "bedrooms",
    "μπάνια":            "bathrooms",
    "μπάνιο":            "bathrooms",
    "έτος κατασκευής":   "year_built",
    "όροφος":            "floor",
    "θέρμανση":          "heating",
}

# Only sale listings.
_SALE_STATUS_SLUG = "pros-pwlhsh"

# Halkidiki bbox sanity for GPS validation.
_HALKIDIKI_LAT_RANGE = (39.0, 41.0)
_HALKIDIKI_LNG_RANGE = (22.0, 24.0)

# Price floor — apply pre-fetch on listing card.
_MIN_PRICE_DEFAULT = 400_000


# =============================================================================
# Helpers
# =============================================================================

def _to_int_euro_eu(text: str) -> Optional[int]:
    """
    Parse EU price string into integer euros.

      "450.000€"      -> 450000
      "1.000.000 €"   -> 1000000
      "320 € / μήνα"  -> 320      (we accept; caller filters)
      ""              -> None
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


def _to_int_simple(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else None


def _to_float_sqm(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"\d+(?:[.,]\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", "."))
    except ValueError:
        return None


def _extract_class_slug(class_str: str, prefix: str) -> Optional[str]:
    """
    Pull e.g. "polygyros" out of "...item property_location-polygyros featured...".
    Returns None if prefix-slug isn't in the class list.
    """
    if not class_str:
        return None
    needle = f"{prefix}-"
    for tok in class_str.split():
        if tok.startswith(needle):
            return tok[len(needle):]
    return None


def _extract_post_id(class_str: str) -> Optional[str]:
    """Pull "2760" out of "item post-2760 cpt_property...".

    Returns None when no "post-{digits}" token present.
    """
    if not class_str:
        return None
    for tok in class_str.split():
        if tok.startswith("post-") and tok[5:].isdigit():
            return tok[5:]
    return None


def _build_location_raw(location_name: Optional[str]) -> str:
    """Always force Halkidiki in the raw location string so geo whitelist hits."""
    if not location_name:
        return "Halkidiki"
    return f"{location_name}, Halkidiki"


# =============================================================================
# Scraper
# =============================================================================

class CleverEstateScraper(EnrichmentMixin, BaseScraper):
    """
    clever-estate.gr — small Greek WordPress site, Chalkidiki only.

    Phase 1 walks /property-listing/ pages 1..N, pre-filtering cards by
    visible price (cuts ~85% of fetches). Phase 2 reads the canonical
    Greek detail page for each surviving card.
    """

    _NLP_FILLABLE_COLUMNS = (
        "size_sqm",
        "land_size_sqm",
        "bedrooms",
        "bathrooms",
        "year_built",
        "levels",
    )

    BASE_URL = _BASE_URL

    def __init__(self) -> None:
        super().__init__()
        self.source_domain = _SOURCE_DOMAIN

    async def fetch_listings(self):
        return await self.collect_urls()

    # ---------------------------------------------------------------
    # URL builders
    # ---------------------------------------------------------------

    def _build_list_url(self, page: int) -> str:
        if page <= 1:
            return f"{_BASE_URL}{_LISTING_PATH}"
        return f"{_BASE_URL}{_LISTING_PATH}page/{page}/"

    def _construct_detail_url(self, post_id: str) -> str:
        """Short URL — server 302s to canonical slug URL on fetch."""
        return f"{_BASE_URL}/?p={post_id}"

    # ---------------------------------------------------------------
    # PHASE 1 — collect URLs (with card-level price pre-filter)
    # ---------------------------------------------------------------

    async def collect_urls(
        self,
        min_price: int = _MIN_PRICE_DEFAULT,
    ) -> List[PropertyTemplate]:
        """
        Walk listing pages, drop rent-only and below-min-price cards.

        Stop conditions:
          1. Page returns no .item.cpt_property cards
          2. Same set of post-IDs as previous page (server fallback to page 1)
          3. _MAX_PAGES exceeded
        """
        all_props: List[PropertyTemplate] = []
        seen_post_ids: set[str] = set()
        page = 1

        while page <= _MAX_PAGES:
            url = self._build_list_url(page)
            logger.info(f"[{self.source_domain}] page {page}: {url}")

            try:
                response = await self.client.get(url)
                parser = LexborHTMLParser(response.text)
                cards = parser.css(".item.cpt_property")

                if not cards:
                    logger.info(
                        f"[{self.source_domain}] no cards on page {page}, "
                        f"stopping"
                    )
                    break

                page_collected = 0
                page_skipped_rent = 0
                page_skipped_price = 0
                page_new_post_ids = 0  # any post-id we haven't seen before
                page_dup_post_ids = 0  # post-id already in seen set

                for card in cards:
                    try:
                        # 1. Duplicate detection: read post-id BEFORE parsing
                        # so we always record it (even if card gets filtered).
                        # Server may loop back to page 1 contents on overflow.
                        cls = card.attributes.get("class", "") or ""
                        pid = _extract_post_id(cls)
                        if pid:
                            if pid in seen_post_ids:
                                page_dup_post_ids += 1
                                continue
                            seen_post_ids.add(pid)
                            page_new_post_ids += 1

                        # 2. Now the actual parse / filter logic
                        result = self._parse_card(card, min_price)
                        if result is None:
                            continue
                        prop, skip_reason = result
                        if prop is not None:
                            all_props.append(prop)
                            page_collected += 1
                        elif skip_reason == "rent":
                            page_skipped_rent += 1
                        elif skip_reason == "below_min_price":
                            page_skipped_price += 1
                    except Exception as e:
                        logger.error(
                            f"[{self.source_domain}] card parse error: {e}"
                        )

                logger.info(
                    f"[{self.source_domain}] page {page}: "
                    f"+{page_collected} kept "
                    f"(skipped {page_skipped_rent} rent, "
                    f"{page_skipped_price} below €{min_price:,}; "
                    f"{page_new_post_ids} unique post-ids, "
                    f"{page_dup_post_ids} duplicates)"
                )

                # Server-loop detector: if EVERY post-id on this page was
                # already seen (duplicate), server is recycling page 1.
                # Tolerate single dup (rare race) — only stop when whole page
                # is dup.
                if (page > 1
                        and page_new_post_ids == 0
                        and page_dup_post_ids > 0):
                    logger.info(
                        f"[{self.source_domain}] page {page} returned only "
                        f"already-seen post-ids ({page_dup_post_ids} dups, "
                        f"0 new), stopping"
                    )
                    break

                await asyncio.sleep(_INTER_PAGE_SLEEP_SEC)
                page += 1

            except Exception as e:
                logger.error(
                    f"[{self.source_domain}] page {page} critical: {e}"
                )
                break

        logger.info(
            f"[{self.source_domain}] Phase 1 done: "
            f"{len(all_props)} URLs across {page - 1} pages"
        )
        return all_props

    def _parse_card(
        self,
        card: LexborNode,
        min_price: int,
    ) -> Optional[Tuple[Optional[PropertyTemplate], Optional[str]]]:
        """
        Return:
          (PropertyTemplate, None)              — kept
          (None, "rent" | "below_min_price")    — skipped
          None                                  — malformed (skipped silently)
        """
        cls = card.attributes.get("class", "") or ""

        # 1. site_property_id from "post-NNNN"
        post_id = _extract_post_id(cls)
        if not post_id:
            return None  # malformed card

        # 2. Status — keep sale only
        status_slug = _extract_class_slug(cls, "property_status")
        if status_slug and status_slug != _SALE_STATUS_SLUG:
            return (None, "rent")

        # 3. Detail URL — from .item-title <a> or figure <a>
        detail_url: Optional[str] = None
        title_a = card.css_first(".item-title a")
        if title_a:
            detail_url = title_a.attributes.get("href")
        if not detail_url:
            fig_a = card.css_first(".item-thumb a")
            if fig_a:
                detail_url = fig_a.attributes.get("href")
        # Fallback: synthesize from post_id (server will redirect)
        if not detail_url:
            detail_url = self._construct_detail_url(post_id)

        # 4. Price from .item-more.property-price
        price_text = None
        price_int = None
        more = card.css_first(".item-more.property-price")
        if more:
            price_text = more.text(strip=True)
            price_int = _to_int_euro_eu(price_text)

        # Card pre-filter: skip below-min-price ONLY if price is known.
        # Cards without price flow to Phase 2 (we fetch detail to learn).
        if price_int is not None and price_int < min_price:
            return (None, "below_min_price")

        # 5. Category from property_type-{slug}
        type_slug = _extract_class_slug(cls, "property_type")
        category = _TYPE_SLUG_TO_CATEGORY.get(type_slug) if type_slug else None

        # 6. Location from property_location-{slug}
        loc_slug = _extract_class_slug(cls, "property_location")
        location_name = _LOCATION_SLUG_TO_NAME.get(loc_slug) if loc_slug else None
        location_raw = _build_location_raw(location_name)

        # 7. Title (Greek) from .item-title
        title_el = card.css_first(".item-title")
        title = title_el.text(strip=True) if title_el else ""

        prop = PropertyTemplate(
            site_property_id=post_id,
            source_domain=self.source_domain,
            url=detail_url,
            price=price_text,
            location_raw=location_raw,
            category=category,
        )
        return (prop, None)

    # ---------------------------------------------------------------
    # PHASE 2 — fetch full details
    # ---------------------------------------------------------------

    async def fetch_details(self, url: str) -> Dict[str, Any]:
        """
        Read canonical Greek detail page and extract:
          - .page-hero .property-price (main price)
          - .property-overview <th>/<td> rows
          - .entry-content <p> description
          - .ci-slider .slides li a[href] gallery
          - #property_map data-lat/data-lng
          - body class for location/category/post-id (defensive re-derivation)
        """
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)
            raw_html = response.text

            data: Dict[str, Any] = {
                "description":     "",
                "price":           None,
                "size_sqm":        None,
                "land_size_sqm":   None,
                "bedrooms":        None,
                "bathrooms":       None,
                "year_built":      None,
                "area":            None,
                "subarea":         None,
                "category":        None,
                "calc_municipality": None,
                "levels":          None,
                "latitude":        None,
                "longitude":       None,
                "images":          [],
                "extra_features":  {},
            }

            # 1. Article class — derives category/location/municipality/post_id.
            # NOTE: body class on detail page only has "postid-NNN" and theme
            # classes. The property_type-XXX/property_location-XXX classes
            # live on the <article class="...cpt_property..."> element.
            article = parser.css_first("article.cpt_property")
            article_cls = article.attributes.get("class", "") if article else ""
            type_slug = _extract_class_slug(article_cls, "property_type")
            if type_slug:
                cat = _TYPE_SLUG_TO_CATEGORY.get(type_slug)
                if cat:
                    data["category"] = cat
            loc_slug = _extract_class_slug(article_cls, "property_location")
            if loc_slug:
                loc_name = _LOCATION_SLUG_TO_NAME.get(loc_slug)
                if loc_name and data.get("area") is None:
                    data["area"] = loc_name
                # calc_municipality routing — same canonical pattern used by
                # halkidiki_estate / greek_exclusive. Pre-routes at scraper
                # level so geo_matcher gets unambiguous input.
                municipality = _LOCATION_SLUG_TO_MUNICIPALITY.get(loc_slug)
                if municipality:
                    data["calc_municipality"] = municipality

            # 2. Price from .page-hero .property-price (or .property-meta)
            self._parse_price(parser, data)

            # 3. Property overview table (Κωδικός, Εμβαδό, …)
            self._parse_overview_table(parser, data)

            # 4. Description
            data["description"] = self._parse_description(parser)

            # 5. Images
            data["images"] = self._collect_image_urls(parser)

            # 6. GPS
            self._parse_coordinates(parser, data)

            # 7. NLP fallback (EnrichmentMixin)
            self._apply_nlp_fallback(data)

            # 8. Quality gate log-only
            if not self._passes_quality_gate(data.get("description")):
                logger.warning(
                    f"[{self.source_domain}] description below quality gate "
                    f"for {url}"
                )

            return {k: v for k, v in data.items() if v is not None}

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] fetch_details error for {url}: {e}"
            )
            return {}

    # ---------------------------------------------------------------
    # Phase 2 sub-parsers
    # ---------------------------------------------------------------

    def _parse_price(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """Price is in .page-hero .property-price (e.g. "400.000€")."""
        for selector in (
            ".page-hero .property-price",
            ".property-meta .property-price",
            ".property-price",
        ):
            el = parser.css_first(selector)
            if not el:
                continue
            text = el.text(strip=True)
            v = _to_int_euro_eu(text)
            if v is not None:
                data["price"] = v
                return

    def _parse_overview_table(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        .property-overview tbody tr structure:
          <tr><th>Κωδικός:</th><td>10009</td></tr>
          <tr><th>Εμβαδό:</th><td>2500m²</td></tr>

        Label routing per _OVERVIEW_LABEL_TO_FIELD. For Land category, the
        Εμβαδό row holds plot area → reroute to land_size_sqm.
        """
        table = parser.css_first(".property-overview")
        if not table:
            return
        for tr in table.css("tr"):
            th = tr.css_first("th")
            td = tr.css_first("td")
            if not th or not td:
                continue
            label = th.text(strip=True).rstrip(":").strip().lower()
            value = td.text(strip=True)
            if not label or not value:
                continue
            field = _OVERVIEW_LABEL_TO_FIELD.get(label)
            if field == "size_sqm":
                # Land disambiguation: "Εμβαδό" on plot listings = plot size
                if data.get("category") == "Land":
                    sqm = _to_float_sqm(value)
                    if sqm is not None and data.get("land_size_sqm") is None:
                        data["land_size_sqm"] = sqm
                else:
                    sqm = _to_float_sqm(value)
                    if sqm is not None and data.get("size_sqm") is None:
                        data["size_sqm"] = sqm
            elif field == "land_size_sqm":
                sqm = _to_float_sqm(value)
                if sqm is not None:
                    data["land_size_sqm"] = sqm
            elif field == "bedrooms":
                n = _to_int_simple(value)
                if n is not None:
                    data["bedrooms"] = n
            elif field == "bathrooms":
                n = _to_int_simple(value)
                if n is not None:
                    data["bathrooms"] = n
            elif field == "year_built":
                n = _to_int_simple(value)
                if n is not None and 1900 < n < 2100:
                    data["year_built"] = n
            elif field == "floor":
                data["extra_features"]["floor"] = value
            elif field == "heating":
                data["extra_features"]["heating"] = value
            elif field == "site_code":
                # Store as extra_features so we keep the agency's own code
                data["extra_features"]["site_code"] = value

    def _parse_description(self, parser: LexborHTMLParser) -> str:
        """
        Description from <p> inside .entry-content (typically one Greek
        paragraph, 1-3 sentences). Falls back to og:description.
        """
        block = parser.css_first(".entry-content")
        if block:
            paragraphs = []
            for p in block.css("p"):
                txt = p.text(separator=" ", strip=True)
                if txt and len(txt) >= 30 and txt not in paragraphs:
                    paragraphs.append(txt)
            if paragraphs:
                return "\n\n".join(paragraphs)
        return self._og_description_fallback(parser)

    def _collect_image_urls(self, parser: LexborHTMLParser) -> List[str]:
        """
        Gallery from .ci-slider .slides li a[href] — these are full-res
        lightbox links (1024x768 typical). Dedup by URL.
        """
        photos: List[str] = []
        for a in parser.css(".ci-slider .slides li a.ci-lightbox"):
            href = a.attributes.get("href", "") or ""
            if href and re.search(r"\.(jpe?g|png|webp)(\?|$)", href, re.IGNORECASE):
                if href not in photos:
                    photos.append(href)

        # Fallback: og:image
        if not photos:
            og = parser.css_first('meta[property="og:image"]')
            if og:
                href = (og.attributes.get("content") or "").strip()
                if href and re.search(r"\.(jpe?g|png|webp)(\?|$)", href, re.IGNORECASE):
                    photos.append(href)

        return photos

    def _parse_coordinates(
        self,
        parser: LexborHTMLParser,
        data: Dict[str, Any],
    ) -> None:
        """
        <div id="property_map" data-lat="40.27" data-lng="23.43"
             data-approximate="1">
        data-approximate="1" → privacy obfuscation marker, ~200m precision.
        """
        m = parser.css_first("#property_map")
        if not m:
            return
        try:
            lat = float(m.attributes.get("data-lat") or "")
            lng = float(m.attributes.get("data-lng") or "")
            if (_HALKIDIKI_LAT_RANGE[0] <= lat <= _HALKIDIKI_LAT_RANGE[1]
                    and _HALKIDIKI_LNG_RANGE[0] <= lng <= _HALKIDIKI_LNG_RANGE[1]):
                data["latitude"] = lat
                data["longitude"] = lng
        except (ValueError, TypeError):
            return

        approx = m.attributes.get("data-approximate") or ""
        if approx == "1":
            data["extra_features"]["gps_type"] = "approximate"
