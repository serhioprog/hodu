"""
Greek Exclusive Properties scraper.

Hybrid extraction strategy:
  1. STRUCTURAL baseline (free, deterministic) — header bar icons,
     address/location, gallery images, coordinates, ID, land size.
  2. FULL DESCRIPTION = body content text from `.wpb_text_column`
     (Premium villas) or `.content.clearfix` (legacy/hotel format).
     This is ~10x longer than og:description and feeds embedding
     similarity, Vision tiebreaker, and the regex+LLM extractors.
  3. REGEX extraction (free) — DataExtractor + extraction_dictionary.py
     pulls amenities and metrics from the description.
  4. LLM fallback ($0.0006/call) — gpt-4o-mini with Pydantic structured
     output. Triggered ONLY when regex yields fewer than
     settings.LLM_EXTRACTION_MIN_REGEX_FEATURES amenities. Kill-switched
     by settings.LLM_EXTRACTION_ENABLED.

Working logic preserved verbatim from previous revision:
  • collect_urls — pagination, card selectors, listing-card metrics
  • _clean_price, _extract_number — value parsers
  • _extract_coordinates — 3-tier fallback (marker → JS regex → bbox)
  • Address → calc_municipality routing for Halkidiki sub-areas
  • Image gallery extraction (4 selector chains + og:image)
  • DOM price fallbacks (.detail h5.price → general scan)
  • DataExtractor.analyze_full_text for amenities

Author trail: original by ssukh (regex-only), restored & extended 2026-05-02.
"""
import re
import asyncio
from typing import Optional

from loguru import logger
from selectolax.lexbor import LexborHTMLParser

from src.scrapers.base import BaseScraper
from src.models.schemas import PropertyTemplate
from src.models.ai_schemas import GreekPropertyExtraction
from src.services.llm_extractor import LLMExtractor
from src.core.config import settings


class GreekExclusiveScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.source_domain = "greekexclusiveproperties.com"
        self.base_url = "https://www.greekexclusiveproperties.com/property-search/"
        # Reused for every fetch_details call. The service is stateless
        # apart from the OpenAI client, so a single instance is fine.
        self._llm_extractor = LLMExtractor()

    # ==========================================================
    # HELPERS (preserved verbatim — these have proven reliable)
    # ==========================================================

    def _clean_price(self, text: str) -> Optional[float]:
        """Умная очистка цены: убирает копейки и мусор, оставляет только евро."""
        if not text:
            return None
        match = re.search(r'([\d.,]+)', text)
        if match:
            raw_num = match.group(1)
            if raw_num.endswith(',00') or raw_num.endswith('.00'):
                raw_num = raw_num[:-3]
            clean_num = re.sub(r'[.,]', '', raw_num)
            try:
                return float(clean_num)
            except ValueError:
                return None
        return None

    def _extract_number(self, text: str) -> Optional[float]:
        if not text:
            return None
        match = re.search(r'([\d.]+)', text.replace(',', ''))
        return float(match.group(1)) if match else None

    def _extract_coordinates(self, html_text: str, parser: LexborHTMLParser):
        """
        Бронебойный метод перехвата координат до выполнения JavaScript.
        Three-tier fallback chain:
          1. <img class="leaflet-marker-icon" alt="... in 40.21,23.77">
          2. JS regex matching Halkidiki bounding box (lat 39-41, lng 22-24)
          3. (further fallbacks in fetch_details from data-attrs / setView)
        """
        # 1. Из marker-картинки (если страница вдруг закэширована сервером)
        marker = parser.css_first(".leaflet-marker-icon")
        if marker:
            alt = marker.attributes.get("alt", "")
            match = re.search(r'in\s+([\d.]+),([\d.]+)', alt)
            if match:
                return float(match.group(1)), float(match.group(2))

        # 2. SMART REGEX: координаты в JS переменных или массивах [40.21..., 23.77...]
        # Халкидики всегда: Широта 39-41, Долгота 22-24
        match = re.search(
            r'((?:39|40|41)\.\d{4,})\s*[,|]\s*((?:22|23|24)\.\d{4,})',
            html_text,
        )
        if match:
            return float(match.group(1)), float(match.group(2))

        return None, None

    # ==========================================================
    # COLLECT URLS — paginated listing scrape (preserved)
    # ==========================================================
    async def collect_urls(self, min_price: int = 400000) -> list[PropertyTemplate]:
        """
        Walks property-search pagination, extracts cards into PropertyTemplate
        seeds (id, url, price, location_raw, size, beds, baths). Details are
        filled later by fetch_details on the per-property URL.
        """
        all_properties: list[PropertyTemplate] = []
        page = 1

        while True:
            logger.info(f"[{self.source_domain}] Парсинг страницы {page}...")

            if page == 1:
                url = (
                    f"{self.base_url}?status[]=houses-for-sale"
                    f"&location[]=halkidiki&min-price={min_price}"
                )
            else:
                url = (
                    f"{self.base_url}page/{page}/?status[0]=houses-for-sale"
                    f"&location[0]=halkidiki&min-price={min_price}"
                )

            try:
                response = await self.client.get(url)
                if response.status_code == 404:
                    break

                parser = LexborHTMLParser(response.text)
                cards = parser.css("article.property-item")

                if not cards:
                    break

                for card in cards:
                    try:
                        link_node = card.css_first("h4 a")
                        if not link_node:
                            continue
                        href = link_node.attributes.get("href")
                        title_text = link_node.text(strip=True)

                        # Best-effort location from card title
                        location_raw = title_text
                        if " in " in title_text.lower():
                            location_raw = title_text.split(" in ", 1)[-1].strip()

                        price_node = card.css_first(".detail h5.price")
                        price_val = (
                            self._clean_price(price_node.text())
                            if price_node else None
                        )

                        article_classes = card.attributes.get("class", "")
                        id_match = re.search(r'post-(\d+)', article_classes)
                        site_id = (
                            id_match.group(1) if id_match
                            else href.strip('/').split('/')[-1]
                        )

                        size_node = card.css_first(".property-meta-size")
                        bed_node = card.css_first(".property-meta-bedrooms")
                        bath_node = card.css_first(".property-meta-bath")

                        prop_data = PropertyTemplate(
                            site_property_id=site_id,
                            source_domain=self.source_domain,
                            url=href,
                            price=price_val,
                            location_raw=location_raw,
                            size_sqm=(
                                self._extract_number(size_node.text())
                                if size_node else None
                            ),
                            bedrooms=(
                                self._extract_number(bed_node.text())
                                if bed_node else None
                            ),
                            bathrooms=(
                                self._extract_number(bath_node.text())
                                if bath_node else None
                            ),
                        )
                        all_properties.append(prop_data)

                    except Exception as e:
                        logger.error(f"[{self.source_domain}] Ошибка карточки: {e}")

                await asyncio.sleep(2)
                page += 1

            except Exception as e:
                logger.error(f"[{self.source_domain}] Ошибка пагинации: {e}")
                break

        return all_properties

    # ==========================================================
    # FULL DESCRIPTION EXTRACTION (NEW)
    # ==========================================================
    # Marketing/footer noise — Greek Exclusive embeds these widgets in
    # `.wpb_text_column` blocks the same as the property body, so length
    # alone cannot distinguish the real description from the footer.
    _NOISE_MARKERS = (
        '"@context"',          # JSON-LD video schema
        "videoobject",         # Schema.org type
        "grex-premium",        # Their internal-links CSS block
        "internal links block",
        "related searches",
        "youtu",               # YouTube embed labels
        ".grex-",              # CSS selector text
        "embedurl",
    )

    # Property-signal keywords — the more present, the more likely this
    # block is the actual property description. Used as primary score.
    _SIGNAL_KEYWORDS = (
        "bedroom", "bathroom", "year built", "year of construction",
        "pool", "garden", "sea view", "fireplace", "kitchen",
        "villa", "land plot", "living area", "floor", "level",
        "balcony", "terrace", "highlights", "property overview",
        "interior layout", "outdoor", "beachfront", "facilities",
    )

    @classmethod
    def _extract_full_description(cls, parser: LexborHTMLParser) -> str:
        """
        Pull the full body content text — replacing the ~200-char og:description
        with the actual 1500-3000 char article. This is critical for:
          • Quality Gate (description ≥ 50 chars)
          • Embedding similarity in MDM (richer text → better matching)
          • Regex amenity extraction (more patterns to match against)
          • LLM fallback (model needs context, not just a tagline)

        Container priority:
          1. `.wpb_text_column`  — modern WPBakery widget (Type 1 + Type 2)
          2. `.content.clearfix` — legacy / hotel-format pages (Type 3)
          3. `<article>`         — last-resort fallback

        Block selection (CRITICAL): Greek Exclusive pages contain MULTIPLE
        `.wpb_text_column` blocks. Some are sidebars / video embeds /
        related-searches widgets that are LARGER than the actual property
        body. Picking by raw length picks the wrong block.

        Strategy: filter out blocks containing known marketing markers,
        then score remaining blocks by property-keyword density. Tiebreak
        by length. The right block always wins because the marketing
        widgets share no real-estate keywords with the property text.
        """
        wpb_blocks: list[tuple[int, int, str]] = []  # (signal, length, text)

        for el in parser.css(".wpb_text_column"):
            txt = el.text(separator="\n", strip=True)
            if len(txt) < 200:
                continue

            lower = txt.lower()
            # Hard reject: marketing/footer markers
            if any(noise in lower for noise in cls._NOISE_MARKERS):
                continue

            signal = sum(1 for kw in cls._SIGNAL_KEYWORDS if kw in lower)
            wpb_blocks.append((signal, len(txt), txt))

        if wpb_blocks:
            # Pick highest signal first, then longest as tiebreaker.
            wpb_blocks.sort(key=lambda x: (x[0], x[1]), reverse=True)
            return wpb_blocks[0][2]

        # Hotel / legacy: content lives in .content.clearfix as flat <p>'s
        legacy = parser.css_first(".content.clearfix")
        if legacy:
            txt = legacy.text(separator="\n", strip=True)
            if len(txt) >= 100:
                return txt

        # Last-resort: largest article on the page
        articles = parser.css("article")
        article_texts = [a.text(separator="\n", strip=True) for a in articles]
        article_texts = [t for t in article_texts if len(t) >= 100]
        if article_texts:
            return max(article_texts, key=len)

        return ""

    # ==========================================================
    # FETCH DETAILS — main deep-parse pipeline
    # ==========================================================
    async def fetch_details(self, url: str) -> dict:
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)

            details: dict = {
                "description": "",
                "price": None, "size_sqm": None, "land_size_sqm": None,
                "bedrooms": None, "bathrooms": None, "year_built": None,
                "area": None, "subarea": None, "category": None,
                "levels": None, "site_last_updated": None,
                "latitude": None, "longitude": None,
                "images": [],
                "extra_features": {},
            }

            # ──────────────────────────────────────────────
            # 1. ADDRESS + calc_municipality routing (preserved)
            # ──────────────────────────────────────────────
            address_node = parser.css_first("address.title")
            if address_node:
                raw_addr = address_node.text(strip=True)
                details["location_raw"] = raw_addr
                lower_addr = raw_addr.lower()

                if any(x in lower_addr for x in [
                    "kassandra", "chanioti", "hanioti", "pefkochori",
                    "polychrono", "kallithea", "afytos", "sani", "fourka",
                    "skioni", "paliouri", "kalandra", "siviri",
                ]):
                    details["calc_municipality"] = "Kassandra"
                elif any(x in lower_addr for x in [
                    "sithonia", "nikiti", "marmaras", "vourvourou",
                    "toroni", "sarti", "kalamitsi", "porto koufo",
                ]):
                    details["calc_municipality"] = "Sithonia"
                elif any(x in lower_addr for x in [
                    "propontida", "moudania", "kallikrat", "flogita",
                    "triglia", "sozopoli",
                ]):
                    details["calc_municipality"] = "Nea Propontida"
                elif any(x in lower_addr for x in [
                    "aristotelis", "ierissos", "ouranoupoli", "nea roda",
                    "amouliani", "olympiada",
                ]):
                    details["calc_municipality"] = "Aristotelis"
                elif any(x in lower_addr for x in [
                    "polygyros", "gerakini", "psakoudia", "ormylia",
                ]):
                    details["calc_municipality"] = "Polygyros"

                parts = [p.strip() for p in raw_addr.split(",")]
                if parts and parts[0].lower() not in [
                    "halkidiki", "chalkidiki", "greece"
                ]:
                    details["area"] = parts[0]

            # ──────────────────────────────────────────────
            # 2. SITE PROPERTY ID + LAND SIZE (preserved)
            # ──────────────────────────────────────────────
            id_node = parser.css_first(".property-meta-id")
            if id_node:
                details["site_property_id"] = id_node.text(strip=True)

            land_node = parser.css_first(".property-meta-lot-size")
            if land_node:
                details["land_size_sqm"] = self._extract_number(land_node.text())

            # ──────────────────────────────────────────────
            # 3. COORDINATES — chained fallbacks (preserved)
            # ──────────────────────────────────────────────
            details["latitude"], details["longitude"] = self._extract_coordinates(
                response.text, parser
            )

            if not details.get("latitude"):
                map_node = (
                    parser.css_first("#property-map")
                    or parser.css_first("[data-lat]")
                )
                if map_node:
                    lat = map_node.attributes.get("data-lat")
                    lng = map_node.attributes.get("data-lng")
                    if lat and lng:
                        try:
                            details["latitude"] = float(lat)
                            details["longitude"] = float(lng)
                        except ValueError:
                            pass

            if not details.get("latitude"):
                script_match = re.search(
                    r'setView\(\[\s*([0-9.]+)\s*,\s*([0-9.]+)\s*\]',
                    response.text,
                )
                if script_match:
                    details["latitude"] = float(script_match.group(1))
                    details["longitude"] = float(script_match.group(2))

            # ──────────────────────────────────────────────
            # 4. PRICE — DOM fallback chain (preserved)
            # ──────────────────────────────────────────────
            if not details.get("price"):
                price_node = (
                    parser.css_first(".detail h5.price")
                    or parser.css_first(".price")
                )
                if price_node:
                    details["price"] = self._clean_price(price_node.text())

                # Last-resort sweep — find any element containing € + digits
                if not details.get("price"):
                    for el in parser.css("h3, h4, h5, span, strong, div"):
                        text = el.text(strip=True).lower()
                        if "€" in text or (
                            "price" in text and any(c.isdigit() for c in text)
                        ):
                            extracted = self._clean_price(text)
                            if extracted:
                                details["price"] = extracted
                                break

            # ──────────────────────────────────────────────
            # 5. IMAGES — all 4 selector chains preserved
            # ──────────────────────────────────────────────
            raw_images: list[str] = []

            # Chain 1: explicit gallery (data-fancybox)
            for a_node in parser.css('a[data-fancybox="gallery-images"]'):
                href = a_node.attributes.get("href")
                if href:
                    raw_images.append(href)

            # Chain 2: slides with link
            if not raw_images:
                for a_tag in parser.css(".slides li a"):
                    href = a_tag.attributes.get("href")
                    if href and href.lower().endswith(
                        (".jpg", ".jpeg", ".png", ".webp")
                    ):
                        raw_images.append(href)

            # Chain 3: slides with img — strip thumbnail dimensions
            if not raw_images:
                for img_tag in parser.css(".slides li img"):
                    src = (
                        img_tag.attributes.get("data-src")
                        or img_tag.attributes.get("src")
                    )
                    if src:
                        # WordPress generates "-300x200" suffixes; remove
                        # them to get the original full-resolution image.
                        high_res_src = re.sub(
                            r'-\d+x\d+(\.[a-zA-Z0-9]+)$',
                            r'.\1',
                            src,
                            flags=re.IGNORECASE,
                        )
                        raw_images.append(high_res_src)

            # Chain 4: featured image
            if not raw_images:
                single_img = parser.css_first(
                    "div.property-featured-image img"
                )
                if single_img:
                    src = (
                        single_img.attributes.get("data-src")
                        or single_img.attributes.get("src")
                    )
                    if src:
                        raw_images.append(src)

            # Last-resort sweep: any /uploads/ image that isn't a logo
            if not raw_images:
                for img_node in parser.css("img"):
                    src = img_node.attributes.get("src", "")
                    if (
                        src
                        and "/uploads/" in src.lower()
                        and "logo" not in src.lower()
                    ):
                        raw_images.append(src)

            # Normalise + dedupe
            for src in raw_images:
                if not src:
                    continue
                src = src.strip()
                if src.startswith("//"):
                    src = "https:" + src
                if src not in details["images"] and not src.endswith(".svg"):
                    details["images"].append(src)

            # og:image — high quality, prepend to top of gallery
            og_img = parser.css_first('meta[property="og:image"]')
            if og_img and og_img.attributes.get("content"):
                img_url = og_img.attributes["content"]
                if img_url not in details["images"]:
                    details["images"].insert(0, img_url)

            # ──────────────────────────────────────────────
            # 6. FULL DESCRIPTION (NEW — replaces og:description)
            # ──────────────────────────────────────────────
            full_description = self._extract_full_description(parser)

            if full_description and len(full_description) >= 100:
                details["description"] = full_description
            else:
                # Fall back to og:description if main content extraction
                # failed (rare — page structure change). Better than empty.
                og_desc = parser.css_first('meta[property="og:description"]')
                if og_desc and og_desc.attributes.get("content"):
                    details["description"] = og_desc.attributes["content"]

            # ──────────────────────────────────────────────
            # 7. REGEX EXTRACTION via DataExtractor (existing)
            # ──────────────────────────────────────────────
            # Run the (now expanded) regex dictionary against the full
            # description. It returns top-level metric keys + an
            # `extra_features` sub-dict.
            smart_data = self.extractor.analyze_full_text(
                details["description"]
            )

            # These keys map to Property *columns* — only fill if empty.
            COLUMN_KEYS = {
                "size_sqm", "land_size_sqm", "bedrooms", "bathrooms",
                "year_built", "levels", "category", "site_property_id",
            }

            # These extended numeric metrics belong in extra_features
            # (no dedicated Property column exists for them).
            EXTRA_NUMERIC_KEYS = {
                "pool_size_sqm", "parking_count", "elevator_count",
                "buildings_count", "rooms_count", "beds_count",
                "renovation_year", "distance_to_sea",
                "living_rooms_count", "kitchens_count",
            }

            for key, value in smart_data.items():
                if value is None:
                    continue
                if key == "extra_features":
                    details["extra_features"].update(value)
                elif key in EXTRA_NUMERIC_KEYS:
                    # Push numeric metric into extra_features as integer
                    try:
                        details["extra_features"][key] = int(float(value))
                    except (TypeError, ValueError):
                        pass
                elif key in COLUMN_KEYS and not details.get(key):
                    # Defensive: prose like "covers 3500 sqm of land" causes
                    # the size_sqm regex to capture the same value as
                    # land_size_sqm. When that happens we have no real
                    # size_sqm signal in the description — better to leave
                    # it None so the value collected from the listing card
                    # in collect_urls (which IS the building footprint)
                    # remains authoritative.
                    if key == "size_sqm" and value == details.get("land_size_sqm"):
                        continue
                    details[key] = value

            # ──────────────────────────────────────────────
            # 8. LLM FALLBACK — only if regex coverage is thin
            # ──────────────────────────────────────────────
            #
            # Trigger conditions (ALL must hold):
            #   - kill-switch enabled
            #   - description has enough material to be worth a call
            #   - regex pulled fewer than the configured threshold
            #
            # On success: we MERGE only NEW keys — regex wins on overlap
            # because it's deterministic and traceable. LLM only fills
            # the gaps it finds.
            should_run_llm = (
                self._llm_extractor.enabled
                and len(details["description"]) >= 200
                and len(details["extra_features"])
                    < settings.LLM_EXTRACTION_MIN_REGEX_FEATURES
            )

            if should_run_llm:
                ctx = (
                    f"{self.source_domain}/"
                    f"{details.get('site_property_id', '?')}"
                )
                logger.info(
                    f"[{self.source_domain}] LLM fallback fired ({ctx}): "
                    f"regex gave {len(details['extra_features'])} features"
                )
                llm_result = await self._llm_extractor.extract(
                    description=details["description"],
                    schema=GreekPropertyExtraction,
                    context=ctx,
                )
                if llm_result is not None:
                    llm_dict = llm_result.to_extra_features()
                    added = 0
                    for k, v in llm_dict.items():
                        if k not in details["extra_features"]:
                            details["extra_features"][k] = v
                            added += 1
                    logger.info(
                        f"[{self.source_domain}] LLM added {added} new features "
                        f"({ctx})"
                    )

            return details

        except Exception as e:
            logger.error(
                f"[{self.source_domain}] Ошибка при глубоком парсинге {url}: {e}"
            )
            return {}