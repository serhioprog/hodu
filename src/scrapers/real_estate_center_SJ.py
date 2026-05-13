import re
import asyncio
import urllib.parse
from loguru import logger
from selectolax.lexbor import LexborHTMLParser
from src.models.schemas import PropertyTemplate
from src.scrapers.base import BaseScraper
from src.scrapers._enrichment_mixin import EnrichmentMixin
from src.scrapers.fetchers import fetcher_funnel
from src.scrapers.fetchers.exceptions import FetcherError


# The plugin exposes its real nonce via window.halkiGrid3Config.filterNonce
# after JS loads. The HTML contains a different "decoy" nonce that fails
# validation. We read the JS global because that's what jQuery handlers
# actually use — verified via network capture of the live site.
_NONCE_JS_PATH = "halkiGrid3Config.filterNonce"

# Default form fields the plugin sends with EVERY load-more request,
# even though they're empty for "no filter applied". Verified from
# captured live request body. Backend likely 400s if these are missing.
_DEFAULT_FILTER_FIELDS = {
    "subcat":      "",
    "custom_code": "",
    "latitude":    "",
    "longitude":   "",
    "status":      "",
    "type":        "",
    "min_price":   "",
    "max_price":   "",
    "beds":        "",
    "baths":       "",
    "distance":    "",
    "sqft_min":    "",
    "sqft_max":    "",
    "lot_min":     "",
    "lot_max":     "",
    "year_min":    "",
    "year_max":    "",
    "garage":      "",
    "stories":     "",
}


class RealEstateCenterScraper(EnrichmentMixin, BaseScraper):
    """
    realestatecenter.gr scraper.

    KEEPS its existing inline NLP fallback (line ~338) which runs
    self.extractor.analyze_full_text over `description + greedy_text`.
    That richer input (greedy_text adds DOM-level "miscellaneous" text)
    has been shown to extract more amenity signals than description alone.

    Inheriting EnrichmentMixin makes helpers available WITHOUT removing
    the inline NLP — non-destructive upgrade. Helpers now usable:
      * _passes_quality_gate() — call at end for visibility (added below)
      * _og_description_fallback() — already implemented inline (line 224)
      * _to_int_euro_safe() / _strip_strikethrough() — available if needed
    """

    def __init__(self):
        super().__init__()
        self.source_domain = "realestatecenter.gr"

    def __init__(self):
        super().__init__()
        self.source_domain = "realestatecenter.gr"
        self.api_url     = "https://realestatecenter.gr/wp-admin/admin-ajax.php"
        self.referer_url = "https://realestatecenter.gr/maps/"

        self.custom_headers = {
            "Accept":           "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        }

    async def collect_urls(self, min_price=400000) -> list[PropertyTemplate]:
        """
        Collects all listings via WP plugin's load-more AJAX.

        Routes through fetcher_funnel.wp_ajax which:
          * Stage 0 (curl_cffi) returns CaptchaDetected (can't bind
            nonce to session) → funnel escalates.
          * Stage 1 (Playwright) opens /maps/, waits for JS to set
            window.halkiGrid3Config.filterNonce, then POSTs from inside
            the browser session — cookies + nonce match what WP expects.

        Each AJAX page is served from the same browser pool, so after
        the first warm-up subsequent pages take ~2-3s each.
        """
        all_properties = []
        current_offset = 0
        seen_ids       = set()
        page_no        = 0

        while True:
            page_no += 1
            logger.info(
                f"[{self.source_domain}] page {page_no}: requesting offset={current_offset}"
            )

            # Build full payload: defaults + offset
            page_data = dict(_DEFAULT_FILTER_FIELDS)
            page_data["offset"] = str(current_offset)

            try:
                response = await fetcher_funnel.wp_ajax(
                    domain        = self.source_domain,
                    ajax_url      = self.api_url,
                    referer_url   = self.referer_url,
                    action        = "halki_filter_properties",
                    nonce_js_path = _NONCE_JS_PATH,
                    extra_data    = page_data,
                    headers       = self.custom_headers,
                )
            except FetcherError as e:
                logger.error(
                    f"[{self.source_domain}] page {page_no}: funnel exhausted "
                    f"({e.error_code}): {e}"
                )
                break

            try:
                data = response.json()
            except Exception:
                logger.warning(
                    f"[{self.source_domain}] page {page_no}: non-JSON body "
                    f"({len(response.text)} chars), stopping pagination"
                )
                break

            if not isinstance(data, dict) or not data.get("success"):
                logger.info(
                    f"[{self.source_domain}] page {page_no}: API returned success=false, stopping"
                )
                break

            html_content = data.get("data", {}).get("html", "")
            if not html_content:
                break

            parser = LexborHTMLParser(html_content)
            cards  = parser.css(".halki-card")
            if not cards:
                break

            new_cards = 0
            for card in cards:
                link_node = (
                    card.css_first("a.btn-redirect-link")
                    or card.css_first("a[href*='/property/']")
                )
                href = link_node.attributes.get("href") if link_node else None
                if not href:
                    continue

                match_id = re.search(r'/property/(\d+)-', href)
                site_id  = match_id.group(1) if match_id else href.split('/')[-2]

                if site_id in seen_ids:
                    continue
                seen_ids.add(site_id)
                new_cards += 1

                card_text = card.text()
                price_val = None
                m_price   = re.search(r'€\s*([\d.,]+)', card_text)
                if m_price:
                    price_val = m_price.group(1).strip()

                sqm_m  = re.search(r'(\d+[.,]?\d*)\s*(?:Sqm|m2|sq)',  card_text, re.I)
                beds_m = re.search(r'(\d+)\s*(?:Bedrooms|Beds|Bedroom)',  card_text, re.I)
                baths_m= re.search(r'(\d+)\s*(?:Bathrooms|Baths|Bathroom)', card_text, re.I)

                prop_data = PropertyTemplate(
                    site_property_id = site_id,
                    source_domain    = self.source_domain,
                    url              = href,
                    price            = price_val,
                    size_sqm         = float(sqm_m.group(1).replace(',', '.')) if sqm_m else None,
                    bedrooms         = int(beds_m.group(1)) if beds_m else None,
                    bathrooms        = int(baths_m.group(1)) if baths_m else None,
                )

                if prop_data.price and prop_data.price >= min_price:
                    all_properties.append(prop_data)

            if new_cards == 0:
                break
            current_offset += len(cards)
            await asyncio.sleep(1)

        logger.info(
            f"[{self.source_domain}] collect_urls done: "
            f"{len(all_properties)} listings ≥ {min_price}€ "
            f"(across {page_no} page(s), {len(seen_ids)} unique IDs seen)"
        )
        return all_properties

    async def fetch_details(self, url: str) -> dict:
        """
        Parse a single property detail page.

        Site structure (verified 2026-05-03):
          * Description in <span class="full-desc">  (JS hides it; we want
            the raw HTML version, NOT the truncated <span class="short-desc">)
          * Features under <div class="specs"> with accordion-content children
            of form <li>Near to: Sea, ...</li>
          * Area, Sub Area, Bedrooms etc in <div class="spec"> blocks with
            <strong> labels.
          * Property ID in <span class="property-id-value"> (alphanumeric like
            "A047", separate from the URL site_property_id like "2353681").

        Old layout (var property_data + .property-description) is gone since
        the site migrated to a new theme. Selectors below match the current
        live HTML.
        """
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)

            details = {
                "description": "", "price": None, "size_sqm": None, "land_size_sqm": None,
                "bedrooms": None, "bathrooms": None, "year_built": None, "area": None,
                "subarea": None, "category": None, "levels": None, "site_last_updated": None,
                "latitude": None, "longitude": None, "images": [], "extra_features": {}
            }

            # ── 1. Description: prefer .full-desc (complete), fallback to og:description
            full_desc_node = parser.css_first("span.full-desc")
            if full_desc_node:
                # full-desc contains <p> tags with paragraphs. Extract with
                # newline separators so paragraph breaks survive in extractor.
                details["description"] = full_desc_node.text(separator="\n", strip=True)

            if not details["description"] or len(details["description"]) < 50:
                # Fallback: short-desc, then og:description
                short_desc_node = parser.css_first("span.short-desc")
                if short_desc_node:
                    short_text = short_desc_node.text(separator="\n", strip=True)
                    if len(short_text) > len(details["description"]):
                        details["description"] = short_text

                og_desc = parser.css_first('meta[property="og:description"]')
                if og_desc and og_desc.attributes.get("content"):
                    og_text = og_desc.attributes["content"]
                    if len(og_text) > len(details["description"]):
                        details["description"] = og_text

            # ── 2. Greedy text fallback for the NLP extractor
            greedy_text = ""
            main_container = (
                parser.css_first(".specs")
                or parser.css_first("main")
                or parser.css_first("article")
                or parser.css_first("body")
            )
            if main_container:
                greedy_text = main_container.text(separator=" ", strip=True)

            # ── 3. Features: collect <li> items inside .specs accordion-content blocks.
            #    Each <li> is structured as "Key: Value, Value, Value", e.g.:
            #      "Near to: Sea, Seaside"
            #      "With View: Sea, Mountain, Nature, Openness, Panoramic"
            #      "Special features: Furnished, Bright, Seaside"
            #
            #    We split each into its own top-level key in extra_features.
            #    This matters because the quality metric counts top-level
            #    JSONB keys: {raw_features: [...3 items...]} = 1 key (bad),
            #    but {near_to: "...", with_view: "...", special_features: "..."}
            #    = 3 keys (good). Same data, better shape.
            #
            #    raw_features list is also kept as a backup for debugging /
            #    re-parsing if normalisation logic changes.
            features_lines = []
            for li in parser.css(".specs .accordion-content li"):
                text = li.text(strip=True)
                if text:
                    features_lines.append(text)

            if features_lines:
                details["extra_features"]["raw_features"] = features_lines

                # Also explode key:value pairs into structured keys.
                for line in features_lines:
                    if ":" not in line:
                        continue
                    key_part, _, value_part = line.partition(":")
                    # Normalise the key: lowercase, spaces → underscores,
                    # strip non-alphanumerics. "With View" → "with_view".
                    norm_key = re.sub(r"[^\w\s]", "", key_part.lower()).strip()
                    norm_key = re.sub(r"\s+", "_", norm_key)
                    value = value_part.strip()
                    if norm_key and value and norm_key not in details["extra_features"]:
                        details["extra_features"][norm_key] = value

            # ── 4. Area & Sub Area from <div class="spec"> blocks
            for spec in parser.css(".specs .spec"):
                heading = spec.css_first("h3")
                if not heading:
                    continue
                heading_text = heading.text(strip=True).lower()
                content_text = spec.text(separator=" ", strip=True)

                if "area" in heading_text and "lot" in heading_text:
                    # "Area: 140 m²  Sub Area: Pefkochori"
                    m_area = re.search(r"Area:\s*([\d.,]+)\s*m", content_text, re.I)
                    if m_area and not details.get("size_sqm"):
                        try:
                            details["size_sqm"] = float(m_area.group(1).replace(",", "."))
                        except ValueError:
                            pass
                    m_sub = re.search(r"Sub\s*Area:\s*([A-Za-zÀ-ÿ\s\-]+)", content_text)
                    if m_sub:
                        details["subarea"] = m_sub.group(1).strip()
                elif heading_text == "features":
                    # "4 Bedrooms • 2 Bathrooms • 140 m²"
                    m_bed = re.search(r"(\d+)\s*Bedroom", content_text, re.I)
                    if m_bed and not details.get("bedrooms"):
                        details["bedrooms"] = int(m_bed.group(1))
                    m_bath = re.search(r"(\d+)\s*Bathroom", content_text, re.I)
                    if m_bath and not details.get("bathrooms"):
                        details["bathrooms"] = int(m_bath.group(1))

            # ── 5. Site-specific property ID (e.g. "A047") — store as extra
            id_node = parser.css_first(".property-id-value")
            if id_node:
                pid = id_node.text(strip=True)
                if pid:
                    details["extra_features"]["site_internal_id"] = pid

            # ── 6. Images from gallery (Elementor swiper, plus generic <img>s)
            for img in parser.css(".swiper-slide img, .elementor-image-gallery img, .property-gallery img"):
                src = img.attributes.get("src") or img.attributes.get("data-src", "")
                if not src:
                    continue
                if src.startswith("//"):
                    src = "https:" + src
                # Strip Elementor-style size suffixes to get the high-res version
                high_res_src = re.sub(r"-\d{2,4}x\d{2,4}(\.[a-zA-Z0-9]+)$", r"\1", src)
                if (
                    high_res_src not in details["images"]
                    and not high_res_src.endswith(".svg")
                    and "logo" not in high_res_src.lower()
                ):
                    details["images"].append(high_res_src)

            # ── 7. Coordinates from any embedded map JS
            if not details.get("latitude"):
                script_match = re.search(
                    r"setView\(\[\s*([0-9.]+)\s*,\s*([0-9.]+)\s*\]", response.text,
                )
                if script_match:
                    details["latitude"] = float(script_match.group(1))
                    details["longitude"] = float(script_match.group(2))

            # ── 8. Run the smart NLP extractor over description + greedy text
            # Using greedy_text (DOM-level fallback text) makes this richer
            # than the mixin's description-only NLP, so we keep this inline.
            full_text_for_nlp = f"{details['description']} \n {greedy_text}"
            smart_data = self.extractor.analyze_full_text(full_text_for_nlp)

            # Apply primary-column patches the same "fill only if missing" way
            for key, value in smart_data.items():
                if value is None or key == "extra_features":
                    continue
                if not details.get(key):
                    details[key] = value

            # extra_features merge — WITH semantic dedup via mixin's map.
            # Without dedup, "parking" (NLP) and "garage" (structural) would
            # both land in extras as visual duplicates. With dedup,
            # structural slug wins, NLP variant is skipped.
            nlp_extras = smart_data.get("extra_features") or {}
            if nlp_extras:
                existing = set(details["extra_features"].keys())
                for k, v in nlp_extras.items():
                    if k in existing:
                        continue
                    related = self._NLP_TO_STRUCTURAL.get(k, set())
                    if related & existing:
                        continue
                    details["extra_features"][k] = v

            # ── 9. Quality Gate — log-only, daily_sync's _should_redeep
            # will trigger a retry if the description is below threshold.
            if not self._passes_quality_gate(details.get("description")):
                logger.warning(
                    f"[{self.source_domain}] description below quality gate "
                    f"for {url}"
                )

            return details

        except Exception as e:
            logger.error(f"Ошибка при парсинге {url}: {e}")
            return {}