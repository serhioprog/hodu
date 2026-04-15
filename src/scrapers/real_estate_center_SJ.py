import re
import asyncio
import urllib.parse
from loguru import logger
from selectolax.lexbor import LexborHTMLParser
from src.models.schemas import PropertyTemplate
from curl_cffi.requests import AsyncSession

class RealEstateCenterScraper:
    def __init__(self):
        self.source_domain = "realestatecenter.gr"
        self.api_url = "https://realestatecenter.gr/wp-admin/admin-ajax.php"
        
        self.headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://realestatecenter.gr",
            "Referer": "https://realestatecenter.gr/maps/",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        }

    async def collect_urls(self, min_price=400000) -> list[PropertyTemplate]:
        all_properties = []
        current_offset = 0 # Используем смещение вместо страниц
        seen_ids = set()

        async with AsyncSession(impersonate="chrome120") as session:
            logger.info(f"[{self.source_domain}] 🕵️‍♂️ Шаг 1: Ищем токен API (nonce)...")
            nonce = ""
            try:
                resp = await session.get("https://realestatecenter.gr/maps/", timeout=20)
                match = re.search(r'nonce["\']?\s*[:=]\s*["\']([a-f0-9]{10})["\']', resp.text)
                if match:
                    nonce = match.group(1)
                    logger.success(f"[{self.source_domain}] 🔑 Найден токен API: {nonce}")
            except Exception as e:
                logger.warning(f"[{self.source_domain}] Ошибка прогрева: {e}")

            while True:
                logger.info(f"[{self.source_domain}] Имитируем кнопку 'Load More' (Пропускаем первые {current_offset} шт.)...")
                
                # 🔥 Идеально чистый Payload, который 100% работает
                payload = {
                    "action": "halki_filter_properties", 
                    "offset": str(current_offset)
                }
                if nonce:
                    payload["nonce"] = nonce

                try:
                    encoded_payload = urllib.parse.urlencode(payload)
                    response = await session.post(self.api_url, data=encoded_payload, headers=self.headers, timeout=30)
                    
                    # Пытаемся прочитать ответ
                    try:
                        data = response.json()
                    except Exception:
                        logger.warning(f"[{self.source_domain}] Сервер вернул не JSON. Скорее всего конец базы.")
                        break

                    # 🔥 ЗАЩИТА ОТ КРАША: Если сервер прислал 0 вместо словаря
                    if not isinstance(data, dict):
                        logger.info(f"[{self.source_domain}] Сервер вернул {data}. Объекты закончились.")
                        break

                    if not data.get("success"):
                        logger.info(f"[{self.source_domain}] Данные закончились (ответ сервера: success=false).")
                        break
                    
                    html_content = data.get("data", {}).get("html", "")
                    if not html_content:
                        logger.info(f"[{self.source_domain}] Пустой HTML блок. Все объекты собраны!")
                        break

                    parser = LexborHTMLParser(html_content)
                    cards = parser.css(".halki-card")
                    
                    if not cards:
                        break

                    new_cards = 0
                    for card in cards:
                        link_node = card.css_first("a.btn-redirect-link") or card.css_first("a[href*='/property/']")
                        href = link_node.attributes.get("href") if link_node else None
                        if not href: continue

                        match_id = re.search(r'/property/(\d+)-', href)
                        site_id = match_id.group(1) if match_id else href.split('/')[-2]

                        if site_id in seen_ids:
                            continue
                        seen_ids.add(site_id)
                        new_cards += 1

                        card_text = card.text()

                        # Строгая цена (защита от записи ID в цену)
                        price_val = None
                        match_price = re.search(r'€\s*([\d.,]+)', card_text)
                        if match_price:
                            price_val = match_price.group(1).strip()

                        # Вытаскиваем характеристики (спальни, площадь) прямо из карточки
                        sqm_m = re.search(r'(\d+[.,]?\d*)\s*(?:Sqm|m2|sq)', card_text, re.I)
                        beds_m = re.search(r'(\d+)\s*(?:Bedrooms|Beds|Bedroom)', card_text, re.I)
                        baths_m = re.search(r'(\d+)\s*(?:Bathrooms|Baths|Bathroom)', card_text, re.I)

                        prop_data = PropertyTemplate(
                            site_property_id=site_id,
                            source_domain=self.source_domain,
                            url=href,
                            price=price_val,
                            size_sqm=float(sqm_m.group(1).replace(',','.')) if sqm_m else None,
                            bedrooms=int(beds_m.group(1)) if beds_m else None,
                            bathrooms=int(baths_m.group(1)) if baths_m else None
                        )

                        # Фильтруем виллы >= 400.000€
                        if prop_data.price and prop_data.price >= min_price:
                            all_properties.append(prop_data)

                    # Если сайт начал отдавать дубликаты
                    if new_cards == 0:
                        logger.warning(f"[{self.source_domain}] Пошли дубликаты. Останавливаем пагинацию.")
                        break

                    # Увеличиваем смещение ровно на количество полученных карточек
                    current_offset += len(cards)
                    logger.success(f"[{self.source_domain}] Пачка выкачана. Найдено > 400k: {len(all_properties)}")
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"[{self.source_domain}] Ошибка API: {e}")
                    break

        return all_properties

    async def fetch_details(self, url: str) -> dict:
        """Глубокий парсинг внутренностей карточки (Основано на разведданных)"""
        try:
            async with AsyncSession(impersonate="chrome120") as session:
                response = await session.get(url, timeout=30)
                html_content = response.text
                parser = LexborHTMLParser(html_content)
                
                details = {
                    "images": [],
                    "extra_features": {},
                    "description": "",
                    "subarea": None,
                    "year_built": None,
                    "land_size_sqm": None,
                    "levels": None,
                    "latitude": None,
                    "longitude": None
                }

                # 1. Фотографии из слайдера
                slides = parser.css(".swiper-slide img")
                for slide in slides:
                    src = slide.attributes.get("src")
                    if src and src not in details["images"]:
                        details["images"].append(src)

                # 2. Локация (Sub Area) - Ищем в параграфах
                for p in parser.css("p"):
                    p_text = p.text(strip=True)
                    if "Sub Area:" in p_text:
                        details["subarea"] = p_text.replace("Sub Area:", "").strip()
                        break

                # 3. Удобства и особенности (#features)
                features_block = parser.css_first("#features")
                if features_block:
                    feature_items = [li.text(strip=True) for li in features_block.css("li")]
                    details["extra_features"]["raw_features"] = " | ".join(feature_items)

                # 4. Координаты (ВЗЛОМ JS Скрипта - Версия с переменными)
                # Ищем const lat = "40.0059436" или var lng = '23.415'
                lat_match = re.search(r'lat\s*=\s*["\']([-\d.]+)["\']', html_content)
                lng_match = re.search(r'lng\s*=\s*["\']([-\d.]+)["\']', html_content)
                
                if lat_match and lng_match:
                    try:
                        details["latitude"] = float(lat_match.group(1))
                        details["longitude"] = float(lng_match.group(1))
                    except ValueError:
                        pass

                # 5. Описание и умный поиск данных внутри текста
                desc_node = parser.css_first(".full-desc")
                if desc_node:
                    desc_text = desc_node.text(separator="\n", strip=True)
                    details["description"] = desc_text

                    # Ищем Type (Category) для этого сайта
                    search_text = (desc_text + " " + url).lower()
                    prop_types = [
                        "apartment", "maisonette", "villa", "detached house", 
                        "residential complex", "hotel", "bungalow", "studio", 
                        "residential building", "residencial building", "shop", 
                        "land plot", "parcel"
                    ]
                    for pt in prop_types:
                        if pt in search_text or pt.replace(" ", "-") in search_text:
                            # Записываем красиво с большой буквы
                            details["category"] = pt.title().replace("Residencial", "Residential")
                            break

                    # Ищем год постройки: "Built in 2012"
                    year_match = re.search(r'Built in\s*(\d{4})', desc_text, re.IGNORECASE)
                    if year_match:
                        details["year_built"] = int(year_match.group(1))

                    # Ищем площадь участка (Land m2): "1,300 sq.m. garden" или "130 sqm private space"
                    land_match = re.search(r'([\d.,]+)\s*(?:sq\.?m\.?|sqm|m²|sq\.? meters?)\s*(?:garden|private space|plot|land)', desc_text, re.IGNORECASE)
                    if land_match:
                        # Убираем запятые (1,300 -> 1300) и переводим в число
                        clean_land = land_match.group(1).replace(',', '')
                        try:
                            details["land_size_sqm"] = float(clean_land)
                        except ValueError:
                            pass

                    # Ищем этажность (Levels)
                    levels_match = re.search(r'(?:across|over)\s+(one|two|three|four|five|\d+)\s+levels?', desc_text, re.IGNORECASE)
                    if levels_match:
                        lvl_val = levels_match.group(1).lower()
                        # 🔥 Сразу сохраняем как строки, чтобы Pydantic был счастлив
                        word_to_num = {'one': "1", 'two': "2", 'three': "3", 'four': "4", 'five': "5"}
                        if lvl_val in word_to_num:
                            details["levels"] = word_to_num[lvl_val]
                        elif lvl_val.isdigit():
                            details["levels"] = str(lvl_val)

                return details

        except Exception as e:
            logger.error(f"Ошибка при парсинге {url}: {e}")
            return {}