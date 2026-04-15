import asyncio
import re
from selectolax.lexbor import LexborHTMLParser
from loguru import logger
from src.scrapers.base import BaseScraper
from src.models.schemas import PropertyTemplate

class GLRealEstateScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.source_domain = "glrealestate.gr"
        self.base_url = "https://glrealestate.gr/listings"

    async def fetch_listings(self):
        """Реализация обязательного метода для запуска через main"""
        return await self.collect_urls()

    #----------------------DEEP PARSING START----------------
    async def fetch_details(self, url: str) -> dict:
        """Парсит детальную информацию со страницы объекта (Версия 5.0 - Рабочее описание + JSON)"""
        try:
            response = await self.client.get(url)
            parser = LexborHTMLParser(response.text)
            
            data = {
                "description": "",
                "price": None,
                "size_sqm": None,
                "land_size_sqm": None,
                "bedrooms": None,
                "bathrooms": None,
                "year_built": None,
                "area": None,
                "subarea": None,
                "category": None,
                "levels": None,
                "site_last_updated": None,
                "latitude": None,
                "longitude": None,
                "images": [],
                "extra_features": {} # <-- Мешок для экстра-функций
            }

            # --- 1. ОПИСАНИЕ ОБЪЕКТА (Твоя рабочая логика) ---
            # Собираем теги <p> изо всех возможных мест, где они могут быть
            desc_nodes = parser.css(".kf_property_des p, .kf_property_detail_uptwon .text p, .kf_property_detail_uptwon p")
            if desc_nodes:
                data["description"] = "\n\n".join([p.text().strip() for p in desc_nodes if len(p.text().strip()) > 5])
            
            # Запасной вариант: если тегов <p> нет, ищем просто блоки с текстом
            if not data["description"]:
                desc_block = parser.css_first(".kf_property_des") or parser.css_first(".kf_property_detail_uptwon .text")
                if desc_block:
                    data["description"] = desc_block.text().strip()

            # --- 2. ТОТАЛЬНЫЙ СБОР ХАРАКТЕРИСТИК (Базовые + Экстра) ---
            target_classes = ".property-right .single, .kf_property_detail_list li, .kf_property_more_ftr li, .kf_property_detail_link li, .kf_property_detail_Essentail ul li"
            
            for item in parser.css(target_classes):
                text = item.text().replace("\n", " ").strip()
                # Защита: пропускаем пустые строки и куски описания (тексты длиннее 100 символов)
                if not text or len(text) > 100: 
                    continue 
                
                key, val = "", ""
                spans = item.css("span")
                
                # Вариант А: <span class="key">Key:</span> <span class="val">Val</span>
                if len(spans) >= 2:
                    key = spans[0].text().replace(":", "").strip().lower()
                    val = spans[1].text().strip()
                # Вариант Б: "Heating: Autonomous"
                elif ":" in text:
                    parts = text.split(":", 1)
                    key = parts[0].strip().lower()
                    val = parts[1].strip()
                # Вариант В: Одиночные фичи "Air condition"
                else:
                    key = re.sub(r'\s+', ' ', text).strip().lower()
                    val = "Yes"

                if not key: 
                    continue

                # --- РАСПРЕДЕЛЕНИЕ ---
                if key in ["price", "τιμή"]: data["price"] = val
                elif key in ["property size", "size", "εμβαδόν", "sq.m."]: data["size_sqm"] = val
                elif key in ["land area", "plot size", "plot", "land", "land size", "εμβαδόν οικοπέδου", "οικόπεδο"]: data["land_size_sqm"] = val
                elif key in ["bedrooms", "bedroom", "υπνοδωμάτια"]: data["bedrooms"] = val
                elif key in ["bathrooms", "bathroom", "μπάνια"]: data["bathrooms"] = val
                elif key in ["year of construction", "year built", "construction year", "έτος κατασκευής"]: data["year_built"] = val
                elif key in ["category", "type", "τύπος"]: data["category"] = val
                elif key in ["area", "region", "περιοχή"]: data["area"] = val
                elif key in ["subarea", "sub-area", "υποπεριοχή"]: data["subarea"] = val
                elif key in ["levels", "level", "floor", "επίπεδα"]: data["levels"] = val
                else:
                    # МАГИЯ: Всё остальное (Отопление, Бассейн и т.д.) летит в JSON
                    clean_key = key.title()
                    data["extra_features"][clean_key] = val

            # --- 3. ДАТА ОБНОВЛЕНИЯ ---
            update_node = parser.css_first(".kf_property_detail_uptwon") or parser.css_first(".fa-calendar")
            if update_node:
                parent = update_node.parent if "fa-calendar" in update_node.attributes.get("class", "") else update_node
                raw_date = parent.text().strip()
                date_match = re.search(r'(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})', raw_date)
                data["site_last_updated"] = date_match.group(1) if date_match else raw_date

            # --- 4. КООРДИНАТЫ (Leaflet setView) ---
            for s in parser.css("script"):
                script_text = s.text()
                if script_text and "setView" in script_text:
                    match = re.search(r"setView\(\[\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*\]", script_text)
                    if match:
                        data["latitude"], data["longitude"] = float(match.group(1)), float(match.group(2))

            # --- 5. ФОТО ---
            for a_tag in parser.css("ul.bxslider li a"):
                href = a_tag.attributes.get("href")
                if href and href not in data["images"]: 
                    data["images"].append(href)

            return data
            
        except Exception as e:
            logger.error(f"Ошибка при глубоком парсинге {url}: {e}")
            return {}
    #----------------------DEEP PARSING END----------------

    async def collect_urls(self, min_price: int = 400000):
        """Сбор URL со страниц поиска"""
        page = 1
        all_properties = []

        while True:
            params = {
                "category": "residential",
                "price_min": min_price,
                "page": page,
                "sort": "id",
                "order": "DESC"
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

                        #price_node = card.css_first(".kf_property_place h5")
                        #location_node = card.css_first(".fa-map-marker")
                        # Умный поиск цены: ищем любой текст в карточке со знаком евро!
                        price_val = None
                        for el in card.css("h5, h6, span, div, strong"):
                            if el.text() and "€" in el.text():
                                price_val = el.text().strip()
                                break
                                
                        location_node = card.css_first(".fa-map-marker")
                        
                        id_node = card.css_first("span")
                        id_text = id_node.text() if id_node else "0"
                        clean_id = id_text.split("#")[-1].strip()

                        prop_data = PropertyTemplate(
                            site_property_id=clean_id,
                            source_domain=self.source_domain,
                            url=url_node.attributes.get("href"),
                            price=price_val,  # <--передаем найденную цену со знаком евро
                            location_raw=location_node.parent.text().strip() if location_node and location_node.parent else None
                        )
                        all_properties.append(prop_data)
                        
                    except Exception as e:
                        logger.error(f"Ошибка парсинга отдельной карточки: {e}")

                logger.info(f"Успешно собрано {len(cards)} объектов со страницы {page}")
                
                await asyncio.sleep(2) 
                page += 1
                
                #стоп для скрапера на 3й странице пагинации
                #if page > 2: 
                #    logger.warning("Достигнут тестовый лимит в 2 страницы.")
                #    break
                    
            except Exception as e:
                logger.error(f"Критическая ошибка на странице {page}: {e}")
                break
            
        return all_properties