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
            """Парсит детальную информацию со страницы объекта (Версия 3.1 - Хирургическая точность)"""
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
                    "images": []
                }

                # --- ХИРУРГИЯ 1: Блок "Essential Details" (Тип, Район, Подрайон, Уровни) ---
                essential_items = parser.css(".kf_property_detail_Essentail ul li")
                for item in essential_items:
                    raw_text = item.text().strip()
                    if not raw_text:
                        continue
                    
                    # Ищем конструкцию "Ключ: Значение"
                    if ":" in raw_text:
                        parts = raw_text.split(":", 1)
                        key = parts[0].strip().lower()
                        val = parts[1].strip()
                        
                        # ИСПРАВЛЕНИЕ: Строгое совпадение ключей, чтобы "Land Area" не попала в "Area"
                        if key in ["category", "type", "τύπος"]:
                            data["category"] = val
                        elif key in ["area", "region", "περιοχή"]:
                            data["area"] = val
                        elif key in ["subarea", "sub-area", "υποπεριοχή"]:
                            data["subarea"] = val
                        elif key in ["levels", "επίπεδα"]:
                            data["levels"] = val

                # --- ХИРУРГИЯ 2: Остальные характеристики (Цена, Площадь, Спальни и т.д.) ---
                for item in parser.css("li, .single"):
                    raw_text = item.text().strip()
                    # Пропускаем строки с Category и Area, чтобы они не перебивали данные из Хирургии 1
                    if not raw_text or "Category:" in raw_text or "Area:" in raw_text:
                        continue
                    
                    key = ""
                    val = ""
                    
                    if ":" in raw_text:
                        parts = raw_text.split(":", 1)
                        key = parts[0].strip().lower()
                        val = parts[1].strip()
                    else:
                        strong = item.css_first("strong")
                        span = item.css_first("span")
                        if strong and span:
                            key = strong.text().replace(":", "").strip().lower()
                            val = span.text().strip()

                    if not key:
                        continue

                    if key in ["price", "τιμή"]: 
                        data["price"] = val
                    elif key in ["property size", "size", "εμβαδόν", "sq.m."]: 
                        data["size_sqm"] = val
                    elif key in ["land area", "plot size", "plot", "land", "land size", "εμβαδόν οικοπέδου", "οικόπεδο"]: 
                        data["land_size_sqm"] = val
                    elif key in ["bedrooms", "bedroom", "υπνοδωμάτια"]: 
                        data["bedrooms"] = val
                    elif key in ["bathrooms", "bathroom", "μπάνια"]: 
                        data["bathrooms"] = val
                    elif key in ["year of construction", "year built", "construction year", "έτος κατασκευής"]: 
                        data["year_built"] = val

                # --- ХИРУРГИЯ 3: Описание объекта ---
                # Ищем теги <p> внутри точного пути, который ты нашел
                desc_nodes = parser.css(".kf_property_detail_uptwon .text p, .kf_property_detail_uptwon p")
                if desc_nodes:
                    # Склеиваем абзацы через двойной перенос строки, чтобы текст читался легко
                    data["description"] = "\n\n".join([p.text().strip() for p in desc_nodes if len(p.text().strip()) > 5])
                else:
                    # Запасной вариант: если разработчики забыли <p> и написали текст прямо в div
                    desc_block = parser.css_first(".kf_property_detail_uptwon .text")
                    if desc_block:
                        data["description"] = desc_block.text().strip()

                # 3. Дата последнего обновления -> kf_property_detail_uptwon
                update_node = parser.css_first(".kf_property_detail_uptwon") or parser.css_first(".fa-calendar")
                if update_node:
                    # Если нашли только иконку календаря, берем весь текст её родительского элемента
                    parent = update_node.parent if "fa-calendar" in update_node.attributes.get("class", "") else update_node
                    raw_date = parent.text().strip()

                    
                    # Вырезаем саму дату регулярным выражением (ищем паттерны вроде 12/05/2024 или 12-05-2024)
                    date_match = re.search(r'(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})', raw_date)
                    if date_match:
                        data["site_last_updated"] = date_match.group(1)
                    else:
                        data["site_last_updated"] = raw_date # Запасной вариант

                # 4. Координаты карты (Leaflet setView)
                for s in parser.css("script"):
                    script_text = s.text()
                    if script_text and "setView" in script_text:
                        # Регулярка улучшена: добавлена поддержка отрицательных координат (-?)
                        match = re.search(r"setView\(\[\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*\]", script_text)
                        if match:
                            data["latitude"] = float(match.group(1))
                            data["longitude"] = float(match.group(2))

                # 5. Описание
                desc_nodes = parser.css(".kf_property_des p")
                if desc_nodes:
                    data["description"] = "\n".join([p.text().strip() for p in desc_nodes if p.text().strip()])
                else:
                    desc_block = parser.css_first(".kf_property_des")
                    if desc_block:
                        data["description"] = desc_block.text().strip()

                # 5. Фото
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

                        price_node = card.css_first(".kf_property_place h5")
                        location_node = card.css_first(".fa-map-marker")
                        
                        id_node = card.css_first("span")
                        id_text = id_node.text() if id_node else "0"
                        clean_id = id_text.split("#")[-1].strip()

                        prop_data = PropertyTemplate(
                            site_property_id=clean_id,
                            source_domain=self.source_domain,
                            url=url_node.attributes.get("href"),
                            price=price_node.text() if price_node else None,
                            location_raw=location_node.parent.text().strip() if location_node and location_node.parent else None
                        )
                        all_properties.append(prop_data)
                        
                    except Exception as e:
                        logger.error(f"Ошибка парсинга отдельной карточки: {e}")

                logger.info(f"Успешно собрано {len(cards)} объектов со страницы {page}")
                
                await asyncio.sleep(2) 
                page += 1
                
                #стоп для скрапера на 3й странице пагинации
                if page > 2: 
                    logger.warning("Достигнут тестовый лимит в 2 страницы.")
                    break
                    
            except Exception as e:
                logger.error(f"Критическая ошибка на странице {page}: {e}")
                break
            
        return all_properties