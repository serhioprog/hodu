import asyncio
from loguru import logger
from sqlalchemy import select

from src.scrapers.gl_real_estate import GLRealEstateScraper
from src.scrapers.real_estate_center_SJ import RealEstateCenterScraper

from src.database.db import async_session_maker
from src.models.domain import Property, PriceHistory, PropertyStatus
from src.models.schemas import PropertyTemplate
from src.database.repository import save_or_update_property, save_media_records
from src.services.media import MediaDownloader
from src.services.geo_matcher import GeoMatcher
from src.models.domain import Property, PriceHistory, PropertyStatus, utcnow



async def daily_sync():
    logger.info("🔄 ЗАПУСК ЕЖЕДНЕВНОЙ СИНХРОНИЗАЦИИ (DELTA SYNC) 🔄")
    
    # Задел на будущее: здесь будут лежать все наши парсеры
    active_scrapers = [
        GLRealEstateScraper(), #Georgios Latsios Real Estate
        RealEstateCenterScraper(), #Real Estate Center Susan Jameson
    ]

    for scraper in active_scrapers:
        domain = scraper.source_domain
        logger.info(f"🌐 Начинаем синхронизацию сайта: {domain}")
        
        # 1. Быстрый сбор текущего состояния сайта
        site_properties = await scraper.collect_urls(min_price=400000)
        
        if not site_properties:
            logger.error(f"❌ Не удалось получить данные с {domain}. Пропускаем.")
            continue

        site_map = {p.site_property_id: p for p in site_properties}
        logger.info(f"📊 Найдено {len(site_map)} объектов на сайте {domain}.")

        async with async_session_maker() as session:
            # 2. КРИТИЧЕСКИЙ ФИЛЬТР: Берем АКТИВНЫЕ объекты ТОЛЬКО ЭТОГО САЙТА
            query = select(Property).where(
                Property.status == PropertyStatus.ACTIVE.value,
                Property.source_domain == domain  # <--- ТА САМАЯ ЗАЩИТА
            )
            result = await session.execute(query)
            db_properties = result.scalars().all()
            
            db_map = {p.site_property_id: p for p in db_properties}
            logger.info(f"📂 В базе сейчас {len(db_map)} активных объектов от {domain}.")

            new_props_to_fetch = []
            delisted_count = 0
            price_changed_count = 0

            logger.info("⚙️ Шаг 2: Анализ расхождений...")

            # --- А) ИЩЕМ СНЯТЫЕ С ПРОДАЖИ (DELISTED) ---
            for db_id, db_prop in db_map.items():
                if db_id not in site_map:
                    db_prop.status = PropertyStatus.DELISTED.value
                    delisted_count += 1
                    logger.warning(f"🔻 Объект {db_id} пропал с {domain}. Статус -> DELISTED.")

            # --- Б) ИЩЕМ ИЗМЕНЕНИЯ ЦЕН И ОБНОВЛЯЕМ ТАЙМЕР ---
            for site_id, site_prop in site_map.items():
                if site_id in db_map:
                    db_prop = db_map[site_id]
                    
                    # Обновляем таймер "последний раз видели на сайте"
                    db_prop.last_checked_at = utcnow()
                    
                    if site_prop.price and db_prop.price and site_prop.price != db_prop.price:
                        logger.info(f"📉 Изменение цены для {site_id}: {db_prop.price}€ -> {site_prop.price}€")
                        
                        history_record = PriceHistory(
                            property_id=db_prop.id,
                            old_price=db_prop.price,
                            new_price=site_prop.price
                        )
                        session.add(history_record)
                        db_prop.price = site_prop.price
                        price_changed_count += 1
                else:
                    # НОВИНКА
                    new_props_to_fetch.append(site_prop)

            await session.commit()
            logger.success(f"✅ Анализ {domain} завершен. Снято: {delisted_count} | Изм.цен: {price_changed_count} | Новых: {len(new_props_to_fetch)}")

            # --- В) ГЛУБОКИЙ ПАРСИНГ НОВИНОК ---
            if new_props_to_fetch:
                logger.info(f"🚀 Шаг 3: Глубокий парсинг {len(new_props_to_fetch)} новинок...")
                
                media_downloader = MediaDownloader()
                geo_matcher = GeoMatcher()
                await geo_matcher.load_locations(session)

                for index, prop_data in enumerate(new_props_to_fetch, 1):
                    try:
                        logger.info(f"➕ [{index}/{len(new_props_to_fetch)}] Добавление новинки ID: {prop_data.site_property_id}")
                        details = await scraper.fetch_details(prop_data.url)
                        
                        if details:
                            base_data = prop_data.model_dump()
                            base_data.update(details)
                            
                            # Вызываем гео-мозг для получения привязки к районам и префектурам
                            geo_info = await geo_matcher.find_best_match(
                                lat=base_data.get("latitude"),
                                lng=base_data.get("longitude"),
                                area_name=base_data.get("area")
                            )
                            # Правильно привязываем ответы GeoMatcher к базе данных
                            if geo_info:
                                base_data.update({
                                    "location_id": geo_info.get("location_id"),
                                    "calc_prefecture": geo_info.get("prefecture"),       # Было: base_data.get("area") - ЭТО ОШИБКА
                                    "calc_municipality": geo_info.get("municipality"),   # Было: base_data.get("subarea") - ЭТО ОШИБКА
                                    "calc_area": geo_info.get("exact_district")          # Точный район из базы ARIS
                                })

                            prop_validated = PropertyTemplate(**base_data)
                            property_uuid, image_urls = await save_or_update_property(session, prop_validated)
                            
                            if image_urls and property_uuid:
                                downloaded_media = await media_downloader.download_images(
                                    prop_validated.site_property_id, 
                                    image_urls
                                )
                                if downloaded_media:
                                    await save_media_records(session, property_uuid, downloaded_media)
                                    
                        await asyncio.sleep(1) 

                    except Exception as e:
                        logger.error(f"❌ Ошибка при добавлении новинки {prop_data.site_property_id}: {e}")
                        continue
            else:
                logger.info(f"📭 База {domain} полностью актуальна.")

    logger.success("🏁 ГЛОБАЛЬНАЯ СИНХРОНИЗАЦИЯ УСПЕШНО ЗАВЕРШЕНА 🏁")

if __name__ == "__main__":
    asyncio.run(daily_sync())