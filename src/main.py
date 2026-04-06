import asyncio
from loguru import logger
from src.scrapers.gl_real_estate import GLRealEstateScraper
from src.database.db import async_session_maker
from src.database.repository import save_or_update_property, save_media_records # Добавили save_media_records
from src.models.schemas import PropertyTemplate
from src.services.media import MediaDownloader # Добавили импорт MediaDownloader

async def main():
    logger.info("🚀 Запуск Full Pipeline: Collector + Deep Extractor")
    
    scraper = GLRealEstateScraper()
    
    # --- ШАГ 1: COLLECTOR ---
    logger.info("--- Этап 1: Сбор ссылок (Collector) ---")
    basic_properties = await scraper.collect_urls(min_price=400000)
    
    if not basic_properties:
        logger.warning("Объекты не найдены. Проверь селекторы или защиту.")
        return

    logger.info(f"Собрано {len(basic_properties)} объектов для глубокого анализа.")

    # --- ШАГ 2: DEEP EXTRACTOR И МЕДИА ---
    logger.info("--- Этап 2: Глубокий парсинг и Загрузка Медиа ---")
    
    # Инициализируем наш загрузчик картинок
    media_downloader = MediaDownloader()
    
    async with async_session_maker() as session:
        for index, prop_data in enumerate(basic_properties, 1):
            try:
                print(f"\n{'='*60}")
                logger.info(f"🏠 ОБЪЕКТ [{index}/{len(basic_properties)}] | ID: {prop_data.site_property_id}")
                logger.info(f"🔗 URL: {prop_data.url}")
                logger.info(f"[{index}/{len(basic_properties)}] Парсинг: {prop_data.site_property_id}")
                details = await scraper.fetch_details(prop_data.url)
                
                #ЖУЧОК ДЛЯ ПРОВЕРКИ РАБОТЫ ПАРСЕРА:
                logger.warning(f"🔍 DEBUG: Что нашел парсер: Район={details.get('area')}, Тип={details.get('category')}")
                
                if details:
                    # Получаем базовые данные
                    base_data = prop_data.model_dump()
                    
                    # Принудительно обновляем базовые данные деталями,
                    # чтобы `details` перетерли пустые значения из `base_data`
                    base_data.update(details)
                    
                    # Валидируем финальный словарь
                    prop_validated = PropertyTemplate(**base_data)

                    # ПРОВЕРОЧНЫЙ ЛОГ:
                    logger.info(f"   🔎 Найдено фото в шаблоне: {len(prop_validated.images)}")
                    
                    # 1. Сохраняем текстовые данные и получаем UUID объекта
                    property_uuid, image_urls = await save_or_update_property(session, prop_validated)
                    
                    # 2. Скачиваем фото (если они есть)
                    if image_urls and property_uuid:
                        logger.info(f"   📸 Скачивание {len(image_urls)} фото...")
                        downloaded_media = await media_downloader.download_images(
                            prop_validated.site_property_id, 
                            image_urls
                        )
                        
                        # 3. Сохраняем пути к фото в базу данных
                        if downloaded_media:
                            await save_media_records(session, property_uuid, downloaded_media)
                            logger.info(f"   ✅ Фото сохранены в БД для {prop_data.site_property_id}")
                
                print(f"{'='*60}\n")
                await asyncio.sleep(1) # Этичная пауза между объектами
                
            except Exception as e:
                logger.error(f"❌ Ошибка объекта {prop_data.url}: {e}")
                continue
            
    logger.success("✅ Milestone 3: Baseline Scraper + Media Engine успешно завершен!")

if __name__ == "__main__":
    asyncio.run(main())