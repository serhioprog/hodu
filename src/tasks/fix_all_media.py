import asyncio
import os
import random

# Принудительно подключаем наш логгер
import src.core.logger
from loguru import logger

from sqlalchemy import select, delete
from src.database.db import async_session_maker
from src.models.domain import Property, Media
from src.services.media import MediaDownloader
from src.database.repository import save_media_records

# Импортируем скраперы
from src.scrapers.gl_real_estate import GLRealEstateScraper
from src.scrapers.real_estate_center_SJ import RealEstateCenterScraper
from src.scrapers.greek_exclusive_properties import GreekExclusiveScraper

async def run_fix():
    logger.info("🚀 Запуск БЕЗОПАСНОЙ хирургической перезакачки медиа!")

    scrapers = [
        GLRealEstateScraper(),
        RealEstateCenterScraper(),
        GreekExclusiveScraper()
    ]
    
    downloader = MediaDownloader()

    async with async_session_maker() as session:
        for scraper in scrapers:
            domain = scraper.source_domain
            logger.info(f"\n{'='*50}\n💉 Начинаем обработку сайта: {domain}\n{'='*50}")

            # 1. Получаем объекты
            result = await session.execute(select(Property).where(Property.source_domain == domain))
            props = result.scalars().all()

            if not props:
                logger.warning(f"⚠️ Объектов {domain} нет в базе. Пропускаем.")
                continue

            logger.info(f"✅ Найдено {len(props)} объектов {domain}. Начинаем умное обновление...")

            # 2. Идем по каждому объекту (БЕЗ массового удаления)
            # 2. Идем по каждому объекту (БЕЗ массового удаления)
            for index, p in enumerate(props, 1):
                logger.info(f"📸 [{domain}] [{index}/{len(props)}] Проверка/Закачка для: {p.site_property_id}")
                
                try:
                    details = await scraper.fetch_details(p.url)

                    if details and details.get("images"):
                        downloaded = await downloader.download_images(
                            domain=domain, 
                            property_id=p.site_property_id, 
                            image_urls=details["images"]
                        )
                        
                        if downloaded:
                            # Удаляем старые связи ТОЛЬКО ПОСЛЕ успешной закачки новых!
                            await session.execute(delete(Media).where(Media.property_id == p.id))
                            await save_media_records(session, p.id, downloaded)
                            logger.success(f"✅ [{domain}] Успешно обновлено {len(downloaded)} фото.")
                        else:
                            logger.warning(f"⚠️ [{domain}] Фото не скачались для {p.site_property_id}. Оставляем старые связи в БД.")
                    else:
                        logger.warning(f"⚠️ [{domain}] Нет фото на сайте для {p.site_property_id}.")

                except Exception as e:
                    logger.error(f"❌ Ошибка обновления {p.site_property_id}: {e}")

                # Плавающая задержка для обхода защиты (2 - 5 секунд)
                await asyncio.sleep(random.uniform(2.0, 5.0))

    logger.success("🎉 Глобальная безопасная операция завершена!")

if __name__ == "__main__":
    asyncio.run(run_fix())