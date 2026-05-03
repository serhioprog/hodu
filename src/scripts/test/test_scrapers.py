import asyncio
import json
from loguru import logger

# Отключаем старичков
# from src.scrapers.real_estate_center_SJ import RealEstateCenterScraper
# from src.scrapers.gl_real_estate import GLRealEstateScraper

# Правильный импорт нашего нового файла
from src.scrapers.greek_exclusive_properties import GreekExclusiveScraper

async def run_tests():
    logger.info("🧪 ЗАПУСК ИЗОЛИРОВАННОГО ТЕСТА (GREEK EXCLUSIVE)...")

    logger.info("=== ТЕСТ 3: Greek Exclusive Scraper ===")
    ge_scraper = GreekExclusiveScraper()
    
    # Запускаем сбор с лимитом от 400 000€
    ge_urls = await ge_scraper.collect_urls(min_price=400000) 
    
    if ge_urls:
        test_prop = ge_urls[0]
        logger.success(f"✅ Базовые данные (Фаза 1 - Карточка):\n{test_prop.model_dump_json(indent=2)}")
        
        logger.info(f"Ныряем внутрь: {test_prop.url}")
        details = await ge_scraper.fetch_details(test_prop.url)
        
        if details:
            # ensure_ascii=False нужен, чтобы греческий язык отображался нормально
            logger.success(f"✅ Глубокий парсинг (Фаза 2 - Данные):\n{json.dumps(details, indent=2, ensure_ascii=False)}")
        else:
            logger.error("❌ GE fetch_details вернул пустоту.")
    else:
        logger.warning("⚠️ GE не нашел объектов.")

if __name__ == "__main__":
    asyncio.run(run_tests())