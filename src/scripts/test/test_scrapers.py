import asyncio
import json
from loguru import logger
from src.scrapers.real_estate_center_SJ import RealEstateCenterScraper
from src.scrapers.gl_real_estate import GLRealEstateScraper

async def run_tests():
    logger.info("🧪 ЗАПУСК РАСШИРЕННОГО ТЕСТА (FULL DATA DUMP)...")

    # --- ТЕСТ 1: SJ Scraper ---
    logger.info("=== ТЕСТ 1: SJ Scraper ===")
    sj_scraper = RealEstateCenterScraper()
    sj_urls = await sj_scraper.collect_urls(min_price=1000000) 
    
    if sj_urls:
        test_prop = sj_urls[0]
        logger.success(f"✅ Базовые данные (Фаза 1 - Карточка):\n{test_prop.model_dump_json(indent=2)}")
        
        logger.info(f"Ныряем внутрь: {test_prop.url}")
        details = await sj_scraper.fetch_details(test_prop.url)
        
        if details:
            # ensure_ascii=False нужен, чтобы греческий язык отображался нормально
            logger.success(f"✅ Глубокий парсинг (Фаза 2 - Регулярки и NLP):\n{json.dumps(details, indent=2, ensure_ascii=False)}")
        else:
            logger.error("❌ SJ fetch_details вернул пустоту.")
    else:
        logger.warning("⚠️ SJ не нашел объектов.")

    print("\n" + "="*60 + "\n")

    # --- ТЕСТ 2: GL Scraper ---
    logger.info("=== ТЕСТ 2: GL Scraper ===")
    gl_scraper = GLRealEstateScraper()
    gl_urls = await gl_scraper.collect_urls(min_price=1000000)
    
    if gl_urls:
        test_prop = gl_urls[0]
        logger.success(f"✅ Базовые данные (Фаза 1 - Карточка):\n{test_prop.model_dump_json(indent=2)}")
        
        logger.info(f"Ныряем внутрь: {test_prop.url}")
        details = await gl_scraper.fetch_details(test_prop.url)
        
        if details:
            logger.success(f"✅ Глубокий парсинг (Фаза 2 - Данные):\n{json.dumps(details, indent=2, ensure_ascii=False)}")
        else:
            logger.error("❌ GL fetch_details вернул пустоту.")
    else:
        logger.warning("⚠️ GL не нашел объектов.")

if __name__ == "__main__":
    asyncio.run(run_tests())