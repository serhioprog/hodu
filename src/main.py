import asyncio
from loguru import logger

# Инициализируем наш кастомный логгер
import src.core.logger
from src.core.http_client import RequestEngine

async def main():
    logger.info("Starting Hodu Scraper - Milestone 0 Test")
    
    engine = RequestEngine()
    test_url = "https://httpbin.org/headers"
    
    try:
        # Делаем запрос через наш движок
        response = await engine.get(test_url)
        logger.success("Successfully fetched data!")
        
        # Печатаем ответ (сайт вернет нам JSON с заголовками, которые он от нас получил)
        logger.info(f"Response JSON: {response.json()}")
        
        # Симуляция ошибки для проверки логгера (если добавишь токены ТГ, придет сообщение)
        logger.error("Simulation: Triggering an error to test the alerting system.")
        
    except Exception as e:
        logger.critical(f"System failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())