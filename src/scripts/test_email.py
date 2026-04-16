import asyncio
from datetime import datetime
from loguru import logger
from src.services.notifier import send_magic_links_to_agents

async def run_test():
    today_str = datetime.now().strftime("%d.%m.%Y")
    logger.info(f"Запускаем тестовую рассылку за {today_str}...")
    
    # Имитируем, что нашли 15 объектов
    await send_magic_links_to_agents(report_date_str=today_str, properties_count=15)
    
    logger.success("Тестовый запуск завершен. Иди проверять почту!")

if __name__ == "__main__":
    asyncio.run(run_test())