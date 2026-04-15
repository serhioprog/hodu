import asyncio
from sqlalchemy import text
from src.database.db import async_session_maker
from loguru import logger

async def reset_site():
    logger.info("🧹 Удаляем старые 'пустые' объекты realestatecenter.gr...")
    async with async_session_maker() as session:
        # Удаляем только объекты этого сайта (glrealestate не трогаем!)
        await session.execute(text("DELETE FROM properties WHERE source_domain = 'realestatecenter.gr';"))
        await session.commit()
        logger.success("✨ База очищена! Теперь скрипт подумает, что все 244 объекта - это новинки.")

if __name__ == "__main__":
    asyncio.run(reset_site())