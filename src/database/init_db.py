import asyncio
from loguru import logger
from src.database.db import engine, Base
from src.models.domain import Property, Media, PriceHistory, SdhtPrefecture, SdhtMunicipality, SdhtArea 

async def init_models():
    async with engine.begin() as conn:
        #удаление всей базы
        logger.info("Удаляем старые кривые таблицы...")
        await conn.run_sync(Base.metadata.drop_all) 
        
        logger.info("Создаем новые чистые таблицы PostgreSQL...")
        await conn.run_sync(Base.metadata.create_all)
        
    logger.success("✅ База данных успешно пересоздана!")

if __name__ == "__main__":
    asyncio.run(init_models())