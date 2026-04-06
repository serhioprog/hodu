import asyncio
from loguru import logger
from src.database.db import engine, Base

# Важно импортировать модели, чтобы Base о них узнал
from src.models.domain import Property, Media, PriceHistory 

async def init_models():
    async with engine.begin() as conn:
        logger.info("Dropping old tables... (if exist)")
        # В проде drop_all убирают, но для старта это удобно
        await conn.run_sync(Base.metadata.drop_all) 
        
        logger.info("Creating new tables...")
        await conn.run_sync(Base.metadata.create_all)
        
    logger.success("Database tables created successfully!")

if __name__ == "__main__":
    asyncio.run(init_models())