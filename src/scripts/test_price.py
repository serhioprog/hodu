import asyncio
from sqlalchemy import update
from src.database.db import async_session_maker
from src.models.domain import Property
from loguru import logger

async def make_fake_discount():
    async with async_session_maker() as session:
        # Ставим всем объектам в базе искусственно завышенную цену (9.999.999 евро)
        await session.execute(update(Property).values(price=9999999))
        await session.commit()
        logger.success("✅ Мы обманули базу! Теперь все виллы стоят 9 999 999 €. Запускай daily_sync!")

if __name__ == "__main__":
    asyncio.run(make_fake_discount())