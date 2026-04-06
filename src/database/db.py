from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

from src.core.config import settings

# Создаем асинхронный движок
engine = create_async_engine(
    settings.database_url,
    echo=False, # Поставь True, если захочешь видеть все SQL-запросы в консоли
    future=True,
    pool_size=20,
    max_overflow=10
)

# Фабрика сессий
async_session_maker = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency Injection для получения сессии БД"""
    async with async_session_maker() as session:
        yield session