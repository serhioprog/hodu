from typing import AsyncGenerator
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from loguru import logger

from src.core.config import settings

engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
    pool_size=20,
    max_overflow=10,
)

# === HNSW per-session tuning =====================================
# Выставляется на каждое физическое соединение, один раз при его создании.
# ef_search = 80: увеличивает recall для cosine NN, мы ок с небольшим ростом latency.
@event.listens_for(engine.sync_engine, "connect")
def _configure_hnsw_ef_search(dbapi_conn, connection_record):
    try:
        cursor = dbapi_conn.cursor()
        cursor.execute("SET hnsw.ef_search = 80")
        cursor.close()
    except Exception as e:
        # GUC может отсутствовать если pgvector < 0.6 или extension не поднят.
        # Не падаем — логируем и продолжаем с дефолтом (ef_search=40).
        logger.warning(f"HNSW ef_search tuning skipped: {e}")

async_session_maker = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session