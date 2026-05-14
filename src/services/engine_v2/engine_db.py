"""
Async DB engine for engine v2 — isolated from src.core.config.

Per the engine-isolation rule (memory: feedback_engine_isolation), this
module reads POSTGRES_* env vars directly via os.getenv and builds its
own async engine. Does NOT import src.core.config or src.database.db.

Loads .env via python-dotenv at module init, but respects pre-existing
env vars (so callers can override POSTGRES_HOST etc. before import).

ORM models are imported from src.models.domain (the one allowed src.*
import — schema is shared by design).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import AsyncGenerator

from loguru import logger
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Load .env at import time, but only fill in vars that aren't already set.
# This lets a caller set POSTGRES_HOST=localhost before importing this
# module and have that override win over the docker-targeted .env value.
try:
    from dotenv import load_dotenv  # python-dotenv ships with pydantic-settings
    _project_root = Path(__file__).resolve().parents[3]  # experiments/.../src/.. = hodu/
    _env_path = _project_root / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=False)
except ImportError:
    pass  # if dotenv missing, env vars must be set externally


def _build_dsn() -> str:
    """Construct the asyncpg DSN from env vars. Raises if required vars missing."""
    try:
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        db = os.environ["POSTGRES_DB"]
    except KeyError as e:
        raise RuntimeError(
            f"engine_db: required env var {e.args[0]!r} not set. "
            f"Set POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB in .env or environment."
        ) from e
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"


engine = create_async_engine(
    _build_dsn(),
    echo=False,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
    pool_size=10,
    max_overflow=5,
)


@event.listens_for(engine.sync_engine, "connect")
def _configure_hnsw_ef_search(dbapi_conn, connection_record) -> None:
    """HNSW per-session tuning: ef_search=80 for higher recall on cosine NN.
    Mirrors src.database.db's setting (consistent behavior with main app)."""
    try:
        cursor = dbapi_conn.cursor()
        cursor.execute("SET hnsw.ef_search = 80")
        cursor.close()
    except Exception as e:  # pragma: no cover — pgvector may be absent in tests
        logger.warning(f"engine_db: HNSW ef_search tuning skipped: {e}")


async_session_maker = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager for a session — for FastAPI / dependency injection."""
    async with async_session_maker() as session:
        yield session
