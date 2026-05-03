"""
Safe migration runner with applied-migrations tracking.

Behavior:
  * First run (table `applied_migrations` does not exist):
      apply ALL .sql files in lexicographic order.
      Migration 006 creates the tracking table and registers itself + earlier ones.
  * Subsequent runs:
      skip any .sql whose filename is already in applied_migrations.
      Apply new ones, then INSERT into applied_migrations.

Why this design (and not Alembic):
  * Our migrations are idempotent SQL files. The only thing we need from
    a migration framework is "don't apply this twice, even if a step
    inside is non-idempotent". That's a 5-line feature, not a framework.
  * No autogeneration, no down-migrations — we're not at that scale yet.

Usage:
    docker exec hodu_scraper python -m src.database.init_db
"""
import asyncio
import sys
from pathlib import Path

import asyncpg
from loguru import logger

from src.core.config import settings

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
TRACKING_TABLE = "applied_migrations"


async def _tracking_table_exists(conn: asyncpg.Connection) -> bool:
    """Return True iff applied_migrations table is present in DB."""
    row = await conn.fetchrow(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_name = $1) AS exists",
        TRACKING_TABLE,
    )
    return bool(row["exists"])


async def _already_applied(conn: asyncpg.Connection) -> set[str]:
    """Read filenames already present in applied_migrations."""
    rows = await conn.fetch(f"SELECT filename FROM {TRACKING_TABLE}")
    return {r["filename"] for r in rows}


async def _record_applied(conn: asyncpg.Connection, filename: str) -> None:
    """Mark filename as applied. Idempotent via ON CONFLICT."""
    await conn.execute(
        f"INSERT INTO {TRACKING_TABLE} (filename) VALUES ($1) "
        f"ON CONFLICT (filename) DO NOTHING",
        filename,
    )


async def _apply_one(conn: asyncpg.Connection, path: Path) -> None:
    """Run one migration file as a single 'simple query' (handles DO blocks)."""
    logger.info(f"→ applying {path.name}")
    sql = path.read_text(encoding="utf-8")
    await conn.execute(sql)
    logger.success(f"✓ {path.name} done")


async def run_migrations() -> None:
    if not MIGRATIONS_DIR.exists():
        logger.error(f"Migrations directory not found: {MIGRATIONS_DIR}")
        sys.exit(1)

    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        logger.warning(f"No .sql files in {MIGRATIONS_DIR}")
        return

    logger.info(f"Found {len(files)} migration file(s). Connecting…")
    conn = await asyncpg.connect(settings.database_url_sync)

    try:
        # On the very first run, the tracking table doesn't exist yet.
        # In that case we apply everything; migration 006 creates the table
        # and registers 001-006 in one go.
        has_tracking = await _tracking_table_exists(conn)
        applied: set[str] = await _already_applied(conn) if has_tracking else set()

        skipped = 0
        ran = 0

        for f in files:
            if f.name in applied:
                logger.info(f"⊝ skipped {f.name} (already applied)")
                skipped += 1
                continue

            await _apply_one(conn, f)
            ran += 1

            # After 006 the tracking table exists. From here on we record
            # every applied file (including 006 itself, but ON CONFLICT
            # handles the case where 006 already inserted itself).
            if await _tracking_table_exists(conn):
                await _record_applied(conn, f.name)

        logger.success(
            f"🏁 Migrations done: {ran} applied, {skipped} skipped, "
            f"{len(files)} total."
        )

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migrations())