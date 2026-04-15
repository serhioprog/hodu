import asyncio
import re
import os
from sqlalchemy import text
from loguru import logger
from src.database.db import async_session_maker
from src.models.domain import LocationArea

async def seed_locations(sql_file_path="src/scripts/property_areas.sql"):
    if not os.path.exists(sql_file_path):
        logger.error(f"❌ Файл {sql_file_path} не найден! Положи его в корень проекта.")
        return

    logger.info("🔍 Читаем SQL дамп и извлекаем локации...")
    
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Магия регулярных выражений: вытаскиваем только первые 8 полей из каждой скобки VALUES
    # (id, country, pref, muni, name_en, name_el, lat, lng)
    pattern = r"\((\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*'([^']*)',\s*'([^']*)',\s*'([\d.]+)',\s*'([\d.]+)'"
    matches = re.finditer(pattern, sql_content)
    
    locations_to_insert = []
    for match in matches:
        locations_to_insert.append(
            LocationArea(
                id=int(match.group(1)),
                country_id=int(match.group(2)),
                prefecture_id=int(match.group(3)),
                municipality_id=int(match.group(4)),
                area_en=match.group(5),
                area_el=match.group(6),
                lat=float(match.group(7)),
                lng=float(match.group(8))
            )
        )

    if not locations_to_insert:
        logger.warning("☹️ Не удалось найти данные в файле. Проверь формат.")
        return

    logger.info(f"✅ Найдено {len(locations_to_insert)} районов. Загружаем в базу...")

    async with async_session_maker() as session:
        try:
            # Очищаем таблицу перед загрузкой (чтобы не было дублей при перезапуске)
            await session.execute(text("TRUNCATE TABLE location_areas CASCADE;"))
            
            session.add_all(locations_to_insert)
            await session.commit()
            logger.success("🚀 Справочник локаций успешно загружен в PostgreSQL!")
        except Exception as e:
            await session.rollback()
            logger.error(f"❌ Ошибка при сохранении в БД: {e}")

if __name__ == "__main__":
    asyncio.run(seed_locations())