import asyncio
import pandas as pd
from sqlalchemy import text
from src.database.db import async_session_maker
from loguru import logger
import os

async def export_to_excel(filename="data/full_real_estate_report.xlsx"):
    logger.info("📊 Формируем ЧИСТЫЙ отчет (только нужные данные)...")
    
    query = """
    SELECT 
        p.site_property_id as "ID объекта",
        p.category as "Тип недвижимости",
        p.price as "Цена (€)",
        p.area as "Район (Area)",
        p.subarea as "Подрайон (Subarea)",
        p.size_sqm as "Площадь дома (м2)",
        p.land_size_sqm as "Площадь участка (м2)",
        p.bedrooms as "Спальни",
        p.bathrooms as "Ванные",
        p.levels as "Уровни/Этажи",
        p.year_built as "Год постройки",
        p.site_last_updated as "Обновлено на сайте",
        p.latitude as "Широта",
        p.longitude as "Долгота",
        p.url as "Ссылка",
        p.description as "Описание",
        COUNT(m.id) as "Кол-во фото"
    FROM properties p
    LEFT JOIN media m ON p.id = m.property_id
    GROUP BY p.id
    ORDER BY p.price DESC NULLS LAST;
    """
    
    try:
        async with async_session_maker() as session:
            result = await session.execute(text(query))
            rows = result.fetchall()
            
            if not rows:
                logger.warning("☹️ База данных пуста.")
                return
                
            df = pd.DataFrame(rows, columns=result.keys())
            os.makedirs("data", exist_ok=True)
            
            df.to_excel(filename, index=False)
            logger.success(f"🚀 ИДЕАЛЬНЫЙ ОТЧЕТ ГОТОВ: {filename}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при формировании отчета: {e}")

if __name__ == "__main__":
    asyncio.run(export_to_excel())