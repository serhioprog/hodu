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

        p.site_property_id as "ID Origin",

        p.source_domain || ' (' || p.status || ')' as "Сайт и Статус",

        p.category as "Type",

        p.price as "Price €",

       

        -- ИСПРАВЛЕНИЕ 1: Добавляем красивые разделители (точки) в миллионы

        STRING_AGG(DISTINCT

            REPLACE(TO_CHAR(ph.old_price, 'FM999G999G999'), ',', '.') || '€ -> ' ||

            REPLACE(TO_CHAR(ph.new_price, 'FM999G999G999'), ',', '.') || '€ (' ||

            TO_CHAR(ph.changed_at, 'DD.MM.YYYY') || ')', '  |  '

        ) as "История изменения цены",

       

        TO_CHAR(p.last_checked_at, 'DD.MM.YYYY HH24:MI') as "Последняя проверка ботом",

        p.area as "Area",

        p.subarea as "Subarea",

        p.size_sqm as "House м2",

        p.land_size_sqm as "Land м2",

        p.bedrooms as "Bedrooms",

        p.bathrooms as "Bathrooms",

        p.levels as "Levels",

        p.year_built as "Year Built",

       

        -- ИСПРАВЛЕНИЕ 2: Принудительно кастуем в ::text

        p.extra_features::text as "Extra Features",

       

        p.site_last_updated as "Last Updated",

        p.latitude as "Широта",

        p.longitude as "Долгота",

        p.url as "Link",

        p.description as "Description",

        p.calc_prefecture as "Префектура (ARIS)",

        p.calc_municipality as "Муниципалитет (ARIS)",

        p.calc_area as "Точный район (ARIS)",

        COUNT(DISTINCT m.id) as "Кол-во фото"

    FROM properties p

    LEFT JOIN media m ON p.id = m.property_id

    LEFT JOIN price_history ph ON p.id = ph.property_id

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

               

            # Превращаем в Pandas DataFrame

            df = pd.DataFrame(rows, columns=result.keys())

           

            # Создаем папку, если ее нет

            os.makedirs("data", exist_ok=True)

           

            # Сохраняем без багованного авто-размера колонок

            df.to_excel(filename, index=False)

            logger.success(f"🚀 ИДЕАЛЬНЫЙ ОТЧЕТ ГОТОВ: {filename}")

           

    except Exception as e:

        logger.error(f"❌ Ошибка при формировании отчета: {e}")



if __name__ == "__main__":

    asyncio.run(export_to_excel())