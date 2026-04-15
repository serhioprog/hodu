from sqlalchemy import text
from src.database.db import async_session_maker
from loguru import logger

class GeoMatcher:
    async def load_locations(self, session=None):
        pass

    async def find_best_match(self, lat: float = None, lng: float = None, area_name: str = None):
        result = {
            "prefecture": "Не определено",
            "municipality": "Не определено",
            "exact_district": area_name or "Не определено"
        }

        if not lat or not lng:
            return result

        async with async_session_maker() as session:
            try:
                # 1. Мы точно знаем имена таблиц из domain.py (все маленькими буквами)
                # 2. Используем NULLIF('', lat), чтобы не было ошибок при пустых строках в координатах
                query = text("""
                    SELECT a.area_en, m.municipality_en, p.prefecture_en
                    FROM sdht_property_areas a
                    JOIN sdht_property_municipalities m ON a.municipality_id = m.id
                    JOIN sdht_property_prefectures p ON m.prefecture_id = p.id
                    ORDER BY (
                        POWER(CAST(NULLIF(a.lat, '') AS FLOAT) - :lat, 2) + 
                        POWER(CAST(NULLIF(a.lng, '') AS FLOAT) - :lng, 2)
                    ) ASC
                    LIMIT 1
                """)
                
                db_res = await session.execute(query, {"lat": lat, "lng": lng})
                row = db_res.fetchone()
                
                if row:
                    result["exact_district"] = row[0]
                    result["municipality"] = row[1]
                    result["prefecture"] = row[2]
                    
            except Exception as e:
                logger.error(f"Ошибка в GeoMatcher: {e}")

        return result

geo_matcher = GeoMatcher()