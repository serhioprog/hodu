import asyncio
from sqlalchemy import text
from src.database.db import async_session_maker
from loguru import logger

async def load_inserts(file_path):
    logger.info(f"Загрузка данных из {file_path}...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        inserts = []
        current_insert = []
        in_insert = False

        # Собираем многострочные SQL-команды в единый блок
        for line in lines:
            if line.startswith("INSERT INTO"):
                in_insert = True
                current_insert.append(line)
            elif in_insert:
                current_insert.append(line)
                # В дампах MySQL конец команды вставки всегда обозначается как ");"
                if line.strip().endswith(");"):
                    inserts.append("".join(current_insert))
                    current_insert = []
                    in_insert = False

        async with async_session_maker() as session:
            inserted_count = 0
            for sql_block in inserts:
                # Очищаем синтаксис от MySQL-мусора
                clean_sql = sql_block.replace('`', '')            # Убираем обратные кавычки
                clean_sql = clean_sql.replace("\\'", "''")        # Правильное экранирование для Postgres
                
                # Принудительно переводим имена в нижний регистр, как мы задали в domain.py
                clean_sql = clean_sql.replace("SdHt_property_prefectures", "sdht_property_prefectures")
                clean_sql = clean_sql.replace("SdHt_property_municipalities", "sdht_property_municipalities")
                clean_sql = clean_sql.replace("SdHt_property_areas", "sdht_property_areas")

                try:
                    await session.execute(text(clean_sql))
                    inserted_count += 1
                except Exception as e:
                    logger.error(f"Ошибка при вставке пакета: {str(e)[:200]}...")
            
            await session.commit()
        
        # Если inserted_count > 0, значит данные реально попали в базу!
        logger.success(f"✅ Загружено {inserted_count} пакетов данных из {file_path}")
    except Exception as e:
        logger.error(f"❌ Ошибка чтения файла {file_path}: {e}")

async def main():
    # Порядок строгий: от главных к подчиненным
    await load_inserts('src/scripts/db_location_seed/SdHt_property_prefectures.sql')
    await load_inserts('src/scripts/db_location_seed/SdHt_property_municipalities.sql')
    await load_inserts('src/scripts/db_location_seed/property_areas.sql')

if __name__ == "__main__":
    asyncio.run(main())