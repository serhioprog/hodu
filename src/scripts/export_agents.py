import asyncio
import csv
import os
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from src.database.db import async_session_maker
from src.models.domain import Agent

async def export_agents_to_excel():
    logger.info("Собираем данные об агентах из базы...")
    
    async with async_session_maker() as session:
        # Загружаем всех агентов и сразу "подтягиваем" их устройства и токены, 
        # чтобы показать статистику
        query = select(Agent).options(
            selectinload(Agent.devices),
            selectinload(Agent.tokens)
        )
        result = await session.execute(query)
        agents = result.scalars().all()

        if not agents:
            logger.warning("В базе пока нет агентов для экспорта.")
            return

        # Сохраняем файл в папку data, которая проброшена на твой Windows
        export_path = "data/agents_export.csv"
        
        # utf-8-sig нужен, чтобы Excel правильно прочитал кириллицу/греческий
        with open(export_path, mode='w', newline='', encoding='utf-8-sig') as file:
            # Используем точку с запятой, так как европейский Excel любит ее больше
            writer = csv.writer(file, delimiter=';')
            
            # Пишем заголовки колонок
            writer.writerow([
                "ID", 
                "Имя", 
                "Email", 
                "Телефон", 
                "Статус", 
                "Дата регистрации", 
                "Привязанных устройств", 
                "Использовано ссылок"
            ])

            # Пишем данные каждого агента
            for agent in agents:
                used_tokens = sum(1 for t in agent.tokens if t.is_used)
                writer.writerow([
                    str(agent.id),
                    agent.name,
                    agent.email,
                    agent.phone or "-",
                    "Активен" if agent.is_active else "Заблокирован",
                    agent.created_at.strftime("%Y-%m-%d %H:%M"),
                    len(agent.devices),
                    used_tokens
                ])

        logger.success(f"✅ Экспорт успешно завершен!")
        logger.success(f"📄 Файл сохранен по пути: {os.path.abspath(export_path)}")

if __name__ == "__main__":
    asyncio.run(export_agents_to_excel())