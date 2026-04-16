import asyncio
from sqlalchemy import delete
from src.database.db import async_session_maker
from src.models.domain import Agent

async def cleanup():
    async with async_session_maker() as session:
        # Удаляем агента с тестовым имейлом
        query = delete(Agent).where(Agent.email == "your_email@example.com")
        await session.execute(query)
        await session.commit()
        print("✅ Тестовый агент удален.")

if __name__ == "__main__":
    asyncio.run(cleanup())