import asyncio
from loguru import logger
import uuid
from datetime import datetime, timedelta, timezone

from src.database.db import engine, async_session_maker
from src.models.domain import Agent, AuthToken

# --- ТВОИ ДАННЫЕ ---
ADMIN_NAME = "Admin (Potato)"
ADMIN_EMAIL = "sssukhomlyn@gmail.com"
# -------------------

def utcnow():
    return datetime.now(timezone.utc)

async def create_first_admin():
    async with async_session_maker() as session:
        # 1. Проверяем, есть ли уже такой агент
        from sqlalchemy import select
        query = select(Agent).where(Agent.email == ADMIN_EMAIL)
        result = await session.execute(query)
        existing_agent = result.scalars().first()

        if existing_agent:
            logger.info(f"Агент {ADMIN_EMAIL} уже существует.")
            agent = existing_agent
        else:
            # 2. Создаем Агента
            logger.info(f"Создаем агента {ADMIN_NAME} ({ADMIN_EMAIL})...")
            agent = Agent(
                name=ADMIN_NAME,
                email=ADMIN_EMAIL,
                is_active=True
            )
            session.add(agent)
            await session.commit()
            await session.refresh(agent)
            logger.success("Агент создан!")

        # 3. Создаем "Вечный" или долгосрочный Токен для входа
        # Сгенерируем красивый читаемый токен для удобства
        admin_token = f"admin-{uuid.uuid4().hex[:8]}" 
        
        logger.info("Генерируем токен доступа...")
        new_token = AuthToken(
            agent_id=agent.id,
            token=admin_token,
            is_used=False,
            # Ставим срок годности на год вперед
            expires_at=utcnow() + timedelta(days=365) 
        )
        
        session.add(new_token)
        await session.commit()

        logger.success(f"✅ Готово! Твоя ссылка для входа:")
        logger.success(f"➡️  http://localhost:8000/auth/{admin_token}")

if __name__ == "__main__":
    asyncio.run(create_first_admin())