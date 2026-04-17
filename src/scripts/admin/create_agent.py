import asyncio
from loguru import logger
import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import select

from src.database.db import async_session_maker
from src.models.domain import Agent, AuthToken

# --- ДАННЫЕ ДЛЯ ПРЕЗЕНТАЦИИ ---
AGENT_NAME = "Aliftina Kagurasovna"
AGENT_EMAIL = "ssergeyproperty@gmail.com"
# ------------------------------

def utcnow():
    return datetime.now(timezone.utc)

async def create_test_client():
    async with async_session_maker() as session:
        # Проверяем, есть ли уже такой клиент
        query = select(Agent).where(Agent.email == AGENT_EMAIL)
        result = await session.execute(query)
        existing_agent = result.scalars().first()

        if existing_agent:
            logger.info(f"Клиент {AGENT_EMAIL} уже существует.")
            agent = existing_agent
        else:
            # Создаем Клиента (is_admin=False сработает по умолчанию)
            logger.info(f"Создаем клиента {AGENT_NAME} ({AGENT_EMAIL})...")
            agent = Agent(
                name=AGENT_NAME,
                email=AGENT_EMAIL,
                is_active=True
            )
            session.add(agent)
            await session.commit()
            await session.refresh(agent)
            logger.success("Клиент успешно создан!")

        # Генерируем читаемый токен
        client_token = f"client-{uuid.uuid4().hex[:8]}" 
        
        logger.info("Генерируем токен доступа...")
        new_token = AuthToken(
            agent_id=agent.id,
            token=client_token,
            is_used=False,
            expires_at=utcnow() + timedelta(days=30) 
        )
        
        session.add(new_token)
        await session.commit()

        logger.success(f"✅ Готово! Ссылка для входа КЛИЕНТА:")
        logger.success(f"➡️  http://localhost:8000/auth/{client_token}")

if __name__ == "__main__":
    asyncio.run(create_test_client())