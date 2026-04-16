import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from loguru import logger
import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import select

from src.database.db import async_session_maker
from src.models.domain import Agent, AuthToken

# Настройки из .env
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
APP_URL = os.getenv("APP_URL", "http://localhost:8000") 

def utcnow():
    return datetime.now(timezone.utc)

async def send_magic_links_to_agents(report_date_str: str, properties_count: int):
    """Генерирует токены и рассылает персональные письма активным агентам"""
    
    if not SMTP_USER or not SMTP_PASS:
        logger.error("❌ Ошибка: SMTP_USER или SMTP_PASS не заданы в .env")
        return

    if properties_count == 0:
        logger.info("Нет новых объектов. Пропускаем рассылку.")
        return

    logger.info(f"Начинаем рассылку для {properties_count} объектов...")

    async with async_session_maker() as session:
        # Берем только активных агентов
        query = select(Agent).where(Agent.is_active == True)
        result = await session.execute(query)
        agents = result.scalars().all()

        if not agents:
            logger.warning("Активные агенты не найдены в базе.")
            return

        try:
            # Подключаемся к Gmail
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            
            for agent in agents:
                # Создаем токен
                token_str = uuid.uuid4().hex
                new_token = AuthToken(
                    agent_id=agent.id,
                    token=token_str,
                    is_used=False,
                    expires_at=utcnow() + timedelta(days=2)
                )
                session.add(new_token)
                await session.commit()

                magic_link = f"{APP_URL}/auth/{token_str}"

                # Собираем письмо
                msg = MIMEMultipart("alternative")
                msg["Subject"] = f"hodu. | Daily Report {report_date_str}"
                msg["From"] = f"hodu. <{SMTP_USER}>"
                msg["To"] = agent.email

                html = f"""
                <html>
                <body style="font-family: sans-serif; color: #222; background-color: #f7f7f7; padding: 20px;">
                    <div style="max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 12px; border: 1px solid #eee;">
                        <h1 style="color: #ff385c; margin-bottom: 20px;">hodu.</h1>
                        <p>Hello, <b>{agent.name}</b>!</p>
                        <p>The real estate report for <b>{report_date_str}</b> is ready.</p>
                        <p style="font-size: 20px; font-weight: bold; margin: 20px 0; color: #111;">
                            {properties_count} new updates (listings & price drops) are ready for review.
                        </p>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="{magic_link}" style="background: #ff385c; color: white; padding: 15px 25px; text-decoration: none; border-radius: 8px; font-weight: bold;">
                                View Report
                            </a>
                        </div>
                        <p style="font-size: 12px; color: #888;">This is a private secure link. Please do not forward this email.</p>
                    </div>
                </body>
                </html>
                """
                msg.attach(MIMEText(html, "html"))
                server.sendmail(SMTP_USER, agent.email, msg.as_string())
                logger.success(f"📧 Sent to {agent.email}")

            server.quit()
        except Exception as e:
            logger.error(f"SMTP Error: {e}")