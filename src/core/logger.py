import sys
import httpx
from loguru import logger
from src.core.config import settings

def send_telegram_alert(message: str):
    """Синхронная отправка алерта в Telegram (выполняется Loguru в фоне)"""
    if not settings.TG_BOT_TOKEN or not settings.TG_CHAT_ID:
        return
        
    url = f"https://api.telegram.org/bot{settings.TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": settings.TG_CHAT_ID,
        "text": f"🚨 <b>Hodu Scraper Alert</b>\n\n<pre>{message}</pre>",
        "parse_mode": "HTML"
    }
    try:
        httpx.post(url, json=payload, timeout=5.0)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

def setup_logger():
    # Удаляем стандартный логгер
    logger.remove() 
    
    # Логи в консоль
    logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
    
    # Логи в файл (ротация каждый день)
    logger.add("logs/scraper_{time:YYYY-MM-DD}.log", rotation="00:00", retention="7 days", level="INFO")
    
    # Критические ошибки отправляем в Telegram (enqueue=True делает это асинхронно, не тормозя парсер)
    logger.add(
        lambda msg: send_telegram_alert(msg), 
        level="ERROR", 
        enqueue=True 
    )

# Инициализируем логгер при импорте модуля
setup_logger()