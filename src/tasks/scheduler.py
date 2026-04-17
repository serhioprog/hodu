import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from datetime import datetime
from sqlalchemy import select, func

from src.database.db import async_session_maker
from src.models.domain import Property, PropertyStatus, SystemSetting # Добавили SystemSetting
from src.tasks.daily_sync import daily_sync
from src.services.notifier import send_magic_links_to_agents

scheduler = AsyncIOScheduler()

async def get_setting(key: str, default: str):
    """Получает настройку из базы или создает дефолтную"""
    async with async_session_maker() as session:
        result = await session.execute(select(SystemSetting).where(SystemSetting.key == key))
        setting = result.scalars().first()
        if not setting:
            setting = SystemSetting(key=key, value=default)
            session.add(setting)
            await session.commit()
            return default
        return setting.value

async def job_parsing():
    """Задача — Синхронизация данных"""
    logger.info("⏰ Запуск ночного скрапинга...")
    try:
        await daily_sync()
        logger.success("✅ Синхронизация завершена.")
    except Exception as e:
        logger.error(f"❌ Ошибка в цикле синхронизации: {e}")

async def job_email_report():
    """Задача — Рассылка отчета и сброс статусов"""
    logger.info("⏰ Подготовка утреннего отчета...")
    async with async_session_maker() as session:
        # Достаем все новинки и изменения цен
        query = select(Property).where(
            Property.status.in_([PropertyStatus.NEW.value, PropertyStatus.PRICE_CHANGED.value])
        )
        result = await session.execute(query)
        pending_props = result.scalars().all()
        count = len(pending_props)

        if count > 0:
            today_str = datetime.now().strftime("%d.%m.%Y")
            
            # Отправляем письмо
            await send_magic_links_to_agents(today_str, count)
            logger.success(f"📧 Отчет за {today_str} отправлен ({count} объектов).")

            # 🔥 ГЛАВНАЯ МАГИЯ: Переводим всё в ACTIVE после отправки письма!
            for p in pending_props:
                p.status = PropertyStatus.ACTIVE.value
            
            await session.commit()
            logger.info("🧹 Статусы NEW и PRICE DROP успешно переведены в ACTIVE.")
        else:
            logger.info("📭 Нет новых объектов для отчета. Письмо не отправлено.")

async def update_schedule():
    """
    Проверяет базу данных и обновляет расписание. 
    Вызывается при старте и раз в 10 минут.
    """
    sync_time = await get_setting('sync_time', '00:01')
    report_time = await get_setting('report_time', '09:30')
    
    # Парсим время
    h_sync, m_sync = map(int, sync_time.split(':'))
    h_repo, m_repo = map(int, report_time.split(':'))

    # Очищаем старые задачи, чтобы не дублировались
    scheduler.remove_all_jobs()
    
    # Добавляем основные задачи
    scheduler.add_job(job_parsing, CronTrigger(hour=h_sync, minute=m_sync), id='job_sync')
    scheduler.add_job(job_email_report, CronTrigger(hour=h_repo, minute=m_repo), id='job_report')
    
    # Добавляем задачу самообновления (проверка настроек в БД каждые 10 минут)
    scheduler.add_job(update_schedule, 'interval', minutes=10, id='job_update_schedule')
    
    logger.info(f"🔄 Расписание обновлено: Парсинг в {sync_time}, Рассылка в {report_time}")

async def main():
    # Первый запуск для настройки
    await update_schedule()
    
    scheduler.start()
    logger.info("🚀 Планировщик запущен и слушает базу данных...")

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())