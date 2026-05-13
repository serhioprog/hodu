"""
Scheduler service. Used ONLY from main.py's FastAPI lifespan —
no standalone `__main__`. Running two independent schedulers (one in
uvicorn, one in a CLI) would produce double jobs.
"""
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from sqlalchemy import select

from src.database.db import async_session_maker
from src.models.domain import Agent, EmailLog, Property, PropertyStatus, SystemSetting
from src.services.notifier import send_magic_links_to_agents
from src.tasks.daily_sync import daily_sync
from src.tasks.probe_job import run_probe_job

# Single process-wide scheduler instance, shared with main.py lifespan.
scheduler = AsyncIOScheduler(timezone="Europe/Athens")


async def get_setting(key: str, default: str) -> str:
    async with async_session_maker() as session:
        res = await session.execute(
            select(SystemSetting).where(SystemSetting.key == key)
        )
        setting = res.scalars().first()
        if setting is None:
            session.add(SystemSetting(key=key, value=default))
            await session.commit()
            return default
        return setting.value


# =============================================================
# JOBS
# =============================================================
async def job_parsing() -> None:
    logger.info("⏰ scheduled: daily_sync()")
    try:
        await daily_sync()
    except Exception as e:
        logger.error(f"daily_sync failed: {e}")


async def job_probe() -> None:
    """Weekly probe of promoted domains.

    Tries to demote domains stuck on higher fetch-funnel stages back to
    stage 0 (cheaper). See src.tasks.probe_job for details. Runs Mondays
    04:00 Europe/Athens — quietest hour, no overlap with daily_sync.
    """
    logger.info("⏰ scheduled: weekly funnel probe")
    try:
        await run_probe_job()
    except Exception as e:
        logger.error(f"probe_job failed: {e}")


async def job_email_report() -> None:
    logger.info("⏰ scheduled: email report")
    async with async_session_maker() as session:
        pending = (await session.execute(
            select(Property).where(
                Property.status.in_([
                    PropertyStatus.NEW,
                    PropertyStatus.PRICE_CHANGED,
                ])
            )
        )).scalars().all()

        # Получаем всех активных агентов для записи в лог
        agents = (await session.execute(
            select(Agent).where(Agent.is_active == True)
        )).scalars().all()

        if not pending:
            logger.info("📭 nothing to report")
            # Запишем в лог, что рассылка пыталась запуститься, но данных не было
            for agent in agents:
                session.add(EmailLog(
                    recipient_email=agent.email,
                    status="NO NEW DATA",
                    properties_count=0
                ))
            await session.commit()
            return

        today = datetime.now().strftime("%d.%m.%Y")
        
        try:
            stats = await send_magic_links_to_agents(today, len(pending))
            error_msg = None
        except Exception as e:
            logger.error(f"Email sending failed: {e}")
            stats = {
                "sent": 0,
                "failed": len(agents),
                "skipped": 0,
                "per_agent": [
                    {"email": a.email, "status": "FAILED",
                     "error": f"unhandled: {str(e)[:180]}"}
                    for a in agents
                ],
            }
            error_msg = str(e)

        any_delivered = stats.get("sent", 0) > 0
        per_agent = stats.get("per_agent", [])

        # Flip properties to ACTIVE if ANY agent received the report —
        # we don't want a single failed delivery to block the workflow.
        if any_delivered:
            for p in pending:
                p.status = PropertyStatus.ACTIVE
            logger.info(f"🧹 {len(pending)} properties flipped to ACTIVE")

        # Bug #3: write EmailLog rows reflecting ACTUAL per-agent outcomes.
        # Previously a single global success flag marked ALL agents as
        # DELIVERED even when some failed. Now per-recipient truth.
        if per_agent:
            for r in per_agent:
                session.add(EmailLog(
                    recipient_email=r["email"],
                    status=r["status"],          # "DELIVERED" or "FAILED"
                    properties_count=len(pending),
                    error_message=r.get("error"),
                ))
        else:
            # Notifier short-circuited (e.g. SMTP not configured) — no
            # per-agent data. Fall back to marking everyone FAILED.
            fallback_err = error_msg or "Dispatch short-circuited (SMTP not configured?)"
            for agent in agents:
                session.add(EmailLog(
                    recipient_email=agent.email,
                    status="FAILED",
                    properties_count=len(pending),
                    error_message=fallback_err,
                ))

        await session.commit()


# =============================================================
# DYNAMIC RESCHEDULING
# =============================================================
async def update_schedule() -> None:
    """
    Reads sync_time / report_time from system_settings and rewires the cron
    triggers. Self-scheduled to run every 10 minutes so admin edits apply
    without a service restart.
    """
    try:
        sync_time   = await get_setting("sync_time",   "00:01")
        report_time = await get_setting("report_time", "09:30")

        h_sync, m_sync = map(int, sync_time.split(":"))
        h_rep,  m_rep  = map(int, report_time.split(":"))

        _upsert_job("job_sync",  job_parsing,      CronTrigger(hour=h_sync, minute=m_sync))
        _upsert_job("job_report", job_email_report, CronTrigger(hour=h_rep,  minute=m_rep))

        # Weekly funnel probe — Mondays 04:00 Europe/Athens.
        # Day-of-week 0=Monday in APScheduler. Using a fixed cron rather
        # than a system_settings entry because this is a low-frequency
        # internal job; we don't expect admins to want to retune it.
        _upsert_job(
            "job_funnel_probe", job_probe,
            CronTrigger(day_of_week=0, hour=4, minute=0),
        )

        if not scheduler.get_job("job_update_schedule"):
            scheduler.add_job(
                update_schedule, "interval",
                minutes=10, id="job_update_schedule",
                misfire_grace_time=600,  # Bug #27
            )

        logger.info(f"🔄 schedule refreshed: sync={sync_time} report={report_time} probe=Mon 04:00")

    except Exception as e:
        logger.error(f"schedule refresh failed (keeping previous): {e}")


def _upsert_job(job_id: str, func, trigger, *, misfire_grace_time: int = 600) -> None:
    """Bug #27: APScheduler's default behaviour drops missed jobs entirely.
    With misfire_grace_time=600s (10 min), if the process restarts within
    10 minutes of a scheduled fire time, the job catches up. Default keeps
    cron jobs reliable across container reboots that overlap with cron
    moments (e.g. compose recreate at 00:00:30 would have skipped the
    00:01 daily_sync — now it catches up on restart)."""
    if scheduler.get_job(job_id):
        scheduler.reschedule_job(job_id, trigger=trigger)
    else:
        scheduler.add_job(
            func, trigger, id=job_id,
            misfire_grace_time=misfire_grace_time,
        )


# =============================================================
# LIFESPAN HOOKS
# =============================================================
async def start_scheduler() -> None:
    """Called from FastAPI lifespan on app startup."""
    if scheduler.running:
        return
    await update_schedule()
    scheduler.start()
    logger.info("🚀 APScheduler started inside FastAPI process")


async def stop_scheduler() -> None:
    """Called from FastAPI lifespan on app shutdown."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("🛑 APScheduler stopped")
