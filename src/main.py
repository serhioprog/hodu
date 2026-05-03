"""
FastAPI entry point.
Mounts static /data, boots the APScheduler inside the uvicorn process,
serves web UI + minimal admin API.
"""
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Optional

from fastapi import FastAPI, Form, HTTPException, Query, Request, BackgroundTasks
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger
from sqlalchemy import Date, cast, delete, func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer, selectinload

from sqlalchemy.dialects.postgresql import insert
from src.models.domain import AIDuplicateFeedback

from src.core.config import settings
from src.database.db import async_session_maker
from src.models.domain import (
    Agent, AgentDevice, AuthToken, ClusterStatus,
    PowerProperty, Property, PropertyCluster, PropertyStatus,
    SystemSetting, ScraperLog, EmailLog
)
from src.tasks.scheduler import start_scheduler, stop_scheduler, job_email_report, job_parsing
from src.web.csrf import CSRFMiddleware

from src.tasks.scheduler import job_email_report

from fastapi.staticfiles import StaticFiles
from sqlalchemy import text as sql_text

# =============================================================
# LIFESPAN — start/stop APScheduler inside uvicorn's event loop
# =============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[lifespan] starting scheduler")
    await start_scheduler()
    try:
        yield
    finally:
        logger.info("[lifespan] stopping scheduler")
        await stop_scheduler()

        # Close any warm Playwright browsers.
        # Imported lazily so the app still starts on hosts where
        # Playwright isn't installed (Sprint 1 deployment).
        try:
            from src.scrapers.fetchers.browser_pool import browser_pool
            await browser_pool.close_all()
        except ImportError:
            pass  # Playwright not installed — nothing to close
        except Exception as e:
            logger.warning(f"[lifespan] error closing browser pool: {e}")


app = FastAPI(title="Hodu Real Estate", lifespan=lifespan)
app.add_middleware(CSRFMiddleware)
app.mount("/data", StaticFiles(directory="/app/data"), name="data")

templates = Jinja2Templates(directory="src/web/templates")

# Подключаем папку со статикой
app.mount("/static", StaticFiles(directory="src/web/static"), name="static")

# =============================================================
# Jinja helpers
# =============================================================
def fix_slashes(path: Optional[str]) -> str:
    if not path:
        return ""
    return path.replace("\\", "/")


def euro(value) -> str:
    """42000 -> '42 000 €'. None -> '—'."""
    if value is None:
        return "—"
    try:
        return f"{int(value):,}".replace(",", " ") + " €"
    except (TypeError, ValueError):
        return "—"

# ==========================Time zone===================================   
from zoneinfo import ZoneInfo
from datetime import datetime, timezone

# Display timezone for the admin/web UI. Source data is stored in UTC; the
# filter converts on render. Keeping it server-side (rather than browser JS)
# ensures consistent display in emails, exports, and screenshots — no flicker.
_DISPLAY_TZ = ZoneInfo("Europe/Athens")


def local_dt_filter(value, fmt: str = "%d.%m.%Y %H:%M") -> str:
    """
    Render a UTC datetime as Europe/Athens local time.

    Accepts:
      - aware datetime (any TZ)   → converted to Europe/Athens
      - naive datetime            → assumed UTC, then converted
      - None / falsy              → "—"
      - non-datetime              → str(value) (defensive: don't crash UI)

    Format default: "DD.MM.YYYY HH:MM" (e.g. "02.05.2026 02:41").
    """
    if not value:
        return "—"
    if not isinstance(value, datetime):
        return str(value)
    # Treat naive datetimes as UTC (matches how SQLAlchemy returns them
    # from postgres TIMESTAMP WITH TIME ZONE columns when the asyncpg driver
    # has been configured with timezone='UTC')
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(_DISPLAY_TZ).strftime(fmt)


templates.env.filters["local_dt"] = local_dt_filter
# ==========================Time zone===================================   
templates.env.filters["fix_path"] = fix_slashes
templates.env.filters["euro"] = euro

# Helper for templates that need "now" at render time (e.g. cluster ageing)
def _now_utc():
    return datetime.now(timezone.utc)

templates.env.globals["now_utc"] = _now_utc

def _ctx(request: Request, **extra) -> dict:
    """Standard Jinja context with CSRF token auto-injected."""
    base = {
        "request": request,
        "csrf_token": getattr(request.state, "csrf_token", ""),
    }
    base.update(extra)
    return base


# =============================================================
# AUTH HELPERS
# =============================================================
async def get_current_agent(request: Request, session: AsyncSession) -> Optional[Agent]:
    cookie = request.cookies.get(settings.SESSION_COOKIE_NAME)
    if not cookie:
        return None

    q = (
        select(AgentDevice)
        .where(AgentDevice.device_cookie == cookie)
        .options(selectinload(AgentDevice.agent))
    )
    device = (await session.execute(q)).scalars().first()

    if device and device.agent and device.agent.is_active:
        # touch last_seen for audit
        device.last_seen_at = datetime.now(timezone.utc)
        await session.commit()
        return device.agent
    return None


async def get_current_admin(request: Request, session: AsyncSession) -> Optional[Agent]:
    agent = await get_current_agent(request, session)
    if agent and agent.is_admin:
        return agent
    return None


def _error(request: Request, message: str, status_code: int = 200):
    return templates.TemplateResponse(
        "error.html",
        _ctx(request, message=message),
        status_code=status_code,
    )


# =============================================================
# AUTHENTICATION (magic link)
# =============================================================
@app.get("/auth/{token}")
async def authenticate(request: Request, token: str):
    async with async_session_maker() as session:
        q = (
            select(AuthToken)
            .where(AuthToken.token == token)
            .options(selectinload(AuthToken.agent))
        )
        auth_token = (await session.execute(q)).scalars().first()

        if auth_token is None:
            return _error(request, "This link is not valid.")

        # TTL enforcement — the big missing fix
        now = datetime.now(timezone.utc)
        if auth_token.expires_at and auth_token.expires_at < now:
            return _error(request, "This link has expired. Please request a new report.")
        if auth_token.is_used:
            return _error(request, "This link has already been used.")
        if not auth_token.agent or not auth_token.agent.is_active:
            return _error(request, "Your account is not active.")

        auth_token.is_used = True

        device_cookie = str(uuid.uuid4())
        session.add(AgentDevice(
            agent_id=auth_token.agent_id,
            device_cookie=device_cookie,
            user_agent_str=request.headers.get("user-agent", "Unknown")[:500],
        ))
        await session.commit()

        response = RedirectResponse(url="/daily-report", status_code=302)
        response.set_cookie(
            key=settings.SESSION_COOKIE_NAME,
            value=device_cookie,
            max_age=30 * 24 * 3600,
            httponly=True,
            samesite="lax",
            secure=settings.COOKIE_SECURE,
        )
        return response


@app.post("/logout")
async def logout(request: Request):
    """Revokes the current device on both sides (DB + browser)."""
    cookie = request.cookies.get(settings.SESSION_COOKIE_NAME)
    if cookie:
        async with async_session_maker() as session:
            await session.execute(
                delete(AgentDevice).where(AgentDevice.device_cookie == cookie)
            )
            await session.commit()

    response = RedirectResponse(url="/auth/revoked", status_code=302)
    response.delete_cookie(settings.SESSION_COOKIE_NAME)
    return response


@app.get("/auth/revoked")
async def auth_revoked(request: Request):
    return _error(request, "You have been signed out on this device.")


# =============================================================
# AGENT DASHBOARD
# =============================================================
@app.get("/daily-report")
async def daily_report(
    request: Request,
    report_date: date = Query(default=None),
):
    if not report_date:
        report_date = datetime.utcnow().date()

    async with async_session_maker() as session:
        agent = await get_current_agent(request, session)
        if not agent:
            return _error(request, "Please use your registered device to view.")

        q = (
            select(Property)
            .options(
                selectinload(Property.media),
                defer(Property.embedding),
                defer(Property.image_phashes),
                defer(Property.content_hash),
            )
            .where(
                or_(
                    cast(Property.created_at, Date) == report_date,
                    cast(Property.updated_at, Date) == report_date,
                )
            )
            .order_by(Property.price.desc())
        )
        properties = (await session.execute(q)).scalars().all()

    return templates.TemplateResponse(
        "dashboard.html",
        _ctx(
            request,
            properties=properties,
            date=report_date.strftime("%d.%m.%Y"),
            current_user=agent,
        ),
    )


@app.get("/property/{prop_id}")
async def property_detail(request: Request, prop_id: str):
    async with async_session_maker() as session:
        agent = await get_current_agent(request, session)
        if not agent:
            return _error(request, "Please use your registered device to view.")

        q = (
            select(Property)
            .options(
                selectinload(Property.media),
                defer(Property.embedding),
                defer(Property.image_phashes),
                selectinload(Property.price_history),
                defer(Property.content_hash),
            )
            .where(Property.id == prop_id)
        )
        prop = (await session.execute(q)).scalars().first()

        if not prop:
            raise HTTPException(status_code=404, detail="Object not found")

        return templates.TemplateResponse(
            "property_detail.html",
            _ctx(request, prop=prop, current_user=agent),
        )


# =============================================================
# ADMIN (single page, tabs)
# =============================================================
@app.get("/admin")
async def admin_dashboard(request: Request):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            return RedirectResponse(url="/daily-report")

        sync_res = await session.execute(
            select(SystemSetting).where(SystemSetting.key == "sync_time")
        )
        repo_res = await session.execute(
            select(SystemSetting).where(SystemSetting.key == "report_time")
        )
        sync_time = sync_res.scalars().first()
        repo_time = repo_res.scalars().first()

        users = (await session.execute(
            select(Agent).order_by(Agent.created_at.desc())
        )).scalars().all()

        properties = (await session.execute(
            select(Property)
            .options(
                selectinload(Property.media),
                defer(Property.embedding),
                defer(Property.image_phashes),
                defer(Property.content_hash),
            )
            .order_by(Property.created_at.desc())
            #.limit(300)  #<-- Лимит показа количества обьектов в пагиннации
        )).scalars().all()

        # 1. Получаем кластеры, ожидающие проверки
        pending_clusters = (await session.execute(
            select(PropertyCluster)
            .options(
                selectinload(PropertyCluster.members).options(
                    defer(Property.embedding),
                    defer(Property.image_phashes),
                    defer(Property.content_hash),
                    selectinload(Property.media),
                )
            )
            .where(PropertyCluster.status == ClusterStatus.PENDING)
            .order_by(PropertyCluster.created_at.desc())
        )).scalars().all()

        # Annotate each pending cluster with a "feedback recurrence" score:
        # the maximum count among any (prop_a, prop_b) pair within this cluster
        # in ai_duplicate_feedbacks. If high, that means admins have rejected
        # a similar match before — surface that to give them the heads-up.
        #
        # We compute this in a single pass: build a lookup of all member-pair
        # rejection counts, then attach max() per cluster as a transient attr.
        pending_cluster_ids = [c.id for c in pending_clusters]
        feedback_counts: dict[str, int] = {}
        if pending_cluster_ids:
            # Count rejections per cluster: how many of this cluster's
            # internal pairs have ever been rejected before? A cluster with
            # high count is "recurring" and likely needs admin attention.
            feedback_rows = (await session.execute(sql_text("""
                WITH cluster_pairs AS (
                    SELECT
                        p1.cluster_id                       AS cluster_id,
                        LEAST(p1.id, p2.id)                 AS prop_a_id,
                        GREATEST(p1.id, p2.id)              AS prop_b_id
                    FROM properties p1
                    JOIN properties p2
                    ON p1.cluster_id = p2.cluster_id
                    AND p1.id < p2.id
                    WHERE p1.cluster_id = ANY(:ids)
                )
                SELECT
                    cp.cluster_id::text                     AS cluster_id,
                    COUNT(*) FILTER (WHERE f.id IS NOT NULL) AS rejected_pairs
                FROM cluster_pairs cp
                LEFT JOIN ai_duplicate_feedbacks f
                ON f.prop_a_id = cp.prop_a_id
                AND f.prop_b_id = cp.prop_b_id
                GROUP BY cp.cluster_id
            """), {"ids": pending_cluster_ids})).all()
            feedback_counts = {row.cluster_id: int(row.rejected_pairs or 0) for row in feedback_rows}

        # Attach as a transient attribute on each cluster object — the
        # template reads it as cluster.feedback_recurrence. No DB schema
        # change needed because we set it on the instance, not the model.
        for c in pending_clusters:
            c.feedback_recurrence = feedback_counts.get(str(c.id), 0)

        # 2. Получаем уже подтвержденные кластеры (берем последние 50, чтобы не перегружать страницу)
        approved_clusters = (await session.execute(
            select(PropertyCluster)
            .options(selectinload(PropertyCluster.members))
            .where(
                PropertyCluster.status == ClusterStatus.APPROVED,
                PropertyCluster.member_count >= 2  # СТРОГИЙ ФИЛЬТР: только группы
            )
            .order_by(PropertyCluster.updated_at.desc())
            .limit(50)
        )).scalars().all()

        # Получаем логи для вкладки Settings
        scraper_logs = (await session.execute(
            select(ScraperLog).order_by(ScraperLog.created_at.desc()).limit(50)
        )).scalars().all()

        email_logs = (await session.execute(
            select(EmailLog).order_by(EmailLog.created_at.desc()).limit(50)
        )).scalars().all()

        return templates.TemplateResponse(
            "admin_dashboard.html",
            _ctx(
                request,
                current_user=admin,
                users=users,
                properties=properties,
                pending_clusters=pending_clusters,
                approved_clusters=approved_clusters,
                sync_time=sync_time.value if sync_time else "00:01",
                report_time=repo_time.value if repo_time else "09:30",
                scraper_logs=scraper_logs,
                email_logs=email_logs,
            ),
        )


# --- admin: users -------------------------------------------------
@app.post("/admin/users/add")
async def add_new_user(
    request: Request,
    name: str = Form(...),
    email: str = Form(...),
    is_admin: bool = Form(False),
):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        existing = (await session.execute(
            select(Agent).where(Agent.email == email)
        )).scalars().first()
        if existing:
            return RedirectResponse(url="/admin", status_code=303)

        session.add(Agent(
            name=name.strip()[:255],
            email=email.strip().lower()[:255],
            is_admin=is_admin,
            is_active=True,
        ))
        await session.commit()
        return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/users/{user_id}/toggle")
async def toggle_user(user_id: str, request: Request):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        user = (await session.execute(
            select(Agent).where(Agent.id == user_id)
        )).scalars().first()
        if user:
            user.is_active = not user.is_active
            await session.commit()
        return RedirectResponse(url="/admin", status_code=303)


# --- admin: settings ---------------------------------------------
@app.post("/admin/settings/update")
async def update_settings(
    request: Request,
    sync_time: str = Form(...),
    report_time: str = Form(...),
):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        for key, value in (("sync_time", sync_time), ("report_time", report_time)):
            res = await session.execute(
                select(SystemSetting).where(SystemSetting.key == key)
            )
            setting = res.scalars().first()
            if setting:
                setting.value = value
            else:
                session.add(SystemSetting(key=key, value=value))
        await session.commit()

    return RedirectResponse(url="/admin", status_code=303)

async def _record_ai_feedback(session: AsyncSession, p1: Property, p2: Property) -> None:
    """Записывает отвергнутую пару в БД (используя ON CONFLICT DO UPDATE)"""
    if p1.id > p2.id:
        p1, p2 = p2, p1 # Гарантируем порядок A < B

    if not p1.content_hash or not p2.content_hash:
        return

    stmt = insert(AIDuplicateFeedback).values(
        prop_a_id=p1.id,
        prop_b_id=p2.id,
        hash_a=p1.content_hash,
        hash_b=p2.content_hash
    ).on_conflict_do_update(
        index_elements=['prop_a_id', 'prop_b_id'],
        set_=dict(hash_a=p1.content_hash, hash_b=p2.content_hash, updated_at=func.now())
    )
    await session.execute(stmt)

async def _dissolve_cluster_with_feedback(
    session: AsyncSession,
    cluster_id: str,
) -> int:
    """
    Полное расформирование кластера:
      1) Bulk INSERT всех C(N,2) пар в ai_duplicate_feedbacks (одна SQL).
      2) UPDATE Property SET cluster_id=NULL для всех members.
      3) DELETE сам кластер (CASCADE снесёт PowerProperty).

    Используется при админ-Reject (PENDING) или Dissolve Group (APPROVED).

    Возвращает количество записанных feedback-пар (для логов / телеметрии).
    Может вернуть 0 если у members нет content_hash — тогда мы не пишем
    feedback (схема требует NOT NULL hash_a / hash_b), но всё равно
    выполняем detach + delete.
    """
    # --- 1. Bulk insert C(N,2) пар одним SQL ----------------------------
    # Нормализация порядка пары: prop_a_id < prop_b_id (через LEAST/GREATEST).
    # Защита от NULL content_hash: WHERE-клауза.
    # Идемпотентность: ON CONFLICT по уникальному ключу (prop_a_id, prop_b_id).
    bulk_insert_sql = text("""
        INSERT INTO ai_duplicate_feedbacks (id, prop_a_id, prop_b_id, hash_a, hash_b)
        SELECT
            gen_random_uuid()           AS id,
            LEAST(p1.id, p2.id)         AS prop_a_id,
            GREATEST(p1.id, p2.id)      AS prop_b_id,
            p1.content_hash             AS hash_a,
            p2.content_hash             AS hash_b
        FROM properties p1
        JOIN properties p2
          ON p2.cluster_id = p1.cluster_id
         AND p2.id > p1.id
        WHERE p1.cluster_id = :cid
          AND p1.content_hash IS NOT NULL
          AND p2.content_hash IS NOT NULL
        ON CONFLICT (prop_a_id, prop_b_id) DO NOTHING
        RETURNING id
    """)
    result = await session.execute(bulk_insert_sql, {"cid": cluster_id})
    feedback_count = len(result.fetchall())

    # --- 2. Detach members ----------------------------------------------
    await session.execute(
        update(Property)
        .where(Property.cluster_id == cluster_id)
        .values(cluster_id=None)
    )

    # --- 3. Delete cluster (CASCADE на power_properties.cluster_id) -----
    await session.execute(
        delete(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )

    return feedback_count

@app.post("/admin/email/send-test")
async def admin_send_test_email(request: Request):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

    # Вызываем боевую функцию прямо сейчас (не дожидаясь крона)
    await job_email_report()
        
    return RedirectResponse(url="/admin", status_code=303)

@app.post("/admin/scrapers/run")
async def admin_run_scrapers(request: Request, background_tasks: BackgroundTasks):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

    # Запускаем парсинг в фоне, чтобы не повесить браузер (это долгий процесс)
    background_tasks.add_task(job_parsing)
        
    return {"status": "started"}


# --- admin: clusters (manual verdict) ----------------------------
async def _manual_verdict(
    session: AsyncSession,
    admin: Agent,
    cluster_id: str,
    new_status: ClusterStatus,
) -> None:
    """
    Apply admin's verdict on a cluster.

    Two paths:
      * APPROVED → lock cluster (verdict_locked=True), audit fields filled.
                   Cluster persists, members stay attached. Matcher on next
                   run will respect the lock.
      * REJECTED → DISSOLVE: write all C(N,2) pairs to ai_duplicate_feedbacks,
                   detach members, delete cluster. No verdict_locked needed
                   because the cluster is gone.
    """
    cluster = (await session.execute(
        select(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )).scalar_one_or_none()
    if cluster is None:
        raise HTTPException(status_code=404, detail="Cluster not found")

    if new_status == ClusterStatus.REJECTED:
        # --- DISSOLVE ----------------------------------------------------
        # Не делаем cluster.status=REJECTED. Не ставим verdict_locked.
        # Просто разбираем кластер и фиксируем feedback.
        feedback_count = await _dissolve_cluster_with_feedback(session, cluster_id)
        logger.info(
            f"[admin] dissolved cluster {cluster_id} "
            f"({feedback_count} feedback pairs recorded)"
        )
    else:
        # --- APPROVE -----------------------------------------------------
        cluster.status            = new_status
        cluster.verdict_locked    = True
        cluster.verdict_locked_at = datetime.now(timezone.utc)
        cluster.verdict_locked_by = admin.id
        logger.info(f"[admin] approved cluster {cluster_id}")

    await session.commit()


@app.post("/admin/clusters/{cluster_id}/approve")
async def admin_cluster_approve(cluster_id: str, request: Request):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)
        await _manual_verdict(session, admin, cluster_id, ClusterStatus.APPROVED)
    return {"status": "ok"}


@app.post("/admin/clusters/{cluster_id}/reject")
async def admin_cluster_reject(cluster_id: str, request: Request):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)
        await _manual_verdict(session, admin, cluster_id, ClusterStatus.REJECTED)
    return {"status": "ok"}


@app.post("/admin/clusters/{cluster_id}/remove/{property_id}")
async def admin_cluster_remove_member(cluster_id: str, property_id: str, request: Request):
    """Хирургическое удаление одного объекта из кластера"""
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        # Ищем объект и отвязываем его от кластера
        q_prop = select(Property).where(Property.id == property_id, Property.cluster_id == cluster_id)
        prop = (await session.execute(q_prop)).scalars().first()
        
        if prop:
            #Находим оставшихся и говорим, что удаляемый с ними больше не дружит
            q_rem = select(Property).where(Property.cluster_id == cluster_id, Property.id != property_id)
            remaining_props = (await session.execute(q_rem)).scalars().all()
            for r_prop in remaining_props:
                await _record_ai_feedback(session, prop, r_prop)

            prop.cluster_id = None
            
            # Обновляем счетчик кластера
            q_cluster = select(PropertyCluster).where(PropertyCluster.id == cluster_id)
            cluster = (await session.execute(q_cluster)).scalars().first()
            
            if cluster:
                cluster.member_count -= 1
                # Если в кластере остался только 1 объект, кластер теряет смысл — удаляем его
                if cluster.member_count < 2:
                    await session.execute(delete(PropertyCluster).where(PropertyCluster.id == cluster_id))
                    
            await session.commit()
            
    return {"status": "ok"}