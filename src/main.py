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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger
from sqlalchemy import and_, Date, cast, delete, exists, func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer, joinedload, selectinload

from sqlalchemy.dialects.postgresql import insert
from src.models.domain import AIDuplicateFeedback

from src.core.config import settings
from src.database.db import async_session_maker

from src.database.feedback_repository import (
    record_feedback_for_cluster_rejection,
    record_feedback_for_property_removal,
    fetch_dissolved_feedbacks,
    count_dissolved_feedbacks,
    VALID_REASON_ATTRIBUTES,
    VALID_FEEDBACK_SOURCES,
)

from src.models.domain import (
    Agent, AgentDevice, AuthToken, ClusterStatus,
    ClusterMemberProposal, ProposalStatus,             # V1-AdminAuthority Sprint 8
    PowerProperty, Property, PropertyCluster, PropertyStatus,
    SystemSetting, ScraperLog, EmailLog,
    cluster_v2_members_table,
)
from src.tasks.scheduler import start_scheduler, stop_scheduler, job_email_report, job_parsing
from src.web.csrf import CSRFMiddleware

from src.tasks.scheduler import job_email_report

from fastapi.staticfiles import StaticFiles
from sqlalchemy import text as sql_text
from fastapi import Form

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

@app.get("/")
async def root_redirect():
    """Silence root-path noise — WSL relay / health probes hit this."""
    return RedirectResponse(url="/admin", status_code=307)

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


async def get_current_reviewer(request: Request, session: AsyncSession) -> Optional[Agent]:
    """
    Returns the agent if they have reviewer-level access — that is, they're
    either an admin OR a reviewer. Used for cluster-management endpoints
    that both roles need (approve / reject / merge / dissolve / remove / etc.).

    Admin implicitly has all reviewer permissions, so callers don't need to
    check both flags. Returns None if the user has neither role (or isn't
    logged in).
    """
    agent = await get_current_agent(request, session)
    if agent and (agent.is_admin or agent.is_reviewer):
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
        # TEMPORARY: allow magic link reuse for multi-device admin access
        # if auth_token.is_used:
        #     return _error(request, "This link has already been used.")
        if not auth_token.agent or not auth_token.agent.is_active:
            return _error(request, "Your account is not active.")

        # auth_token.is_used = True  # TEMPORARY: keep link reusable

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

        # "Updates" view — show только properties с актуальным изменением:
        # NEW (added today), PRICE_CHANGED (price drop/raise), DELISTED (removed from source).
        # ACTIVE properties not shown — они уже видены, ничего нового.
        # Order: most recently updated first → newest changes на top.
        q = (
            select(Property)
            .options(
                selectinload(Property.media),
                defer(Property.embedding),
                defer(Property.image_phashes),
                defer(Property.content_hash),
            )
            .where(
                Property.status.in_([
                    PropertyStatus.NEW,
                    PropertyStatus.PRICE_CHANGED,
                    PropertyStatus.DELISTED,
                ])
            )
            .order_by(Property.updated_at.desc())
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
        # Sprint 7 Phase C: reviewer also gets in (sees Properties + Duplicates).
        # Template hides Users/Settings tabs based on user.is_admin.
        user = await get_current_reviewer(request, session)
        if not user:
            return RedirectResponse(url="/daily-report")

        # Admin-only data: skip loading for reviewers to save queries
        # (their template never renders Users/Settings/Scrapers/Email tabs).
        if user.is_admin:
            sync_res = await session.execute(
                select(SystemSetting).where(SystemSetting.key == "sync_time")
            )
            repo_res = await session.execute(
                select(SystemSetting).where(SystemSetting.key == "report_time")
            )
            sync_time = sync_res.scalars().first()
            repo_time = repo_res.scalars().first()

            # Sprint 9: per-day-of-week schedule. 14 SystemSetting rows
            # (sync_time_{mon..sun} + report_time_{mon..sun}). Value "HH:MM"
            # = enabled, "" = disabled for that day. Rows are auto-seeded by
            # scheduler's _schedule_per_day on first boot from legacy
            # sync_time/sync_days/report_time/report_days. Single batched
            # query — 14 rows max, no perf concern.
            _DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
            _per_day_rows = (await session.execute(
                select(SystemSetting).where(
                    SystemSetting.key.in_(
                        [f"sync_time_{d}"   for d in _DAYS]
                        + [f"report_time_{d}" for d in _DAYS]
                    )
                )
            )).scalars().all()
            _per_day = {r.key: r.value for r in _per_day_rows}
            sync_per_day   = {d: _per_day.get(f"sync_time_{d}",   "") for d in _DAYS}
            report_per_day = {d: _per_day.get(f"report_time_{d}", "") for d in _DAYS}

            users = (await session.execute(
                select(Agent).order_by(Agent.created_at.desc())
            )).scalars().all()
        else:
            sync_time = None
            repo_time = None
            sync_per_day = {}
            report_per_day = {}
            users = []

        # Sprint 7 Task A: Properties теперь грузятся через /api/admin/properties
        # (server-side pagination в DataTables). Здесь не делаем запрос —
        # передаём пустой список чтобы template не падал на {% for %}.
        # См. также: /api/admin/properties и /api/admin/properties/filters
        properties = []

        # 1a. Engine 1 pending clusters (legacy InternalDuplicateDetector)
        pending_clusters_v1 = (await session.execute(
            select(PropertyCluster)
            .options(
                selectinload(PropertyCluster.members).options(
                    defer(Property.embedding),
                    defer(Property.image_phashes),
                    defer(Property.content_hash),
                    selectinload(Property.media),
                ),
                selectinload(PropertyCluster.verdict_locked_by_agent),
            )
            .where(
                PropertyCluster.status == ClusterStatus.PENDING,
                PropertyCluster.engine_version == '1',
            )
            .order_by(PropertyCluster.created_at.desc())
        )).scalars().all()

        # 1b. Engine 2 pending clusters (HybridEngine v2) — admin only.
        # Sprint 7 Task C: reviewers don't see Engine 2 anywhere, so skip
        # the query entirely instead of fetching + hiding.
        if user.is_admin:
            pending_clusters_v2 = (await session.execute(
                select(PropertyCluster)
                .options(
                    selectinload(PropertyCluster.members_v2).options(
                        defer(Property.embedding),
                        defer(Property.image_phashes),
                        defer(Property.content_hash),
                        selectinload(Property.media),
                    ),
                    selectinload(PropertyCluster.verdict_locked_by_agent),
                )
                .where(
                    PropertyCluster.status == ClusterStatus.PENDING,
                    PropertyCluster.engine_version == '2',
                )
                .order_by(PropertyCluster.created_at.desc())
            )).scalars().all()
        else:
            pending_clusters_v2 = []

        # Annotate each pending cluster with a "feedback recurrence" score:
        # the maximum count among any (prop_a, prop_b) pair within this cluster
        # in ai_duplicate_feedbacks. If high, that means admins have rejected
        # a similar match before — surface that to give them the heads-up.
        #
        # We compute this in a single pass: build a lookup of all member-pair
        # rejection counts, then attach max() per cluster as a transient attr.
        pending_cluster_ids = [c.id for c in pending_clusters_v1 + pending_clusters_v2]
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
        for c in pending_clusters_v1 + pending_clusters_v2:
            c.feedback_recurrence = feedback_counts.get(str(c.id), 0)

        # 1c. Engine 1 member proposals (V1-AdminAuthority Sprint 8)
        # ============================================================
        # Engine 1 creates these when it detects a new candidate matching
        # an already-APPROVED+locked cluster. Admin decides per-proposal:
        #   * APPROVE → property added to cluster, member_count++
        #   * REJECT  → evidence pairs blacklisted in ai_duplicate_feedbacks
        # Either way, the target cluster's APPROVED verdict is preserved
        # (NEVER reverted, NEVER unlocked, PowerObject NEVER deleted).
        # Engine 2 doesn't currently produce proposals (USE_NEW_DUPLICATE_ENGINE=False).
        pending_proposals_v1 = (await session.execute(
            select(ClusterMemberProposal)
            .where(ClusterMemberProposal.status == ProposalStatus.PENDING)
            .order_by(ClusterMemberProposal.proposed_at.desc())
        )).scalars().all()

        # Hydrate cluster + candidate via batch queries (no relationships
        # defined on ClusterMemberProposal — mirrors AIDuplicateFeedback pattern)
        proposal_cluster_ids = list({p.cluster_id for p in pending_proposals_v1})
        proposal_property_ids = list({p.property_id for p in pending_proposals_v1})

        proposal_clusters_by_id = {}
        proposal_candidates_by_id = {}

        if proposal_cluster_ids:
            cluster_rows = (await session.execute(
                select(PropertyCluster)
                .options(
                    selectinload(PropertyCluster.members).options(
                        defer(Property.embedding),
                        defer(Property.image_phashes),
                        defer(Property.content_hash),
                        selectinload(Property.media),
                    ),
                    selectinload(PropertyCluster.verdict_locked_by_agent),
                )
                .where(PropertyCluster.id.in_(proposal_cluster_ids))
            )).scalars().all()
            proposal_clusters_by_id = {c.id: c for c in cluster_rows}

        if proposal_property_ids:
            candidate_rows = (await session.execute(
                select(Property)
                .options(
                    defer(Property.embedding),
                    defer(Property.image_phashes),
                    defer(Property.content_hash),
                    selectinload(Property.media),
                )
                .where(Property.id.in_(proposal_property_ids))
            )).scalars().all()
            proposal_candidates_by_id = {p.id: p for p in candidate_rows}

        # Attach hydrated objects as transient attrs — template reads
        # proposal.cluster_obj and proposal.candidate_obj
        for p in pending_proposals_v1:
            p.cluster_obj = proposal_clusters_by_id.get(p.cluster_id)
            p.candidate_obj = proposal_candidates_by_id.get(p.property_id)

        # 2a. Engine 1 approved clusters — paginated.
        # Page driven by ?approved_v1_page=N (0-indexed). 25 per page.
        APPROVED_PAGE_SIZE = 25
        approved_v1_page = max(0, int(request.query_params.get('approved_v1_page', '0') or 0))

        total_approved_v1 = await session.scalar(
            select(func.count(PropertyCluster.id))
            .where(
                PropertyCluster.status == ClusterStatus.APPROVED,
                PropertyCluster.engine_version == '1',
                PropertyCluster.member_count >= 2,
            )
        ) or 0

        approved_clusters_v1 = (await session.execute(
            select(PropertyCluster)
            .options(
                selectinload(PropertyCluster.members).options(
                    defer(Property.embedding),
                    defer(Property.image_phashes),
                    defer(Property.content_hash),
                    selectinload(Property.media),
                ),
                selectinload(PropertyCluster.verdict_locked_by_agent),
            )
            .where(
                PropertyCluster.status == ClusterStatus.APPROVED,
                PropertyCluster.engine_version == '1',
                PropertyCluster.member_count >= 2,
            )
            .order_by(PropertyCluster.created_at.desc())
            .offset(approved_v1_page * APPROVED_PAGE_SIZE)
            .limit(APPROVED_PAGE_SIZE)
        )).scalars().all()

        # 2b. Engine 2 approved clusters — paginated. Admin-only.
        approved_v2_page = max(0, int(request.query_params.get('approved_v2_page', '0') or 0))

        if user.is_admin:
            total_approved_v2 = await session.scalar(
                select(func.count(PropertyCluster.id))
                .where(
                    PropertyCluster.status == ClusterStatus.APPROVED,
                    PropertyCluster.engine_version == '2',
                    PropertyCluster.member_count >= 2,
                )
            ) or 0

            approved_clusters_v2 = (await session.execute(
                select(PropertyCluster)
                .options(
                    selectinload(PropertyCluster.members_v2).options(
                        defer(Property.embedding),
                        defer(Property.image_phashes),
                        defer(Property.content_hash),
                        selectinload(Property.media),
                    ),
                    selectinload(PropertyCluster.verdict_locked_by_agent),
                )
                .where(
                    PropertyCluster.status == ClusterStatus.APPROVED,
                    PropertyCluster.engine_version == '2',
                    PropertyCluster.member_count >= 2,
                )
                .order_by(PropertyCluster.created_at.desc())
                .offset(approved_v2_page * APPROVED_PAGE_SIZE)
                .limit(APPROVED_PAGE_SIZE)
            )).scalars().all()
        else:
            total_approved_v2 = 0
            approved_clusters_v2 = []

        # Получаем логи для вкладки Settings
        # Получаем логи для вкладки Settings (admin only — reviewer
        # не видит Services tab, queries бесполезны для них)
        if user.is_admin:
            scraper_logs = (await session.execute(
                select(ScraperLog).order_by(ScraperLog.created_at.desc()).limit(50)
            )).scalars().all()

            email_logs = (await session.execute(
                select(EmailLog).order_by(EmailLog.created_at.desc()).limit(50)
            )).scalars().all()
        else:
            scraper_logs = []
            email_logs = []


        # Sprint 7 Task B.2: Single counts per engine — properties not in
        # any active cluster of that engine. Engine 1 visible to reviewers
        # AND admins; Engine 2 admin-only (Task C).
        total_single_e1 = await session.scalar(
            select(func.count(Property.id))
            .where(Property.cluster_id.is_(None))
        ) or 0
        if user.is_admin:
            total_single_e2 = await session.scalar(
                select(func.count(Property.id))
                .where(~exists().where(
                    cluster_v2_members_table.c.property_id == Property.id
                ))
            ) or 0
        else:
            total_single_e2 = 0

        # Sprint 7: per-engine Dissolved display.
        # Both engines READ all feedback rows for learning (shared),
        # but the UI shows them split by source_engine_version so admin
        # knows WHICH engine's cluster they rejected.
        total_dissolved_v1 = await count_dissolved_feedbacks(session, source_engine_version='1')
        dissolved_feedbacks_v1 = await fetch_dissolved_feedbacks(
            session, limit=50, offset=0, source_engine_version='1'
        )
        if user.is_admin:
            total_dissolved_v2 = await count_dissolved_feedbacks(session, source_engine_version='2')
            dissolved_feedbacks_v2 = await fetch_dissolved_feedbacks(
                session, limit=50, offset=0, source_engine_version='2'
            )
        else:
            total_dissolved_v2 = 0
            dissolved_feedbacks_v2 = []

        return templates.TemplateResponse(
            "admin_dashboard.html",
            _ctx(
                request,
                current_user=user,
                users=users,
                properties=properties,
                pending_clusters_v1=pending_clusters_v1,
                pending_clusters_v2=pending_clusters_v2,
                pending_proposals_v1=pending_proposals_v1,
                approved_clusters_v1=approved_clusters_v1,
                approved_clusters_v2=approved_clusters_v2,
                approved_v2_page=approved_v2_page,
                total_approved_v1=total_approved_v1,
                total_approved_v2=total_approved_v2,
                approved_v1_page=approved_v1_page,
                approved_page_size=APPROVED_PAGE_SIZE,
                total_dissolved_v1=total_dissolved_v1,
                total_dissolved_v2=total_dissolved_v2,
                total_single_e1=total_single_e1,
                total_single_e2=total_single_e2,
                dissolved_feedbacks_v1=dissolved_feedbacks_v1,
                dissolved_feedbacks_v2=dissolved_feedbacks_v2,
                sync_time=sync_time.value if sync_time else "00:01",
                report_time=repo_time.value if repo_time else "09:30",
                sync_per_day=sync_per_day,
                report_per_day=report_per_day,
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
    role: str = Form("agent"),
):
    """
    Create new user. Role is one of 'agent' / 'reviewer' / 'admin'.

    Sprint 7 Phase C iteration 2: returns JSON (not Redirect) so the
    admin_dashboard.html user modal can stay open on error and show a
    message. AJAX-friendly response avoids the CSRF middleware
    body-consumption issue with classic <form method=post>.
    """
    if role not in ("agent", "reviewer", "admin"):
        raise HTTPException(status_code=400, detail="role must be agent/reviewer/admin")

    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        existing = (await session.execute(
            select(Agent).where(Agent.email == email.strip().lower())
        )).scalars().first()
        if existing:
            raise HTTPException(status_code=400, detail="Email already exists")

        session.add(Agent(
            name=name.strip()[:255],
            email=email.strip().lower()[:255],
            is_admin=(role == "admin"),
            is_reviewer=(role == "reviewer"),
            is_active=True,
        ))
        await session.commit()
        return {"status": "ok"}


@app.post("/admin/users/{user_id}/toggle")
async def toggle_user(user_id: str, request: Request):
    """
    Toggle user's is_active flag. JSON response so the Manage modal can
    update its Block/Unblock button label in place without a full reload.

    Refuses to block the calling admin themselves — last-line safety net
    against accidental self-lockout (the frontend also hides the button
    for the current user, this is belt-and-suspenders).
    """
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        user = (await session.execute(
            select(Agent).where(Agent.id == user_id)
        )).scalars().first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        if user.id == admin.id:
            raise HTTPException(status_code=400, detail="Cannot block yourself")

        user.is_active = not user.is_active
        await session.commit()
        return {"status": "ok", "is_active": user.is_active}


# --- admin: settings ---------------------------------------------
@app.post("/admin/users/{user_id}/update")
async def update_user(
    user_id: str,
    request: Request,
    name: str = Form(...),
    email: str = Form(...),
    role: str = Form("agent"),
):
    """
    Update existing user's name / email / role.

    Role is one of 'agent' / 'reviewer' / 'admin'. Safety net: admin
    cannot demote themselves (would lose access to /admin if the demoted
    admin was the only one).

    Email uniqueness is enforced — if you try to set this user's email
    to one that already belongs to another user, returns 400.
    """
    if role not in ("agent", "reviewer", "admin"):
        raise HTTPException(status_code=400, detail="role must be agent/reviewer/admin")

    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        user = (await session.execute(
            select(Agent).where(Agent.id == user_id)
        )).scalars().first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Self-demote guard — can't remove your own admin rights
        if user.id == admin.id and role != "admin":
            raise HTTPException(
                status_code=400,
                detail="Cannot demote yourself from admin",
            )

        new_email = email.strip().lower()[:255]
        if new_email != user.email:
            # Check uniqueness — somebody else might already own this email
            conflict = (await session.execute(
                select(Agent).where(
                    Agent.email == new_email,
                    Agent.id != user.id,
                )
            )).scalars().first()
            if conflict:
                raise HTTPException(status_code=400, detail="Email already taken by another user")

        user.name = name.strip()[:255]
        user.email = new_email
        user.is_admin = (role == "admin")
        user.is_reviewer = (role == "reviewer")

        await session.commit()
        return {"status": "ok"}
    
@app.post("/admin/users/{user_id}/send-link")
async def send_magic_link_endpoint(user_id: str, request: Request):
    """
    Generate a fresh magic-link token for a single user and try to deliver
    via email. Always returns the URL so admin can copy/paste manually
    (Telegram, etc.) — useful for onboarding new users or when SMTP is
    misconfigured / APP_URL doesn't match the current tunnel URL.

    Frontend (Manage modal "Send Magic Link" button) reads:
      - `sent`  → toast "Sent to user@email.com" if True
      - `url`   → always shown with a copy button as manual fallback
      - `error` → SMTP error if delivery failed

    Refuses to send to blocked users (is_active=False).
    """
    from src.services.notifier import send_magic_link_to_agent

    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        # Quick presence check before delegating to notifier — gives the
        # frontend a clean 404/400 response instead of the notifier's
        # ValueError wrapped in 500.
        user = (await session.execute(
            select(Agent).where(Agent.id == user_id)
        )).scalars().first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if not user.is_active:
            raise HTTPException(
                status_code=400,
                detail="Cannot send magic link to a blocked user. Unblock first.",
            )

    # Notifier opens its own session — keeps this handler's session lifetime
    # short and avoids holding a connection during the (slow) SMTP roundtrip.
    try:
        result = await send_magic_link_to_agent(str(user.id))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[admin/send-link] unexpected error for {user.id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error generating magic link: {str(e)[:200]}",
        )

    logger.info(
        f"[admin/send-link] admin={admin.email} target={user.email} "
        f"sent={result['sent']} error={result.get('error') or 'none'}"
    )

    return {
        "status": "ok",
        "url":    result["url"],
        "sent":   result["sent"],
        "email":  result["email"],
        "error":  result["error"],
    }

# Sprint 9: per-day-of-week schedule helpers
_SETTINGS_DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def _valid_hhmm(s: str) -> bool:
    """'HH:MM' validator — strict zero-padded form."""
    if not s or ":" not in s:
        return False
    h_s, _, m_s = s.partition(":")
    if not (h_s.isdigit() and m_s.isdigit() and len(h_s) == 2 and len(m_s) == 2):
        return False
    h, m = int(h_s), int(m_s)
    return 0 <= h <= 23 and 0 <= m <= 59


@app.post("/admin/settings/update")
async def update_settings(request: Request):
    """
    Sprint 9: per-day-of-week schedule. Form posts 28 fields:
        sync_enabled_{day}   — checkbox (only present when checked) × 7 days
        sync_time_{day}      — HH:MM input × 7 days
        report_enabled_{day} — same for email report
        report_time_{day}    — same

    A day is enabled iff its checkbox is present AND its time validates as
    HH:MM. Otherwise we save "" so the scheduler removes that day's job.
    Old single-row sync_time/report_time settings are left untouched (no
    longer read by scheduler).

    Returns JSON for the AJAX-submit frontend; triggers update_schedule()
    immediately so cron triggers refresh before the toast appears (instead
    of waiting up to 10 min for the periodic tick).
    """
    from src.tasks.scheduler import update_schedule as _refresh_schedule

    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        form = await request.form()

        for day in _SETTINGS_DAYS:
            for prefix in ("sync", "report"):
                enabled  = form.get(f"{prefix}_enabled_{day}") is not None
                raw_time = (form.get(f"{prefix}_time_{day}") or "").strip()
                final    = raw_time if (enabled and _valid_hhmm(raw_time)) else ""

                key = f"{prefix}_time_{day}"
                res = await session.execute(
                    select(SystemSetting).where(SystemSetting.key == key)
                )
                setting = res.scalars().first()
                if setting:
                    setting.value = final
                else:
                    session.add(SystemSetting(key=key, value=final))

        await session.commit()

    # Immediate APScheduler refresh — admin sees the new schedule in the
    # next log line, not 10 min later (the periodic update_schedule tick).
    try:
        await _refresh_schedule()
    except Exception as e:
        logger.error(f"[admin/settings/update] post-save schedule refresh failed: {e}")

    return JSONResponse({"status": "ok"})

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
      2) Detach members (UPDATE для E1, CASCADE для E2).
      3) DELETE сам кластер (CASCADE снесёт PowerProperty + junction).

    Используется при админ-Reject (PENDING) или Dissolve Group (APPROVED).

    Sprint 7 V2-Fix-1: engine-aware. E1 uses Property.cluster_id FK,
    E2 uses cluster_v2_members junction. Both paths write identical
    feedback semantics so engine learns from rejection regardless of
    which engine detected the cluster. Previously E2 reject silently
    wrote 0 feedback rows because the SQL only matched E1's FK.

    Возвращает количество записанных feedback-пар (для логов / телеметрии).
    Может вернуть 0 если у members нет content_hash — тогда мы не пишем
    feedback (схема требует NOT NULL hash_a / hash_b), но всё равно
    выполняем detach + delete.
    """
    # Sprint 7 V2-Fix-1: determine engine first — affects feedback SQL path
    cluster = (await session.execute(
        select(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )).scalar_one_or_none()
    if cluster is None:
        return 0  # idempotent — concurrent dissolve already happened

    # --- 1. Bulk insert C(N,2) пар одним SQL ----------------------------
    # Engine-aware: members live in different places per engine.
    # Нормализация порядка пары: prop_a_id < prop_b_id (через LEAST/GREATEST).
    # Защита от NULL content_hash: WHERE-клауза.
    # Идемпотентность: ON CONFLICT по уникальному ключу (prop_a_id, prop_b_id).
    if cluster.engine_version == '2':
        # Engine 2: members via cluster_v2_members junction
        bulk_insert_sql = text("""
            INSERT INTO ai_duplicate_feedbacks (id, prop_a_id, prop_b_id, hash_a, hash_b)
            SELECT
                gen_random_uuid()           AS id,
                LEAST(p1.id, p2.id)         AS prop_a_id,
                GREATEST(p1.id, p2.id)      AS prop_b_id,
                p1.content_hash             AS hash_a,
                p2.content_hash             AS hash_b
            FROM cluster_v2_members m1
            JOIN cluster_v2_members m2
              ON m1.cluster_id = m2.cluster_id
             AND m2.property_id > m1.property_id
            JOIN properties p1 ON p1.id = m1.property_id
            JOIN properties p2 ON p2.id = m2.property_id
            WHERE m1.cluster_id = :cid
              AND p1.content_hash IS NOT NULL
              AND p2.content_hash IS NOT NULL
            ON CONFLICT (prop_a_id, prop_b_id) DO NOTHING
            RETURNING id
        """)
    else:
        # Engine 1: members via Property.cluster_id FK (legacy path)
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
    # E1: explicit UPDATE clearing Property.cluster_id FK
    # E2: skip — CASCADE on cluster_v2_members.cluster_id handles cleanup
    #     automatically when we DELETE the cluster row below
    if cluster.engine_version != '2':
        await session.execute(
            update(Property)
            .where(Property.cluster_id == cluster_id)
            .values(cluster_id=None)
        )

    # --- 3. Delete cluster ----------------------------------------------
    # CASCADE handles: power_properties.cluster_id (both engines) +
    # cluster_v2_members.cluster_id (E2 only, no-op for E1).
    await session.execute(
        delete(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )

    return feedback_count

async def _silent_dissolve_cluster(
    session: AsyncSession,
    cluster_id: str,
) -> None:
    """
    Тихий роспуск кластера — БЕЗ записи в ai_duplicate_feedbacks.

    Семантически отличается от _dissolve_cluster_with_feedback:
      • _dissolve_cluster_with_feedback — admin reject ("эти НЕ дубликаты"),
        пишет feedback по всем парам, чтобы детектор больше не предлагал.
      • _silent_dissolve_cluster — manual-merge cleanup ("эти ARE дубликаты,
        но в другом кластере теперь"). Старый кластер просто исчезает,
        оставшимся members'ам cluster_id обнуляется. Никаких feedback.

    Используется когда manual merge забирает members из существующего
    кластера и оставляет его с < 2 членами — кластер теряет смысл.

    Idempotent: безопасно вызывать дважды (concurrent admin actions).
    """
    # Idempotency guard — concurrent operation might already have dissolved.
    exists = await session.scalar(
        select(PropertyCluster.id).where(PropertyCluster.id == cluster_id)
    )
    if not exists:
        return

    # 1. Detach остающиеся members (если такие ещё есть)
    await session.execute(
        update(Property)
        .where(Property.cluster_id == cluster_id)
        .values(cluster_id=None)
    )
    # 2. Delete cluster (CASCADE на power_properties.cluster_id)
    await session.execute(
        delete(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )

async def _silent_dissolve_cluster_v2(
    session: AsyncSession,
    cluster_id: str,
) -> None:
    """
    Engine 2 version of _silent_dissolve_cluster.

    Engine 2 clusters track membership via cluster_v2_members junction
    (NOT Property.cluster_id FK). Both the junction rows AND the cluster
    row need cleanup. Since junction has ondelete=CASCADE on cluster_id,
    deleting the cluster automatically removes junction rows — single
    DELETE suffices.

    Idempotent: safe to call on a non-existent or already-deleted cluster.
    """
    exists = await session.scalar(
        select(PropertyCluster.id).where(PropertyCluster.id == cluster_id)
    )
    if not exists:
        return

    # CASCADE on cluster_v2_members.cluster_id handles junction cleanup
    await session.execute(
        delete(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )


async def _delete_feedback_for_pairs(
    session: AsyncSession,
    property_ids: list[str],
) -> int:
    """
    Удалить ai_duplicate_feedbacks rows для всех пар (a, b) среди property_ids.

    Используется в manual-merge: admin сейчас явно говорит "эти ARE
    duplicates", значит любой предыдущий "эти NOT duplicates" feedback
    становится недействителен. Иначе следующий запуск детектора снова
    отфильтрует эти пары и кластер развалится.

    Set-based DELETE: матчит rows независимо от ordering convention
    (хотя все writers нормализуют prop_a_id<prop_b_id, а БД constraint
    enforce'ит только uniqueness — так что несколько защитных слоёв
    здесь полезны).

    Возвращает количество удалённых строк (для логирования).
    """
    if len(property_ids) < 2:
        return 0

    result = await session.execute(text("""
        DELETE FROM ai_duplicate_feedbacks
        WHERE prop_a_id::text = ANY(:ids)
          AND prop_b_id::text = ANY(:ids)
          AND prop_a_id != prop_b_id
        RETURNING id
    """), {"ids": property_ids})
    return len(result.fetchall())

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

    Sprint 7 Task C: Engine 2 clusters are admin-only. Reviewers calling
    this with an engine_version='2' cluster get a 403. Engine 1 stays
    open to both admin and reviewer (existing behaviour).
    """
    cluster = (await session.execute(
        select(PropertyCluster).where(PropertyCluster.id == cluster_id)
    )).scalar_one_or_none()
    if cluster is None:
        raise HTTPException(status_code=404, detail="Cluster not found")

    # Engine 2 access gate — reviewers blocked
    if cluster.engine_version == '2' and not admin.is_admin:
        raise HTTPException(
            status_code=403,
            detail="Engine 2 operations require admin role",
        )

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

# =============================================================
# BULK CLUSTER VERDICT — Group C
# =============================================================
# Insert this BLOCK in main.py right AFTER the existing
# `admin_cluster_remove_member` endpoint (around line 722).
#
# It reuses the existing _manual_verdict() helper, so all the
# same audit logic (verdict_locked, dissolve+feedback, etc) is
# preserved — we just iterate.
#
# Request shape (form-encoded, same CSRF + cookies as singletons):
#     csrf_token=<token>
#     action=approve | reject
#     cluster_ids=id1,id2,id3      (comma-separated UUIDs)
#
# Response:
#     {"status": "ok",
#      "results": {"approved": 5, "rejected": 0, "errors": [...]}}

from fastapi import Form
# ^ if not already imported at top of main.py — add it there once.


@app.post("/admin/clusters/bulk-verdict")
async def admin_clusters_bulk_verdict(
    request: Request,
    action: str = Form(...),
    cluster_ids: str = Form(...),  # comma-separated
):
    """
    Apply the same verdict (approve OR reject) to many clusters at once.

    Implemented as a loop over _manual_verdict, not as a single bulk SQL
    statement, because:
      - reject path needs to dissolve each cluster and write its own
        feedback pairs (different cluster = different members)
      - we want partial success: if cluster #4 fails, clusters 1-3
        already committed. Caller sees per-cluster outcome.

    The frontend animates each card removal in sequence — single-action
    UX consistency. No Redirect, frontend handles DOM update on its own.
    """
    if action not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="action must be approve|reject")

    ids = [s.strip() for s in cluster_ids.split(",") if s.strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="no cluster_ids provided")
    if len(ids) > 100:
        # Defensive cap — admin tools shouldn't be the way to bulk-modify
        # thousands of rows. If this ever fires, paginate the UI.
        raise HTTPException(status_code=400, detail="too many clusters in one call (max 100)")

    target_status = (
        ClusterStatus.APPROVED if action == "approve"
        else ClusterStatus.REJECTED
    )

    results = {"approved": 0, "rejected": 0, "errors": []}

    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        for cid in ids:
            try:
                await _manual_verdict(session, admin, cid, target_status)
                if action == "approve":
                    results["approved"] += 1
                else:
                    results["rejected"] += 1
            except HTTPException as e:
                # 404 (cluster not found) — typically harmless race with
                # another admin tab. Record it but keep processing.
                results["errors"].append({"cluster_id": cid, "error": e.detail})
            except Exception as e:
                logger.warning(f"[admin/bulk] failed cluster {cid}: {e}")
                results["errors"].append({"cluster_id": cid, "error": str(e)})

    logger.info(
        f"[admin/bulk] {action}: "
        f"approved={results['approved']} rejected={results['rejected']} "
        f"errors={len(results['errors'])}"
    )
    return {"status": "ok", "results": results}

@app.get("/admin/clusters/recurrence-check")
async def admin_clusters_recurrence_check(
    request: Request,
    property_ids: str = Query(...),
):
    """
    Pre-merge dry-check: how many of the candidate-merge pairs were
    previously rejected by admins as not-duplicates?

    Used by frontend to show a warning dialog before a destructive merge.
    Read-only — no CSRF needed (GET method, csrf middleware skips).

    Query: ?property_ids=uuid1,uuid2,uuid3
    Returns: {"count": N} — number of feedback rows whose pair (a, b)
             is fully contained in the input ids set.
    """
    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        ids = [s.strip() for s in property_ids.split(",") if s.strip()]
        if len(ids) < 2:
            return {"count": 0}

        # Set-based lookup with column::text cast — portable across drivers,
        # works even if a future writer ever inserts denormalized (b, a) pair.
        row = (await session.execute(text("""
            SELECT COUNT(*) AS cnt
            FROM ai_duplicate_feedbacks f
            WHERE f.prop_a_id::text = ANY(:ids)
              AND f.prop_b_id::text = ANY(:ids)
              AND f.prop_a_id != f.prop_b_id
        """), {"ids": ids})).first()

        return {"count": int(row.cnt or 0)}


@app.post("/admin/clusters/manual-merge")
async def admin_clusters_manual_merge(
    request: Request,
    property_ids: str = Form(...),
    engine_versions: str = Form("1"),
):
    """
    Manual merge: create new APPROVED cluster(s) from a set of selected
    properties. Sprint 7 Task F: now engine-aware — accepts engine_versions
    parameter, can create cluster in Engine 1, Engine 2, or both.

    engine_versions semantics
    -------------------------
    "1"      → only Engine 1 (default — backward-compat for old callers)
    "2"      → only Engine 2 (admin-only; reviewer auto-stripped to "1")
    "1,2"    → BOTH engines (used by Properties tab where admin's intent
               "these are duplicates" applies universally)

    Engine 1 path: Property.cluster_id FK reassignment + silent_dissolve
                   of old E1 clusters that drop below 2 members.
    Engine 2 path: junction table (cluster_v2_members) insert/delete +
                   silent_dissolve_v2 of old E2 clusters.

    For "1,2" both paths run sequentially in the same transaction.
    Returns dict of created cluster IDs keyed by engine.
    """
    # --- Parse + validate engine_versions ----------------------------
    engines = [
        e.strip() for e in engine_versions.split(",")
        if e.strip() in ("1", "2")
    ]
    if not engines:
        engines = ["1"]
    engines = sorted(set(engines))  # dedupe, sorted for determinism

    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        # Reviewer can't create Engine 2 clusters — silently strip.
        # If that leaves us with no engines, that's a 403 (e.g. reviewer
        # explicitly asked for engine_versions="2").
        if "2" in engines and not admin.is_admin:
            engines = [e for e in engines if e != "2"]
            if not engines:
                raise HTTPException(
                    status_code=403,
                    detail="Engine 2 cluster creation requires admin role",
                )

        # --- Parse + validate property_ids ----------------------------
        ids = [s.strip() for s in property_ids.split(",") if s.strip()]
        if len(ids) < 2:
            raise HTTPException(status_code=400, detail="need at least 2 properties")
        if len(ids) > 100:
            raise HTTPException(status_code=400, detail="too many properties in one merge (max 100)")

        # --- Lookup all properties ------------------------------------
        props = (await session.execute(
            select(Property).where(Property.id.in_(ids))
        )).scalars().all()

        found_ids = {str(p.id) for p in props}
        missing = set(ids) - found_ids
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"property {missing.pop()} not found",
            )

        # ═══════════════════════════════════════════════════════════════
        # Sprint 7 Bug Fix — homogeneous selection validation
        # ═══════════════════════════════════════════════════════════════
        # Prevents silent cluster destruction. Admin must explicitly pick
        # one of these scenarios:
        #
        #   (1) New cluster from singletons   — all props have NO cluster
        #       membership in either engine
        #   (2) Split existing cluster        — all props belong to the
        #       SAME cluster (e1 OR e2), AND it's a subset (cluster has
        #       members left after split)
        #
        # Mixed / cross-cluster scenarios block with 400 + actionable
        # error message guiding the admin to the right flow.
        # ═══════════════════════════════════════════════════════════════

        # Query Engine 2 memberships (Engine 1 is on Property.cluster_id)
        e2_membership_rows = (await session.execute(
            select(
                cluster_v2_members_table.c.property_id,
                cluster_v2_members_table.c.cluster_id,
            )
            .where(cluster_v2_members_table.c.property_id.in_(ids))
        )).all()

        # prop_id → set of (engine, cluster_id) tuples it belongs to.
        # frozenset for hashability — same membership = same profile.
        prop_e2_clusters: dict[str, set] = {}
        for row in e2_membership_rows:
            prop_e2_clusters.setdefault(
                str(row.property_id), set()
            ).add(row.cluster_id)

        def _membership_of(p: Property) -> frozenset:
            """All cluster memberships of this property, across engines."""
            m = set()
            if p.cluster_id is not None:
                m.add(('e1', p.cluster_id))
            for e2_cid in prop_e2_clusters.get(str(p.id), set()):
                m.add(('e2', e2_cid))
            return frozenset(m)

        profiles = {str(p.id): _membership_of(p) for p in props}
        singletons = [pid for pid, m in profiles.items() if not m]
        clustered  = [pid for pid, m in profiles.items() if m]

        # Rule violation #1: Mixed singletons + cluster members
        if singletons and clustered:
            short = lambda x: x[:8].upper()
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Cannot merge singletons with cluster members. "
                    f"Already in cluster ({len(clustered)}): "
                    f"{', '.join(short(p) for p in clustered[:3])}"
                    f"{'…' if len(clustered) > 3 else ''}. "
                    f"Singletons ({len(singletons)}): "
                    f"{', '.join(short(p) for p in singletons[:3])}"
                    f"{'…' if len(singletons) > 3 else ''}. "
                    f"To extend an existing cluster, remove conflicting members "
                    f"from their old cluster first, then merge as singletons."
                ),
            )

        # Rule violation #2: Cross-cluster merge (members from 2+ clusters)
        unique_profiles = set(profiles.values())
        if len(unique_profiles) > 1:
            cluster_short_ids = set()
            for prof in unique_profiles:
                for _, cid in prof:
                    cluster_short_ids.add(str(cid)[:8].upper())
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Cannot merge properties from different clusters: "
                    f"{', '.join(sorted(cluster_short_ids)[:3])}"
                    f"{'…' if len(cluster_short_ids) > 3 else ''}. "
                    f"To move a property between clusters, first remove it "
                    f"from its current cluster, then merge."
                ),
            )

        # Rule violation #3: Selected entire cluster (no leftover after split)
        if clustered:
            common_profile = next(iter(unique_profiles))
            for engine, cluster_id in common_profile:
                if engine == 'e1':
                    total_members = (await session.execute(
                        select(func.count(Property.id))
                        .where(Property.cluster_id == cluster_id)
                    )).scalar() or 0
                else:  # 'e2'
                    total_members = (await session.execute(
                        select(func.count(cluster_v2_members_table.c.property_id))
                        .where(cluster_v2_members_table.c.cluster_id == cluster_id)
                    )).scalar() or 0
                if total_members == len(props):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Selected ALL members of cluster "
                            f"{str(cluster_id)[:8].upper()} ({total_members}). "
                            f"This would destroy the original cluster's audit "
                            f"trail with no net change. To rename or restart "
                            f"the cluster, use the Manage modal's actions instead."
                        ),
                    )

        # ═══════════════════════════════════════════════════════════════
        # Validation passed: either all singletons, or all subset of one cluster
        # ═══════════════════════════════════════════════════════════════

        new_cluster_ids: dict[str, str] = {}
        now = datetime.now(timezone.utc)

        # ============================================================
        # ENGINE 1 path — Property.cluster_id FK
        # ============================================================
        if "1" in engines:
            source_clusters_e1 = {p.cluster_id for p in props if p.cluster_id is not None}

            new_e1 = PropertyCluster(
                status=ClusterStatus.APPROVED,
                engine_version="1",
                verdict_locked=True,
                verdict_locked_at=now,
                verdict_locked_by=admin.id,
                member_count=len(ids),
                ai_score=None,
                phash_matches=None,
                notes=(
                    f"Manual merge of {len(ids)} properties (engine=1, "
                    f"sources={sorted(str(c) for c in source_clusters_e1)})"
                    if source_clusters_e1 else
                    f"Manual merge of {len(ids)} singleton properties (engine=1)"
                ),
            )
            session.add(new_e1)
            await session.flush()
            new_e1_id = new_e1.id

            # Reassign Property.cluster_id
            for p in props:
                p.cluster_id = new_e1_id
            await session.flush()

            # Cleanup old E1 clusters
            for old_cid in source_clusters_e1:
                count = (await session.execute(
                    select(func.count(Property.id)).where(Property.cluster_id == old_cid)
                )).scalar() or 0
                if count >= 2:
                    await session.execute(
                        update(PropertyCluster)
                        .where(PropertyCluster.id == old_cid)
                        .values(member_count=count, updated_at=now)
                    )
                else:
                    await _silent_dissolve_cluster(session, old_cid)

            new_cluster_ids["1"] = str(new_e1_id)

        # ============================================================
        # ENGINE 2 path — cluster_v2_members junction
        # ============================================================
        if "2" in engines:
            # Identify source E2 clusters BEFORE we delete junction rows
            source_e2_rows = (await session.execute(
                select(cluster_v2_members_table.c.cluster_id)
                .where(cluster_v2_members_table.c.property_id.in_(ids))
                .distinct()
            )).scalars().all()
            source_clusters_e2 = set(source_e2_rows)

            new_e2 = PropertyCluster(
                status=ClusterStatus.APPROVED,
                engine_version="2",
                verdict_locked=True,
                verdict_locked_at=now,
                verdict_locked_by=admin.id,
                member_count=len(ids),
                ai_score=None,
                phash_matches=None,
                notes=(
                    f"Manual merge of {len(ids)} properties (engine=2, "
                    f"sources={sorted(str(c) for c in source_clusters_e2)})"
                    if source_clusters_e2 else
                    f"Manual merge of {len(ids)} singleton properties (engine=2)"
                ),
            )
            session.add(new_e2)
            await session.flush()
            new_e2_id = new_e2.id

            # Remove OLD junction rows for these properties (from any prior E2 cluster)
            await session.execute(
                delete(cluster_v2_members_table)
                .where(cluster_v2_members_table.c.property_id.in_(ids))
            )

            # INSERT new junction rows
            await session.execute(
                insert(cluster_v2_members_table).values([
                    {"cluster_id": new_e2_id, "property_id": pid}
                    for pid in ids
                ])
            )

            # Cleanup old E2 clusters
            for old_cid in source_clusters_e2:
                count = (await session.execute(
                    select(func.count(cluster_v2_members_table.c.property_id))
                    .where(cluster_v2_members_table.c.cluster_id == old_cid)
                )).scalar() or 0
                if count >= 2:
                    await session.execute(
                        update(PropertyCluster)
                        .where(PropertyCluster.id == old_cid)
                        .values(member_count=count, updated_at=now)
                    )
                else:
                    await _silent_dissolve_cluster_v2(session, old_cid)

            new_cluster_ids["2"] = str(new_e2_id)

        # ============================================================
        # Common — erase prior "NOT duplicates" feedback for these pairs.
        # Universal: applies regardless of which engine we created clusters in.
        # ============================================================
        deleted_feedback = await _delete_feedback_for_pairs(session, ids)

        await session.commit()

        logger.info(
            f"[admin/manual-merge] created {len(new_cluster_ids)} cluster(s) "
            f"in engines={engines} (clusters={new_cluster_ids}, "
            f"properties={len(ids)}, deleted_feedback={deleted_feedback})"
        )

    # --- PowerObject regen — fail-silent, separate session per cluster ---
    try:
        from src.services.power_object_generator import PowerObjectGenerator
        generator = PowerObjectGenerator()
        async with async_session_maker() as fresh_session:
            for cid in new_cluster_ids.values():
                try:
                    result = await generator.generate_for_cluster(fresh_session, cid)
                    if result is None:
                        logger.info(
                            f"[admin/manual-merge] power regen deferred for {cid}"
                        )
                except Exception as inner_e:
                    logger.warning(
                        f"[admin/manual-merge] power regen failed for {cid} "
                        f"(non-fatal): {inner_e}"
                    )
    except Exception as e:
        logger.warning(f"[admin/manual-merge] power regen setup failed: {e}")

    return {
        "status": "ok",
        "cluster_ids": new_cluster_ids,
        "engines": engines,
    }


@app.post("/admin/clusters/{cluster_id}/approve")
async def admin_cluster_approve(cluster_id: str, request: Request):
    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)
        await _manual_verdict(session, admin, cluster_id, ClusterStatus.APPROVED)
    return {"status": "ok"}


@app.post("/admin/clusters/{cluster_id}/reject")
async def admin_cluster_reject(
    cluster_id: str,
    request: Request,
    reason_attributes: str = Form(""),
    reason_text: str = Form(""),
):
    """
    Reject cluster (= dissolve it and record all pairs as NOT-duplicate
    feedback). Sprint 6 Phase A: accepts optional structured reasoning
    so Phase C ML training can learn from WHY the admin rejected.

    Sprint 7 V2-Fix-3: engine-aware member fetch. E1 uses
    Property.cluster_id; E2 uses cluster_v2_members junction. Previously
    V2 reject silently lost structured reasoning because the members
    query found 0 rows (Property.cluster_id is NULL for E2 members).

    reason_attributes: comma-separated taxonomy keys (see
    VALID_REASON_ATTRIBUTES in src/database/feedback_repository.py).
    reason_text: free-form admin note, ≤1000 chars (truncated).
    Both default to empty → existing callers behave identically.
    """
    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        attrs = [a.strip() for a in reason_attributes.split(",") if a.strip()]
        text_clean = reason_text.strip() or None

        if attrs or text_clean:
            # Sprint 7 V2-Fix-3: fetch cluster first to pick the right
            # member-lookup strategy. If cluster doesn't exist (concurrent
            # dissolve), skip feedback write — _manual_verdict below will
            # 404 cleanly.
            cluster = (await session.execute(
                select(PropertyCluster).where(PropertyCluster.id == cluster_id)
            )).scalar_one_or_none()

            if cluster is not None:
                # Engine-aware member lookup:
                #   E1 → Property.cluster_id FK
                #   E2 → cluster_v2_members junction
                if cluster.engine_version == '2':
                    members = (await session.execute(
                        select(Property)
                        .join(
                            cluster_v2_members_table,
                            Property.id == cluster_v2_members_table.c.property_id,
                        )
                        .where(cluster_v2_members_table.c.cluster_id == cluster_id)
                    )).scalars().all()
                else:
                    members = (await session.execute(
                        select(Property).where(Property.cluster_id == cluster_id)
                    )).scalars().all()

                # Structured-feedback write happens BEFORE the dissolve so the
                # upsert inserts rows with reason_attributes/feedback_source
                # set. The bulk INSERT inside _dissolve_cluster_with_feedback
                # then uses ON CONFLICT DO NOTHING — it no-ops on these rows
                # and the structured reasoning is preserved.
                n_written = await record_feedback_for_cluster_rejection(
                    session, members,
                    reason_attributes=attrs,
                    reason_text=text_clean,
                )
                logger.info(
                    f"[admin/reject] cluster {cluster_id} engine={cluster.engine_version} "
                    f"→ wrote {n_written} feedback rows "
                    f"(members={len(members)}, attrs={attrs}, "
                    f"text_len={len(text_clean or '')})"
                )

        await _manual_verdict(session, admin, cluster_id, ClusterStatus.REJECTED)
    return {"status": "ok"}


@app.post("/admin/clusters/{cluster_id}/remove/{property_id}")
async def admin_cluster_remove_member(
    cluster_id: str,
    property_id: str,
    request: Request,
    reason_attributes: str = Form(""),
    reason_text: str = Form(""),
):
    """Хирургическое удаление одного объекта из кластера.

    Sprint 6 Phase A: accepts optional reason_attributes (comma-separated
    taxonomy keys) and reason_text (free-form note ≤1000 chars). Feedback
    rows for (removed, each remaining) are written via
    record_feedback_for_property_removal with feedback_source='manual_split'.
    Both reason fields are optional — existing callers behave identically.

    Sprint 7 V2-Fix-2: engine-aware. Previously assumed engine 1 via
    Property.cluster_id and silently did nothing for E2 clusters
    (members live in cluster_v2_members junction, not the FK). Admin
    would click trash → no visible change, no error. Now branches on
    cluster.engine_version + returns explicit 404 if cluster/property
    missing (was: silent no-op).
    """
    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        # Sprint 7 V2-Fix-2: fetch cluster first — determines engine + access
        cluster = (await session.execute(
            select(PropertyCluster).where(PropertyCluster.id == cluster_id)
        )).scalars().first()
        if not cluster:
            raise HTTPException(status_code=404, detail="Cluster not found")

        # Engine 2 access gate — reviewers blocked (parity with _manual_verdict)
        if cluster.engine_version == '2' and not admin.is_admin:
            raise HTTPException(
                status_code=403,
                detail="Engine 2 operations require admin role",
            )

        # Fetch the property being removed (engine-agnostic)
        prop = (await session.execute(
            select(Property).where(Property.id == property_id)
        )).scalars().first()
        if not prop:
            raise HTTPException(status_code=404, detail="Property not found")

        # Engine-aware membership verify + remaining-members lookup
        if cluster.engine_version == '2':
            # E2: membership lives in cluster_v2_members junction
            membership_exists = (await session.execute(
                select(cluster_v2_members_table.c.property_id)
                .where(
                    cluster_v2_members_table.c.cluster_id == cluster_id,
                    cluster_v2_members_table.c.property_id == property_id,
                )
            )).scalar_one_or_none() is not None

            if not membership_exists:
                # Idempotent: property already not in this cluster (e.g. concurrent removal)
                return {"status": "ok", "removed": False}

            # Remaining members via junction join
            remaining_props = (await session.execute(
                select(Property)
                .join(
                    cluster_v2_members_table,
                    Property.id == cluster_v2_members_table.c.property_id,
                )
                .where(
                    cluster_v2_members_table.c.cluster_id == cluster_id,
                    cluster_v2_members_table.c.property_id != property_id,
                )
            )).scalars().all()
        else:
            # E1: membership via Property.cluster_id FK
            if prop.cluster_id != cluster_id:
                # Idempotent: property already not in this cluster
                return {"status": "ok", "removed": False}

            remaining_props = (await session.execute(
                select(Property).where(
                    Property.cluster_id == cluster_id,
                    Property.id != property_id,
                )
            )).scalars().all()

        # Write feedback (engine-agnostic — feedback_repository works on
        # Property objects directly, no engine awareness needed here)
        attrs = [a.strip() for a in reason_attributes.split(",") if a.strip()]
        text_clean = reason_text.strip() or None
        n_written = await record_feedback_for_property_removal(
            session, prop, remaining_props,
            reason_attributes=attrs,
            reason_text=text_clean,
        )
        logger.info(
            f"[admin/remove] removed property {prop.id} from cluster "
            f"{cluster_id} (engine={cluster.engine_version}) → wrote "
            f"{n_written} feedback rows "
            f"(attrs={attrs}, text_len={len(text_clean or '')})"
        )

        # Engine-aware detach
        if cluster.engine_version == '2':
            # E2: DELETE junction row
            await session.execute(
                delete(cluster_v2_members_table).where(
                    cluster_v2_members_table.c.cluster_id == cluster_id,
                    cluster_v2_members_table.c.property_id == property_id,
                )
            )
        else:
            # E1: clear Property.cluster_id FK
            prop.cluster_id = None

        # Update member count + dissolve if too small (engine-agnostic — both
        # engines store member_count on PropertyCluster row)
        cluster.member_count -= 1
        if cluster.member_count < 2:
            # CASCADE handles junction (E2) + power_properties (both)
            await session.execute(
                delete(PropertyCluster).where(PropertyCluster.id == cluster_id)
            )

        await session.commit()
    return {"status": "ok", "removed": True}


# =============================================================
# V1-AdminAuthority (Sprint 8) — Cluster member proposals
# =============================================================
# Engine 1 creates proposals when new candidate members are detected for
# locked+APPROVED clusters (instead of reverting them, which was Sprint 7
# Task E behavior). Admin reviews queue here and decides per-proposal:
#   * APPROVE → property added to cluster, member_count++
#   * REJECT  → pairs blacklisted in ai_duplicate_feedbacks; cluster intact

@app.post("/admin/proposals/{proposal_id}/approve")
async def admin_proposal_approve(
    proposal_id: str,
    request: Request,
    reason: str = Form(""),
):
    """Admin APPROVE: property added to cluster, member_count++."""
    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        from src.database.cluster_proposal_repository import approve_proposal
        try:
            await approve_proposal(
                session,
                proposal_id=uuid.UUID(proposal_id),
                agent_id=admin.id,
                reason=reason.strip() or None,
            )
            await session.commit()
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    return {"status": "ok"}


@app.post("/admin/proposals/{proposal_id}/reject")
async def admin_proposal_reject(
    proposal_id: str,
    request: Request,
    reason_attributes: str = Form(""),
    reason_text: str = Form(""),
):
    """Admin REJECT: blacklist evidence pairs in ai_duplicate_feedbacks.

    Cluster + property otherwise unchanged. Blacklist entries prevent
    Engine 1 from re-proposing the same pairs in future runs.

    Accepts the same form fields as feedback_modal.html (reason_attributes
    comma-separated taxonomy keys + reason_text free-form note) so the
    existing chip-based modal can be reused without front-end changes.
    """
    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        # Combine structured chips + free text into single audit string,
        # preserving the chip metadata in square brackets for parseability.
        attrs = [a.strip() for a in reason_attributes.split(",") if a.strip()]
        text_clean = reason_text.strip()
        parts = []
        if attrs:
            parts.append(f"[{','.join(attrs)}]")
        if text_clean:
            parts.append(text_clean)
        reason_combined = " ".join(parts) if parts else None

        from src.database.cluster_proposal_repository import reject_proposal
        try:
            await reject_proposal(
                session,
                proposal_id=uuid.UUID(proposal_id),
                agent_id=admin.id,
                reason=reason_combined,
            )
            await session.commit()
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    return {"status": "ok"}

@app.get("/admin/dissolved/data")
async def admin_dissolved_data(
    request: Request,
    source: str | None = None,
    attribute: str | None = None,
    domain: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    """JSON data for /admin/dissolved page filtering. Hit by Task A.4's
    AJAX. limit is capped at 200 to keep responses bounded."""
    async with async_session_maker() as session:
        admin = await get_current_reviewer(request, session)
        if not admin:
            raise HTTPException(status_code=403)

        feedbacks = await fetch_dissolved_feedbacks(
            session,
            feedback_source=source,
            reason_attribute=attribute,
            domain=domain,
            limit=min(limit, 200),
            offset=offset,
        )
        total = await count_dissolved_feedbacks(
            session,
            feedback_source=source,
            reason_attribute=attribute,
            domain=domain,
        )

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "feedbacks": feedbacks,
    }

# =============================================================
# PROPERTIES JSON API (Sprint 7 Task A)
# =============================================================
# Server-side paginated API for Properties tab. Replaces the
# server-rendering of all 2000+ rows in /admin — DataTables now
# uses serverSide:true and calls this endpoint per page/sort/filter.
#
# Same reviewer-level access gate as /admin: admin OR reviewer can
# read. No write operations here.
#
# This API also powers (Sprint 7 Task B) the Single sub-tabs under
# Duplicates → Engine 1 / Engine 2 via cluster_filter query param,
# so any future column / sort / filter additions land in one place.
# =============================================================
@app.get("/api/admin/properties")
async def api_admin_properties(
    request: Request,
    page: int = Query(1, ge=1),
    pageSize: int = Query(25, ge=1, le=200),
    sortBy: str = Query("created_at"),
    sortDir: str = Query("desc"),
    search: str = Query(""),
    source: str = Query(""),
    area: str = Query(""),
    property_type: str = Query("", alias="type"),
    status: str = Query(""),
    priceMin: Optional[int] = Query(None),
    priceMax: Optional[int] = Query(None),
    cluster_filter: str = Query("any"),
):
    """
    Paginated JSON API for the admin Properties table.

    Query parameters
    ----------------
    page / pageSize     1-indexed pagination (cap 200/page to bound payload)
    sortBy / sortDir    Whitelisted column (see SORTABLE_COLUMNS) + asc|desc
    search              Case-insensitive LIKE across site_id, source,
                        calc_municipality, area, category
    source              Exact source_domain
    area                Match either calc_municipality OR area column
    type                Exact category
    status              Exact PropertyStatus enum value
    priceMin / priceMax Inclusive price bounds (€)
    cluster_filter      'any' (default) | 'e1_only' | 'single_e1' |
                        'e2_only' | 'single_e2'

    Returns
    -------
    {
      "data": [ {id, site_property_id, source_domain, price, size_sqm,
                 category, area, status, is_active, last_checked_at,
                 main_image, e1_cluster, e2_cluster}, ... ],
      "total": int,
      "page": int,
      "pageSize": int
    }

    Engine semantics
    ----------------
    Property.cluster_id FK always points to engine 1 cluster (engine 2
    uses cluster_v2_members junction with viewonly=True). So:
      e1 membership  -> Property.cluster_id IS NOT NULL
      e2 membership  -> EXISTS (cluster_v2_members WHERE property_id=...)

    e1_cluster info comes via joinedload (single LEFT JOIN in main query).
    e2_cluster info comes from a separate batched query keyed by the
    current page's property IDs — keeps the main query simple and
    avoids potential row multiplication if a property somehow ended up
    in multiple engine-2 clusters.
    """
    # Whitelist — never allow arbitrary column names from the wire
    SORTABLE_COLUMNS = {
        "site_property_id":  Property.site_property_id,
        "source_domain":     Property.source_domain,
        "area":              Property.calc_municipality,  # UI shows calc_municipality as "Area"
        "category":          Property.category,
        "size_sqm":          Property.size_sqm,
        "price":             Property.price,
        "status":            Property.status,
        "last_checked_at":   Property.last_checked_at,
        "created_at":        Property.created_at,
    }
    sort_col  = SORTABLE_COLUMNS.get(sortBy, Property.created_at)
    sort_expr = sort_col.desc() if sortDir == "desc" else sort_col.asc()

    async with async_session_maker() as session:
        user = await get_current_reviewer(request, session)
        if not user:
            raise HTTPException(status_code=403)

        # Sprint 7 Task C: Engine 2 filters require admin. Reviewers
        # can only ask for any/e1_only/single_e1. Forcing 403 here so
        # the URL can't be hand-crafted to reveal Engine 2 membership.
        if cluster_filter in ("e2_only", "single_e2") and not user.is_admin:
            raise HTTPException(
                status_code=403,
                detail="Engine 2 filters require admin role",
            )

        # --- Build filter conditions ---------------------------------
        conditions = []

        if search:
            s = f"%{search.lower()}%"
            conditions.append(or_(
                func.lower(Property.site_property_id).like(s),
                func.lower(Property.source_domain).like(s),
                func.lower(Property.calc_municipality).like(s),
                func.lower(Property.area).like(s),
                func.lower(Property.category).like(s),
            ))
        if source:
            conditions.append(Property.source_domain == source)
        if area:
            # Area filter matches either calc_municipality OR area field
            # (template falls back from calc_municipality to area when displaying)
            conditions.append(or_(
                Property.calc_municipality == area,
                Property.area == area,
            ))
        if property_type:
            conditions.append(Property.category == property_type)
        if status:
            conditions.append(Property.status == status)
        if priceMin is not None:
            conditions.append(Property.price >= priceMin)
        if priceMax is not None:
            conditions.append(Property.price <= priceMax)

        # --- Cluster filtering --------------------------------------
        if cluster_filter == "e1_only":
            conditions.append(Property.cluster_id.isnot(None))
        elif cluster_filter == "single_e1":
            conditions.append(Property.cluster_id.is_(None))
        elif cluster_filter == "e2_only":
            conditions.append(exists().where(
                cluster_v2_members_table.c.property_id == Property.id
            ))
        elif cluster_filter == "single_e2":
            conditions.append(~exists().where(
                cluster_v2_members_table.c.property_id == Property.id
            ))
        # 'any' or unrecognized → no extra clause

        where_clause = and_(*conditions) if conditions else None

        # --- Count (total matching filter) --------------------------
        count_stmt = select(func.count(Property.id))
        if where_clause is not None:
            count_stmt = count_stmt.where(where_clause)
        total = await session.scalar(count_stmt) or 0

        # --- Paginated fetch ----------------------------------------
        offset = (page - 1) * pageSize
        stmt = (
            select(Property)
            .options(
                selectinload(Property.media),
                joinedload(Property.cluster),  # engine 1 cluster
                defer(Property.embedding),
                defer(Property.image_phashes),
                defer(Property.content_hash),
            )
            .order_by(sort_expr)
            .offset(offset)
            .limit(pageSize)
        )
        if where_clause is not None:
            stmt = stmt.where(where_clause)

        properties = (await session.execute(stmt)).unique().scalars().all()

        # --- Engine 2 cluster lookup (batched) ----------------------
        prop_ids = [str(p.id) for p in properties]
        e2_map: dict[str, dict] = {}
        if prop_ids:
            e2_rows = (await session.execute(
                select(
                    cluster_v2_members_table.c.property_id,
                    PropertyCluster.id,
                    PropertyCluster.status,
                    PropertyCluster.member_count,
                )
                .join(
                    PropertyCluster,
                    PropertyCluster.id == cluster_v2_members_table.c.cluster_id,
                )
                .where(
                    PropertyCluster.engine_version == "2",
                    cluster_v2_members_table.c.property_id.in_(prop_ids),
                )
            )).all()
            for row in e2_rows:
                pid = str(row.property_id)
                if pid not in e2_map:  # defensive — keep first if duplicates
                    e2_map[pid] = {
                        "id": str(row.id),
                        "status": row.status.value if hasattr(row.status, "value") else str(row.status),
                        "member_count": row.member_count,
                    }

        # --- Serialize ---------------------------------------------
        def serialize(prop: Property) -> dict:
            main_media = prop.media[0] if prop.media else None
            return {
                "id":                str(prop.id),
                "site_property_id":  prop.site_property_id,
                "source_domain":     prop.source_domain,
                "price":             prop.price,
                "size_sqm":          int(prop.size_sqm) if prop.size_sqm else None,
                "category":          prop.category,
                "area":              prop.calc_municipality or prop.area,
                "status":            (prop.status.value if hasattr(prop.status, "value") else str(prop.status)),
                "is_active":         prop.is_active,
                "last_checked_at":   prop.last_checked_at.isoformat() if prop.last_checked_at else None,
                "main_image":        (main_media.local_file_path.replace("\\", "/") if main_media and main_media.local_file_path else None),
                "e1_cluster": (
                    {
                        "id":            str(prop.cluster.id),
                        "status":        prop.cluster.status.value if hasattr(prop.cluster.status, "value") else str(prop.cluster.status),
                        "member_count":  prop.cluster.member_count,
                    } if prop.cluster else None
                ),
                # Sprint 7 Task C: e2_cluster omitted for reviewers — Engine 2 is admin-only
                "e2_cluster": e2_map.get(str(prop.id)) if user.is_admin else None,
            }

        data = [serialize(p) for p in properties]

    return {
        "data": data,
        "total": total,
        "page": page,
        "pageSize": pageSize,
    }


@app.get("/api/admin/properties/filters")
async def api_admin_properties_filters(request: Request):
    """
    Distinct filter values for the Properties tab dropdowns.

    Loaded once on page open by the frontend (vs. computed from
    visible rows the old way, which only saw the current page).
    Small response (~2KB for 13 sources + ~30 areas + ~10 categories).
    """
    async with async_session_maker() as session:
        user = await get_current_reviewer(request, session)
        if not user:
            raise HTTPException(status_code=403)

        sources = (await session.execute(
            select(Property.source_domain)
            .distinct()
            .where(Property.source_domain.isnot(None))
            .order_by(Property.source_domain)
        )).scalars().all()

        # Area = calc_municipality (preferred), fallback to area column.
        # COALESCE handles cases where calc_municipality wasn't computed yet.
        areas_raw = (await session.execute(
            select(func.coalesce(Property.calc_municipality, Property.area).label("a"))
            .distinct()
            .order_by("a")
        )).scalars().all()

        types = (await session.execute(
            select(Property.category)
            .distinct()
            .where(Property.category.isnot(None))
            .order_by(Property.category)
        )).scalars().all()

        return {
            "sources": [s for s in sources if s],
            "areas":   [a for a in areas_raw if a],
            "types":   [t for t in types if t],
        }