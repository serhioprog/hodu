import os
import uuid  # ДОБАВЛЕНО для генерации куки
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import RedirectResponse  # ДОБАВЛЕНО для редиректа
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, cast, Date, or_
from sqlalchemy.orm import selectinload
from datetime import datetime, date
from fastapi import Form

from src.database.db import async_session_maker
# ДОБАВЛЕНО: Импортируем новые модели безопасности
from src.models.domain import Property, Agent, AgentDevice, AuthToken, SystemSetting

app = FastAPI(title="Hodu Real Estate")

# В Docker корень приложения — /app. Папка с данными — /app/data
# Монтируем так, чтобы URL /data/media/... вел в /app/data/media/...
app.mount("/data", StaticFiles(directory="/app/data"), name="data")

templates = Jinja2Templates(directory="src/web/templates")

# Фильтр для замены обратных слэшей на прямые (на всякий случай)
def fix_slashes(path):
    if not path: return ""
    return path.replace("\\", "/")

templates.env.filters["fix_path"] = fix_slashes


# ==========================================
# НОВЫЙ БЛОК: СИСТЕМА БЕЗОПАСНОСТИ (B2B)
# ==========================================
async def get_current_agent(request: Request, session):
    """Охранник: проверяет наличие правильной Cookie в браузере"""
    cookie = request.cookies.get("hodu_session")
    if not cookie:
        return None
    
    # Ищем эту Cookie в базе Устройств
    query = select(AgentDevice).where(AgentDevice.device_cookie == cookie).options(selectinload(AgentDevice.agent))
    result = await session.execute(query)
    device = result.scalars().first()
    
    # Пускаем только если устройство найдено и агент Активен
    if device and device.agent.is_active:
        return device.agent
    return None

async def get_current_admin(request: Request, session):
    agent = await get_current_agent(request, session)
    if agent and agent.is_admin:
        return agent
    return None

@app.get("/auth/{token}")
async def authenticate(request: Request, token: str):
    """Маршрут для входа по уникальной ссылке из письма"""
    async with async_session_maker() as session:
        query = select(AuthToken).where(AuthToken.token == token).options(selectinload(AuthToken.agent))
        result = await session.execute(query)
        auth_token = result.scalars().first()

        # Если токена нет, он использован или агент отключен -> Отказ
        if not auth_token or auth_token.is_used or not auth_token.agent.is_active:
            return templates.TemplateResponse("error.html", {
                "request": request, 
                "message": "Ссылка недействительна, устарела или уже была использована на другом устройстве."
            })

        # Помечаем токен как использованный
        auth_token.is_used = True

        # Генерируем "Печать" (Cookie)
        device_cookie = str(uuid.uuid4())
        user_agent_str = request.headers.get("user-agent", "Unknown Browser")

        # Сохраняем устройство в базу
        new_device = AgentDevice(
            agent_id=auth_token.agent_id,
            device_cookie=device_cookie,
            user_agent_str=user_agent_str
        )
        session.add(new_device)
        await session.commit()

        # Перенаправляем на Дашборд и ставим Cookie в браузер на 30 дней
        response = RedirectResponse(url="/daily-report", status_code=302)
        response.set_cookie(
            key="hodu_session",
            value=device_cookie,
            max_age=30 * 24 * 3600,
            httponly=True,
            samesite="lax"
        )
        return response
# ==========================================


@app.get("/daily-report")
async def daily_report(request: Request, report_date: date = Query(default=None)):
    if not report_date:
        report_date = datetime.utcnow().date()
    
    async with async_session_maker() as session:
        # Проверка доступа
        agent = await get_current_agent(request, session)
        if not agent:
            return templates.TemplateResponse("error.html", {
                "request": request, 
                "message": "Please use your registered device to view"
            })

        query = select(Property).options(
            selectinload(Property.media)
        ).where(
            or_(
                cast(Property.created_at, Date) == report_date,
                cast(Property.updated_at, Date) == report_date
            )
        ).order_by(Property.price.desc())
        
        result = await session.execute(query)
        properties = result.scalars().all()
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request, 
        "properties": properties, 
        "date": report_date.strftime("%d.%m.%Y"),
        "current_user": agent  # 🔥 ПЕРЕДАЕМ АГЕНТА, ЧТОБЫ ПОКАЗАТЬ КНОПКУ АДМИНКИ
    })

@app.get("/property/{prop_id}")
async def property_detail(request: Request, prop_id: str):
    async with async_session_maker() as session:
        # Проверка доступа
        agent = await get_current_agent(request, session)
        if not agent:
            return templates.TemplateResponse("error.html", {
                "request": request, 
                "message": "Please use your registered device to view"
            })

        query = select(Property).options(
            selectinload(Property.media)
        ).where(Property.id == prop_id)
        
        result = await session.execute(query)
        prop = result.scalars().first()
        
        if not prop:
            raise HTTPException(status_code=404, detail="Object not found")
            
        # 🔥 ИСПРАВЛЕНИЕ ЗДЕСЬ: Добавили current_user
        return templates.TemplateResponse("property_detail.html", {
            "request": request, 
            "prop": prop,
            "current_user": agent 
        })

@app.get("/admin/users")
async def admin_users(request: Request):
    async with async_session_maker() as session:
        # Проверка на админа
        agent = await get_current_agent(request, session)
        if not agent or not getattr(agent, 'is_admin', False):
            return RedirectResponse(url="/daily-report")
            
        # Получаем всех агентов
        result = await session.execute(select(Agent).order_by(Agent.created_at.desc()))
        users = result.scalars().all()
        
        return templates.TemplateResponse("admin_users.html", {
            "request": request, 
            "users": users,
            "current_user": agent # 🔥 ПЕРЕДАЕМ АГЕНТА СЮДА ТОЖЕ
        })

@app.post("/admin/users/{user_id}/toggle")
async def toggle_user(user_id: str, request: Request):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin or not getattr(admin, 'is_admin', False): raise HTTPException(status_code=403)
            
        query = select(Agent).where(Agent.id == user_id)
        result = await session.execute(query)
        user = result.scalars().first()
        
        if user:
            user.is_active = not user.is_active
            await session.commit()
            
        # 🔥 Возвращаем на единую админку
        return RedirectResponse(url="/admin", status_code=303)  
    
@app.get("/admin/settings")
async def admin_settings(request: Request):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            return RedirectResponse(url="/daily-report")

        # Получаем текущие настройки
        sync_res = await session.execute(select(SystemSetting).where(SystemSetting.key == 'sync_time'))
        repo_res = await session.execute(select(SystemSetting).where(SystemSetting.key == 'report_time'))
        
        sync_time = sync_res.scalars().first()
        repo_time = repo_res.scalars().first()

        return templates.TemplateResponse("admin_settings.html", {
            "request": request,
            "sync_time": sync_time.value if sync_time else "00:01",
            "report_time": repo_time.value if repo_time else "09:30"
        })

@app.post("/admin/settings/update")
async def update_settings(request: Request, sync_time: str = Form(...), report_time: str = Form(...)):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin: raise HTTPException(status_code=403)

        for key, value in [('sync_time', sync_time), ('report_time', report_time)]:
            res = await session.execute(select(SystemSetting).where(SystemSetting.key == key))
            setting = res.scalars().first()
            if setting:
                setting.value = value
            else:
                session.add(SystemSetting(key=key, value=value))
        
        await session.commit()
        # 🔥 Возвращаем на единую админку
        return RedirectResponse(url="/admin", status_code=303)
    
@app.get("/admin")
async def admin_dashboard(request: Request):
    async with async_session_maker() as session:
        admin = await get_current_admin(request, session)
        if not admin:
            return RedirectResponse(url="/daily-report")

        # 1. Получаем настройки
        sync_res = await session.execute(select(SystemSetting).where(SystemSetting.key == 'sync_time'))
        repo_res = await session.execute(select(SystemSetting).where(SystemSetting.key == 'report_time'))
        sync_time = sync_res.scalars().first()
        repo_time = repo_res.scalars().first()

        # 2. Получаем пользователей
        users_res = await session.execute(select(Agent).order_by(Agent.created_at.desc()))
        users = users_res.scalars().all()

        # 3. Получаем объекты (последние 300 штук для таблицы)
        # 3. Получаем объекты (последние 300 штук для таблицы)
        props_res = await session.execute(
            select(Property)
            .options(selectinload(Property.media))
            .order_by(Property.created_at.desc())
            #.limit(300) #ЛИМИТ, ЧТОБЫ ПОКАЗАТЬ ВСЕ ОБЪЕКТЫ В АДМИНКЕ (для ускорения загрузки)
        )
        properties = props_res.scalars().all()

        return templates.TemplateResponse("admin_dashboard.html", {
            "request": request,
            "current_user": admin,
            "users": users,
            "properties": properties,
            "sync_time": sync_time.value if sync_time else "00:01",
            "report_time": repo_time.value if repo_time else "09:30"
        })