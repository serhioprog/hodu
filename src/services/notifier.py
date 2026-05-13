"""
Async magic-link mailer.

Design decisions:
  * aiosmtplib instead of smtplib — no event-loop blocking on SSL handshake.
  * Tokens are generated upfront and staged in a list. We commit them
    as one batch AFTER every email is sent, so a mid-flight SMTP crash
    does NOT leave the DB with tokens that were never actually delivered.
  * agent.name is HTML-escaped (XSS protection — admin can set any name).
  * SMTP connection is always closed in `finally`.
  * TTL on tokens stays at 48h, but `/auth/{token}` will now enforce it
    (see main.py / partition 3).
"""
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from typing import Optional
from uuid import UUID
import secrets

import aiosmtplib
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.database.db import async_session_maker
from src.models.domain import Agent, AuthToken


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _build_html(agent_name: str, report_date_str: str, count: int, link: str) -> str:
    safe_name = escape(agent_name or "there")
    safe_date = escape(report_date_str)
    safe_link = escape(link, quote=True)
    return f"""
<html>
<body style="font-family: sans-serif; color: #222; background-color: #f7f7f7; padding: 20px;">
    <div style="max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 12px; border: 1px solid #eee;">
        <h1 style="color: #ff385c; margin-bottom: 20px;">hodu.</h1>
        <p>Hello, <b>{safe_name}</b>!</p>
        <p>The real estate report for <b>{safe_date}</b> is ready.</p>
        <p style="font-size: 20px; font-weight: bold; margin: 20px 0; color: #111;">
            {count} new updates (listings &amp; price drops) ready for review.
        </p>
        <div style="text-align: center; margin: 30px 0;">
            <a href="{safe_link}" style="background: #ff385c; color: white; padding: 15px 25px;
               text-decoration: none; border-radius: 8px; font-weight: bold;">View Report</a>
        </div>
        <p style="font-size: 12px; color: #888;">This is a private secure link. Do not forward this email.</p>
    </div>
</body>
</html>
"""


async def _stage_tokens(
    session: AsyncSession, agents: list[Agent]
) -> list[tuple[Agent, AuthToken]]:
    """Create AuthToken rows in-memory (not flushed/committed yet)."""
    staged: list[tuple[Agent, AuthToken]] = []
    expires_at = _utcnow() + timedelta(hours=settings.TOKEN_TTL_HOURS)
    for agent in agents:
        token = AuthToken(
            agent_id=agent.id,
            token=secrets.token_urlsafe(32),   # stronger than uuid4.hex
            is_used=False,
            expires_at=expires_at,
        )
        session.add(token)
        staged.append((agent, token))
    # flush so tokens have primary keys if we ever want to reference them
    await session.flush()
    return staged


async def send_magic_links_to_agents(
    report_date_str: str,
    properties_count: int,
) -> dict[str, int]:
    """
    Returns stats: {
        "sent": N, "failed": M, "skipped": K,
        "per_agent": [
            {"email": str, "status": "DELIVERED"|"FAILED", "error": str|None},
            ...
        ]
    }
    Skipped = inactive agents, or zero-property report.
    per_agent is empty when dispatch short-circuits (no properties,
    SMTP not configured, no active agents) — caller can fall back
    to its own agent list in that case (Bug #3).
    """
    stats: dict = {"sent": 0, "failed": 0, "skipped": 0, "per_agent": []}

    if properties_count == 0:
        logger.info("[Notifier] no new properties — skipping dispatch")
        return stats

    if not (settings.SMTP_USER and settings.SMTP_PASS and settings.SMTP_HOST):
        logger.error("[Notifier] SMTP credentials are not configured — abort")
        return stats

    async with async_session_maker() as session:
        agents = (await session.execute(
            select(Agent).where(Agent.is_active.is_(True))
        )).scalars().all()

        if not agents:
            logger.warning("[Notifier] no active agents")
            return stats

        # Stage all tokens in one go — commit only on success path.
        staged = await _stage_tokens(session, agents)

        smtp: Optional[aiosmtplib.SMTP] = None
        try:
            smtp = aiosmtplib.SMTP(
                hostname=settings.SMTP_HOST,
                port=settings.SMTP_PORT,
                start_tls=True,
                timeout=20,
            )
            await smtp.connect()
            await smtp.login(settings.SMTP_USER, settings.SMTP_PASS)

            for agent, token in staged:
                try:
                    link = f"{settings.APP_URL}/auth/{token.token}"

                    msg = MIMEMultipart("alternative")
                    msg["Subject"] = f"hodu. | Daily Report {report_date_str}"
                    msg["From"]    = f"hodu. <{settings.SMTP_USER}>"
                    msg["To"]      = agent.email
                    msg.attach(MIMEText(
                        _build_html(agent.name, report_date_str, properties_count, link),
                        "html",
                    ))

                    await smtp.send_message(msg)
                    stats["sent"] += 1
                    stats["per_agent"].append({
                        "email": agent.email,
                        "status": "DELIVERED",
                        "error": None,
                    })
                    logger.success(f"[Notifier] sent -> {agent.email}")

                except Exception as e:
                    stats["failed"] += 1
                    stats["per_agent"].append({
                        "email": agent.email,
                        "status": "FAILED",
                        "error": str(e)[:200],
                    })
                    logger.error(f"[Notifier] send -> {agent.email} FAILED: {e}")
                    # Invalidate the token so an un-delivered email can't be
                    # abused if the address is typosquatted by an attacker.
                    token.is_used = True

        except Exception as e:
            logger.error(f"[Notifier] SMTP connect/login failed: {e}")
            # All staged tokens become unusable — rollback the whole batch.
            await session.rollback()
            stats["failed"] = len(staged)
            stats["sent"] = 0
            err = f"SMTP connect/login: {str(e)[:180]}"
            stats["per_agent"] = [
                {"email": a.email, "status": "FAILED", "error": err}
                for a, _ in staged
            ]
            return stats

        finally:
            if smtp is not None:
                try:
                    await smtp.quit()
                except Exception:
                    pass

        await session.commit()
        logger.info(f"[Notifier] dispatch done: {stats}")
        return stats