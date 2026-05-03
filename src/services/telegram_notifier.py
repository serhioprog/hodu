"""
Telegram notification service.

Used to send post-sync reports to a Telegram channel/group via Bot API.
Silently no-ops if TG_BOT_TOKEN or TG_CHAT_ID are not configured.

Why a dedicated module (separate from notifier.py):
  notifier.py handles email magic-links via SMTP.
  telegram_notifier.py handles operational alerts via Telegram Bot API.
Different transport, different escaping rules, different failure modes.

Markdown escaping:
  Telegram uses MarkdownV2 which requires escaping a long list of chars.
  We use plain HTML mode instead — much simpler, only 3 chars to escape
  (<, >, &), and all our reports are bot-generated so no XSS surface.
"""
from __future__ import annotations

import asyncio
from html import escape as html_escape
from typing import Optional

import httpx
from loguru import logger

from src.core.config import settings


_TG_API = "https://api.telegram.org"


class TelegramNotifier:
    """
    Async Telegram Bot API client. Stateless — safe to instantiate
    multiple times. Network calls have a 10s timeout each.

    Uses HTML parse_mode (not Markdown / MarkdownV2) because:
      - HTML escaping is trivial (3 chars vs ~18 in MarkdownV2)
      - Bot-generated content has no user input → no XSS risk
      - We only need bold (<b>), italic (<i>), code (<code>), <pre>
    """

    TIMEOUT_SECONDS = 10.0
    MAX_MESSAGE_LEN = 4096          # Telegram hard limit for message text

    def __init__(self) -> None:
        self._token: Optional[str]   = settings.TG_BOT_TOKEN
        self._chat_id: Optional[str] = settings.TG_CHAT_ID

    @property
    def enabled(self) -> bool:
        """True if both BOT_TOKEN and CHAT_ID are configured."""
        return bool(self._token and self._chat_id)

    @staticmethod
    def escape(text: str) -> str:
        """Escape user/log data for HTML parse mode."""
        return html_escape(str(text), quote=False)

    async def send(self, text: str, *, silent: bool = False) -> bool:
        """
        Send a single message. Truncated to MAX_MESSAGE_LEN if too long.
        Returns True on success, False on any failure (NEVER raises).

        silent=True   → no notification sound on user device (use for
                        routine reports). False = ping (use for alerts).
        """
        if not self.enabled:
            logger.debug("[TG] notifier disabled (no token or chat_id)")
            return False

        if not text:
            return False

        # Hard truncate — better a partial message than an API rejection
        if len(text) > self.MAX_MESSAGE_LEN:
            text = text[: self.MAX_MESSAGE_LEN - 32] + "\n\n... <i>truncated</i>"

        url = f"{_TG_API}/bot{self._token}/sendMessage"
        payload = {
            "chat_id":                  self._chat_id,
            "text":                     text,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
            "disable_notification":     silent,
        }

        try:
            async with httpx.AsyncClient(timeout=self.TIMEOUT_SECONDS) as client:
                resp = await client.post(url, json=payload)

            if resp.status_code == 200:
                return True

            # Useful diagnostics — TG returns clear JSON errors
            try:
                err = resp.json().get("description", resp.text)
            except Exception:
                err = resp.text
            logger.error(
                f"[TG] sendMessage failed (HTTP {resp.status_code}): {err}"
            )
            return False

        except httpx.TimeoutException:
            logger.error(f"[TG] timeout after {self.TIMEOUT_SECONDS}s")
            return False
        except Exception as e:
            logger.exception(f"[TG] unexpected error: {e}")
            return False


# Module-level singleton — re-import safe, costs nothing to instantiate.
telegram_notifier = TelegramNotifier()