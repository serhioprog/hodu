import asyncio
from curl_cffi.requests import AsyncSession
from fake_useragent import UserAgent
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.core.config import settings

# Генерирует случайные User-Agent'ы
ua = UserAgent(os='windows', browsers=['chrome'])

class RequestEngine:
    def __init__(self):
        # В curl_cffi прокси передается строкой напрямую в запрос
        self.proxy = settings.PROXY_URL if settings.PROXY_URL else None
        
    def _get_headers(self) -> dict:
        return {
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "Referer": "https://glrealestate.gr/",
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        # Ловим только ошибки запросов и нашу ошибку блокировки
        retry=retry_if_exception_type((Exception, ValueError)),
        before_sleep=lambda retry_state: logger.warning(
            f"Запрос не удался, попытка {retry_state.attempt_number}. "
            f"Ждем перед ретраем..."
        )
    )
    async def get(self, url: str, params: dict = None):
        headers = self._get_headers()
        # Добавляем рандомный UA в каждый запрос
        headers["User-Agent"] = ua.random
        
        async with AsyncSession(impersonate="chrome120") as session:
            logger.debug(f"Запрос (CURL): {url} с параметрами {params}")
            
            response = await session.get(
                url, 
                params=params, 
                headers=headers, 
                proxy=self.proxy,
                timeout=30,
                verify=False
            )
            
            # Предпросмотр ответа для отладки
            content_snippet = response.text[:200].strip().replace('\n', '')
            logger.debug(f"Превью ответа: {content_snippet}...")
            
            # Проверяем на блокировку Cloudflare (код 403 или наличие текста)
            if response.status_code == 403 or "Just a moment..." in response.text:
                logger.error(f"Обнаружена защита Cloudflare на {url}!")
                raise ValueError("Cloudflare block")

            # Проверяем на другие ошибки HTTP
            if response.status_code != 200:
                logger.error(f"Ошибка HTTP: {response.status_code}")
                response.raise_for_status()
                
            return response