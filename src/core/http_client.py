import os
import httpx
from fake_useragent import UserAgent
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.core.config import settings

# Генерирует случайные User-Agent'ы реальных браузеров
ua = UserAgent(os='windows', browsers=['chrome', 'edge', 'firefox'])

class RequestEngine:
    def __init__(self):
        self.proxies = {"all://": settings.PROXY_URL} if settings.PROXY_URL else None
        # Таймаут: 5 сек на подключение, 15 сек на чтение ответа
        self.timeout = httpx.Timeout(15.0, connect=5.0)
        
    def _get_headers(self) -> dict:
        return {
            "User-Agent": ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    # Декоратор tenacity: 3 попытки, пауза между ними увеличивается (2с, 4с, 8с)
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        before_sleep=lambda retry_state: logger.warning(f"Request failed, retrying... Attempt {retry_state.attempt_number}")
    )
    async def get(self, url: str, params: dict = None) -> httpx.Response:
        headers = self._get_headers()
        
        async with httpx.AsyncClient(proxies=self.proxies, timeout=self.timeout, verify=False) as client:
            logger.debug(f"Fetching: {url}")
            response = await client.get(url, headers=headers, params=params)
            
            response.raise_for_status()
            
            # Защита от Cloudflare
            if "Just a moment..." in response.text or "cloudflare" in response.text.lower():
                logger.error(f"Cloudflare block detected on {url}!")
                raise ValueError("Cloudflare block")
                
            return response