import asyncio
from curl_cffi.requests import AsyncSession
from fake_useragent import UserAgent
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.core.config import settings

ua = UserAgent(os='windows', browsers=['chrome'])

class RequestEngine:
    def __init__(self):
        self.proxy = settings.PROXY_URL if settings.PROXY_URL else None
        
    def _get_headers(self) -> dict:
        return {
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "User-Agent": ua.random
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception, ValueError)),
        before_sleep=lambda retry_state: logger.warning(f"Запрос не удался, попытка {retry_state.attempt_number}. Ждем...")
    )
    async def get(self, url: str, params: dict = None, headers: dict = None):
        req_headers = self._get_headers()
        if headers: req_headers.update(headers)
        
        async with AsyncSession(impersonate="chrome120") as session:
            response = await session.get(url, params=params, headers=req_headers, proxy=self.proxy, timeout=30, verify=False)
            self._check_response(response, url)
            return response

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception, ValueError)),
        before_sleep=lambda retry_state: logger.warning(f"POST запрос не удался, попытка {retry_state.attempt_number}. Ждем...")
    )
    async def post(self, url: str, data: dict | str = None, headers: dict = None):
        req_headers = self._get_headers()
        if headers: req_headers.update(headers)

        async with AsyncSession(impersonate="chrome120") as session:
            response = await session.post(url, data=data, headers=req_headers, proxy=self.proxy, timeout=30, verify=False)
            self._check_response(response, url)
            return response

    def _check_response(self, response, url: str):
        """Единая проверка на Cloudflare и ошибки"""
        if response.status_code == 403 or "Just a moment..." in response.text:
            logger.error(f"Обнаружена защита Cloudflare на {url}!")
            raise ValueError("Cloudflare block")
        if response.status_code not in (200, 201):
            logger.error(f"Ошибка HTTP: {response.status_code} на {url}")
            response.raise_for_status()