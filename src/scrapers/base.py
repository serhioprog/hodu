from abc import ABC, abstractmethod
from loguru import logger
from src.core.http_client import RequestEngine

class BaseScraper(ABC):
    def __init__(self):
        # Единый движок для ВСЕХ скраперов (экономит память, управляет прокси)
        self.client = RequestEngine()
        self.source_domain = ""

    @abstractmethod
    async def collect_urls(self, min_price: int = 400000):
        """Метод для сбора списка объектов (Фаза 1)"""
        pass

    @abstractmethod
    async def fetch_details(self, url: str):
        """Метод для парсинга внутренностей карточки (Фаза 2)"""
        pass