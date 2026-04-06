from abc import ABC, abstractmethod
from loguru import logger
from src.core.http_client import RequestEngine

class BaseScraper(ABC):
    def __init__(self):
        self.client = RequestEngine()
        self.source_domain = ""

    @abstractmethod
    async def fetch_listings(self):
        """Метод для сбора списка объектов"""
        pass

    @abstractmethod
    async def fetch_details(self, url: str):
        """Метод для парсинга внутренностей карточки"""
        pass