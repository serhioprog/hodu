import os
import hashlib
import asyncio
from pathlib import Path
from loguru import logger
from curl_cffi.requests import AsyncSession

# Подключаем сервис для вычисления перцептивного хэша (для поиска дубликатов)
from src.services.phash_service import PHashService

ROOT_DIR = Path(__file__).resolve().parent.parent.parent

class MediaDownloader:
    def __init__(self, base_dir: str = "data/media"):
        # Создаем базовую папку data/media, если ее нет
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    async def download_images(self, domain: str, property_id: str, image_urls: list[str]) -> list[dict]:
        """
        Скачивает картинки и ДОПОЛНИТЕЛЬНО считает pHash для каждой.
        Раскладывает по структуре: data/media/{domain}/{property_id}/
        Возвращает list[{url, local_path, is_main, phash}]
        """
        if not image_urls:
            return []

        # Создаем подпапку: data/media/domain.com/1711
        prop_dir = self.base_dir / str(domain) / str(property_id)
        prop_dir.mkdir(parents=True, exist_ok=True)

        downloaded_media = []
        
        # Используем curl_cffi для обхода защиты при скачивании
        async with AsyncSession(impersonate="chrome120") as session:
            for idx, url in enumerate(image_urls):
                try:
                    # Генерируем уникальное имя файла (01_ab83kd.jpg)
                    ext = url.split('.')[-1].split('?')[0]
                    if len(ext) > 4 or not ext: 
                        ext = "jpg"
                    filename = f"{idx+1:02d}_{hashlib.md5(url.encode()).hexdigest()[:6]}.{ext}"
                    file_path = prop_dir / filename

                    content: bytes | None = None

                    # Проверяем, есть ли файл на диске
                    if file_path.exists():
                        # Если файл уже скачан — читаем его байты, чтобы посчитать хэш
                        content = file_path.read_bytes()
                    else:
                        # Если файла нет — скачиваем
                        response = await session.get(url, timeout=15, verify=False)
                        if response.status_code == 200:
                            content = response.content
                            # Сохраняем файл на диск с помощью write_bytes (более короткий синтаксис)
                            file_path.write_bytes(content)
                        else:
                            logger.error(f"Не удалось скачать фото {url}: статус {response.status_code}")
                            continue

                    # Вычисляем pHash из байтов картинки (для ИИ-сравнения объектов)
                    phash = PHashService.compute_from_bytes(content) if content else None

                    # Добавляем в итоговый список
                    downloaded_media.append({
                        "url": url,
                        "local_path": str(file_path),
                        "is_main": idx == 0, # Первое фото считаем главным
                        "phash": phash
                    })

                except Exception as e:
                    logger.error(f"Ошибка обработки/скачивания фото {url}: {e}")

                await asyncio.sleep(0.2) # Небольшая пауза между запросами
                
        return downloaded_media