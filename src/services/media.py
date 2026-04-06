import os
import hashlib
import asyncio
from pathlib import Path
from loguru import logger
from curl_cffi.requests import AsyncSession

class MediaDownloader:
    def __init__(self, base_dir: str = "data/media"):
        # Создаем папку data/media, если ее нет
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    async def download_images(self, property_id: str, image_urls: list[str]) -> list[dict]:
        """
        Скачивает картинки и возвращает список словарей с путями.
        """
        if not image_urls:
            return []

        # Создаем подпапку для конкретного объекта (напр. data/media/1711)
        prop_dir = self.base_dir / str(property_id)
        prop_dir.mkdir(parents=True, exist_ok=True)

        downloaded_media = []
        
        # Используем curl_cffi для обхода защиты при скачивании
        async with AsyncSession(impersonate="chrome120") as session:
            for idx, url in enumerate(image_urls):
                try:
                    # Генерируем уникальное имя файла (01_ab83kd.jpg)
                    ext = url.split('.')[-1].split('?')[0]
                    if len(ext) > 4 or not ext: ext = "jpg"
                    filename = f"{idx+1:02d}_{hashlib.md5(url.encode()).hexdigest()[:6]}.{ext}"
                    file_path = prop_dir / filename

                    # Если файл уже скачан — пропускаем
                    if file_path.exists():
                        downloaded_media.append({
                            "url": url,
                            "local_path": str(file_path),
                            "is_main": idx == 0 # Первое фото считаем главным
                        })
                        continue

                    # Скачиваем
                    response = await session.get(url, timeout=15, verify=False)
                    if response.status_code == 200:
                        # Сохраняем файл на диск
                        with open(file_path, 'wb') as f:
                            f.write(response.content)
                        
                        downloaded_media.append({
                            "url": url,
                            "local_path": str(file_path),
                            "is_main": idx == 0
                        })
                except Exception as e:
                    logger.error(f"Ошибка скачивания фото {url}: {e}")

                await asyncio.sleep(0.2) # Небольшая пауза между фото
                
        return downloaded_media