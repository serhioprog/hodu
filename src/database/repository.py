from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.models.domain import Property, Media
from src.models.schemas import PropertyTemplate # <-- Убедись что тут PropertyTemplate
from loguru import logger
import uuid

async def save_or_update_property(session: AsyncSession, data: PropertyTemplate):
    """Сохраняет объект и возвращает его внутренний UUID и список ссылок на фото"""
    try:
        property_dict = data.model_dump()
        # Извлекаем список картинок. Если их нет, будет пустой список []
        images = property_dict.pop("images", [])
        
        query = select(Property).where(Property.url == data.url)
        result = await session.execute(query)
        existing_prop = result.scalar_one_or_none()

        property_uuid = None

        if existing_prop:
            for key, value in property_dict.items():
                setattr(existing_prop, key, value)
            property_uuid = existing_prop.id
        else:
            new_prop = Property(**property_dict)
            session.add(new_prop)
            await session.flush()
            property_uuid = new_prop.id
        
        await session.commit()
        
        # ВАЖНО: Мы возвращаем UUID и список images обратно в main.py
        return property_uuid, images

    except Exception as e:
        await session.rollback()
        logger.error(f"Ошибка БД при сохранении объекта: {e}")
        raise e

async def save_media_records(session: AsyncSession, property_uuid: uuid.UUID, media_data: list[dict]):
    """Сохраняет пути к фотографиям в привязке к объекту"""
    try:
        for img in media_data:
            # Проверяем, нет ли уже такого фото в базе
            query = select(Media).where(Media.property_id == property_uuid, Media.image_url == img["url"])
            result = await session.execute(query)
            if not result.scalar_one_or_none():
                new_media = Media(
                    property_id=property_uuid,
                    image_url=img["url"],
                    local_file_path=img["local_path"],
                    is_main_photo=img["is_main"]
                )
                session.add(new_media)
        await session.commit()
    except Exception as e:
        await session.rollback()
        logger.error(f"Ошибка сохранения медиа: {e}")