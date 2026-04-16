from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.models.domain import Property, Media, PropertyStatus
from src.models.schemas import PropertyTemplate
from loguru import logger
import uuid

async def save_or_update_property(session: AsyncSession, data: PropertyTemplate):
    """Умное сохранение: не затирает статусы и отслеживает новинки"""
    try:
        property_dict = data.model_dump()
        images = property_dict.pop("images", [])
        
        # Ищем по URL
        query = select(Property).where(Property.url == data.url)
        result = await session.execute(query)
        existing_prop = result.scalar_one_or_none()

        property_uuid = None

        if existing_prop:
            # Список полей, которые нельзя перезаписывать вслепую (чтобы не убить статус PRICE_CHANGED)
            protected_fields = {"status", "is_active", "previous_price", "last_seen_at"}
            
            for key, value in property_dict.items():
                if key not in protected_fields and value is not None:
                    setattr(existing_prop, key, value)
            property_uuid = existing_prop.id
        else:
            # Это новый объект — ставим статус NEW
            property_dict["status"] = PropertyStatus.NEW.value
            property_dict["is_active"] = True
            
            new_prop = Property(**property_dict)
            session.add(new_prop)
            await session.flush()
            property_uuid = new_prop.id
        
        await session.commit()
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