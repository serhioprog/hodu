import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Float, Text, Boolean, DateTime, ForeignKey, Enum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import enum

from src.database.db import Base
from sqlalchemy import UniqueConstraint # Добавь в импорты

def utcnow():
    return datetime.now(timezone.utc)

class PropertyStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    DELISTED = "DELISTED"
    SOLD = "SOLD"

class Property(Base):
    __tablename__ = "properties"

    # Идентификаторы
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    site_property_id = Column(String(50), index=True, nullable=False) # ID с сайта донора (например, 1418)
    source_domain = Column(String(100), nullable=False) # glrealestate.gr
    url = Column(String(500), unique=True, nullable=False)

    # Основные данные
    price = Column(Integer, nullable=True)
    size_sqm = Column(Float, nullable=True)
    land_size_sqm = Column(Float, nullable=True)
    bedrooms = Column(Integer, nullable=True)
    bathrooms = Column(Integer, nullable=True)
    year_built = Column(Integer, nullable=True)
    
    area = Column(String, nullable=True)          # Кассандра и т.д.
    subarea = Column(String, nullable=True)       # Пефкохори и т.д.
    category = Column(String, nullable=True)      # Вилла / Апартаменты
    levels = Column(String, nullable=True)        # Этажность
    description = Column(Text, nullable=True)     # Теперь используем Text для длинных описаний
    site_last_updated = Column(String, nullable=True) # Дата строкой
    
    # Локация
    location_raw = Column(String(255), nullable=True) # Как написано на сайте (напр. Sithonia)
    latitude = Column(Float, nullable=True) # Вытащим из карты
    longitude = Column(Float, nullable=True)
    
    # Описание и мета
    #site_last_updated = Column(DateTime(timezone=True), nullable=True)
    status = Column(String, default=PropertyStatus.ACTIVE.value, index=True)
    
    # Системные даты
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    # Связи
    media = relationship("Media", back_populates="property", cascade="all, delete-orphan")
    price_history = relationship("PriceHistory", back_populates="property", cascade="all, delete-orphan")
    
    __table_args__ = (
        UniqueConstraint('source_domain', 'site_property_id', name='_source_site_uc'),
    )

class Media(Base):
    __tablename__ = "media"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    property_id = Column(UUID(as_uuid=True), ForeignKey("properties.id", ondelete="CASCADE"))
    
    image_url = Column(String(1000), nullable=False) # Оригинальный URL с сайта
    local_file_path = Column(String(500), nullable=True) # Путь на нашем сервере/S3 после скачивания
    is_main_photo = Column(Boolean, default=False) # Главное фото карточки
    
    created_at = Column(DateTime(timezone=True), default=utcnow)

    property = relationship("Property", back_populates="media")


class PriceHistory(Base):
    __tablename__ = "price_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    property_id = Column(UUID(as_uuid=True), ForeignKey("properties.id", ondelete="CASCADE"))
    
    old_price = Column(Integer, nullable=True)
    new_price = Column(Integer, nullable=False)
    
    changed_at = Column(DateTime(timezone=True), default=utcnow)

    property = relationship("Property", back_populates="price_history")