import uuid
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Float, Text, Boolean, DateTime, ForeignKey, Enum, BigInteger
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import enum

from src.database.db import Base
from sqlalchemy import UniqueConstraint

def utcnow():
    return datetime.now(timezone.utc)

class PropertyStatus(str, enum.Enum):
    PRICE_CHANGED = "PRICE_CHANGED"
    NEW = "NEW"
    ACTIVE = "ACTIVE"
    DELISTED = "DELISTED"
    SOLD = "SOLD"

class Property(Base):
    __tablename__ = "properties"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    site_property_id = Column(String(50), index=True, nullable=False)
    source_domain = Column(String(100), nullable=False)
    url = Column(String(500), unique=True, nullable=False)

    price = Column(Integer, nullable=True)
    previous_price = Column(Integer, nullable=True)
    size_sqm = Column(Float, nullable=True)
    land_size_sqm = Column(Float, nullable=True)
    bedrooms = Column(Integer, nullable=True)
    bathrooms = Column(Integer, nullable=True)
    year_built = Column(Integer, nullable=True)
    
    area = Column(String, nullable=True)
    subarea = Column(String, nullable=True)
    category = Column(String, nullable=True)
    levels = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    site_last_updated = Column(String, nullable=True)
    
    location_raw = Column(String(255), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    
    status = Column(String, default=PropertyStatus.NEW.value, index=True)
    is_active = Column(Boolean, default=True, index=True)
    extra_features = Column(JSONB, nullable=True, default=dict)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    last_checked_at = Column(DateTime(timezone=True), default=utcnow)
    last_seen_at = Column(DateTime(timezone=True), default=utcnow)

    media = relationship("Media", back_populates="property", cascade="all, delete-orphan")
    price_history = relationship("PriceHistory", back_populates="property", cascade="all, delete-orphan")
    
    # 🔥 ИСПРАВЛЕНО: Ссылаемся на правильную таблицу
    location_id = Column(Integer, ForeignKey("sdht_property_areas.id"), nullable=True)
    calc_prefecture = Column(String(255), nullable=True)
    calc_municipality = Column(String(255), nullable=True)
    calc_area = Column(String(255), nullable=True)
    
    __table_args__ = (
        UniqueConstraint('source_domain', 'site_property_id', name='_source_site_uc'),
    )

class Media(Base):
    __tablename__ = "media"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    property_id = Column(UUID(as_uuid=True), ForeignKey("properties.id", ondelete="CASCADE"))
    image_url = Column(String(1000), nullable=False)
    local_file_path = Column(String(500), nullable=True)
    is_main_photo = Column(Boolean, default=False)
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

# ==========================================
# НОВЫЙ БЛОК ЛОКАЦИЙ (Полностью совпадает с SQL дампами)
# ==========================================

class SdhtPrefecture(Base):
    __tablename__ = "sdht_property_prefectures"

    id = Column(Integer, primary_key=True)
    geometry_location_id = Column(Integer, nullable=True)
    country_id = Column(Integer, nullable=True)
    prefecture_en = Column(String(255), nullable=True)
    prefecture_el = Column(String(255), nullable=True)
    lat = Column(String(100), nullable=True)
    lng = Column(String(100), nullable=True)
    zoom = Column(Integer, nullable=True)
    created_at = Column(String(100), nullable=True)
    updated_at = Column(String(100), nullable=True)
    # Эти колонки просил SQL:
    active = Column(Integer, nullable=True)
    deleted = Column(Integer, nullable=True)

class SdhtMunicipality(Base):
    __tablename__ = "sdht_property_municipalities"

    id = Column(Integer, primary_key=True)
    country_id = Column(Integer, nullable=True)
    prefecture_id = Column(Integer, ForeignKey("sdht_property_prefectures.id"), nullable=True)
    geometry_location_id = Column(Integer, nullable=True)
    municipality_en = Column(String(255), nullable=True)
    municipality_el = Column(String(255), nullable=True)
    lat = Column(String(100), nullable=True)
    lng = Column(String(100), nullable=True)
    zoom = Column(Integer, nullable=True)
    # Эти колонки просил SQL:
    dom_city_id = Column(Integer, nullable=True)
    xe_city_id = Column(Integer, nullable=True)
    fer_city_id = Column(Integer, nullable=True)
    created_at = Column(String(100), nullable=True)
    updated_at = Column(String(100), nullable=True)
    active = Column(Integer, nullable=True)
    deleted = Column(Integer, nullable=True)

class SdhtArea(Base):
    __tablename__ = "sdht_property_areas"

    id = Column(Integer, primary_key=True)
    country_id = Column(Integer, nullable=True)
    prefecture_id = Column(Integer, ForeignKey("sdht_property_prefectures.id"), nullable=True)
    municipality_id = Column(Integer, ForeignKey("sdht_property_municipalities.id"), nullable=True)
    area_en = Column(String(255), nullable=True)
    area_el = Column(String(255), nullable=True)
    lat = Column(String(100), nullable=True)
    lng = Column(String(100), nullable=True)
    zoom = Column(Integer, nullable=True)
    # Эти колонки просил SQL (geometric_location - это длинный текст с массивами PHP):
    geometric_location = Column(Text, nullable=True)
    pref_sp_id = Column(String(255), nullable=True)
    mun_sp_id = Column(String(255), nullable=True)
    area_sp_id = Column(String(255), nullable=True)
    area_xe_id = Column(String(255), nullable=True)
    postal = Column(String(255), nullable=True)
    mortgage_office_type_id = Column(String(255), nullable=True)
    created_at = Column(String(100), nullable=True)
    updated_at = Column(String(100), nullable=True)
    active = Column(Integer, nullable=True)
    deleted = Column(Integer, nullable=True)

    # ==========================================
# НОВЫЙ БЛОК: СИСТЕМА B2B ДОСТУПА (АГЕНТЫ И ТОКЕНЫ)
# ==========================================

class Agent(Base):
    """Таблица клиентов (Агентов по недвижимости)"""
    __tablename__ = "agents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    phone = Column(String(50), nullable=True)  # Для будущих SMS
    
    # Главный рубильник: если False, агент не получит письмо и не зайдет на сайт
    is_active = Column(Boolean, default=True) 
    is_admin = Column(Boolean, default=False)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    
    # Связи
    devices = relationship("AgentDevice", back_populates="agent", cascade="all, delete-orphan")
    tokens = relationship("AuthToken", back_populates="agent", cascade="all, delete-orphan")

class SystemSetting(Base):
    """Таблица для хранения настроек (время парсинга, время рассылки)"""
    __tablename__ = "system_settings"
    
    id = Column(Integer, primary_key=True)
    key = Column(String(50), unique=True, index=True) # 'sync_time' или 'report_time'
    value = Column(String(100)) # Например, '00:01'
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

class AgentDevice(Base):
    """Таблица 'подписанных' браузеров агентов (Cookie)"""
    __tablename__ = "agent_devices"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id = Column(UUID(as_uuid=True), ForeignKey("agents.id", ondelete="CASCADE"), nullable=False)
    
    # То самое длинное значение Cookie, которое мы положим агенту в браузер
    device_cookie = Column(String(255), unique=True, index=True, nullable=False)
    
    # Инфа о браузере, чтобы ты видел, откуда он заходил (например, "Safari on iPhone")
    user_agent_str = Column(String(500), nullable=True) 
    
    last_seen_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    
    agent = relationship("Agent", back_populates="devices")


class AuthToken(Base):
    """Таблица Магических Ссылок (и OTP кодов)"""
    __tablename__ = "auth_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id = Column(UUID(as_uuid=True), ForeignKey("agents.id", ondelete="CASCADE"), nullable=False)
    
    # Это либо длинный токен для ссылки (xY7z9P...), либо 4 цифры для SMS
    token = Column(String(255), unique=True, index=True, nullable=False)
    
    # Если True, значит по ссылке уже кликнули. Второй раз она не сработает.
    is_used = Column(Boolean, default=False)
    
    # Когда истекает срок действия (например, через 24 часа)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    
    agent = relationship("Agent", back_populates="tokens")