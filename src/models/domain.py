"""
ORM domain models for Hodu Real Estate.

Single source of truth for DB schema. DDL for special types (pgvector,
HNSW, enums) lives in src/migrations/*.sql — create_type=False here.

UTC contract:
  * utcnow() returns aware UTC datetime.
  * All DateTime columns are timezone=True; asyncpg roundtrips them as UTC.
  * Code comparing timestamps must use datetime.now(timezone.utc) — never
    datetime.now() (which returns naive local time and silently breaks
    cooldown / TTL / freshness checks).
"""
import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    ARRAY, Boolean, Column, DateTime, Float,
    ForeignKey, Integer, Numeric, SmallInteger, String, Text, UniqueConstraint,
    Enum as SAEnum,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from pgvector.sqlalchemy import Vector

from src.database.db import Base


# =============================================================
# ENUMS
# =============================================================

class PropertyStatus(str, enum.Enum):
    NEW = "NEW"
    ACTIVE = "ACTIVE"
    PRICE_CHANGED = "PRICE_CHANGED"
    DELISTED = "DELISTED"
    SOLD = "SOLD"


class ClusterStatus(str, enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    """RESERVED / command-flag only. Bug #45: REJECTED is never actually
    persisted to cluster.status — it's used as a parameter value to
    _manual_verdict() that triggers cluster dissolution (DSU break +
    feedback rows in ai_duplicate_feedbacks). When admin clicks 'Reject',
    the cluster row is DELETED, not marked REJECTED. Kept in the enum
    only because removing it would require migration + touching ~5 call
    sites in main.py. Future: rename ClusterAction enum separate from
    ClusterStatus state enum."""


def utcnow() -> datetime:
    """Aware UTC datetime. NEVER use datetime.now() (naive, local TZ)."""
    return datetime.now(timezone.utc)


# =============================================================
# CLUSTER
# =============================================================

class PropertyCluster(Base):
    __tablename__ = "property_clusters"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    status = Column(
        SAEnum(ClusterStatus, name="cluster_status", create_type=False),
        nullable=False,
        default=ClusterStatus.PENDING,
        index=True,
    )

    member_count = Column(Integer, nullable=False, default=0)

    # --- Admin manual override ------------------------------------
    # When True, InternalDuplicateDetector MUST NOT touch `status`.
    # Note: REJECTED clusters are NEVER locked — they get dissolved entirely
    # (members detached, cluster deleted) by the reject endpoint.
    verdict_locked = Column(Boolean, nullable=False, default=False)
    verdict_locked_at = Column(DateTime(timezone=True), nullable=True)
    verdict_locked_by = Column(
        UUID(as_uuid=True),
        ForeignKey("agents.id", ondelete="SET NULL"),
        nullable=True,
    )

    # --- External uniqueness audit trail -------------------------
    last_external_is_unique = Column(Boolean, nullable=True)
    last_external_check_at  = Column(DateTime(timezone=True), nullable=True)
    power_generated_at      = Column(DateTime(timezone=True), nullable=True)
    notes                   = Column(Text, nullable=True)

    # --- AI analytics fields (added in migration 005) -------------
    # ai_score:      max embedding similarity among edges connecting members
    # phash_matches: max pHash photo matches among edges connecting members
    ai_score      = Column(Float,   nullable=True)
    phash_matches = Column(Integer, nullable=True)

    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    members = relationship(
        "Property",
        back_populates="cluster",
        foreign_keys="Property.cluster_id",
    )
    power_object = relationship(
        "PowerProperty",
        back_populates="cluster",
        uselist=False,
        cascade="all, delete-orphan",
    )


# =============================================================
# CORE DOMAIN: Property, Media, PriceHistory
# =============================================================

class Property(Base):
    __tablename__ = "properties"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    site_property_id = Column(String(255),  index=True, nullable=False)
    source_domain    = Column(String(100), nullable=False)
    url              = Column(String(500), unique=True, nullable=False)

    # --- Facts ----------------------------------------------------
    price          = Column(Integer, nullable=True)
    previous_price = Column(Integer, nullable=True)
    size_sqm       = Column(Float,   nullable=True)
    land_size_sqm  = Column(Float,   nullable=True)
    bedrooms       = Column(Integer, nullable=True)
    bathrooms      = Column(Integer, nullable=True)
    year_built     = Column(Integer, nullable=True)

    area      = Column(String, nullable=True)
    subarea   = Column(String, nullable=True)
    category  = Column(String, nullable=True)
    levels    = Column(String, nullable=True)

    description       = Column(Text,         nullable=True)
    site_last_updated = Column(String,       nullable=True)
    location_raw      = Column(String(255),  nullable=True)
    latitude          = Column(Float,        nullable=True)
    longitude         = Column(Float,        nullable=True)

    # --- Status (ENUM type in DB, created by migration) ----------
    status = Column(
        SAEnum(PropertyStatus, name="property_status", create_type=False),
        nullable=False,
        default=PropertyStatus.NEW,
        index=True,
    )
    is_active = Column(Boolean, default=True, index=True)

    extra_features = Column(JSONB, nullable=True, default=dict, server_default="{}")

    # --- Timestamps (all UTC via utcnow()) ----------------------
    created_at      = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at      = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    last_checked_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    last_seen_at    = Column(DateTime(timezone=True), default=utcnow, nullable=False)

    # --- Lifecycle: details re-fetch cooldown --------------------
    details_fetch_attempts = Column(Integer, nullable=False, default=0, server_default="0")
    last_details_fetch_at  = Column(DateTime(timezone=True), nullable=True)

    # --- Location FK (Sdht hierarchy) ----------------------------
    location_id       = Column(Integer, ForeignKey("sdht_property_areas.id"), nullable=True)
    calc_prefecture   = Column(String(255), nullable=True)
    calc_municipality = Column(String(255), nullable=True)
    calc_area         = Column(String(255), nullable=True)

    # --- MDM / AI ------------------------------------------------
    embedding     = Column(Vector(1536), nullable=True)
    content_hash  = Column(String(64),   nullable=True, index=True)
    cluster_id    = Column(
        UUID(as_uuid=True),
        ForeignKey("property_clusters.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    image_phashes = Column(ARRAY(String), nullable=False, default=list, server_default="{}")

    # --- Relationships -------------------------------------------
    media         = relationship("Media",        back_populates="property", cascade="all, delete-orphan")
    price_history = relationship("PriceHistory", back_populates="property", cascade="all, delete-orphan")
    cluster       = relationship(
        "PropertyCluster",
        back_populates="members",
        foreign_keys=[cluster_id],
    )

    __table_args__ = (
        UniqueConstraint('source_domain', 'site_property_id', name='_source_site_uc'),
    )


class Media(Base):
    __tablename__ = "media"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    property_id     = Column(UUID(as_uuid=True), ForeignKey("properties.id", ondelete="CASCADE"))
    image_url       = Column(String(1000), nullable=False)
    local_file_path = Column(String(500),  nullable=True)
    is_main_photo   = Column(Boolean,       default=False)
    created_at      = Column(DateTime(timezone=True), default=utcnow)

    property = relationship("Property", back_populates="media")


class PriceHistory(Base):
    __tablename__ = "price_history"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    property_id = Column(UUID(as_uuid=True), ForeignKey("properties.id", ondelete="CASCADE"))
    old_price   = Column(Integer, nullable=True)
    new_price   = Column(Integer, nullable=False)
    changed_at  = Column(DateTime(timezone=True), default=utcnow)

    property = relationship("Property", back_populates="price_history")


# =============================================================
# LOCATION DICTIONARY (Sdht_* — imported from external SQL dumps)
# =============================================================

class SdhtPrefecture(Base):
    __tablename__ = "sdht_property_prefectures"

    id                   = Column(Integer, primary_key=True)
    geometry_location_id = Column(Integer, nullable=True)
    country_id           = Column(Integer, nullable=True)
    prefecture_en        = Column(String(255), nullable=True)
    prefecture_el        = Column(String(255), nullable=True)
    lat                  = Column(String(100), nullable=True)
    lng                  = Column(String(100), nullable=True)
    zoom                 = Column(Integer, nullable=True)
    created_at           = Column(String(100), nullable=True)
    updated_at           = Column(String(100), nullable=True)
    active               = Column(Integer, nullable=True)
    deleted              = Column(Integer, nullable=True)


class SdhtMunicipality(Base):
    __tablename__ = "sdht_property_municipalities"

    id                   = Column(Integer, primary_key=True)
    country_id           = Column(Integer, nullable=True)
    prefecture_id        = Column(Integer, ForeignKey("sdht_property_prefectures.id"), nullable=True)
    geometry_location_id = Column(Integer, nullable=True)
    municipality_en      = Column(String(255), nullable=True)
    municipality_el      = Column(String(255), nullable=True)
    lat                  = Column(String(100), nullable=True)
    lng                  = Column(String(100), nullable=True)
    zoom                 = Column(Integer, nullable=True)
    dom_city_id          = Column(Integer, nullable=True)
    xe_city_id           = Column(Integer, nullable=True)
    fer_city_id          = Column(Integer, nullable=True)
    created_at           = Column(String(100), nullable=True)
    updated_at           = Column(String(100), nullable=True)
    active               = Column(Integer, nullable=True)
    deleted              = Column(Integer, nullable=True)


class SdhtArea(Base):
    __tablename__ = "sdht_property_areas"

    id                      = Column(Integer, primary_key=True)
    country_id              = Column(Integer, nullable=True)
    prefecture_id           = Column(Integer, ForeignKey("sdht_property_prefectures.id"), nullable=True)
    municipality_id         = Column(Integer, ForeignKey("sdht_property_municipalities.id"), nullable=True)
    area_en                 = Column(String(255), nullable=True)
    area_el                 = Column(String(255), nullable=True)
    lat                     = Column(String(100), nullable=True)
    lng                     = Column(String(100), nullable=True)
    zoom                    = Column(Integer, nullable=True)
    geometric_location      = Column(Text,         nullable=True)
    pref_sp_id              = Column(String(255), nullable=True)
    mun_sp_id               = Column(String(255), nullable=True)
    area_sp_id              = Column(String(255), nullable=True)
    area_xe_id              = Column(String(255), nullable=True)
    postal                  = Column(String(255), nullable=True)
    mortgage_office_type_id = Column(String(255), nullable=True)
    created_at              = Column(String(100), nullable=True)
    updated_at              = Column(String(100), nullable=True)
    active                  = Column(Integer, nullable=True)
    deleted                 = Column(Integer, nullable=True)


# =============================================================
# B2B ACCESS: Agents, Devices, Tokens, Settings
# =============================================================

class Agent(Base):
    __tablename__ = "agents"

    id        = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name      = Column(String(255), nullable=False)
    email     = Column(String(255), unique=True, index=True, nullable=False)
    phone     = Column(String(50),  nullable=True)
    is_active = Column(Boolean,     default=True)
    is_admin  = Column(Boolean,     default=False)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    devices = relationship("AgentDevice", back_populates="agent", cascade="all, delete-orphan")
    tokens  = relationship("AuthToken",   back_populates="agent", cascade="all, delete-orphan")


class AgentDevice(Base):
    __tablename__ = "agent_devices"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id       = Column(UUID(as_uuid=True), ForeignKey("agents.id", ondelete="CASCADE"), nullable=False)
    device_cookie  = Column(String(255), unique=True, index=True, nullable=False)
    user_agent_str = Column(String(500), nullable=True)
    last_seen_at   = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    created_at     = Column(DateTime(timezone=True), default=utcnow)

    agent = relationship("Agent", back_populates="devices")


class AuthToken(Base):
    __tablename__ = "auth_tokens"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id   = Column(UUID(as_uuid=True), ForeignKey("agents.id", ondelete="CASCADE"), nullable=False)
    token      = Column(String(255), unique=True, index=True, nullable=False)
    is_used    = Column(Boolean, default=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    agent = relationship("Agent", back_populates="tokens")


class SystemSetting(Base):
    __tablename__ = "system_settings"

    id         = Column(Integer, primary_key=True)
    key        = Column(String(50), unique=True, index=True)
    value      = Column(String(100))
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


# =============================================================
# MDM: External cache + PowerProperty master record
# =============================================================

class ExternalPropertyCache(Base):
    __tablename__ = "external_property_cache"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    external_source = Column(String(100), nullable=False)
    external_id     = Column(String(255), nullable=False)
    canonical_text  = Column(Text,        nullable=False)
    content_hash    = Column(String(64),  nullable=False, index=True)
    embedding       = Column(Vector(1536), nullable=True)
    raw_payload     = Column(JSONB,       nullable=False)
    expires_at      = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at      = Column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint('external_source', 'external_id', name='_ext_source_id_uc'),
    )


class PowerProperty(Base):
    __tablename__ = "power_properties"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    cluster_id = Column(
        UUID(as_uuid=True),
        ForeignKey("property_clusters.id", ondelete="CASCADE"),
        nullable=False, unique=True,
    )

    description = Column(Text, nullable=False)
    features    = Column(JSONB, nullable=False, default=dict, server_default="{}")

    price         = Column(Integer, nullable=True, index=True)
    size_sqm      = Column(Float,   nullable=True)
    land_size_sqm = Column(Float,   nullable=True)
    bedrooms      = Column(Integer, nullable=True)
    bathrooms     = Column(Integer, nullable=True)
    year_built    = Column(Integer, nullable=True)
    category      = Column(String(100), nullable=True, index=True)

    calc_prefecture   = Column(String(255), nullable=True)
    calc_municipality = Column(String(255), nullable=True, index=True)
    calc_area         = Column(String(255), nullable=True)
    latitude          = Column(Float, nullable=True)
    longitude         = Column(Float, nullable=True)

    image_urls        = Column(ARRAY(String), nullable=False, default=list, server_default="{}")
    image_local_paths = Column(ARRAY(String), nullable=False, default=list, server_default="{}")

    # Added in migration 005. Filled by external image enhancer (n8n webhook).
    # Empty list means: enhancer was disabled or failed; UI should fall back
    # to image_urls / image_local_paths.
    enhanced_media_urls = Column(ARRAY(String), nullable=False, default=list, server_default="{}")

    source_property_ids = Column(ARRAY(UUID(as_uuid=True)), nullable=False, default=list, server_default="{}")
    source_domains      = Column(ARRAY(String),             nullable=False, default=list, server_default="{}")

    generated_at   = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    regenerated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    cluster = relationship("PropertyCluster", back_populates="power_object")


class AIDuplicateFeedback(Base):
    __tablename__ = "ai_duplicate_feedbacks"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    prop_a_id  = Column(UUID(as_uuid=True), ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True)
    prop_b_id  = Column(UUID(as_uuid=True), ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True)
    hash_a     = Column(String(64), nullable=False)
    hash_b     = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    # --- Sprint 6 Phase A: structured rejection reasoning (migration 014) ---
    # reason_attributes: subset of the 9-attribute taxonomy (see
    # src/database/feedback_repository.py:VALID_REASON_ATTRIBUTES) admin
    # cited as the reason this pair is NOT a duplicate. Empty list for
    # legacy rows (feedback_source='migration').
    # feedback_source: provenance — 'admin_reject' / 'manual_split' /
    # 'cluster_dissolve' / 'migration'. See migration 014 column comment.
    reason_attributes = Column(JSONB, nullable=False, server_default="[]")
    reason_text       = Column(Text, nullable=True)
    feedback_source   = Column(String(20), nullable=False, server_default="admin_reject")

    __table_args__ = (
        UniqueConstraint('prop_a_id', 'prop_b_id', name='uix_ai_feedback_pair'),
    )


# =============================================================
# Operational Logs: Scrapers & Emails
# =============================================================

class ScraperLog(Base):
    __tablename__ = "scraper_logs"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_domain    = Column(String(100), nullable=False, index=True)
    status           = Column(String(50), nullable=False)  # 'SUCCESS' | 'ERROR' | 'RUNNING'
    processed_count  = Column(Integer, default=0)
    new_count        = Column(Integer, default=0)
    duration_seconds = Column(Integer, nullable=True)
    error_message    = Column(Text, nullable=True)
    created_at       = Column(DateTime(timezone=True), default=utcnow, index=True)


class EmailLog(Base):
    __tablename__ = "email_logs"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    recipient_email  = Column(String(255), nullable=False, index=True)
    status           = Column(String(50), nullable=False)  # 'DELIVERED' | 'FAILED' | 'NO NEW DATA'
    properties_count = Column(Integer, default=0)
    error_message    = Column(Text, nullable=True)
    created_at       = Column(DateTime(timezone=True), default=utcnow, index=True)


# =============================================================
# Engine v2 (Pass 6) — Cache + Mismerge flags
# =============================================================

class EnginePairCache(Base):
    """Per-pair scoring cache for engine v2.

    Source: RESEARCH.md §12.5.7 + research/HYBRID_DESIGN.md §4.2.
    Migration: 011_engine_pair_cache.sql.

    Cache key is `pair_key` (canonical "lower_uuid:greater_uuid").
    Invalidation triggers:
      1. Either property's content_hash changes (description / price /
         etc. updated by re-scrape) -> cache row is stale if stored
         hashes don't match current. Engine recomputes.
      2. engine_version mismatch -> entire cache effectively
         invalidated for the older version.
      3. Optional TTL — default NULL (no expiry).

    No FK to properties — verbatim HYBRID_DESIGN spec. Orphan rows
    are harmless (next lookup invalidates via content_hash mismatch).
    """
    __tablename__ = "engine_pair_cache"

    pair_key       = Column(Text,            primary_key=True)
    engine_version = Column(Text,            nullable=False, index=True)
    a_content_hash = Column(Text,            nullable=False)
    b_content_hash = Column(Text,            nullable=False)
    verdict        = Column(Text,            nullable=False)   # duplicate | different | uncertain
    confidence     = Column(Float,           nullable=True)
    reasoning      = Column(Text,            nullable=True)
    tier_emitted   = Column(SmallInteger,    nullable=False)   # 0 | 1 | 2 | 3
    cost_usd       = Column(Numeric(10, 6),  nullable=False, default=0)
    scored_at      = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    expires_at     = Column(DateTime(timezone=True), nullable=True)


class MismergeFlag(Base):
    """Engine-emitted flag for pairs in APPROVED clusters that
    violate hard rules. Engine flags but NEVER auto-dissolves
    (spec §11). Admin reviews and sets admin_action.

    Source: RESEARCH.md §12.5.9 (multi_cluster_bridge) + §12.5.11.
    Migration: 012_mismerge_flags.sql.

    flag_type values: 'year_diff_outlier' | 'engine_t0_disagrees' |
                      'multi_cluster_bridge' | 'pattern'.
    admin_action values: 'confirm_keep' | 'dissolve' | 'ignore' |
                         NULL (pending).

    UNIQUE(cluster_id, pair_a_id, pair_b_id, flag_type) makes
    repeated detection idempotent — pending flags don't re-emit.
    """
    __tablename__ = "mismerge_flags"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    cluster_id      = Column(
        UUID(as_uuid=True),
        ForeignKey("property_clusters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    pair_a_id       = Column(
        UUID(as_uuid=True),
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=False,
    )
    pair_b_id       = Column(
        UUID(as_uuid=True),
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=False,
    )
    flag_type       = Column(Text,        nullable=False)
    flag_reason     = Column(Text,        nullable=False)
    detected_at     = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    admin_action    = Column(Text,        nullable=True)
    admin_action_at = Column(DateTime(timezone=True), nullable=True)
    admin_action_by = Column(Text,        nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "cluster_id", "pair_a_id", "pair_b_id", "flag_type",
            name="uix_mismerge_flag_pair",
        ),
    )


class EngineV2Prediction(Base):
    """Phase 1-2 shadow-mode output for engine v2.

    Source: RESEARCH.md §12.5.7 (table #4) + §12.5.10 (write site).
    Migration: 013_engine_v2_predictions.sql.

    During shadow phase the new engine writes only here; the old engine
    continues to own property_clusters. Diff between this table and
    property_clusters drives daily Telegram comparison until Phase 3
    cut-over, after which this table is dropped.

    UNIQUE(pair_key, scored_at) preserves prediction history per pair
    across scrape runs — supports drift analysis over time.
    """
    __tablename__ = "engine_v2_predictions"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pair_key      = Column(Text,            nullable=False)            # "lower_uuid:greater_uuid"
    a_id          = Column(
        UUID(as_uuid=True),
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=False,
    )
    b_id          = Column(
        UUID(as_uuid=True),
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=False,
    )
    verdict       = Column(Text,            nullable=False)            # duplicate | different | uncertain
    confidence    = Column(Float,           nullable=True)
    reasoning     = Column(Text,            nullable=True)
    tier_emitted  = Column(SmallInteger,    nullable=False)            # 0 | 1 | 2 | 3
    cost_usd      = Column(Numeric(10, 6),  nullable=False, default=0)
    scored_at     = Column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "pair_key", "scored_at",
            name="uix_engine_v2_predictions_pair_scored",
        ),
    )