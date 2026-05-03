"""
Central settings. Reads from .env (see .env.example).
All services import 'settings' from here — single source of truth.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # === DATABASE ===================================================
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432

    # === TELEGRAM ALERTS ============================================
    TG_BOT_TOKEN: str | None = None
    TG_CHAT_ID: str | None = None

    # === HTTP / PROXY ===============================================
    PROXY_URL: str | None = None

    # === FETCH FUNNEL ===============================================
    # Sprint 1 introduces a multi-stage fetch chain. These settings let
    # the orchestrator be tuned without code changes.
    #
    # Promotion: after this many CONSECUTIVE successful fetches on a
    # stage HIGHER than the domain's current preferred_stage, the funnel
    # persists the new preferred_stage to scraper_routing table.
    FUNNEL_PROMOTE_AFTER_SUCCESSES: int = 3

    # Master enable for stage 1 (Playwright + stealth). Set to False to
    # force everything through stage 0 only — useful as an emergency
    # rollback. Will be honoured by Sprint 2 when stage 1 is wired up.
    FUNNEL_STAGE1_ENABLED: bool = True

    # Per-stage timeouts in seconds — overrides the fetcher's default.
    FUNNEL_STAGE0_TIMEOUT_SECONDS: int = 30
    FUNNEL_STAGE1_TIMEOUT_SECONDS: int = 45  # Playwright is slower

    # === SMTP (magic-link emails) ===================================
    SMTP_HOST: str | None = None
    SMTP_PORT: int = 587
    SMTP_USER: str | None = None
    SMTP_PASS: str | None = None
    APP_URL: str = "http://localhost:8000"

    # === OPENAI / MDM AI PIPELINE ===================================
    OPENAI_API_KEY: str  # Обязательное поле! Без него Pydantic не даст запустить сервер
    OPENAI_EMBEDDING_MODEL: str = "text-embedding-3-small"
    OPENAI_CHAT_MODEL: str = "gpt-4o"

    # --- LLM EXTRACTION (per-scraper amenity extraction fallback) ---
    # Used when regex extraction yields too few features (see scraper logic).
    # Kill-switch: set LLM_EXTRACTION_ENABLED=false in .env to disable
    # without redeploying the code.
    LLM_EXTRACTION_ENABLED: bool = True
    LLM_EXTRACTION_MODEL: str = "gpt-4o-mini"
    LLM_EXTRACTION_MAX_TOKENS: int = 500
    # Minimum number of regex-extracted features below which the LLM
    # fallback fires. Tune higher if regex coverage improves.
    LLM_EXTRACTION_MIN_REGEX_FEATURES: int = 5

    # --- Vision Tie-Breaker (Партия 5) ---
    VISION_TIEBREAKER_ENABLED:    bool  = False
    """Master switch. If False, Vision is never called and gray-zone pairs
    behave as before (go to PENDING for admin review)."""

    VISION_MAX_PAIRS_PER_RUN:     int   = 50
    """Hard cap on Vision API calls per detector run. At ~$0.011/call,
    50 calls = ~$0.55 worst-case."""

    VISION_CONFIDENCE_THRESHOLD:  float = 0.8
    """Minimum confidence for Vision verdict to be authoritative.
    Below this threshold, the pair stays in PENDING (admin decides)."""

    # === INTERNAL MATCHER THRESHOLDS ================================
    SIM_AUTO_MERGE: float = 0.985
    SIM_REJECT: float = 0.920
    PHASH_HAMMING_THRESHOLD: int = 3
    PHASH_MIN_MATCHES_FOR_BYPASS: int = 2
    MAX_PAIRS_PER_PROPERTY: int = 50
    PHASH_MIN_MATCHES:       int   = 2

    PHASH_STOCK_MIN_PROPS: int = 5
    """Hash counted on more than N distinct properties is treated as 'stock photo'
    (beach, sunset, agency logo) and ignored in image-overlap calculations."""

    # === EXTERNAL DB CHECK ==========================================
    EXTERNAL_API_BASE_URL: str | None = None
    EXTERNAL_API_KEY: str | None = None
    EXTERNAL_CACHE_TTL_HOURS: int = 24
    EXTERNAL_RECHECK_HOURS: int = 24

    # === POWER OBJECT PIPELINE ======================================
    POWER_PIPELINE_CONCURRENCY: int = 10

    # === PROPERTY LIFECYCLE =========================================
    # Stop re-fetching details for a source that naturally lacks year_built etc.
    DETAILS_FETCH_MAX_ATTEMPTS: int = 5
    DETAILS_FETCH_COOLDOWN_DAYS: int = 7

    # Fail-safe: if a scraper returns much less than what is in DB for the domain,
    # it likely crashed mid-run. Abort DELISTED phase for that domain.
    DELIST_MIN_COVERAGE_RATIO: float = 0.5  # site must return >=50% of known-active

    # === SECURITY ===================================================
    # Used for CSRF double-submit cookie signing. MUST be overridden in prod.
    CSRF_SECRET: str = "change-me-in-production"
    SESSION_COOKIE_NAME: str = "hodu_session"
    CSRF_COOKIE_NAME: str = "hodu_csrf"
    COOKIE_SECURE: bool = False  # set True behind HTTPS

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def database_url_sync(self) -> str:
        """Plain psycopg-style URL for raw asyncpg migrations."""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


settings = Settings()