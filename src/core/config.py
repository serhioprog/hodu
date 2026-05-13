"""
Central settings. Reads from .env (see .env.example).
All services import 'settings' from here — single source of truth.
"""
from pydantic import field_validator
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

    # --- TLS verification policy (Bug #4) -------------------------
    TLS_VERIFY_DEFAULT: bool = False
    """Whether to verify TLS certs on outbound scraping requests by
    default. Bug #4: currently False to preserve historical behaviour —
    many Greek real-estate sites are served with broken cert chains
    (intermediate cert issues, expired chains alongside valid leaf).
    Production hardening path: investigate each scraped domain's cert
    state, populate TLS_VERIFY_DOMAIN_OVERRIDES with known-broken
    domains as exceptions, then flip this default to True. Until then
    we accept the global MITM risk in exchange for working scrapers.
    Tracked as Sprint 7+ hardening task."""

    TLS_VERIFY_DOMAIN_OVERRIDES: dict[str, bool] = {}
    """Per-domain TLS verify override. Key = bare domain (no scheme,
    no www), Value = True (verify) or False (skip). Empty dict means
    'use TLS_VERIFY_DEFAULT for everything'. Future state will be
    TLS_VERIFY_DEFAULT=True with explicit entries here for the few
    domains we know have broken certs. Bug #4."""

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

    TOKEN_TTL_HOURS: int = 48
    """Lifetime of magic-link auth tokens in hours. Used by notifier.py
    when staging AuthToken rows and by /auth/{token} handler for
    expiry enforcement. Was hardcoded module-level constant in
    notifier.py, moved here for tunability (Bug #34)."""

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

    # === ENGINE V2 (Pass 6 shadow phase) ============================
    USE_NEW_DUPLICATE_ENGINE: bool = False
    """Phase 1 shadow-mode toggle for engine v2.
    When True, daily_sync._run_mdm_pipeline() runs the new HybridEngine
    BEFORE the old InternalDuplicateDetector on a separate session.
    New engine writes only engine_v2_predictions + engine_pair_cache;
    no property_clusters writes. Old engine continues unchanged.
    Failures in the shadow branch are logged with traceback but never
    block the production pipeline.
    Set in .env: USE_NEW_DUPLICATE_ENGINE=true to enable."""

# === INTERNAL MATCHER THRESHOLDS ================================
    SIM_AUTO_MERGE: float = 0.985
    SIM_REJECT: float = 0.920

    UNIQUE_SIM_THRESHOLD: float = 0.95
    """Threshold used by ExternalUniqueFinder. If any external candidate
    has similarity >= this value, the cluster is considered NOT unique
    (already known to the external system). Was hardcoded in
    external_unique_finder.py, moved here for tunability (Bug #15)."""

    PHASH_HAMMING_THRESHOLD: int = 3
    """
    STRICT hamming threshold (in bits) for IN-PROPERTY pHash dedup.

    Used by PowerObjectGenerator when building the canonical image set
    of a cluster — we want only visually distinct photos to accumulate
    in the power object, so the threshold is tight (~5% bit difference).

    NOTE: This is intentionally different from PHashService.HAMMING_THRESHOLD
    (= 6 bits, ~9% difference), which is the LOOSE threshold used by the
    duplicate detector when comparing photos ACROSS two properties
    (tolerant to JPEG re-compression and cropping across sites).

    Two values, two purposes:
      - 3 bits (here): strict, for de-duplicating photos that should be
        treated as the same image within a single property's photo set.
      - 6 bits (in PHashService): loose, for finding duplicate-photo
        evidence between two different properties from different scrapers.
    """

    MAX_PAIRS_PER_PROPERTY: int = 50
    PHASH_MIN_MATCHES: int = 2
    """Minimum number of matching pHashes between two properties for a
    'phash bypass' match (skip embedding similarity check). Used in
    internal_duplicate_detector. Note: a duplicate constant
    PHASH_MIN_MATCHES_FOR_BYPASS used to exist here too but was never
    referenced anywhere — removed for clarity (Bug #61)."""

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

    @field_validator("CSRF_SECRET")
    @classmethod
    def _csrf_secret_must_be_overridden(cls, v: str) -> str:
        """Bug #5: refuse to boot if CSRF_SECRET is still the placeholder.

        Generate a random one with:
            python -c "import secrets; print(secrets.token_urlsafe(64))"
        and put it in .env as CSRF_SECRET=<value>.
        """
        if v == "change-me-in-production":
            raise ValueError(
                "CSRF_SECRET still has the default placeholder value. "
                "Override it in .env. Generate with: "
                'python -c "import secrets; print(secrets.token_urlsafe(64))"'
            )
        return v

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

def should_verify_tls(url_or_domain: str) -> bool:
    """Decide whether to verify TLS for a given URL or bare domain.
    
    Bug #4: centralizes the TLS-verify decision (was hardcoded False at
    every call site). Lookup priority:
      1. Exact domain match in TLS_VERIFY_DOMAIN_OVERRIDES
      2. Fallback to TLS_VERIFY_DEFAULT
    
    Strips 'www.' prefix and scheme/path before lookup so a single
    config entry like 'example.com' matches both 'www.example.com'
    and 'https://example.com/foo'.
    
    Examples (with TLS_VERIFY_DEFAULT=False, no overrides):
        should_verify_tls("https://example.com/foo") -> False
        should_verify_tls("example.com")             -> False
    
    With TLS_VERIFY_DEFAULT=True, TLS_VERIFY_DOMAIN_OVERRIDES={
        'broken-site.gr': False
    }:
        should_verify_tls("https://broken-site.gr/x") -> False
        should_verify_tls("good-site.gr")             -> True
    """
    from urllib.parse import urlparse
    raw = url_or_domain.strip().lower()
    if "://" in raw:
        host = urlparse(raw).netloc
    else:
        host = raw.split("/", 1)[0]  # tolerate "domain/path" too
    if host.startswith("www."):
        host = host[4:]
    # Strip port if present
    if ":" in host:
        host = host.split(":", 1)[0]
    return settings.TLS_VERIFY_DOMAIN_OVERRIDES.get(host, settings.TLS_VERIFY_DEFAULT)