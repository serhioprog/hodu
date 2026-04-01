from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # database setup
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str = "localhost" # our host
    POSTGRES_PORT: int = 5432
    
    # telegram notifications
    TG_BOT_TOKEN: str | None = None
    TG_CHAT_ID: str | None = None
    
    # future proxy way
    PROXY_URL: str | None = None

    # way for Pydantic to search variables in file .env
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def database_url(self) -> str:
        """Generating connection line for SQLAlchemy"""
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

settings = Settings()