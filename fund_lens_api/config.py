"""Application configuration using Pydantic Settings."""

from functools import lru_cache

from pydantic import PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # API Settings
    app_name: str = "FundLens API"
    app_version: str = "0.1.0"
    app_description: str = "Campaign finance data API for FundLens"
    debug: bool = False

    # Database
    database_url: str

    @field_validator("database_url", mode="before")
    @classmethod
    def validate_database_url(cls, v: str | PostgresDsn) -> str:
        """Ensure database URL is a string for SQLAlchemy."""
        if isinstance(v, str):
            return v
        return str(v)

    # CORS
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8080"]
    cors_allow_credentials: bool = True
    cors_allow_methods: list[str] = ["*"]
    cors_allow_headers: list[str] = ["*"]

    # Pagination
    default_page_size: int = 50
    max_page_size: int = 1000

    # Rate Limiting
    rate_limit_enabled: bool = True
    rate_limit_default: str = "100/minute"  # General endpoints
    rate_limit_search: str = "30/minute"    # Search endpoints
    rate_limit_stats: str = "60/minute"     # Stats endpoints


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
