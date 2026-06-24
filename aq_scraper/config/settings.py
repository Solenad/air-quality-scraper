from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # OpenAQ API (optional — anonymous access works too)
    OPENAQ_API_KEY: str | None = None

    # Comma-separated location IDs for historical mode (overrides built-in defaults)
    OPENAQ_LOCATION_IDS: str = ""

    # Start date for historical pulls (ISO 8601 format)
    OPENAQ_DATE_FROM: str = "2021-01-01T00:00:00Z"

    # WAQI API (required)
    WAQI_API_KEY: str = ""
