from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # MySQL connection
    DB_HOST: str = "localhost"
    DB_PORT: int = 3306
    DB_USER: str = ""
    DB_PASS: str = ""
    DB_NAME: str = ""

    # OpenAQ API (optional)
    OPENAQ_API_KEY: str | None = None

    @property
    def async_dsn(self) -> str:
        return (
            f"mysql+aiomysql://{self.DB_USER}:{self.DB_PASS}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )
