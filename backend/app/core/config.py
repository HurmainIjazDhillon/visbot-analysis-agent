from pathlib import Path
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_ENV_FILE = Path(__file__).resolve().parents[3] / ".env"


class Settings(BaseSettings):
    app_name: str = "VisBot Analysis"
    groq_api_key: str = ""
    groq_model: str = "lgpt-oss-120b"
    database_url: str = "sqlite+pysqlite:///:memory:"
    local_timezone: str = "Asia/Karachi"
    local_time_offset_hours: int = 3
    openremote_schema: str = "openremote"

    model_config = SettingsConfigDict(
        env_file=(str(ROOT_ENV_FILE), ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
