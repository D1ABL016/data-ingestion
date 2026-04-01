from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    """Loads from environment variables and, if present, repo-root `.env`.

    Copy `sample.env` → `.env` for local development. Values in the process
    environment override `.env`.
    """

    model_config = SettingsConfigDict(
        env_file=_REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    database_url: str = Field(
        default="",
        description=(
            "Full PostgreSQL URL (asyncpg). If empty, built from POSTGRES_HOST, "
            "POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB."
        ),
    )
    postgres_host: str = Field(
        default="localhost",
        description="DB host when DATABASE_URL is not set",
    )
    postgres_port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        description="DB port when DATABASE_URL is not set",
    )
    postgres_user: str = Field(
        default="postgres",
        description="DB user when DATABASE_URL is not set",
    )
    postgres_password: str = Field(
        default="postgres",
        description="DB password when DATABASE_URL is not set (URL-encoded when building URL)",
    )
    postgres_db: str = Field(
        default="data_ingestion",
        description="Database name when DATABASE_URL is not set",
    )
    upload_dir: str = Field(
        default="/tmp/uploads",
        description="Directory for temporary CSV uploads",
    )
    max_upload_mb: int = Field(
        default=200,
        ge=1,
        description="Max multipart upload size in MB",
    )
    debug: bool = Field(
        default=False,
        description="SQL echo and verbose 500 errors (development only)",
    )

    @model_validator(mode="before")
    @classmethod
    def assemble_database_url(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        merged = dict(data)
        raw_url = merged.get("database_url")
        if raw_url is not None and str(raw_url).strip():
            merged["database_url"] = str(raw_url).strip()
            return merged
        host = str(merged.get("postgres_host") or "localhost")
        port = int(merged.get("postgres_port") or 5432)
        user = str(merged.get("postgres_user") or "postgres")
        password = str(merged.get("postgres_password") or "postgres")
        db = str(merged.get("postgres_db") or "data_ingestion")
        pw = quote_plus(password)
        merged["database_url"] = (
            f"postgresql+asyncpg://{user}:{pw}@{host}:{port}/{db}"
        )
        return merged


settings = Settings()
