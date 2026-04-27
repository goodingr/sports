"""Typed settings for the FastAPI service."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from src.data.config import PROJECT_ROOT, _load_dotenv_if_available


DEFAULT_RELEASE_LEAGUES = (
    "NBA",
    "NFL",
    "NCAAB",
    "NHL",
    "CFB",
    "EPL",
    "LALIGA",
    "BUNDESLIGA",
    "SERIEA",
    "LIGUE1",
)


def _csv_env(*names: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        return tuple(part.strip() for part in raw.split(",") if part.strip())
    return default


def _path_env(*names: str, default: Path) -> Path:
    for name in names:
        raw = os.getenv(name)
        if raw:
            path = Path(raw).expanduser()
            return path if path.is_absolute() else PROJECT_ROOT / path
    return default


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


class APISettings(BaseModel):
    cors_origins: tuple[str, ...] = (
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    )
    db_path: Path = PROJECT_ROOT / "data" / "betting.db"
    release_leagues: tuple[str, ...] = DEFAULT_RELEASE_LEAGUES
    odds_freshness_minutes: int = Field(default=180, ge=0)
    predictions_freshness_minutes: int = Field(default=24 * 60, ge=0)
    min_disk_free_mb: int = Field(default=512, ge=0)
    models_dir: Path = PROJECT_ROOT / "models"

    clerk_secret_key: str | None = None
    clerk_api_url: str = "https://api.clerk.com"
    clerk_frontend_api_url: str | None = None
    clerk_issuer: str | None = None
    clerk_jwks_url: str | None = None
    clerk_jwt_key: str | None = None
    clerk_request_timeout_seconds: float = Field(default=3.0, gt=0)
    clerk_cache_ttl_seconds: int = Field(default=60, ge=0)
    clerk_jwt_clock_skew_seconds: int = Field(default=60, ge=0)
    clerk_authorized_parties: tuple[str, ...] = ()

    @field_validator("cors_origins")
    @classmethod
    def _disallow_wildcard_cors(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if "*" in value:
            raise ValueError("Wildcard CORS origins are not allowed for this API")
        return value

    @field_validator("release_leagues")
    @classmethod
    def _normalize_leagues(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(league.upper() for league in value)


@lru_cache(maxsize=1)
def get_settings() -> APISettings:
    _load_dotenv_if_available()
    cors_origins = _csv_env(
        "API_CORS_ORIGINS",
        "CORS_ORIGINS",
        default=APISettings.model_fields["cors_origins"].default,
    )
    release_leagues = _csv_env("RELEASE_LEAGUES", default=DEFAULT_RELEASE_LEAGUES)
    models_dir = _path_env("MODELS_DIR", "MODEL_DIR", default=PROJECT_ROOT / "models")
    db_path = _path_env(
        "SPORTS_DB_PATH",
        "DATABASE_PATH",
        "DB_PATH",
        default=PROJECT_ROOT / "data" / "betting.db",
    )

    frontend_api_url = os.getenv("CLERK_FRONTEND_API_URL")
    issuer = os.getenv("CLERK_ISSUER") or frontend_api_url
    authorized_parties = _csv_env("CLERK_AUTHORIZED_PARTIES", default=())

    return APISettings(
        cors_origins=cors_origins,
        db_path=db_path,
        release_leagues=release_leagues,
        odds_freshness_minutes=_int_env("ODDS_FRESHNESS_MINUTES", 180),
        predictions_freshness_minutes=_int_env("PREDICTIONS_FRESHNESS_MINUTES", 24 * 60),
        min_disk_free_mb=_int_env("MIN_DISK_FREE_MB", 512),
        models_dir=models_dir,
        clerk_secret_key=os.getenv("CLERK_SECRET_KEY"),
        clerk_api_url=os.getenv("CLERK_API_URL", "https://api.clerk.com"),
        clerk_frontend_api_url=frontend_api_url,
        clerk_issuer=issuer.rstrip("/") if issuer else None,
        clerk_jwks_url=os.getenv("CLERK_JWKS_URL"),
        clerk_jwt_key=os.getenv("CLERK_JWT_KEY") or os.getenv("CLERK_PEM_PUBLIC_KEY"),
        clerk_request_timeout_seconds=_float_env("CLERK_REQUEST_TIMEOUT_SECONDS", 3.0),
        clerk_cache_ttl_seconds=_int_env("CLERK_CACHE_TTL_SECONDS", 60),
        clerk_jwt_clock_skew_seconds=_int_env("CLERK_JWT_CLOCK_SKEW_SECONDS", 60),
        clerk_authorized_parties=authorized_parties,
    )
