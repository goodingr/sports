"""Configuration helpers for data ingestion modules."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _load_dotenv_if_available(dotenv_path: Optional[Path] = None) -> None:
    """Load environment variables from a .env file when python-dotenv is installed."""

    try:
        from dotenv import load_dotenv  # type: ignore import-not-found
    except ModuleNotFoundError:
        return

    load_dotenv(dotenv_path)


@dataclass
class OddsAPISettings:
    api_key: str
    region: str = "us"
    market: str = "h2h"
    sport: str = "americanfootball_nfl"
    base_url: str = "https://api.the-odds-api.com/v4"
    min_fetch_interval_minutes: int = 30

    @classmethod
    def from_env(cls, dotenv_path: Optional[Path] = None) -> "OddsAPISettings":
        _load_dotenv_if_available(dotenv_path)

        api_key = os.getenv("ODDS_API_KEY")
        if not api_key:
            raise RuntimeError("ODDS_API_KEY must be set in the environment or .env file")

        region = os.getenv("ODDS_API_REGION", "us")
        market = os.getenv("ODDS_API_MARKET", "h2h")
        sport = os.getenv("ODDS_API_SPORT", "americanfootball_nfl")
        base_url = os.getenv("ODDS_API_BASE_URL", "https://api.the-odds-api.com/v4")
        try:
            min_fetch_interval = int(os.getenv("ODDS_API_MIN_FETCH_MINUTES", "30"))
        except ValueError:
            min_fetch_interval = 30

        return cls(
            api_key=api_key,
            region=region,
            market=market,
            sport=sport,
            base_url=base_url,
            min_fetch_interval_minutes=min_fetch_interval,
        )


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"


def ensure_directories() -> None:
    """Guarantee core data directories exist."""

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_env(dotenv_path: Optional[Path] = None) -> dict[str, str]:
    """Return environment variables after loading optional .env file."""

    _load_dotenv_if_available(dotenv_path)
    return dict(os.environ)

