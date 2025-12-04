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


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"


class OddsAPIKeyManager:
    """Manages multiple API keys and rotation."""
    
    INDEX_FILE = PROJECT_ROOT / ".odds_api_key_index"

    @classmethod
    def get_available_keys(cls) -> list[str]:
        """Load all ODDS_API_KEY* from environment."""
        keys = []
        # Primary key
        k1 = os.getenv("ODDS_API_KEY")
        if k1:
            keys.append(k1)
        
        # Secondary keys (ODDS_API_KEY_2, _3, etc.)
        i = 2
        while True:
            k = os.getenv(f"ODDS_API_KEY_{i}")
            if not k:
                break
            keys.append(k)
            i += 1
        return keys

    @classmethod
    def get_current_index(cls) -> int:
        """Read current key index from file."""
        if not cls.INDEX_FILE.exists():
            return 0
        try:
            return int(cls.INDEX_FILE.read_text().strip())
        except (ValueError, OSError):
            return 0

    @classmethod
    def set_current_index(cls, index: int) -> None:
        """Persist current key index."""
        try:
            cls.INDEX_FILE.write_text(str(index))
        except OSError:
            pass

    @classmethod
    def get_current_key(cls) -> str:
        """Get the currently active API key."""
        keys = cls.get_available_keys()
        if not keys:
            raise RuntimeError("No ODDS_API_KEY found in environment")
        
        idx = cls.get_current_index()
        if idx >= len(keys):
            idx = 0
            cls.set_current_index(0)
            
        return keys[idx]

    @classmethod
    def rotate_key(cls) -> str:
        """Switch to the next available key."""
        keys = cls.get_available_keys()
        if not keys:
            raise RuntimeError("No ODDS_API_KEY found")
            
        current = cls.get_current_index()
        next_idx = (current + 1) % len(keys)
        cls.set_current_index(next_idx)
        return keys[next_idx]


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

        # Use the key manager to get the current key
        api_key = OddsAPIKeyManager.get_current_key()

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


def ensure_directories() -> None:
    """Guarantee core data directories exist."""

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_env(dotenv_path: Optional[Path] = None) -> dict[str, str]:
    """Return environment variables after loading optional .env file."""

    _load_dotenv_if_available(dotenv_path)
    return dict(os.environ)
