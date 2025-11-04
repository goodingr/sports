"""Fetch NFL odds data from The Odds API and cache as JSON snapshots."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Optional

import requests

from src.db.loaders import load_odds_snapshot

from .config import RAW_DATA_DIR, OddsAPISettings, ensure_directories


LOGGER = logging.getLogger(__name__)


def fetch_odds(settings: OddsAPISettings) -> Dict[str, Any]:
    """Call The Odds API for the configured sport/market."""

    url = f"{settings.base_url}/sports/{settings.sport}/odds"
    params = {
        "apiKey": settings.api_key,
        "regions": settings.region,
        "markets": settings.market,
        "oddsFormat": "american",
        "dateFormat": "iso",
    }

    response = requests.get(url, params=params, timeout=10)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - network guard
        if response.status_code == 401:
            raise RuntimeError(
                "Received 401 Unauthorized from The Odds API. "
                "Confirm that your plan includes odds-history access and that the API key is valid."
            ) from exc
        raise

    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "request": {"url": url, "params": params},
        "results": response.json(),
        "rate_limit_remaining": response.headers.get("x-requests-remaining"),
        "rate_limit_reset": response.headers.get("x-requests-reset"),
    }

    return payload


def write_snapshot(data: Dict[str, Any], out_dir: Path) -> Path:
    """Persist odds snapshot to data/raw/odds/<timestamp>.json."""

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"odds_{timestamp}.json"
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return output_path


def fetch_odds_history(settings: OddsAPISettings, target_datetime: datetime) -> Dict[str, Any]:
    url = f"{settings.base_url}/sports/{settings.sport}/odds-history"
    params = {
        "apiKey": settings.api_key,
        "regions": settings.region,
        "markets": settings.market,
        "oddsFormat": "american",
        "dateFormat": "iso",
        "date": target_datetime.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }

    response = requests.get(url, params=params, timeout=10)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - network guard
        if response.status_code == 401:
            raise RuntimeError(
                "Received 401 Unauthorized from The Odds API when requesting odds history. "
                "This endpoint requires a paid plan; verify access or adjust the backfill range."
            ) from exc
        raise

    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "requested_date": target_datetime.isoformat(),
        "request": {"url": url, "params": params},
        "results": response.json(),
        "rate_limit_remaining": response.headers.get("x-requests-remaining"),
        "rate_limit_reset": response.headers.get("x-requests-reset"),
    }

    return payload


def run(
    dotenv_path: Path | None = None,
    *,
    sport_key: str | None = None,
    market: str | None = None,
    region: str | None = None,
) -> Path:
    """Fetch odds data and store the snapshot."""

    ensure_directories()

    settings = OddsAPISettings.from_env(dotenv_path)
    if sport_key:
        settings.sport = sport_key
    if market:
        settings.market = market
    if region:
        settings.region = region
    raw_odds_dir = RAW_DATA_DIR / "odds"
    data = fetch_odds(settings)
    sport_dir = raw_odds_dir / settings.sport
    output_path = write_snapshot(data, sport_dir)

    load_odds_snapshot(data, raw_path=str(output_path), region=settings.region, sport_key=settings.sport)
    LOGGER.info("Saved odds snapshot to %s", output_path)
    return output_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download NFL odds from The Odds API")
    parser.add_argument(
        "--dotenv",
        type=Path,
        default=None,
        help="Optional path to .env file containing ODDS_API_KEY",
    )
    parser.add_argument(
        "--sport",
        default=None,
        help="The Odds API sport key (e.g., americanfootball_nfl, basketball_nba)",
    )
    parser.add_argument(
        "--market",
        default=None,
        help="Odds market to request (default from settings, typically h2h)",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Odds region (e.g., us, uk)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    run(args.dotenv, sport_key=args.sport, market=args.market, region=args.region)


if __name__ == "__main__":
    main()

