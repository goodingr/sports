"""Fetch odds data from The Odds API and cache as JSON snapshots."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

from src.db.loaders import load_odds_snapshot

from .config import RAW_DATA_DIR, OddsAPISettings, ensure_directories


LOGGER = logging.getLogger(__name__)

# Maps our league codes to The Odds API sport keys so CLI users can invoke leagues.
LEAGUE_TO_SPORT_KEY = {
    "NFL": "americanfootball_nfl",
    "CFB": "americanfootball_ncaaf",
    "NBA": "basketball_nba",
    "EPL": "soccer_epl",
    "LALIGA": "soccer_spain_la_liga",
    "BUNDESLIGA": "soccer_germany_bundesliga",
    "SERIEA": "soccer_italy_serie_a",
    "LIGUE1": "soccer_france_ligue_one",
}


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
        if response.status_code == 404:
            LOGGER.warning(
                "Odds endpoint returned 404 for sport %s (url=%s). Returning empty payload.",
                settings.sport,
                url,
            )
            return {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "request": {"url": url, "params": params},
                "results": [],
                "rate_limit_remaining": response.headers.get("x-requests-remaining"),
                "rate_limit_reset": response.headers.get("x-requests-reset"),
                "error": "404 Not Found",
            }
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


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    candidate = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def _load_recent_snapshot(
    sport_dir: Path, settings: OddsAPISettings
) -> Tuple[Optional[Path], Optional[Dict[str, Any]], Optional[float]]:
    """Return a cached snapshot path/data if it is still fresh."""

    ttl_minutes = settings.min_fetch_interval_minutes
    if ttl_minutes <= 0 or not sport_dir.exists():
        return None, None, None

    snapshots = sorted(sport_dir.glob("odds_*.json"))
    if not snapshots:
        return None, None, None

    now = datetime.now(timezone.utc)
    max_age = timedelta(minutes=ttl_minutes)

    for path in reversed(snapshots):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        params = data.get("request", {}).get("params", {})
        if params.get("regions") != settings.region or params.get("markets") != settings.market:
            continue

        fetched_at = _parse_timestamp(data.get("fetched_at"))
        if not fetched_at:
            continue
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)

        age = now - fetched_at
        if age <= max_age:
            return path, data, age.total_seconds() / 60.0

    return None, None, None


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
    force_refresh: bool = False,
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
    sport_dir = raw_odds_dir / settings.sport

    if not force_refresh:
        cached_path, cached_data, age_minutes = _load_recent_snapshot(sport_dir, settings)
        if cached_path and cached_data is not None and age_minutes is not None:
            LOGGER.info(
                "Skipping Odds API call for %s: cached snapshot from %s is only %.1f minutes old",
                settings.sport,
                cached_data.get("fetched_at"),
                age_minutes,
            )
            load_odds_snapshot(cached_data, raw_path=str(cached_path), region=settings.region, sport_key=settings.sport)
            return cached_path

    data = fetch_odds(settings)
    output_path = write_snapshot(data, sport_dir)

    load_odds_snapshot(data, raw_path=str(output_path), region=settings.region, sport_key=settings.sport)
    LOGGER.info("Saved odds snapshot to %s", output_path)
    return output_path


def _resolve_sport_key(league: Optional[str], explicit_sport: Optional[str]) -> Optional[str]:
    if league:
        league_normalized = league.upper()
        if league_normalized not in LEAGUE_TO_SPORT_KEY:
            raise ValueError(
                f"Unsupported league '{league}'. Choose from: {', '.join(sorted(LEAGUE_TO_SPORT_KEY))}"
            )
        league_sport = LEAGUE_TO_SPORT_KEY[league_normalized]
        if explicit_sport and explicit_sport != league_sport:
            raise ValueError(
                f"Conflicting sport inputs: league '{league}' maps to '{league_sport}' but '--sport' was "
                f"set to '{explicit_sport}'. Specify only one."
            )
        return league_sport
    return explicit_sport


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download odds from The Odds API")
    parser.add_argument(
        "--dotenv",
        type=Path,
        default=None,
        help="Optional path to .env file containing ODDS_API_KEY",
    )
    parser.add_argument(
        "--league",
        choices=sorted(LEAGUE_TO_SPORT_KEY.keys()),
        help="Optional league identifier (e.g., NBA, NFL). Overrides --sport with the correct Odds API key.",
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
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Bypass cached snapshots and always hit The Odds API",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    sport_key = _resolve_sport_key(args.league, args.sport)
    run(
        args.dotenv,
        sport_key=sport_key,
        market=args.market,
        region=args.region,
        force_refresh=args.force_refresh,
    )


if __name__ == "__main__":
    main()

