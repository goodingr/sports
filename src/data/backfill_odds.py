"""Backfill historical odds from The Odds API into raw storage and SQLite."""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import RAW_DATA_DIR
from .ingest_odds import OddsAPISettings, fetch_odds_history
from src.db.loaders import load_odds_snapshot


LOGGER = logging.getLogger(__name__)


def _parse_iso_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:  # pragma: no cover - CLI guard
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def _date_range(start: datetime, end: datetime, step_days: int):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=step_days)


def backfill(
    settings: OddsAPISettings,
    start_date: datetime,
    end_date: datetime,
    *,
    sleep_seconds: float = 0.0,
    step_days: int = 1,
    overwrite: bool = False,
) -> None:
    history_dir = RAW_DATA_DIR / "odds" / settings.sport / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    if step_days < 1:
        raise ValueError("step_days must be >= 1")

    for target in _date_range(start_date, end_date, step_days):
        LOGGER.info("Fetching odds history for %s %s", settings.sport, target.date())
        try:
            payload = fetch_odds_history(settings, target)
        except Exception as exc:  # pragma: no cover - network guard
            LOGGER.error("Failed to fetch odds for %s: %s", target.date(), exc)
            break
        timestamp = target.strftime("%Y-%m-%d")
        snapshot_path = history_dir / f"odds_{timestamp}.json"
        if snapshot_path.exists() and not overwrite:
            LOGGER.debug("Skipping existing snapshot %s", snapshot_path)
        else:
            snapshot_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        load_odds_snapshot(payload, raw_path=str(snapshot_path), region=settings.region, sport_key=settings.sport)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill historical odds from The Odds API")
    parser.add_argument("start_date", type=_parse_iso_date, help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", type=_parse_iso_date, help="End date inclusive (YYYY-MM-DD)")
    parser.add_argument(
        "--sport",
        default="americanfootball_nfl",
        help="The Odds API sport key (e.g., americanfootball_nfl, basketball_nba)",
    )
    parser.add_argument(
        "--market",
        default="h2h",
        help="Odds market to request (default: h2h)",
    )
    parser.add_argument(
        "--region",
        default="us",
        help="Odds region (default: us)",
    )
    parser.add_argument(
        "--dotenv",
        type=Path,
        default=None,
        help="Optional path to .env file containing ODDS_API_KEY",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.5,
        help="Seconds to sleep between requests to respect rate limits",
    )
    parser.add_argument(
        "--step-days",
        type=int,
        default=1,
        help="Number of days to skip between requests (default: 1)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-fetch and overwrite existing snapshots if they already exist",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    if args.end_date < args.start_date:
        raise SystemExit("end_date must be on or after start_date")

    settings = OddsAPISettings.from_env(args.dotenv)
    settings.sport = args.sport
    settings.market = args.market
    settings.region = args.region

    backfill(
        settings,
        args.start_date,
        args.end_date,
        sleep_seconds=args.sleep,
        step_days=args.step_days,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()

