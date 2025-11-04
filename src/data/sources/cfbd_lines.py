"""Ingest historical college football moneyline data from CollegeFootballData."""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime
from typing import Iterable, List, Optional

import requests

from .utils import SourceDefinition, source_run, write_json
from src.db.loaders import load_odds_snapshot

def _safe_float(value: object) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


LOGGER = logging.getLogger(__name__)

CFBD_LINES_ENDPOINT = "https://api.collegefootballdata.com/lines"
DEFAULT_TIMEOUT = 30
DEFAULT_SLEEP = 0.25


def _get_api_key() -> str | None:
    return os.getenv("CFBD_API_KEY")


def _season_type_label(season_type: str) -> str:
    normalized = season_type.lower()
    if normalized in {"regular", "reg"}:
        return "regular"
    if normalized in {"postseason", "post", "bowl"}:
        return "postseason"
    return normalized


def _fetch_lines(
    year: int,
    *,
    season_type: str = "regular",
    timeout: int = DEFAULT_TIMEOUT,
) -> List[dict]:
    api_key = _get_api_key()
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    else:
        LOGGER.warning("CFBD_API_KEY not set; requests may be rate-limited or rejected")

    params = {
        "year": year,
        "seasonType": _season_type_label(season_type),
        "division": "fbs",
    }

    response = requests.get(CFBD_LINES_ENDPOINT, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _normalize_provider(name: str) -> str:
    return name.lower().replace(" ", "_")


def _build_event(game: dict, season_type: str) -> Optional[dict]:
    commence_time = game.get("startDate") or game.get("start_date")
    if not commence_time:
        return None

    home = game.get("homeTeam") or game.get("home_team")
    away = game.get("awayTeam") or game.get("away_team")
    if not home or not away:
        return None

    bookmakers = []
    for line in game.get("lines", []):
        provider = line.get("provider")
        if not provider:
            continue

        markets = []

        home_moneyline = _safe_float(line.get("homeMoneyline") or line.get("home_moneyline"))
        away_moneyline = _safe_float(line.get("awayMoneyline") or line.get("away_moneyline"))
        if home_moneyline is not None and away_moneyline is not None:
            markets.append(
                {
                    "key": "h2h",
                    "outcomes": [
                        {"name": home, "price": home_moneyline},
                        {"name": away, "price": away_moneyline},
                    ],
                }
            )

        spread = _safe_float(line.get("spread"))
        if spread is not None:
            home_spread_price = _safe_float(line.get("spreadOdds") or line.get("homeSpreadOdds"))
            away_spread_price = _safe_float(line.get("awaySpreadOdds"))
            markets.append(
                {
                    "key": "spreads",
                    "outcomes": [
                        {
                            "name": home,
                            "point": spread,
                            "price": home_spread_price,
                        },
                        {
                            "name": away,
                            "point": -spread,
                            "price": away_spread_price,
                        },
                    ],
                }
            )

        total = _safe_float(line.get("overUnder"))
        if total is not None:
            over_price = _safe_float(line.get("overOdds"))
            under_price = _safe_float(line.get("underOdds"))
            markets.append(
                {
                    "key": "totals",
                    "outcomes": [
                        {"name": "Over", "point": total, "price": over_price},
                        {"name": "Under", "point": total, "price": under_price},
                    ],
                }
            )

        if not markets:
            continue

        bookmakers.append(
            {
                "key": _normalize_provider(provider),
                "title": provider,
                "markets": markets,
            }
        )

    if not bookmakers:
        return None

    return {
        "id": str(game.get("id")),
        "sport_key": "americanfootball_ncaaf",
        "sport_title": season_type,
        "commence_time": commence_time,
        "home_team": home,
        "away_team": away,
        "bookmakers": bookmakers,
    }


def ingest(
    *,
    seasons: Iterable[int] | None = None,
    season_type: str = "regular",
    timeout: int = DEFAULT_TIMEOUT,
    sleep: float = DEFAULT_SLEEP,
) -> str:
    seasons_list = list({int(season) for season in (seasons or [])})
    if not seasons_list:
        raise ValueError("At least one season must be provided for CFBD lines ingestion")

    definition = SourceDefinition(
        key="cfbd_lines",
        name="CollegeFootballData lines",
        league="CFB",
        category="odds",
        url=CFBD_LINES_ENDPOINT,
        default_frequency="daily",
        storage_subdir="cfb/cfbd_lines",
    )

    all_events: List[dict] = []

    with source_run(definition) as run:
        run.set_raw_path(run.storage_dir)

        for index, season in enumerate(sorted(seasons_list), 1):
            LOGGER.info("Fetching CFBD lines for %s %s", season, season_type)
            games = _fetch_lines(season, season_type=season_type, timeout=timeout)

            raw_path = run.make_path(f"lines_{season}_{season_type}.json")
            write_json(games, raw_path)
            run.record_file(raw_path, metadata={"season": season, "records": len(games)}, records=len(games))

            for game in games:
                event = _build_event(game, season_type)
                if event:
                    all_events.append(event)

            if index < len(seasons_list):
                time.sleep(max(sleep, 0))

        if not all_events:
            message = "No bookmaker data returned"
            run.set_message(message)
            LOGGER.warning(message)
            return message

        payload = {
            "results": all_events,
            "fetched_at": datetime.utcnow().isoformat(),
            "source": "cfbd_lines",
        }

        load_odds_snapshot(payload, raw_path=str(run.storage_dir), sport_key="americanfootball_ncaaf")

        run.set_records(len(all_events))
        run.set_message(f"Captured {len(all_events)} odds rows across {len(seasons_list)} seasons")

    return f"{len(all_events)} odds rows ingested"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest CollegeFootballData lines")
    parser.add_argument("--seasons", nargs="+", type=int, required=True, help="Season years to fetch (e.g. 2023 2024)")
    parser.add_argument("--season-type", default="regular", help="Season type (regular or postseason)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Request timeout in seconds")
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Seconds to sleep between season requests (default: 0.25)",
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

    ingest(
        seasons=args.seasons,
        season_type=args.season_type,
        timeout=args.timeout,
        sleep=args.sleep,
    )


if __name__ == "__main__":
    main()


