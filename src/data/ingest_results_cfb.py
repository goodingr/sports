"""Ingest FBS schedules and results from CollegeFootballData."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from typing import Iterable, List

import pandas as pd
import requests

from src.db.loaders import load_schedules
from src.data.team_mappings import CFB_ALIASES

LOGGER = logging.getLogger(__name__)
CFBD_GAMES_ENDPOINT = "https://api.collegefootballdata.com/games"
DEFAULT_TIMEOUT = 30


def _get_api_key() -> str | None:
    return os.getenv("CFBD_API_KEY")


def _season_type_label(season_type: str) -> str:
    normalized = season_type.lower()
    if normalized in {"regular", "reg"}:
        return "regular"
    if normalized in {"postseason", "post", "bowl"}:
        return "postseason"
    return normalized


def _is_fbs_team(name: str | None) -> bool:
    if not name:
        return False
    key = name.lower()
    return key in CFB_ALIASES


def _fetch_games(year: int, season_type: str, *, timeout: int) -> List[dict]:
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

    response = requests.get(CFBD_GAMES_ENDPOINT, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _transform(games: Iterable[dict]) -> pd.DataFrame:
    records: List[dict] = []

    for game in games:
        home_team = game.get("home_team") or game.get("homeTeam")
        away_team = game.get("away_team") or game.get("awayTeam")
        if not (_is_fbs_team(str(home_team)) and _is_fbs_team(str(away_team))):
            continue

        game_id = game.get("id")
        if game_id is None:
            continue

        start = game.get("start_date") or game.get("startDate")
        start_dt = pd.to_datetime(start) if start else None
        gameday = start_dt.date().isoformat() if start_dt else None
        gametime = start_dt.time().strftime("%H:%M:%S") if start_dt else None
        weekday = start_dt.strftime("%A") if start_dt else None

        season_type = game.get("season_type") or game.get("seasonType") or "regular"
        game_type = "REG" if season_type.lower() == "regular" else "POST"

        completed = bool(game.get("completed"))
        home_points = game.get("home_points")
        away_points = game.get("away_points")
        if home_points is None:
            home_points = game.get("homePoints")
        if away_points is None:
            away_points = game.get("awayPoints")
        if not completed or home_points is None or away_points is None:
            home_points = None
            away_points = None

        records.append(
            {
                "game_id": f"CFB_{game_id}",
                "season": int(game.get("season")),
                "game_type": game_type,
                "week": game.get("week"),
                "gameday": gameday,
                "gametime": gametime,
                "weekday": weekday,
                "home_team": str(home_team),
                "home_team_name": str(home_team),
                "away_team": str(away_team),
                "away_team_name": str(away_team),
                "home_score": home_points,
                "away_score": away_points,
                "spread_line": None,
                "total_line": None,
                "home_moneyline": None,
                "away_moneyline": None,
                "stadium": game.get("venue"),
                "source_version": "cfbd",
            }
        )

    df = pd.DataFrame.from_records(records)
    return df


def ingest(
    *,
    seasons: Iterable[int] | None = None,
    season_type: str = "regular",
    timeout: int = DEFAULT_TIMEOUT,
) -> str:
    seasons_list = list(seasons or [])
    if not seasons_list:
        raise ValueError("At least one season must be provided for CFB ingestion")

    all_frames: List[pd.DataFrame] = []
    for year in seasons_list:
        LOGGER.info("Fetching CollegeFootballData games for %s (%s)", year, season_type)
        games = _fetch_games(int(year), season_type, timeout=timeout)
        frame = _transform(games)
        if frame.empty:
            LOGGER.warning("No CFB games returned for %s %s", year, season_type)
            continue
        all_frames.append(frame)

    if not all_frames:
        LOGGER.info("No CollegeFootballData games to ingest")
        return ""

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates("game_id")
    load_schedules(
        combined,
        source_version="cfbd",
        league="CFB",
        sport_name="Football",
        default_market="moneyline",
    )
    LOGGER.info("Stored %d CFB games", len(combined))
    return f"{len(combined)} games ingested"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest College Football schedules and results")
    parser.add_argument("seasons", nargs="+", type=int, help="Season years to fetch (e.g. 2024)")
    parser.add_argument("--season-type", default="regular", help="Season type: regular or postseason")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Request timeout in seconds")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    ingest(seasons=args.seasons, season_type=args.season_type, timeout=args.timeout)


if __name__ == "__main__":
    main()
