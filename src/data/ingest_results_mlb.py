"""Pull MLB schedules and results via pybaseball."""

from __future__ import annotations

import argparse
import logging
from typing import Iterable, List

import pandas as pd

try:
    import pybaseball  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "pybaseball is required for ingest_results_mlb. Install it with `poetry add pybaseball`."
    ) from exc

from src.db.loaders import load_schedules

from .config import RAW_DATA_DIR, ensure_directories


LOGGER = logging.getLogger(__name__)


def _to_int_list(seasons: Iterable[int | str]) -> List[int]:
    parsed: List[int] = []
    for season in seasons:
        value = int(season)
        if value < 1871:
            raise ValueError("MLB seasons before 1871 are not supported")
        parsed.append(value)
    parsed.sort()
    return parsed


def fetch_schedules(seasons: List[int]) -> pd.DataFrame:
    """Fetch MLB schedules using pybaseball."""
    LOGGER.info("Downloading MLB schedules for seasons: %s", seasons)
    all_games = []
    
    for season in seasons:
        try:
            LOGGER.info("Fetching %s season schedule...", season)
            # pybaseball.schedule_and_record returns team schedules
            # We need to get game logs instead for individual games
            schedule = pybaseball.schedule_and_record(season, season)
            if schedule is not None and not schedule.empty:
                schedule["season"] = season
                all_games.append(schedule)
        except Exception as exc:
            LOGGER.warning("Failed to fetch %s season: %s", season, exc)
            continue
    
    if not all_games:
        return pd.DataFrame()
    
    return pd.concat(all_games, ignore_index=True)


def _transform_to_games(df: pd.DataFrame) -> pd.DataFrame:
    """Transform pybaseball schedule data to our game format."""
    if df.empty:
        return df
    
    # pybaseball.schedule_and_record returns team-level data, not game-level
    # We need to use team_game_logs or another function for individual games
    # For now, this is a placeholder that will need to be updated based on actual pybaseball output
    
    # Try to use team_game_logs if available
    records: List[dict[str, object]] = []
    
    # Get unique teams and seasons
    if "Team" in df.columns and "season" in df.columns:
        for (team, season), group in df.groupby(["Team", "season"]):
            try:
                # Get game logs for this team/season
                game_logs = pybaseball.team_game_logs(team, season)
                if game_logs is not None and not game_logs.empty:
                    # Transform game logs to our format
                    for _, game in game_logs.iterrows():
                        # Extract game information
                        # Note: pybaseball game logs structure may vary
                        # This is a template that needs to be adjusted based on actual output
                        record = {
                            "game_id": f"MLB_{game.get('Date', '')}_{team}_{game.get('Opp', '')}",
                            "season": season,
                            "game_type": "REG",
                            "week": None,  # MLB doesn't use weeks
                            "gameday": None,
                            "gametime": None,
                            "weekday": None,
                            "home_team": None,
                            "home_team_name": None,
                            "away_team": None,
                            "away_team_name": None,
                            "home_score": None,
                            "away_score": None,
                            "spread_line": None,
                            "total_line": None,
                            "home_moneyline": None,
                            "away_moneyline": None,
                            "stadium": None,
                            "source_version": "pybaseball",
                        }
                        records.append(record)
            except Exception as exc:
                LOGGER.debug("Failed to get game logs for %s %s: %s", team, season, exc)
                continue
    
    if not records:
        LOGGER.warning("No game records extracted from pybaseball data")
        return pd.DataFrame()
    
    return pd.DataFrame.from_records(records)


def run(seasons: List[int]) -> None:
    """Fetch and load MLB schedules."""
    ensure_directories()
    
    # For now, use a simpler approach: fetch team schedules
    # This will need to be enhanced to get individual game results
    schedules = fetch_schedules(seasons)
    
    if schedules.empty:
        LOGGER.warning("No MLB schedules found for seasons %s", seasons)
        return
    
    # Save raw data
    file_tag = f"mlb_{seasons[0]}_{seasons[-1]}"
    schedules_path = RAW_DATA_DIR / "results" / f"schedules_{file_tag}.parquet"
    schedules.to_parquet(schedules_path, index=False)
    LOGGER.info("Saved MLB schedules to %s", schedules_path)
    
    # Transform to games format
    games = _transform_to_games(schedules)
    
    if games.empty:
        LOGGER.warning("No MLB games extracted from schedules")
        return
    
    load_schedules(
        games,
        source_version="pybaseball",
        league="MLB",
        sport_name="Baseball",
        default_market="moneyline",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download MLB schedules and results")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=[2023, 2024],
        help="List of MLB seasons (years) to download",
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
    seasons = _to_int_list(args.seasons)
    run(seasons)


if __name__ == "__main__":
    main()


