"""Pull NBA schedules and results via nba_api."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder

from src.db.loaders import load_schedules

from .config import RAW_DATA_DIR, ensure_directories


LOGGER = logging.getLogger(__name__)


def _season_to_string(season: int) -> str:
    return f"{season}-{str(season + 1)[-2:]}"


def _fetch_game_logs(seasons: Iterable[int], season_type: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for season in seasons:
        season_str = _season_to_string(season)
        LOGGER.info("Downloading NBA %s data for %s", season_type, season_str)
        finder = leaguegamefinder.LeagueGameFinder(
            league_id_nullable="00",
            season_nullable=season_str,
            season_type_nullable=season_type,
        )
        df = finder.get_data_frames()[0]
        df["season_year"] = season
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _transform_to_games(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    records: List[dict[str, object]] = []
    grouped = df.groupby("GAME_ID")
    for game_id, group in grouped:
        if len(group) < 2:
            continue

        home_rows = group[group["MATCHUP"].str.contains(" vs. ", na=False)]
        away_rows = group[group["MATCHUP"].str.contains(" @ ", na=False)]
        if home_rows.empty or away_rows.empty:
            continue

        home_row = home_rows.iloc[0]
        away_row = away_rows.iloc[0]

        home_wl = str(home_row.get("WL") or "").strip().upper()
        away_wl = str(away_row.get("WL") or "").strip().upper()
        is_final = home_wl in {"W", "L"} and away_wl in {"W", "L"}

        try:
            game_date_str = str(home_row["GAME_DATE"]).strip()
        except Exception:
            game_date_str = ""

        game_date = None
        for fmt in ("%b %d, %Y", "%Y-%m-%d"):
            if not game_date_str:
                break
            try:
                game_date = datetime.strptime(game_date_str, fmt).date()
                break
            except ValueError:
                continue

        record = {
            "game_id": f"NBA_{game_id}",
            "season": int(home_row.get("season_year") or 0),
            "game_type": "REG",
            "week": None,
            "gameday": game_date.isoformat() if game_date else None,
            "gametime": None,
            "weekday": game_date.strftime("%A") if game_date else None,
            "home_team": home_row["TEAM_ABBREVIATION"],
            "home_team_name": home_row["TEAM_NAME"],
            "away_team": away_row["TEAM_ABBREVIATION"],
            "away_team_name": away_row["TEAM_NAME"],
            "home_score": int(home_row["PTS"]) if is_final else None,
            "away_score": int(away_row["PTS"]) if is_final else None,
            "spread_line": None,
            "total_line": None,
            "home_moneyline": None,
            "away_moneyline": None,
            "stadium": None,
            "source_version": "nba_api",
        }
        records.append(record)

    return pd.DataFrame.from_records(records)


def _to_int_list(seasons: Iterable[int | str]) -> List[int]:
    parsed: List[int] = []
    for season in seasons:
        value = int(season)
        if value < 2000:
            raise ValueError("NBA seasons before 2000 are not supported")
        parsed.append(value)
    parsed.sort()
    return parsed


def run(seasons: List[int], season_type: str = "Regular Season") -> None:
    ensure_directories()
    logs = _fetch_game_logs(seasons, season_type)
    games = _transform_to_games(logs)

    if games.empty:
        LOGGER.warning("No NBA games found for seasons %s", seasons)
        return

    file_tag = f"nba_{seasons[0]}_{seasons[-1]}"
    schedules_path = RAW_DATA_DIR / "results" / f"schedules_{file_tag}.parquet"
    games.to_parquet(schedules_path, index=False)
    LOGGER.info("Saved NBA schedules to %s", schedules_path)

    load_schedules(
        games,
        source_version="nba_api",
        league="NBA",
        sport_name="Basketball",
        default_market="moneyline",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download NBA schedules and results")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=list(range(2015, 2024)),
        help="List of NBA seasons (e.g., 2023 for 2023-24 season)",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        choices=["Regular Season", "Playoffs"],
        help="NBA season type",
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
    seasons = _to_int_list(args.seasons)
    run(seasons, season_type=args.season_type)


if __name__ == "__main__":
    main()

