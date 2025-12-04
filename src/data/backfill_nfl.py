"""Pull historical NFL schedules and betting data via nfl_data_py."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List

import pandas as pd

try:
    import nfl_data_py as nfl  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "nfl_data_py is required for ingest_results. Install it with `poetry add nfl-data-py`."
    ) from exc

from src.db.loaders import load_schedules

from .config import RAW_DATA_DIR, ensure_directories


LOGGER = logging.getLogger(__name__)


BETTING_COLUMNS = {
    "season",
    "game_type",
    "week",
    "gameday",
    "weekday",
    "gametime",
    "away_team",
    "home_team",
    "away_score",
    "home_score",
    "away_moneyline",
    "home_moneyline",
    "spread_line",
    "away_spread_odds",
    "home_spread_odds",
    "total_line",
    "under_odds",
    "over_odds",
    "game_id",
    "gsis",
    "pfr",
}


def _to_int_list(seasons: Iterable[int | str]) -> List[int]:
    parsed: List[int] = []
    for season in seasons:
        value = int(season)
        if value < 1999:
            raise ValueError("Seasons before 1999 are not supported by nfl_data_py outputs")
        parsed.append(value)
    parsed.sort()
    return parsed


def fetch_schedules(seasons: List[int]) -> pd.DataFrame:
    LOGGER.info("Downloading schedules (with betting lines) for seasons: %s", seasons)
    return nfl.import_schedules(seasons)


def write_dataframe(df: pd.DataFrame, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    LOGGER.debug("Wrote %d rows to %s", len(df), out_path)
    return out_path


def run(seasons: List[int]) -> None:
    ensure_directories()

    schedules = fetch_schedules(seasons)
    schedules_path = RAW_DATA_DIR / "results" / f"schedules_{seasons[0]}_{seasons[-1]}.parquet"
    write_dataframe(schedules, schedules_path)
    LOGGER.info("Saved schedules to %s", schedules_path)

    betting = schedules[list(BETTING_COLUMNS & set(schedules.columns))].copy()
    betting_path = RAW_DATA_DIR / "results" / f"betting_{seasons[0]}_{seasons[-1]}.parquet"
    write_dataframe(betting, betting_path)
    LOGGER.info("Saved betting snapshot (derived from schedules) to %s", betting_path)

    load_schedules(schedules, league="NFL", sport_name="Football", default_market="moneyline")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download historical NFL schedules and betting data")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=list(range(1999, 2024)),
        help="List of NFL seasons (years) to download",
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

