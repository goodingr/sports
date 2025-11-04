"""Download NFL injury reports from the nflverse repository."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List

import pandas as pd

from src.db.loaders import store_injury_reports

from .utils import SourceDefinition, source_run, write_dataframe

try:
    import nfl_data_py as nfl  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "nfl_data_py is required for nflverse injury ingestion. Install it with `poetry add nfl-data-py`."
    ) from exc


LOGGER = logging.getLogger(__name__)


CANONICAL_COLUMNS = {
    "team": "team_code",
    "club_code": "team_code",
    "club": "team_name",
    "team_name": "team_name",
    "season": "season",
    "week": "week",
    "game_week": "week",
    "player_name": "player_name",
    "player_display_name": "player_name",
    "display_name": "player_name",
    "position": "position",
    "injury_body_part": "detail",
    "injury_notes": "notes",
    "report_day": "report_date",
    "report_date": "report_date",
    "game_date": "game_date",
    "injury_game_status": "status",
    "injury_status": "status",
    "practice_status": "practice_status",
}


def _canonically_rename(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {col: CANONICAL_COLUMNS[col] for col in df.columns if col in CANONICAL_COLUMNS}
    return df.rename(columns=rename_map)


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 60) -> str:  # noqa: ARG001
    """Fetch aggregated NFL injury reports."""

    definition = SourceDefinition(
        key="nflverse_injuries",
        name="nflverse injury reports",
        league="NFL",
        category="injuries",
        url="https://github.com/nflverse/nflverse-data",
        default_frequency="daily",
        storage_subdir="nfl/injuries",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)

        season_list: List[int]
        if seasons:
            season_list = [int(season) for season in seasons]
        else:
            current = datetime.utcnow().year
            season_list = [current - 1, current]

        LOGGER.info("Importing nflverse injuries via nfl_data_py for seasons %s", season_list)
        df = nfl.import_injuries(season_list)
        df = _canonically_rename(df)

        parquet_path = run.make_path("injuries.parquet")
        write_dataframe(df, parquet_path)
        run.record_file(
            parquet_path,
            metadata={"rows": len(df), "seasons": season_list},
            records=len(df),
        )

        if len(df):
            store_injury_reports(
                df[[
                    col
                    for col in df.columns
                    if col
                    in {
                        "team_code",
                        "team_name",
                        "player_name",
                        "position",
                        "status",
                        "practice_status",
                        "report_date",
                        "game_date",
                        "detail",
                        "notes",
                        "season",
                        "week",
                    }
                ]].copy(),
                league="NFL",
                source_key="nflverse_injuries",
            )

        run.set_records(len(df))
        run.set_message(f"Captured {len(df)} nflverse injury rows via nfl_data_py")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]

