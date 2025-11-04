"""Pull NBA game logs via the official nba_api client."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog  # type: ignore import-not-found

from .utils import SourceDefinition, source_run, write_dataframe


LOGGER = logging.getLogger(__name__)


def _normalize_seasons(seasons: Iterable[int] | None) -> List[int]:
    if seasons:
        return sorted({int(season) for season in seasons})
    current = datetime.now().year
    return [current]


def _season_string(season: int) -> str:
    return f"{season}-{str(season + 1)[-2:]}"


def ingest(
    *,
    seasons: Iterable[int] | None = None,
    season_type: str = "Regular Season",
) -> str:
    """Download league game logs for the given seasons."""

    season_list = _normalize_seasons(seasons)
    definition = SourceDefinition(
        key="nba_api",
        name="NBA Stats API",
        league="NBA",
        category="logs",
        url="https://stats.nba.com/",
        default_frequency="daily",
        storage_subdir="nba/nba_api",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        total_rows = 0
        all_frames: List[pd.DataFrame] = []

        for season in season_list:
            season_str = _season_string(season)
            LOGGER.info("Requesting nba_api LeagueGameLog for %s (%s)", season, season_type)
            endpoint = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type)
            df = endpoint.get_data_frames()[0]
            df["season"] = season
            all_frames.append(df)

            parquet_path = run.make_path(f"league_gamelog_{season}.parquet")
            write_dataframe(df, parquet_path)
            run.record_file(
                parquet_path,
                season=season,
                metadata={"season": season, "season_type": season_type},
                records=len(df),
            )
            total_rows += len(df)

        if total_rows:
            run.set_records(total_rows)
            run.set_message(f"Captured {total_rows} nba_api game log rows")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]

