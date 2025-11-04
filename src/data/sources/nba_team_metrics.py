"""Export NBA team estimated metrics via nba_api."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List

import pandas as pd

from .utils import SourceDefinition, source_run, write_dataframe

try:
    from nba_api.stats.endpoints import teamestimatedmetrics  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "nba_api is required for NBA team metrics. Install it with `poetry add nba-api`."
    ) from exc


LOGGER = logging.getLogger(__name__)


def _season_string(season_year: int) -> str:
    return f"{season_year}-{str(season_year + 1)[-2:]}"


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 30) -> str:  # noqa: ARG001
    definition = SourceDefinition(
        key="nba_team_metrics",
        name="NBA team estimated metrics",
        league="NBA",
        category="advanced_metrics",
        url="https://stats.nba.com/",
        default_frequency="weekly",
        storage_subdir="nba/team_metrics",
    )

    season_list: List[int]
    if seasons:
        season_list = sorted({int(season) for season in seasons})
    else:
        current = datetime.utcnow().year
        season_list = [current - 1, current]

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        frames: List[pd.DataFrame] = []
        for season_year in season_list:
            season_str = _season_string(season_year)
            LOGGER.info("Fetching NBA team metrics for %s", season_str)
            endpoint = teamestimatedmetrics.TeamEstimatedMetrics(season=season_str)
            df = endpoint.get_data_frames()[0]
            df["season"] = season_year
            frames.append(df)

        if not frames:
            run.set_message("No NBA team metrics retrieved")
            run.set_raw_path(run.storage_dir)
            return output_dir

        metrics = pd.concat(frames, ignore_index=True)
        metrics.rename(columns={"TEAM_ABBREVIATION": "team"}, inplace=True)

        path = run.make_path("team_metrics.parquet")
        write_dataframe(metrics, path)
        run.record_file(path, metadata={"rows": len(metrics)}, records=len(metrics))

        run.set_records(len(metrics))
        run.set_message(f"Captured {len(metrics)} NBA team metric rows")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]

