"""Aggregate NFL team season metrics from nflfastR play-by-play."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List

import pandas as pd

from .utils import SourceDefinition, source_run, write_dataframe

try:
    import nfl_data_py as nfl  # type: ignore import-not-found
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "nfl_data_py is required for NFL team metrics. Install it with `poetry add nfl-data-py`."
    ) from exc


LOGGER = logging.getLogger(__name__)


def _aggregate_team_metrics(pbp: pd.DataFrame) -> pd.DataFrame:
    relevant = pbp[pbp["play_type"].isin({"pass", "run"})].copy()

    offense = (
        relevant.groupby(["season", "posteam"], dropna=False)
        .agg(
            off_plays=("play_id", "count"),
            off_epa_total=("epa", "sum"),
            off_success_rate=("success", "mean"),
        )
        .reset_index()
        .rename(columns={"posteam": "team"})
    )
    offense["off_epa_per_play"] = offense["off_epa_total"] / offense["off_plays"].replace(0, pd.NA)
    offense.drop(columns=["off_epa_total"], inplace=True)

    defense = (
        relevant.groupby(["season", "defteam"], dropna=False)
        .agg(
            def_plays=("play_id", "count"),
            def_epa_total=("epa", "sum"),
            def_success_allowed=("success", "mean"),
        )
        .reset_index()
        .rename(columns={"defteam": "team"})
    )
    defense["def_epa_per_play"] = -defense["def_epa_total"] / defense["def_plays"].replace(0, pd.NA)
    defense["def_success_rate"] = 1 - defense["def_success_allowed"]
    defense.drop(columns=["def_epa_total", "def_success_allowed"], inplace=True)

    metrics = offense.merge(defense, on=["season", "team"], how="outer")
    metrics["team"] = metrics["team"].astype(str)
    return metrics


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 60) -> str:  # noqa: ARG001
    definition = SourceDefinition(
        key="nfl_team_metrics",
        name="NFL team EPA metrics",
        league="NFL",
        category="advanced_metrics",
        url="https://github.com/nflverse/nflfastR-data",
        default_frequency="weekly",
        storage_subdir="nfl/team_metrics",
    )

    season_list: List[int]
    if seasons:
        season_list = sorted({int(season) for season in seasons})
    else:
        current = datetime.utcnow().year
        season_list = list(range(current - 4, current + 1))

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        LOGGER.info("Importing nflfastR play-by-play for seasons %s", season_list)
        pbp = nfl.import_pbp_data(season_list)
        metrics = _aggregate_team_metrics(pbp)

        path = run.make_path("team_metrics.parquet")
        write_dataframe(metrics, path)
        run.record_file(path, metadata={"rows": len(metrics)}, records=len(metrics))

        run.set_records(len(metrics))
        run.set_message(f"Captured {len(metrics)} NFL team metric rows")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]

