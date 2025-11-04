"""Ingest play-by-play data from the public nflfastR repository."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Sequence

import pandas as pd
import requests

from .utils import SourceDefinition, source_run


LOGGER = logging.getLogger(__name__)

BASE_URL = "https://github.com/nflverse/nflfastR-data/raw/master/data"


def _normalize_seasons(seasons: Iterable[int] | None) -> Sequence[int]:
    if seasons:
        return sorted({int(season) for season in seasons})
    current = datetime.now().year
    return [current]


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 120) -> str:
    """Download nflfastR play-by-play parquet dumps and register them in the warehouse."""

    season_list = _normalize_seasons(seasons)
    definition = SourceDefinition(
        key="nflfastr",
        name="nflfastR play-by-play",
        league="NFL",
        category="play_by_play",
        url="https://github.com/nflverse/nflfastR-data",
        default_frequency="daily",
        storage_subdir="nfl/nflfastr",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        for season in season_list:
            url = f"{BASE_URL}/play_by_play_{season}.parquet"
            LOGGER.info("Downloading nflfastR play-by-play for %s", season)
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()

            filename = f"play_by_play_{season}.parquet"
            dest = run.make_path(filename)
            dest.write_bytes(response.content)

            records = None
            try:
                df = pd.read_parquet(dest)
                records = len(df)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Unable to read parquet for %s to count rows: %s", season, exc)

            run.record_file(
                dest,
                season=season,
                metadata={"url": url, "filename": filename},
                records=records,
            )

        run.set_raw_path(run.storage_dir)
        if season_list:
            run.set_message(f"Downloaded seasons {season_list[0]}-{season_list[-1]}")

    return output_dir


__all__ = ["ingest"]

