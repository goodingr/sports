"""CFB-specific dataset generation logic."""
from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

from .shared import (
    DatasetPaths,
    build_base_dataset,
    load_latest_parquet,
    merge_espn_odds,
    normalize_advanced_team_codes,
)

LOGGER = logging.getLogger(__name__)


def merge_advanced_stats(dataset: pd.DataFrame) -> pd.DataFrame:
    advanced_stats = load_latest_parquet("cfb", "advanced_stats", "advanced_stats.parquet")
    if advanced_stats.empty:
        return dataset
    
    advanced_stats = normalize_advanced_team_codes(advanced_stats, "CFB")
    return dataset.merge(
        advanced_stats,
        on=["team", "season"],
        how="left"
    )


def build_dataset(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    dataset = build_base_dataset(seasons, "CFB")
    if dataset.empty:
        return dataset

    dataset = merge_espn_odds(dataset, "CFB")
    dataset = merge_advanced_stats(dataset)
    
    # Save processed dataset
    output_path = paths.processed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output_path, index=False)
    LOGGER.info("Wrote processed dataset to %s", output_path)
    
    return dataset
