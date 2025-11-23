"""NHL-specific dataset generation logic."""
from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

from .shared import (
    DatasetPaths,
    add_team_form_features,
    build_base_dataset,
    merge_espn_odds,
)

LOGGER = logging.getLogger(__name__)


def build_dataset(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    dataset = build_base_dataset(seasons, "NHL")
    if dataset.empty:
        return dataset

    dataset = merge_espn_odds(dataset, "NHL")
    dataset = add_team_form_features(dataset)
    
    # Save processed dataset
    output_path = paths.processed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output_path, index=False)
    LOGGER.info("Wrote processed dataset to %s", output_path)
    
    return dataset
