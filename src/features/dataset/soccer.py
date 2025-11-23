"""Soccer-specific dataset generation logic."""
from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

from src.features import soccer_features
from .shared import (
    DatasetPaths,
    build_base_dataset,
    implied_probability,
    load_latest_parquet,
    merge_opponent_features,
    normalize_advanced_team_codes,
)

LOGGER = logging.getLogger(__name__)


def merge_football_data_features(
    dataset: pd.DataFrame,
    league: str,
    seasons: Iterable[int],
    games_df: pd.DataFrame,
) -> pd.DataFrame:
    odds = soccer_features.load_football_data_odds(league, seasons, games_df)
    if odds.empty:
        return dataset

    dataset = dataset.merge(odds, on=["game_id", "team"], how="left")
    dataset = merge_opponent_features(dataset, odds, "opponent_")

    if "fd_b365_ml_american" in dataset.columns:
        mask = dataset["moneyline"].isna() & dataset["fd_b365_ml_american"].notna()
        if mask.any():
            dataset.loc[mask, "moneyline"] = dataset.loc[mask, "fd_b365_ml_american"]
            dataset.loc[mask, "implied_prob"] = implied_probability(
                dataset.loc[mask, "fd_b365_ml_american"]
            )
    if "fd_ps_ml_american" in dataset.columns:
        mask = dataset["moneyline"].isna() & dataset["fd_ps_ml_american"].notna()
        if mask.any():
            dataset.loc[mask, "moneyline"] = dataset.loc[mask, "fd_ps_ml_american"]
            dataset.loc[mask, "implied_prob"] = implied_probability(
                dataset.loc[mask, "fd_ps_ml_american"]
            )
    return dataset


def merge_understat_features(
    dataset: pd.DataFrame,
    league: str,
    seasons: Iterable[int],
    games_df: pd.DataFrame,
) -> pd.DataFrame:
    understat = soccer_features.build_understat_features(league, seasons, games_df)
    if understat.empty:
        return dataset
    dataset = dataset.merge(understat, on=["game_id", "team"], how="left")
    dataset = merge_opponent_features(dataset, understat, "opponent_")
    return dataset


def merge_advanced_stats(dataset: pd.DataFrame, league: str) -> pd.DataFrame:
    advanced_stats = load_latest_parquet("soccer", "advanced_stats", "advanced_stats.parquet")
    if advanced_stats.empty:
        return dataset
    
    advanced_stats = advanced_stats[advanced_stats["league"] == league].copy()
    if advanced_stats.empty:
        return dataset

    advanced_stats = normalize_advanced_team_codes(advanced_stats, league)
    return dataset.merge(
        advanced_stats,
        on=["team", "season"],
        how="left"
    )


def build_dataset(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    league = paths.league
    dataset = build_base_dataset(seasons, league)
    if dataset.empty:
        return dataset

    dataset = merge_advanced_stats(dataset, league)

    # Reconstruct games_lookup for soccer features
    games_lookup = dataset[dataset["is_home"] == 1][
        ["game_id", "season", "game_datetime", "team", "opponent"]
    ].rename(columns={"game_datetime": "start_time_utc", "team": "home_team", "opponent": "away_team"})
    
    dataset = merge_football_data_features(dataset, league, seasons, games_lookup)
    dataset = merge_understat_features(dataset, league, seasons, games_lookup)

    output_path = paths.processed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output_path, index=False)
    LOGGER.info("Wrote processed dataset to %s", output_path)
    
    return dataset
