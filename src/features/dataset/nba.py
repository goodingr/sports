"""NBA-specific dataset generation logic."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from typing import Iterable

from src.data.team_mappings import normalize_team_code
from .shared import (
    DatasetPaths,
    add_opponent_feature_mirrors,
    build_base_dataset,
    convert_line_to_float,
    implied_probability,
    load_latest_csv,
    load_latest_parquet,
    merge_espn_odds,
)
from src.features.player_features import build_player_features

LOGGER = logging.getLogger(__name__)





def merge_team_metrics(dataset: pd.DataFrame) -> pd.DataFrame:
    metrics = load_latest_parquet("nba", "team_metrics", "team_metrics.parquet")
    if metrics.empty:
        return dataset
    if "team" in metrics.columns:
        metrics["team"] = metrics["team"].astype(str)
    elif "TEAM_ABBREVIATION" in metrics.columns:
        metrics["team"] = metrics["TEAM_ABBREVIATION"].astype(str)
    elif "TEAM_NAME" in metrics.columns:
        metrics["team"] = metrics["TEAM_NAME"].apply(lambda name: normalize_team_code("NBA", str(name)))
    else:
        return dataset
    metrics = metrics[["season", "team", "E_OFF_RATING", "E_DEF_RATING", "E_NET_RATING", "E_PACE"]]
    return dataset.merge(metrics, on=["season", "team"], how="left")


def merge_rolling_metrics(dataset: pd.DataFrame) -> pd.DataFrame:
    rolling_metrics = load_latest_parquet("nba", "rolling_metrics", "rolling_metrics.parquet")
    if not rolling_metrics.empty:
        rolling_metrics["team"] = rolling_metrics["team"].astype(str)
        dataset_game_dt = pd.to_datetime(dataset["game_datetime"], errors="coerce", utc=True)
        dataset["game_date"] = dataset_game_dt.dt.tz_localize(None).dt.normalize()
        rolling_game_dt = pd.to_datetime(rolling_metrics["game_date"], errors="coerce", utc=True)
        rolling_metrics["game_date"] = rolling_game_dt.dt.tz_localize(None).dt.normalize()
        dataset = dataset.merge(
            rolling_metrics[
                [
                    "team",
                    "game_date",
                    "rolling_win_pct_3",
                    "rolling_point_diff_3",
                    "rolling_off_rating_3",
                    "rolling_def_rating_3",
                    "rolling_net_rating_3",
                    "rolling_pace_3",
                    "rolling_win_pct_5",
                    "rolling_point_diff_5",
                    "rolling_off_rating_5",
                    "rolling_def_rating_5",
                    "rolling_net_rating_5",
                    "rolling_pace_5",
                    "rolling_win_pct_10",
                    "rolling_point_diff_10",
                    "rolling_off_rating_10",
                    "rolling_def_rating_10",
                    "rolling_net_rating_10",
                    "rolling_pace_10",
                    "rolling_win_pct_15",
                    "rolling_point_diff_15",
                    "rolling_off_rating_15",
                    "rolling_def_rating_15",
                    "rolling_net_rating_15",
                    "rolling_pace_15",
                    "rolling_win_pct_20",
                    "rolling_point_diff_20",
                    "rolling_off_rating_20",
                    "rolling_def_rating_20",
                    "rolling_net_rating_20",
                    "rolling_pace_20",
                ]
            ],
            on=["team", "game_date"],
            how="left",
        )
    return dataset


def add_opponent_features(dataset: pd.DataFrame) -> pd.DataFrame:
    mirror_columns = []
    for window in [3, 5, 10, 15, 20]:
        mirror_columns.extend([
            f"rolling_win_pct_{window}",
            f"rolling_point_diff_{window}",
            f"rolling_off_rating_{window}",
            f"rolling_def_rating_{window}",
            f"rolling_net_rating_{window}",
            f"rolling_pace_{window}",
        ])
    
    dataset = add_opponent_feature_mirrors(dataset, mirror_columns)
    
    # Calculate adjusted metrics
    for window in [3, 5, 10, 15, 20]:
        # Adjusted Off Rating = Team Off Rtg - Opponent Def Rtg
        dataset[f"adj_rolling_off_rating_{window}"] = (
            dataset[f"rolling_off_rating_{window}"] - dataset[f"opponent_rolling_def_rating_{window}"]
        )
        # Adjusted Def Rating = Team Def Rtg - Opponent Off Rtg
        dataset[f"adj_rolling_def_rating_{window}"] = (
            dataset[f"rolling_def_rating_{window}"] - dataset[f"opponent_rolling_off_rating_{window}"]
        )
        # Adjusted Net Rating = Team Net Rtg - Opponent Net Rtg
        dataset[f"adj_rolling_net_rating_{window}"] = (
            dataset[f"rolling_net_rating_{window}"] - dataset[f"opponent_rolling_net_rating_{window}"]
        )
        # Pace differential
        dataset[f"adj_rolling_pace_{window}"] = (
            dataset[f"rolling_pace_{window}"] - dataset[f"opponent_rolling_pace_{window}"]
        )
        
    return dataset


    return dataset


def merge_player_features(dataset: pd.DataFrame) -> pd.DataFrame:
    """Merge player-aggregated features into the dataset."""
    player_features = build_player_features("NBA")
    if player_features.empty:
        return dataset
        
    # Merge on game_id and team
    return dataset.merge(player_features, on=["game_id", "team"], how="left")


def build_dataset(paths: DatasetPaths, seasons: Iterable[int]) -> pd.DataFrame:
    dataset = build_base_dataset(seasons, "NBA")
    if dataset.empty:
        return dataset

    dataset = merge_espn_odds(dataset, "NBA")
    
    # Fill missing moneyline with ESPN odds if available
    mask = dataset["moneyline"].isna() & dataset["espn_moneyline_close"].notna()
    if mask.any():
        dataset.loc[mask, "moneyline"] = dataset.loc[mask, "espn_moneyline_close"]
        dataset.loc[mask, "implied_prob"] = implied_probability(dataset.loc[mask, "espn_moneyline_close"])

    dataset = merge_team_metrics(dataset)
    dataset = merge_rolling_metrics(dataset)
    dataset = merge_player_features(dataset)
    dataset = add_opponent_features(dataset)

    output_path = paths.processed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output_path, index=False)
    LOGGER.info("Wrote processed dataset to %s", output_path)
    
    return dataset
