
import pandas as pd
import pytest
from unittest.mock import patch
from src.features.dataset.nba import merge_rolling_metrics

@patch("src.features.dataset.nba.load_latest_parquet")
def test_merge_rolling_metrics(mock_load_parquet):
    # Create sample games dataframe with home and away rows
    games_df = pd.DataFrame({
        "game_id": ["game1", "game1", "game2", "game2"],
        "team": ["LAL", "GSW", "BOS", "MIA"],
        "opponent": ["GSW", "LAL", "MIA", "BOS"],
        "game_datetime": pd.to_datetime([
            "2023-10-25 19:00:00", "2023-10-25 19:00:00",
            "2023-10-26 19:00:00", "2023-10-26 19:00:00"
        ]).tz_localize("UTC")
    })

    # Create sample rolling metrics dataframe
    metrics_df = pd.DataFrame({
        "team": ["LAL", "GSW", "BOS", "MIA"],
        "game_date": pd.to_datetime(["2023-10-25", "2023-10-25", "2023-10-26", "2023-10-26"]),
        "rolling_win_pct_3": [0.5, 0.6, 0.7, 0.8],
        "rolling_point_diff_3": [2.0, 3.0, 4.0, 5.0],
        "rolling_off_rating_3": [110.0, 112.0, 108.0, 105.0],
        "rolling_def_rating_3": [105.0, 110.0, 100.0, 102.0],
        "rolling_net_rating_3": [5.0, 2.0, 8.0, 3.0],
        "rolling_pace_3": [100.0, 102.0, 98.0, 96.0],
        "rolling_win_pct_5": [0.5, 0.6, 0.7, 0.8],
        "rolling_point_diff_5": [2.0, 3.0, 4.0, 5.0],
        "rolling_off_rating_5": [109.0, 111.0, 107.0, 104.0],
        "rolling_def_rating_5": [104.0, 109.0, 99.0, 101.0],
        "rolling_net_rating_5": [5.0, 2.0, 8.0, 3.0],
        "rolling_pace_5": [100.0, 102.0, 98.0, 96.0],
        "rolling_win_pct_10": [0.5, 0.6, 0.7, 0.8],
        "rolling_point_diff_10": [2.0, 3.0, 4.0, 5.0],
        "rolling_off_rating_10": [108.0, 110.0, 106.0, 103.0],
        "rolling_def_rating_10": [103.0, 108.0, 98.0, 100.0],
        "rolling_net_rating_10": [5.0, 2.0, 8.0, 3.0],
        "rolling_pace_10": [100.0, 102.0, 98.0, 96.0],
        "rolling_win_pct_15": [0.5, 0.6, 0.7, 0.8],
        "rolling_point_diff_15": [2.0, 3.0, 4.0, 5.0],
        "rolling_off_rating_15": [108.0, 110.0, 106.0, 103.0],
        "rolling_def_rating_15": [103.0, 108.0, 98.0, 100.0],
        "rolling_net_rating_15": [5.0, 2.0, 8.0, 3.0],
        "rolling_pace_15": [100.0, 102.0, 98.0, 96.0],
        "rolling_win_pct_20": [0.5, 0.6, 0.7, 0.8],
        "rolling_point_diff_20": [2.0, 3.0, 4.0, 5.0],
        "rolling_off_rating_20": [108.0, 110.0, 106.0, 103.0],
        "rolling_def_rating_20": [103.0, 108.0, 98.0, 100.0],
        "rolling_net_rating_20": [5.0, 2.0, 8.0, 3.0],
        "rolling_pace_20": [100.0, 102.0, 98.0, 96.0],
    })
    
    mock_load_parquet.return_value = metrics_df

    # Merge metrics (which now includes add_opponent_features)
    # Note: merge_rolling_metrics only merges. add_opponent_features is separate in nba.py
    # But in test_nba.py we are testing merge_rolling_metrics?
    # Wait, I added add_opponent_features to build_dataset, NOT merge_rolling_metrics.
    # So I should test add_opponent_features separately or update the test to call it.
    
    from src.features.dataset.nba import merge_rolling_metrics, add_opponent_features
    merged_df = merge_rolling_metrics(games_df)
    merged_df = add_opponent_features(merged_df)

    # Verify columns exist
    expected_cols = [
        "rolling_off_rating_3",
        "rolling_def_rating_3",
        "rolling_net_rating_3",
        "rolling_pace_3",
        "rolling_off_rating_5",
        "rolling_off_rating_10",
        "rolling_off_rating_15",
        "rolling_off_rating_20",
        "opponent_rolling_off_rating_3",
        "adj_rolling_off_rating_3",
    ]
    
    for col in expected_cols:
        assert col in merged_df.columns, f"Missing column: {col}"

    # Verify values for game1 (LAL vs GSW)
    # LAL (index 0)
    # LAL off rating: 110.0
    # GSW def rating: 110.0
    # Adj off rating: 110.0 - 110.0 = 0.0
    assert merged_df.loc[0, "rolling_off_rating_3"] == 110.0
    assert merged_df.loc[0, "opponent_rolling_def_rating_3"] == 110.0
    assert merged_df.loc[0, "adj_rolling_off_rating_3"] == 0.0
    
    # GSW (index 1)
    # GSW off rating: 112.0
    # LAL def rating: 105.0
    # Adj off rating: 112.0 - 105.0 = 7.0
    assert merged_df.loc[1, "rolling_off_rating_3"] == 112.0
    assert merged_df.loc[1, "opponent_rolling_def_rating_3"] == 105.0
    assert merged_df.loc[1, "adj_rolling_off_rating_3"] == 7.0

@patch("src.features.dataset.nba.load_latest_parquet")
def test_merge_rolling_metrics_missing_data(mock_load_parquet):
    # Test with missing metrics for one team
    games_df = pd.DataFrame({
        "game_id": ["game1", "game1"],
        "team": ["LAL", "GSW"],
        "opponent": ["GSW", "LAL"],
        "game_datetime": pd.to_datetime(["2023-10-25 19:00:00", "2023-10-25 19:00:00"]).tz_localize("UTC")
    })

    # Only LAL has metrics
    metrics_df = pd.DataFrame({
        "team": ["LAL"],
        "game_date": pd.to_datetime(["2023-10-25"]),
        "rolling_win_pct_3": [0.5],
        "rolling_point_diff_3": [2.0],
        "rolling_off_rating_3": [110.0],
        "rolling_def_rating_3": [105.0],
        "rolling_net_rating_3": [5.0],
        "rolling_pace_3": [100.0],
        "rolling_win_pct_5": [0.5],
        "rolling_point_diff_5": [2.0],
        "rolling_off_rating_5": [109.0],
        "rolling_def_rating_5": [104.0],
        "rolling_net_rating_5": [5.0],
        "rolling_pace_5": [100.0],
        "rolling_win_pct_10": [0.5],
        "rolling_point_diff_10": [2.0],
        "rolling_off_rating_10": [108.0],
        "rolling_def_rating_10": [103.0],
        "rolling_net_rating_10": [5.0],
        "rolling_pace_10": [100.0],
        "rolling_win_pct_15": [0.5],
        "rolling_point_diff_15": [2.0],
        "rolling_off_rating_15": [108.0],
        "rolling_def_rating_15": [103.0],
        "rolling_net_rating_15": [5.0],
        "rolling_pace_15": [100.0],
        "rolling_win_pct_20": [0.5],
        "rolling_point_diff_20": [2.0],
        "rolling_off_rating_20": [108.0],
        "rolling_def_rating_20": [103.0],
        "rolling_net_rating_20": [5.0],
        "rolling_pace_20": [100.0],
    })
    
    mock_load_parquet.return_value = metrics_df

    merged_df = merge_rolling_metrics(games_df)

    # Home team (LAL) should have data
    assert merged_df.loc[0, "rolling_off_rating_3"] == 110.0
    
    # Away team (BOS) should be NaN
    assert pd.isna(merged_df.loc[1, "rolling_off_rating_3"])
