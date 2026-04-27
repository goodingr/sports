from __future__ import annotations

import pandas as pd
import pytest

from src.models.train_betting import (
    FeatureContract,
    rolling_origin_validate,
)


def _synthetic_totals_frame() -> pd.DataFrame:
    rows = []
    targets = [0, 1, 1, 0, 1, 0, 1, 1]
    for idx, target in enumerate(targets):
        line = 220.5 + (idx % 3)
        actual_total = line + 3 if target else line - 3
        rows.append(
            {
                "game_id": f"GAME{idx}",
                "league": "NBA",
                "start_time_utc": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(days=idx),
                "snapshot_time_utc": pd.Timestamp("2025-12-31", tz="UTC") + pd.Timedelta(days=idx),
                "line": line,
                "over_moneyline": -110,
                "under_moneyline": -110,
                "over_no_vig_prob": 0.5,
                "under_no_vig_prob": 0.5,
                "market_hold": 0.0476,
                "hours_before_start": 24.0,
                "opening_line": line - 0.5,
                "line_movement": 0.5,
                "book_line_count": 2,
                "line_std": 0.25,
                "home_score_for_l5": 112,
                "away_score_for_l5": 108,
                "home_score_against_l5": 106,
                "away_score_against_l5": 110,
                "home_rest_days": 2.0,
                "away_rest_days": 3.0,
                "rest_diff": -1.0,
                "score_for_l5_diff": 4,
                "score_against_l5_diff": -4,
                "actual_total": actual_total,
                "target_over": target,
            }
        )
    return pd.DataFrame(rows)


def test_feature_contract_rejects_leakage_columns():
    df = _synthetic_totals_frame()
    contract = FeatureContract(
        market="totals",
        target_column="target_over",
        feature_columns=["line", "actual_total"],
        market_probability_column="over_no_vig_prob",
    )

    with pytest.raises(ValueError, match="leakage"):
        contract.validate(df)


def test_market_only_rolling_validation_reports_quality_metrics():
    df = _synthetic_totals_frame()
    contract = FeatureContract(
        market="totals",
        target_column="target_over",
        feature_columns=[
            "line",
            "over_no_vig_prob",
            "under_no_vig_prob",
            "line_movement",
            "home_score_for_l5",
            "away_score_for_l5",
        ],
        market_probability_column="over_no_vig_prob",
    )

    predictions, summary = rolling_origin_validate(
        df,
        contract,
        model_type="market_only",
        edge_threshold=0.0,
        folds=2,
        min_train_size=4,
    )

    assert len(predictions) == 4
    assert summary["validation_rows"] == 4
    assert summary["brier_score"] == summary["market_brier_score"]
    assert summary["betting_rule"]["bets"] > 0
