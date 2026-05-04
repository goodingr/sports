from __future__ import annotations

import pandas as pd
import pytest

from src.models.train_betting import (
    FeatureContract,
    FoldSafeFeaturePruner,
    _contract_for_market,
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


def test_training_contract_includes_available_canonical_signals():
    totals = _synthetic_totals_frame()
    totals["home_soccer_ust_team_xg_avg_l5"] = 1.4
    totals["away_soccer_ust_team_xga_avg_l5"] = 1.1
    totals["home_nba_rolling_pace_5"] = 99.2
    totals["away_injuries_out"] = 2
    totals_contract = _contract_for_market("totals", totals)

    assert "home_soccer_ust_team_xg_avg_l5" in totals_contract.feature_columns
    assert "away_soccer_ust_team_xga_avg_l5" in totals_contract.feature_columns
    assert "home_nba_rolling_pace_5" in totals_contract.feature_columns
    assert "away_injuries_out" in totals_contract.feature_columns

    moneyline = pd.DataFrame(
        {
            "target_win": [1, 0],
            "no_vig_prob": [0.52, 0.48],
            "team_soccer_ust_team_xg_avg_l5": [1.5, 1.2],
            "opponent_soccer_ust_team_xga_avg_l5": [1.1, 1.3],
            "team_nba_rolling_pace_5": [100.0, 98.0],
            "team_injuries_out": [1, 0],
            "close_moneyline": [-110, 105],
        }
    )
    moneyline_contract = _contract_for_market("moneyline", moneyline)

    assert "team_soccer_ust_team_xg_avg_l5" in moneyline_contract.feature_columns
    assert "opponent_soccer_ust_team_xga_avg_l5" in moneyline_contract.feature_columns
    assert "team_nba_rolling_pace_5" in moneyline_contract.feature_columns
    assert "team_injuries_out" in moneyline_contract.feature_columns
    assert "close_moneyline" not in moneyline_contract.feature_columns


def test_fold_safe_feature_pruner_drops_all_missing_training_fold_columns():
    pruner = FoldSafeFeaturePruner()
    train = pd.DataFrame(
        {
            "all_missing_in_fold": [None, None, None],
            "signal": [1.0, 2.0, 3.0],
        }
    )
    test = pd.DataFrame(
        {
            "all_missing_in_fold": [99.0],
            "signal": [4.0],
        }
    )

    transformed = pruner.fit(train).transform(test)

    assert list(transformed.columns) == ["signal"]
    assert transformed.iloc[0]["signal"] == 4.0


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
