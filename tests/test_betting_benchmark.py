from __future__ import annotations

import pandas as pd

from src.models.betting_benchmark import (
    PredictionVariant,
    RollingConfig,
    StrictGate,
    rank_predeclared_rules,
    rolling_origin_predictions_for_variants,
)
from src.models.train_betting import FeatureContract


def _synthetic_totals_frame() -> pd.DataFrame:
    targets = [0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1]
    rows = []
    for idx, target in enumerate(targets):
        line = 220.5 + (idx % 2)
        actual_total = line + 4 if target else line - 4
        rows.append(
            {
                "game_id": f"GAME{idx}",
                "league": "NBA",
                "start_time_utc": pd.Timestamp("2026-01-01", tz="UTC")
                + pd.Timedelta(days=idx),
                "snapshot_time_utc": pd.Timestamp("2025-12-31", tz="UTC")
                + pd.Timedelta(days=idx),
                "line": line,
                "over_moneyline": -110,
                "under_moneyline": -110,
                "best_over_moneyline": -105,
                "best_under_moneyline": -105,
                "over_no_vig_prob": 0.5,
                "under_no_vig_prob": 0.5,
                "line_movement": 0.25 if target else -0.25,
                "signal": float(target),
                "actual_total": actual_total,
                "target_over": target,
            }
        )
    return pd.DataFrame(rows)


def test_residual_shrink_variant_is_rolling_origin_and_calibrated() -> None:
    df = _synthetic_totals_frame()
    contract = FeatureContract(
        market="totals",
        target_column="target_over",
        feature_columns=["line", "over_no_vig_prob", "line_movement", "signal"],
        market_probability_column="over_no_vig_prob",
    )
    variant = PredictionVariant(
        id="market_residual_shrink_050_sigmoid",
        residual=True,
        shrinkage=0.5,
        calibration="sigmoid",
    )

    predictions_by_variant = rolling_origin_predictions_for_variants(
        df,
        contract,
        model_type="logistic",
        variants=[variant],
        rolling=RollingConfig(
            folds=2,
            min_train_size=6,
            calibration_folds=2,
            min_calibration_size=3,
        ),
    )

    predictions = predictions_by_variant[variant.id]
    assert len(predictions) == 6
    assert predictions["validation_fold"].min() == 1
    assert predictions["predicted_prob"].between(0.0, 1.0).all()
    assert predictions["uncalibrated_prob"].between(0.0, 1.0).all()
    assert set(predictions["prediction_variant"]) == {variant.id}
    assert "residual_signal" in predictions.columns
    assert predictions["calibration_method"].notna().all()


def test_ranked_rules_include_market_baseline_and_strict_gate() -> None:
    start = pd.Timestamp("2026-01-01", tz="UTC")
    bets = pd.DataFrame(
        [
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "edge": 0.08,
                "profit": 90.91,
                "predicted_prob": 0.70,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 20.0,
                "actual_value": 90.91,
                "start_time_utc": start,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "edge": 0.07,
                "profit": 90.91,
                "predicted_prob": 0.69,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 18.0,
                "actual_value": 90.91,
                "start_time_utc": start + pd.Timedelta(days=1),
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "market_only",
                "prediction_variant": "baseline",
                "side": "over",
                "edge": 0.0,
                "profit": 90.91,
                "predicted_prob": 0.52,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 0.0,
                "actual_value": 90.91,
                "start_time_utc": start,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "market_only",
                "prediction_variant": "baseline",
                "side": "over",
                "edge": 0.0,
                "profit": -100.0,
                "predicted_prob": 0.52,
                "no_vig_market_prob": 0.52,
                "won": False,
                "expected_value": 0.0,
                "actual_value": -100.0,
                "start_time_utc": start + pd.Timedelta(days=1),
            },
        ]
    )
    rules = [
        {
            "id": "nba_totals_logistic_residual_over_edge_002",
            "status": "candidate",
            "kind": "candidate",
            "market": "totals",
            "league": "NBA",
            "model_type": "logistic",
            "prediction_variant": "market_residual_shrink_050_sigmoid",
            "side": "over",
            "min_edge": 0.02,
            "residual": True,
            "shrinkage": 0.5,
            "calibration": "sigmoid",
        },
        {
            "id": "nba_totals_market_baseline_over_edge_000",
            "status": "candidate",
            "kind": "baseline",
            "market": "totals",
            "league": "NBA",
            "model_type": "market_only",
            "prediction_variant": "baseline",
            "side": "over",
            "min_edge": 0.0,
            "residual": False,
            "shrinkage": 1.0,
            "calibration": "none",
        },
    ]

    ranked = rank_predeclared_rules(
        bets,
        rules,
        StrictGate(
            min_bets_narrow=3,
            min_roi=-1.0,
            min_bootstrap_roi_low=-1.0,
            bootstrap_samples=200,
        ),
    )
    candidate = next(row for row in ranked if row["kind"] == "candidate")

    assert all("market_baseline_comparison" in row for row in ranked)
    assert candidate["market_baseline_comparison"]["market_only"]["bets"] == 2
    assert candidate["publishable"] is False
    assert "sample_size_below_3" in candidate["strict_gate_failures"]
    assert "candidate_rule:" in candidate["candidate_rule_yaml"]
