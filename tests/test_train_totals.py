from __future__ import annotations

import hashlib
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest

from src.models.totals_calibration import split_totals_dates
from src.models.train_totals import train_totals_model


def test_training_estimates_scale_without_fitting_calibration_or_test_results(tmp_path: Path):
    dates = pd.date_range("2025-01-01T18:00:00Z", periods=45, freq="D").repeat(2)
    line = 210.5 + np.arange(len(dates)) % 15
    frame = pd.DataFrame(
        {
            "league": "NBA",
            "game_id": [f"g{index}" for index in range(len(dates))],
            "start_time_utc": dates,
            "total_close": line,
            "spread_close": 0.0,
            "home_moneyline_close": -130.0,
            "away_moneyline_close": 110.0,
            "actual_total": line + np.sin(np.arange(len(dates))) * 20,
        }
    )
    source = tmp_path / "input.parquet"
    frame.to_parquet(source, index=False)
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    artifacts = train_totals_model(
        "NBA",
        "random_forest",
        input_path=source,
        models_dir=tmp_path / "models",
        metrics_dir=tmp_path / "metrics",
    )
    bundle = joblib.load(artifacts.model_path)
    train, calibration, test = split_totals_dates(frame)
    features = bundle["feature_names"]
    predicted = bundle["regressor"].predict(calibration[features])
    expected_scale = np.sqrt(np.mean((calibration["actual_total"] - predicted) ** 2))
    assert bundle["residual_std"] == pytest.approx(expected_scale)
    assert bundle["uncertainty_source"] == "chronological_calibration_rmse"
    assert pd.Timestamp(bundle["train_end"]) < calibration["start_time_utc"].min()
    assert pd.Timestamp(bundle["calibration_end"]) < test["start_time_utc"].min()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash

    changed = frame.copy()
    changed.loc[test.index, "actual_total"] += 1000
    other_source = tmp_path / "changed-test.parquet"
    changed.to_parquet(other_source, index=False)
    repeated = train_totals_model(
        "NBA",
        "random_forest",
        input_path=other_source,
        models_dir=tmp_path / "other-models",
        metrics_dir=tmp_path / "other-metrics",
    )
    other_bundle = joblib.load(repeated.model_path)
    assert other_bundle["residual_std"] == pytest.approx(bundle["residual_std"], abs=1e-12)
    np.testing.assert_allclose(
        other_bundle["regressor"].predict(test[features]),
        bundle["regressor"].predict(test[features]),
        rtol=0,
        atol=1e-12,
    )
    changed.loc[calibration.index, "actual_total"] += 100
    changed.to_parquet(other_source, index=False)
    recalibrated = train_totals_model(
        "NBA",
        "random_forest",
        input_path=other_source,
        models_dir=tmp_path / "recalibrated-models",
        metrics_dir=tmp_path / "recalibrated-metrics",
    )
    recalibrated_bundle = joblib.load(recalibrated.model_path)
    assert recalibrated_bundle["residual_std"] != bundle["residual_std"]
    np.testing.assert_allclose(
        recalibrated_bundle["regressor"].predict(test[features]),
        bundle["regressor"].predict(test[features]),
        rtol=0,
        atol=1e-12,
    )
    assert len(train) + len(calibration) + len(test) == len(frame)
