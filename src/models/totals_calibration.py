"""Chronological partitions and held-out uncertainty for totals regressors."""

from __future__ import annotations

import numpy as np
import pandas as pd

_MIN_PARTITION_ROWS = 10
_RESULT_LAG = pd.Timedelta(hours=6)


def _aware_utc_timestamp(value: object, column: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Totals calibration requires valid {column} values") from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise RuntimeError(f"Totals calibration requires timezone-aware {column} values")
    return timestamp.tz_convert("UTC")


def split_totals_dates(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split UTC game dates about 60/20/20 and purge labels unavailable at later decisions.

    Frames without snapshot_time_utc use game starts as their decision-time boundary.
    """
    if "start_time_utc" not in frame.columns:
        raise RuntimeError("Totals calibration requires a start_time_utc column")
    starts = pd.DatetimeIndex(
        [_aware_utc_timestamp(value, "start_time_utc") for value in frame["start_time_utc"]]
    )
    order = np.argsort(starts.asi8, kind="stable")
    starts = starts.take(order)
    decisions = starts
    if "snapshot_time_utc" in frame.columns:
        decisions = pd.DatetimeIndex(
            [
                _aware_utc_timestamp(value, "snapshot_time_utc")
                for value in frame["snapshot_time_utc"]
            ]
        ).take(order)
        if not (decisions < starts).all():
            raise RuntimeError(
                "Totals calibration requires snapshot_time_utc before start_time_utc"
            )
    dates = starts.normalize()
    unique_dates = dates.unique()
    if len(unique_dates) < 3:
        raise RuntimeError("Totals calibration requires at least three distinct UTC dates")

    calibration_date_index = min(max(round(len(unique_dates) * 0.6), 1), len(unique_dates) - 2)
    test_date_index = min(
        max(round(len(unique_dates) * 0.8), calibration_date_index + 1), len(unique_dates) - 1
    )
    calibration_date = unique_dates[calibration_date_index]
    test_date = unique_dates[test_date_index]
    calibration_mask = (dates >= calibration_date) & (dates < test_date)
    test_mask = dates >= test_date
    first_calibration = decisions[calibration_mask].min()
    first_test = decisions[test_mask].min()

    train_boundary = min(first_calibration, first_test) - _RESULT_LAG
    train_mask = (dates < calibration_date) & (starts < train_boundary)
    calibration_mask &= starts < first_test - _RESULT_LAG
    train = frame.iloc[order[train_mask]].copy()
    calibration = frame.iloc[order[calibration_mask]].copy()
    test = frame.iloc[order[test_mask]].copy()
    sizes = (len(train), len(calibration), len(test))
    if min(sizes) < _MIN_PARTITION_ROWS:
        raise RuntimeError(
            "Totals calibration requires at least 10 rows in each partition after a 6-hour purge "
            f"(train={sizes[0]}, calibration={sizes[1]}, test={sizes[2]})"
        )
    return train, calibration, test


def calibration_residual_scale(actual: np.ndarray, forecast: np.ndarray) -> float:
    """Return RMS prediction error from the supplied held-out calibration observations."""
    actual_values = np.asarray(actual, dtype=float)
    forecast_values = np.asarray(forecast, dtype=float)
    if (
        actual_values.ndim != 1
        or actual_values.shape != forecast_values.shape
        or actual_values.size == 0
        or not np.isfinite(actual_values).all()
        or not np.isfinite(forecast_values).all()
    ):
        raise RuntimeError("Calibration requires equal nonempty finite one-dimensional arrays")
    with np.errstate(over="ignore", invalid="ignore"):
        residuals = actual_values - forecast_values
    if not np.isfinite(residuals).all():
        raise RuntimeError("Calibration residuals must be finite")
    magnitude = float(np.max(np.abs(residuals)))
    if magnitude == 0:
        raise RuntimeError("Calibration residual scale is zero and cannot identify uncertainty")
    scale = magnitude * float(np.sqrt(np.mean(np.square(residuals / magnitude))))
    if not np.isfinite(scale) or scale <= 0:
        raise RuntimeError("Calibration residual scale must be finite and strictly positive")
    return scale
