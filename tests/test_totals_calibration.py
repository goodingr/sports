from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.models.totals_calibration import calibration_residual_scale, split_totals_dates


def _frame(days=10, rows_per_day=10):
    return pd.DataFrame(
        [
            {
                "game_id": f"day_{day}_game_{game}",
                "start_time_utc": pd.Timestamp("2026-01-01T12:00:00Z") + pd.Timedelta(days=day),
                "actual_total": 200.0 + game,
            }
            for day in range(days)
            for game in range(rows_per_day)
        ]
    )


def _utc_dates(frame):
    return set(pd.to_datetime(frame["start_time_utc"], utc=True, format="mixed").dt.date)


def test_split_keeps_utc_dates_together_and_preserves_duplicate_indices():
    frame = _frame().sample(frac=1, random_state=42)
    frame.index = [0] * len(frame)
    original = frame.copy(deep=True)
    train, calibration, test = split_totals_dates(frame)

    assert (len(train), len(calibration), len(test)) == (60, 20, 20)
    assert (len(_utc_dates(train)), len(_utc_dates(calibration)), len(_utc_dates(test))) == (
        6,
        2,
        2,
    )
    assert _utc_dates(train).isdisjoint(_utc_dates(calibration))
    assert _utc_dates(calibration).isdisjoint(_utc_dates(test))
    assert train["start_time_utc"].max() < calibration["start_time_utc"].min()
    assert calibration["start_time_utc"].max() < test["start_time_utc"].min()
    assert all(
        part["start_time_utc"].is_monotonic_increasing for part in (train, calibration, test)
    )
    assert len(set(pd.concat([train, calibration, test])["game_id"])) == len(frame)
    pd.testing.assert_frame_equal(frame, original)
    train.iloc[0, train.columns.get_loc("actual_total")] = -1
    pd.testing.assert_frame_equal(frame, original)


def test_split_groups_mixed_offsets_by_their_utc_dates():
    frame = _frame(days=3)
    frame["start_time_utc"] = [
        f"2026-01-0{1 + day}T22:30:00-05:00" if game % 2 else f"2026-01-0{2 + day}T03:30:00+00:00"
        for day in range(3)
        for game in range(10)
    ]
    parts = split_totals_dates(frame)
    assert [len(part) for part in parts] == [10, 10, 10]
    assert [_utc_dates(part) for part in parts] == [
        {pd.Timestamp("2026-01-02").date()},
        {pd.Timestamp("2026-01-03").date()},
        {pd.Timestamp("2026-01-04").date()},
    ]


def test_purge_uses_first_actual_observation_and_removes_all_boundary_ties():
    frame = _frame()
    for day in (7, 9):
        frame.loc[frame["start_time_utc"].dt.day == day, "start_time_utc"] = pd.Timestamp(
            f"2026-01-{day:02d}T05:00:00Z"
        )
    extra = [
        {
            "game_id": f"{partition}_{label}_{copy}",
            "start_time_utc": pd.Timestamp(f"2026-01-{day:02d}T{time}:00Z"),
            "actual_total": 200,
        }
        for partition, day in (("train", 6), ("calibration", 8))
        for label, time in (("retained", "22:59"), ("boundary", "23:00"), ("purged", "23:30"))
        for copy in range(2)
    ]
    frame = pd.concat([frame, pd.DataFrame(extra)], ignore_index=True)
    train, calibration, test = split_totals_dates(frame)
    assert (len(train), len(calibration), len(test)) == (62, 22, 20)
    for name, part in (("train", train), ("calibration", calibration)):
        retained = set(part["game_id"])
        assert {f"{name}_retained_0", f"{name}_retained_1"} <= retained
        assert not any("boundary" in game_id or "purged" in game_id for game_id in retained)
    assert calibration["start_time_utc"].min() - train["start_time_utc"].max() > pd.Timedelta(
        hours=6
    )
    assert test["start_time_utc"].min() - calibration["start_time_utc"].max() > pd.Timedelta(
        hours=6
    )


def test_split_rejects_insufficient_dates_and_small_partitions():
    with pytest.raises(RuntimeError, match="at least three distinct UTC dates"):
        split_totals_dates(_frame(days=2, rows_per_day=100))
    with pytest.raises(RuntimeError, match="at least 10 rows in each partition"):
        split_totals_dates(_frame(days=3, rows_per_day=9))

    frame = _frame(days=3)
    frame.loc[frame["start_time_utc"].dt.day == 1, "start_time_utc"] = pd.Timestamp(
        "2026-01-01T20:00:00Z"
    )
    frame.loc[frame["start_time_utc"].dt.day == 2, "start_time_utc"] = pd.Timestamp(
        "2026-01-02T01:00:00Z"
    )
    with pytest.raises(RuntimeError, match="train=0, calibration=10, test=10"):
        split_totals_dates(frame)


@pytest.mark.parametrize("boundary", ["calibration", "test"])
def test_purge_uses_quote_time_when_prior_game_starts_before_next_game(boundary):
    frame = _frame(days=3)
    if boundary == "calibration":
        starts = ["2026-01-01T20:00:00Z", "2026-01-02T04:00:00Z", "2026-01-03T12:00:00Z"]
        expected_sizes = "train=0, calibration=10, test=10"
    else:
        starts = ["2026-01-01T12:00:00Z", "2026-01-02T20:00:00Z", "2026-01-03T04:00:00Z"]
        expected_sizes = "train=10, calibration=0, test=10"
    frame["start_time_utc"] = pd.to_datetime(np.repeat(starts, 10), utc=True)
    frame["snapshot_time_utc"] = frame["start_time_utc"] - pd.Timedelta(hours=12)

    with pytest.raises(RuntimeError, match=expected_sizes):
        split_totals_dates(frame)
    assert [len(part) for part in split_totals_dates(frame.drop(columns="snapshot_time_utc"))] == [
        10,
        10,
        10,
    ]


def test_purge_uses_earliest_partition_quote_and_keeps_earlier_training_rows():
    frame = _frame()
    frame["snapshot_time_utc"] = frame["start_time_utc"] - pd.Timedelta(hours=1)
    frame.loc[frame["start_time_utc"].dt.day == 7, "start_time_utc"] = pd.Timestamp(
        "2026-01-07T01:00:00Z"
    )
    frame.loc[frame["start_time_utc"].dt.day == 7, "snapshot_time_utc"] = pd.Timestamp(
        "2026-01-07T00:00:00Z"
    )
    frame.loc[frame["start_time_utc"].dt.day == 8, "snapshot_time_utc"] = pd.Timestamp(
        "2026-01-06T22:00:00Z"
    )
    for game_id, start in [("too_recent", "16:30"), ("boundary", "16:00"), ("safe", "15:59")]:
        frame.loc[len(frame)] = [
            game_id,
            pd.Timestamp(f"2026-01-06T{start}:00Z"),
            200.0,
            pd.Timestamp(f"2026-01-06T{start}:00Z") - pd.Timedelta(hours=1),
        ]
    train, calibration, test = split_totals_dates(frame.sample(frac=1, random_state=42))
    assert "safe" in set(train["game_id"])
    assert "too_recent" not in set(train["game_id"])
    assert "boundary" not in set(train["game_id"])
    assert train["start_time_utc"].max() + pd.Timedelta(hours=6) < min(
        calibration["snapshot_time_utc"].min(), test["snapshot_time_utc"].min()
    )
    assert (
        calibration["start_time_utc"].max() + pd.Timedelta(hours=6)
        < test["snapshot_time_utc"].min()
    )


@pytest.mark.parametrize("value", ["2026-01-01T11:00:00", None, "invalid"])
def test_split_rejects_missing_or_naive_snapshot_timestamps(value):
    frame = _frame()
    frame["snapshot_time_utc"] = (frame["start_time_utc"] - pd.Timedelta(hours=1)).astype(object)
    frame.loc[0, "snapshot_time_utc"] = value
    with pytest.raises(RuntimeError, match="snapshot_time_utc"):
        split_totals_dates(frame)


@pytest.mark.parametrize("lag", [0, 1])
def test_split_rejects_snapshots_at_or_after_start(lag):
    frame = _frame()
    frame["snapshot_time_utc"] = frame["start_time_utc"] - pd.Timedelta(hours=1)
    frame.loc[0, "snapshot_time_utc"] = frame.loc[0, "start_time_utc"] + pd.Timedelta(hours=lag)
    with pytest.raises(RuntimeError, match="snapshot_time_utc before start_time_utc"):
        split_totals_dates(frame)


@pytest.mark.parametrize("value", ["2026-01-01T12:00:00", None, "invalid"])
def test_split_rejects_missing_or_naive_timestamps(value):
    frame = _frame().astype({"start_time_utc": object})
    frame.loc[0, "start_time_utc"] = value
    with pytest.raises(RuntimeError, match="start_time_utc"):
        split_totals_dates(frame)


def test_scale_is_rms_of_supplied_calibration_errors_including_bias():
    actual = np.array([103.0, 96.0])
    forecast = np.array([100.0, 100.0])
    assert calibration_residual_scale(actual, forecast) == pytest.approx(np.sqrt(12.5))
    assert calibration_residual_scale(np.array([102.0] * 10), np.array([100.0] * 10)) == 2.0
    np.testing.assert_array_equal(actual, [103.0, 96.0])
    np.testing.assert_array_equal(forecast, [100.0, 100.0])


@pytest.mark.parametrize(
    ("actual", "forecast"),
    [
        ([], []),
        ([1.0, 2.0], [1.0]),
        ([[1.0, 2.0]], [[1.0, 2.0]]),
        ([np.nan], [0.0]),
        ([1.0], [np.inf]),
        ([1.0], [1.0]),
        ([1e308], [-1e308]),
    ],
)
def test_scale_rejects_unidentifiable_or_nonfinite_errors(actual, forecast):
    with pytest.raises(RuntimeError, match="Calibration"):
        calibration_residual_scale(np.asarray(actual), np.asarray(forecast))


def test_scale_does_not_overflow_for_large_finite_rms():
    assert calibration_residual_scale(np.array([1e200, -1e200]), np.zeros(2)) == pytest.approx(
        1e200
    )
