import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from src.predict import paper_entry_poll as entry

NOW = datetime(2026, 11, 7, 21, 44, tzinfo=timezone.utc)


def _clock(monkeypatch, *values):
    clocks = iter(values)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(clocks).astimezone(tz)

    monkeypatch.setattr(entry, "datetime", Clock)


def _capture_fixture(quoted, predictions, counts, monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger=entry.LOGGER.name)
    trial = entry.load_trial()
    engine = SimpleNamespace(
        totals_model_sha256=trial.model_sha256,
        predict_pinned_totals=Mock(return_value=predictions),
    )
    ledger = Mock(return_value=counts)
    record_failure = Mock()
    monkeypatch.setattr(entry, "capture_predictions", ledger)
    monkeypatch.setattr(entry, "record_capture_failure", record_failure)
    due = {
        game: entry._ClosingFixture(game, 1, 1, 2, 1, start, start)
        for game, start in quoted.items()
    }
    return trial, engine, ledger, record_failure, due


def _trace(caplog):
    prefix = "Paper entry inference "
    [message] = [record.message for record in caplog.records if record.message.startswith(prefix)]
    return json.loads(message.removeprefix(prefix))


@pytest.mark.parametrize("late", [True, False], ids=["deadline-crossed", "wrong-snapshot"])
def test_late_and_wrong_snapshot_are_distinct_when_ledger_has_same_missing_reason(
    late, monkeypatch, caplog
):
    post = NOW + timedelta(minutes=2 if late else 0)
    _clock(monkeypatch, NOW, post)
    quoted = {"fixture": NOW + timedelta(minutes=16)}
    forecasts = pd.DataFrame([{
        "game_id": "fixture",
        "selected_totals_snapshot_id": "target" if late else "another",
        "over_predicted_prob": 0.987654,
    }])
    trial, engine, ledger, failure, due = _capture_fixture(
        quoted, forecasts, {"prediction_missing": 1}, monkeypatch, caplog
    )
    assert entry._capture(Path("unused.db"), trial, engine, due, quoted, {"fixture": "target"}) == 0
    assert ledger.call_args.args[1].empty
    failure.assert_not_called()
    trace = _trace(caplog)
    assert trace["target_snapshot_ids"] == ["target"]
    assert trace["pre_inference_checked_at"] == NOW.isoformat()
    assert trace["post_inference_checked_at"] == post.isoformat()
    assert trace["post_inference_cutoff_rows"] == int(late)
    assert trace["wrong_snapshot_rows"] == int(not late)
    assert trace["wrong_snapshot_ids"] == ([] if late else ["another"])
    assert trace["ledger_outcomes"] == {"prediction_missing": 1}
    assert "0.987654" not in caplog.text


def test_trace_separates_pre_inference_cutoff_and_missing_engine_fixture(monkeypatch, caplog):
    _clock(monkeypatch, NOW, NOW)
    quoted = {
        "too_late": NOW + timedelta(minutes=14),
        "missing": NOW + timedelta(minutes=16),
        "available": NOW + timedelta(minutes=30),
    }
    forecasts = pd.DataFrame([{"game_id": "available", "selected_totals_snapshot_id": "target"}])
    outcomes = {"below_edge_threshold": 1, "prediction_missing": 2}
    trial, engine, ledger, failure, due = _capture_fixture(
        quoted, forecasts, outcomes, monkeypatch, caplog
    )
    snapshots = {"too_late": "earlier", "missing": "target", "available": "target"}
    assert entry._capture(Path("unused.db"), trial, engine, due, quoted, snapshots) == 0
    assert engine.predict_pinned_totals.call_args.kwargs["fixture_ids"] == {"available", "missing"}
    assert ledger.call_args.args[1]["game_id"].tolist() == ["available"]
    failure.assert_not_called()
    trace = _trace(caplog)
    assert trace["target_snapshot_ids"] == ["earlier", "target"]
    assert trace["quoted_fixtures"] == 3
    assert trace["eligible_fixtures"] == 2
    assert trace["pre_inference_cutoff_fixtures"] == 1
    assert trace["engine_missing_fixtures"] == 1
    assert trace["post_inference_cutoff_rows"] == trace["wrong_snapshot_rows"] == 0
    assert trace["ledger_outcomes"] == outcomes


@pytest.mark.parametrize("eligible", [True, False], ids=["empty-inference", "no-eligible-fixture"])
def test_empty_projection_keeps_original_exit_behavior(eligible, monkeypatch, caplog):
    _clock(monkeypatch, NOW)
    quoted = {"fixture": NOW + timedelta(minutes=16 if eligible else 14)}
    trial, engine, ledger, failure, due = _capture_fixture(
        quoted, None, {"prediction_missing": 1}, monkeypatch, caplog
    )
    args = (Path("unused.db"), trial, engine, due, quoted, {"fixture": "target"})
    if eligible:
        with pytest.raises(RuntimeError, match="produced no predictions"):
            entry._capture(*args)
    else:
        assert entry._capture(*args) == 0
    assert engine.predict_pinned_totals.called == eligible
    assert ledger.call_args.args[1] is None
    failure.assert_not_called()
    trace = _trace(caplog)
    assert trace["pre_inference_cutoff_fixtures"] == int(not eligible)
    assert trace["engine_missing_fixtures"] == int(eligible)
    assert trace["post_inference_checked_at"] is None
