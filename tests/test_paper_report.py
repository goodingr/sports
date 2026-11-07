from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from src.predict.paper_report import build_paper_report

REVIEW_AT = datetime.fromisoformat("2027-05-31T00:00:00+00:00")
FINAL_REVIEW_AT = REVIEW_AT + timedelta(days=7)
CAPTURED_AT = REVIEW_AT - timedelta(days=4)
START_AT = CAPTURED_AT + timedelta(hours=1)


@pytest.fixture
def ledger():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE paper_trials (trial_id, registered_at, specification);
        CREATE TABLE paper_decisions (
            trial_id, game_id, start_time_utc, captured_at, model_probability, market_probability
        );
        CREATE TABLE paper_settlements (
            trial_id, game_id, settled_at, outcome, profit_units, clv_points
        );
        """
    )
    conn.execute(
        "INSERT INTO paper_trials VALUES (?, ?, ?)",
        (
            "totals",
            (CAPTURED_AT - timedelta(days=1)).isoformat(),
            json.dumps(
                {
                    "review_at": REVIEW_AT.isoformat(),
                    "min_decisions": 2,
                    "min_roi": 0.05,
                    "min_clv_coverage": 0.8,
                }
            ),
        ),
    )
    yield conn
    conn.close()


def _record(
    ledger,
    game_id,
    *,
    captured_at=CAPTURED_AT,
    start_at=START_AT,
    settled_at=REVIEW_AT,
    outcome="win",
    profit=1.0,
    clv=0.5,
):
    ledger.execute(
        "INSERT INTO paper_decisions VALUES (?, ?, ?, ?, ?, ?)",
        ("totals", game_id, start_at.isoformat(), captured_at.isoformat(), 0.7, 0.5),
    )
    if settled_at is not None:
        ledger.execute(
            "INSERT INTO paper_settlements VALUES (?, ?, ?, ?, ?, ?)",
            ("totals", game_id, settled_at.isoformat(), outcome, profit, clv),
        )


def _trial(ledger, observed_at):
    report = build_paper_report(ledger, observed_at)
    json.dumps(report, allow_nan=False)
    return report["trials"][0]


def test_counts_pushes_and_excludes_voids_from_evidence(ledger):
    _record(ledger, "win")
    _record(ledger, "push", outcome="push", profit=0.0, clv=0.0)
    _record(ledger, "loss", outcome="loss", profit=-1.0, clv=None)
    _record(ledger, "void", outcome="void", profit=None, clv=100.0)
    _record(ledger, "pending", settled_at=None)
    trial = _trial(ledger, REVIEW_AT)
    metrics = trial["metrics"]
    assert trial["review_status"] == "grading"
    assert trial["review_verdict"] is None
    assert metrics["decisions"] == 5
    assert metrics["graded_decisions"] == 3
    assert metrics["pending"] == metrics["wins"] == metrics["losses"] == 1
    assert metrics["pushes"] == metrics["voids"] == 1
    assert metrics["roi"] == 0.0
    assert metrics["brier_count"] == metrics["clv_count"] == 2
    assert metrics["model_brier"] == pytest.approx(0.29)
    assert metrics["market_brier"] == pytest.approx(0.25)
    assert metrics["clv_coverage"] == pytest.approx(2 / 3)
    assert metrics["avg_clv_points"] == pytest.approx(0.25)
    assert metrics["clv_win_rate"] == pytest.approx(0.5)
    assert metrics["bootstrap_roi_interval"] is None


def test_review_excludes_deadline_captures_and_settlements_after_grace(ledger):
    _record(ledger, "on_time")
    _record(
        ledger,
        "settled_late",
        captured_at=REVIEW_AT - timedelta(microseconds=1),
        start_at=REVIEW_AT + timedelta(hours=1),
        settled_at=FINAL_REVIEW_AT + timedelta(microseconds=1),
    )
    _record(ledger, "capture_at_deadline", captured_at=REVIEW_AT, settled_at=None)
    _record(ledger, "capture_after_deadline", captured_at=REVIEW_AT + timedelta(seconds=1))
    before_deadline = _trial(ledger, REVIEW_AT - timedelta(seconds=1))
    assert before_deadline["review_status"] == "collecting"
    assert before_deadline["metrics"]["pending"] == 1
    at_deadline = _trial(ledger, REVIEW_AT)
    assert at_deadline["review_status"] == "grading"
    assert at_deadline["metrics"]["decisions"] == 2
    assert at_deadline["metrics"]["pending"] == 1
    for observed_at in (FINAL_REVIEW_AT, FINAL_REVIEW_AT + timedelta(days=1)):
        completed = _trial(ledger, observed_at)
        assert completed["review_verdict"] == "fail"
        assert "unresolved_decisions" in completed["review_failures"]
        assert completed["metrics"] == at_deadline["metrics"]


def test_complete_review_reports_seeded_date_cluster_interval(ledger):
    _record(ledger, "first", profit=1.0)
    _record(ledger, "second", profit=1.0)
    _record(ledger, "third", start_at=START_AT + timedelta(days=1), profit=0.5)
    trial = _trial(ledger, FINAL_REVIEW_AT)
    assert trial["review_status"] == "complete"
    assert trial["review_verdict"] == "pass"
    assert trial["review_failures"] == []
    assert trial["metrics"]["bootstrap_utc_dates"] == 2
    assert trial["metrics"]["roi"] == pytest.approx(5 / 6)
    assert trial["metrics"]["bootstrap_roi_interval"] == {
        "low": 0.5,
        "median": pytest.approx(5 / 6),
        "high": 1.0,
    }


def test_empty_trial_has_no_estimates(ledger):
    trial = _trial(ledger, FINAL_REVIEW_AT)
    assert trial["review_verdict"] == "fail"
    assert trial["metrics"]["decisions"] == 0
    assert trial["metrics"]["profit_units"] == 0.0
    for key in ("roi", "bootstrap_roi_interval", "model_brier", "clv_coverage"):
        assert trial["metrics"][key] is None
