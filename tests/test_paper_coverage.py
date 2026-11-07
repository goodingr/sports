from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from src.predict.paper_coverage import capture_coverage
from src.predict.paper_report import build_paper_report

REGISTERED_AT = datetime.fromisoformat("2026-11-07T03:30:00+00:00")
REVIEW_AT = datetime.fromisoformat("2027-02-01T05:00:00+00:00")


@pytest.fixture
def ledger():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE paper_capture_runs (run_id INTEGER PRIMARY KEY, trial_id, observed_at, status);
        CREATE TABLE paper_fixture_observations (
            run_id, game_id, start_time_utc, outcome, snapshot_id, PRIMARY KEY (run_id, game_id)
        );
        """
    )
    yield conn
    conn.close()


def _run(ledger, run_id, at, *, trial_id="nba", status="evaluated", fixtures=()):
    ledger.execute(
        "INSERT INTO paper_capture_runs VALUES (?, ?, ?, ?)",
        (run_id, trial_id, at.isoformat(), status),
    )
    ledger.executemany(
        "INSERT INTO paper_fixture_observations VALUES (?, ?, ?, ?, ?)",
        [
            (run_id, game_id, (at + timedelta(hours=1)).isoformat(), outcome, None)
            for game_id, outcome in fixtures
        ],
    )


def _coverage(ledger, observed_at=REVIEW_AT):
    coverage = capture_coverage(ledger, "nba", REGISTERED_AT, REVIEW_AT, observed_at)
    json.dumps(coverage, allow_nan=False)
    return coverage


def test_missing_coverage_schema_is_distinct_from_zero_observations():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        assert _coverage(conn) == {"available": False}
        conn.execute("CREATE TABLE paper_capture_runs (run_id)")
        assert _coverage(conn) == {"available": False}
    finally:
        conn.close()


def test_empty_coverage_has_no_verified_estimate(ledger):
    coverage = _coverage(ledger)
    assert coverage["available"] is True
    assert coverage["runs"] == coverage["observed_fixtures"] == 0
    assert coverage["fully_evaluated_share"] is None
    assert coverage["first_run_at"] is None
    assert coverage["first_fixture_observed_at"] is None
    assert coverage["last_run_at"] is None
    assert coverage["last_run_status"] is None
    assert coverage["attempts_by_outcome"] == {}


def test_coverage_keeps_run_attempts_and_deduplicates_fixture_evaluations(ledger):
    _run(ledger, 1, REGISTERED_AT, status="failed")
    _run(
        ledger,
        2,
        REGISTERED_AT + timedelta(minutes=1),
        fixtures=[
            ("a", "below_edge_threshold"),
            ("b", "stale_quote"),
            ("c", "prediction_missing"),
            ("d", "recorded"),
        ],
    )
    _run(
        ledger,
        3,
        REGISTERED_AT + timedelta(minutes=2),
        fixtures=[
            ("a", "below_edge_threshold"),
            ("b", "recorded"),
            ("c", "prediction_missing"),
            ("d", "already_recorded"),
        ],
    )
    _run(ledger, 4, REGISTERED_AT + timedelta(minutes=3), status="no_predictions")
    coverage = _coverage(ledger)
    assert coverage["runs"] == 4
    assert coverage["first_run_at"] == REGISTERED_AT.isoformat()
    assert (
        coverage["first_fixture_observed_at"] == (REGISTERED_AT + timedelta(minutes=1)).isoformat()
    )
    assert coverage["last_run_status"] == "no_predictions"
    assert coverage["observed_fixtures"] == 4
    assert coverage["fully_evaluated_fixtures"] == 3
    assert coverage["fully_evaluated_share"] == 0.75
    assert coverage["below_edge_fixtures"] == 1
    assert coverage["attempts_by_outcome"] == {
        "already_recorded": 1,
        "below_edge_threshold": 2,
        "prediction_missing": 2,
        "recorded": 2,
        "stale_quote": 1,
    }


def test_coverage_honors_registration_observation_and_strict_capture_cutoff(ledger):
    _run(ledger, 1, REGISTERED_AT - timedelta(microseconds=1))
    _run(ledger, 2, REGISTERED_AT)
    _run(ledger, 3, REGISTERED_AT + timedelta(microseconds=1))
    _run(ledger, 4, REVIEW_AT - timedelta(microseconds=1))
    _run(ledger, 5, REVIEW_AT)
    _run(ledger, 6, REVIEW_AT + timedelta(microseconds=1))
    _run(ledger, 7, REGISTERED_AT, trial_id="other")
    assert _coverage(ledger, REGISTERED_AT)["runs"] == 1
    coverage = _coverage(ledger)
    assert coverage["runs"] == 3
    assert coverage["last_run_at"] == (REVIEW_AT - timedelta(microseconds=1)).isoformat()
    assert _coverage(ledger, REVIEW_AT + timedelta(days=100)) == coverage


def test_last_run_uses_time_then_run_id_to_break_ties(ledger):
    _run(ledger, 1, REGISTERED_AT, status="no_predictions")
    _run(ledger, 2, REGISTERED_AT, status="failed")
    _run(ledger, 3, REGISTERED_AT - timedelta(seconds=1), status="evaluated")
    assert _coverage(ledger)["last_run_status"] == "failed"


def test_report_exposes_coverage_without_changing_trial_verdict(ledger):
    ledger.executescript(
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
    ledger.execute(
        "INSERT INTO paper_trials VALUES (?, ?, ?)",
        (
            "nba",
            REGISTERED_AT.isoformat(),
            json.dumps(
                {
                    "review_at": REVIEW_AT.isoformat(),
                    "min_decisions": 150,
                    "min_roi": 0.05,
                    "min_clv_coverage": 0.8,
                }
            ),
        ),
    )
    _run(ledger, 1, REGISTERED_AT, fixtures=[("a", "below_edge_threshold")])
    trial = build_paper_report(ledger, REGISTERED_AT)["trials"][0]
    assert trial["review_status"] == "collecting"
    assert trial["review_verdict"] is None
    assert trial["metrics"]["decisions"] == 0
    assert trial["capture_coverage"]["observed_fixtures"] == 1
    assert trial["capture_coverage"]["fully_evaluated_share"] == 1.0
