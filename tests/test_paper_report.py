from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from src.predict.paper_report import build_paper_report
from src.predict.paper_settlement_history import ensure_settlement_history

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


def _revise(ledger, game_id, at, *, outcome, profit, clv=0.5, home_score=110):
    ensure_settlement_history(ledger)
    ledger.execute(
        """
        INSERT INTO paper_settlement_history (
            trial_id, game_id, settled_at, outcome, profit_units, home_score, clv_points
        ) VALUES ('totals', ?, ?, ?, ?, ?, ?)
        """,
        (game_id, at.isoformat(), outcome, profit, home_score, clv),
    )
    if outcome == "pending":
        ledger.execute(
            "DELETE FROM paper_settlements WHERE trial_id = 'totals' AND game_id = ?",
            (game_id,),
        )
    else:
        ledger.execute(
            """
            UPDATE paper_settlements SET settled_at = ?, outcome = ?, profit_units = ?, clv_points = ?
            WHERE trial_id = 'totals' AND game_id = ?
            """,
            (at.isoformat(), outcome, profit, clv, game_id),
        )


def test_observed_report_uses_the_grade_known_then_instead_of_latest_current_grade(ledger):
    _record(ledger, "corrected", settled_at=REVIEW_AT - timedelta(days=1))
    correction_at = REVIEW_AT + timedelta(days=1)
    _revise(ledger, "corrected", correction_at, outcome="loss", profit=-1.0, clv=-0.5)
    before = _trial(ledger, correction_at - timedelta(microseconds=1))
    corrected = _trial(ledger, correction_at)
    assert before["metrics"]["wins"] == corrected["metrics"]["losses"] == 1
    assert before["metrics"]["roi"] == 1.0
    assert corrected["metrics"]["roi"] == -1.0
    assert before["metrics"]["model_brier"] == pytest.approx(0.09)
    assert corrected["metrics"]["model_brier"] == pytest.approx(0.49)
    assert before["metrics"]["avg_clv_points"] == 0.5
    assert corrected["metrics"]["avg_clv_points"] == -0.5
    assert before["late_grade_changes"] == corrected["late_grade_changes"] == 0


@pytest.mark.parametrize("correction_offset", [-1, 0, 1])
def test_final_review_includes_only_grade_revisions_at_or_before_cutoff(ledger, correction_offset):
    _record(ledger, "corrected", settled_at=REVIEW_AT)
    original = _trial(ledger, FINAL_REVIEW_AT)
    _revise(
        ledger,
        "corrected",
        FINAL_REVIEW_AT + timedelta(microseconds=correction_offset),
        outcome="loss",
        profit=-1.0,
    )
    completed = _trial(ledger, FINAL_REVIEW_AT + timedelta(days=1))
    if correction_offset <= 0:
        assert completed["metrics"]["losses"] == 1
        assert completed["metrics"]["roi"] == -1.0
        assert completed["late_grade_changes"] == 0
    else:
        assert completed["metrics"] == original["metrics"]
        assert completed["review_verdict"] == original["review_verdict"]
        assert completed["review_failures"] == original["review_failures"]
        assert completed["late_grade_changes"] == 1


def test_source_retraction_before_cutoff_returns_decision_to_pending(ledger):
    _record(ledger, "withdrawn", settled_at=REVIEW_AT)
    withdrawn_at = FINAL_REVIEW_AT - timedelta(hours=1)
    _revise(ledger, "withdrawn", withdrawn_at, outcome="pending", profit=0, clv=None)
    assert _trial(ledger, withdrawn_at - timedelta(microseconds=1))["metrics"]["wins"] == 1
    withdrawn = _trial(ledger, FINAL_REVIEW_AT)
    assert withdrawn["metrics"]["pending"] == withdrawn["metrics"]["decisions"] == 1
    assert withdrawn["metrics"]["graded_decisions"] == 0
    assert withdrawn["metrics"]["profit_count"] == 0
    assert withdrawn["metrics"]["clv_count"] == withdrawn["metrics"]["brier_count"] == 0
    assert withdrawn["metrics"]["roi"] is None
    assert "unresolved_decisions" in withdrawn["review_failures"]
    assert withdrawn["late_grade_changes"] == 0


def test_late_source_retraction_flags_frozen_evidence_without_rewriting_metrics(ledger):
    _record(ledger, "withdrawn", settled_at=REVIEW_AT)
    original = _trial(ledger, FINAL_REVIEW_AT)
    withdrawn_at = FINAL_REVIEW_AT + timedelta(hours=1)
    _revise(ledger, "withdrawn", withdrawn_at, outcome="pending", profit=0, clv=None)
    report = _trial(ledger, withdrawn_at)
    assert report["metrics"] == original["metrics"]
    assert report["late_grade_changes"] == 1


def test_harmless_score_revision_does_not_flag_a_material_evidence_change(ledger):
    _record(ledger, "same-grade", settled_at=REVIEW_AT)
    original = _trial(ledger, FINAL_REVIEW_AT)
    revised_at = FINAL_REVIEW_AT + timedelta(hours=1)
    _revise(ledger, "same-grade", revised_at, outcome="win", profit=1.0, home_score=120)
    report = _trial(ledger, revised_at)
    assert report["metrics"] == original["metrics"]
    assert report["late_grade_changes"] == 0


@pytest.mark.parametrize(("profit", "clv"), [(0.8, 0.5), (1.0, -0.5)])
def test_late_profit_or_clv_correction_flags_material_change_with_unchanged_outcome(
    ledger, profit, clv
):
    _record(ledger, "same-outcome", settled_at=REVIEW_AT)
    original = _trial(ledger, FINAL_REVIEW_AT)
    revised_at = FINAL_REVIEW_AT + timedelta(hours=1)
    _revise(ledger, "same-outcome", revised_at, outcome="win", profit=profit, clv=clv)
    report = _trial(ledger, revised_at)
    assert report["metrics"] == original["metrics"]
    assert report["late_grade_changes"] == 1


def test_latest_grade_reverted_to_frozen_evidence_clears_material_change_count(ledger):
    _record(ledger, "reverted", settled_at=REVIEW_AT)
    original = _trial(ledger, FINAL_REVIEW_AT)
    loss_at = FINAL_REVIEW_AT + timedelta(hours=1)
    _revise(ledger, "reverted", loss_at, outcome="loss", profit=-1.0)
    assert _trial(ledger, loss_at)["late_grade_changes"] == 1
    reverted_at = loss_at + timedelta(hours=1)
    _revise(ledger, "reverted", reverted_at, outcome="win", profit=1.0)
    report = _trial(ledger, reverted_at)
    assert report["metrics"] == original["metrics"]
    assert report["late_grade_changes"] == 0


def test_late_changes_ignore_decisions_captured_at_trial_deadline(ledger):
    _record(ledger, "late-decision", captured_at=REVIEW_AT)
    revised_at = FINAL_REVIEW_AT + timedelta(hours=1)
    _revise(ledger, "late-decision", revised_at, outcome="loss", profit=-1.0)
    report = _trial(ledger, revised_at)
    assert report["metrics"]["decisions"] == 0
    assert report["late_grade_changes"] == 0
