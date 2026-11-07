"""Prospective schedule coverage, including windows with no capture runs."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.predict.paper_schedule import known_window_coverage, record_known_fixtures

REGISTERED = datetime(2026, 11, 7, tzinfo=timezone.utc)
REVIEW = REGISTERED + timedelta(days=2)


@pytest.fixture
def ledger():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE sports (sport_id PRIMARY KEY, league);
        INSERT INTO sports VALUES (1, 'NBA'), (2, 'NHL');
        CREATE TABLE games (
            game_id PRIMARY KEY, sport_id, start_time_utc, home_team_id, away_team_id, status
        );
        CREATE TABLE game_results (game_id PRIMARY KEY, home_score, away_score);
        CREATE TABLE paper_fixture_schedule (
            trial_id, game_id, observed_at, start_time_utc, home_team_id, away_team_id,
            PRIMARY KEY (trial_id, game_id, start_time_utc, home_team_id, away_team_id)
        );
        CREATE TABLE paper_capture_runs (run_id INTEGER PRIMARY KEY, trial_id, observed_at);
        CREATE TABLE paper_fixture_observations (run_id, game_id, start_time_utc);
        """
    )
    yield conn
    conn.close()


def _known(ledger, game, start, *, seen=REGISTERED, home=1, away=2, trial="trial"):
    ledger.execute(
        "INSERT INTO paper_fixture_schedule VALUES (?, ?, ?, ?, ?, ?)",
        (trial, game, seen.isoformat(), start.isoformat(), home, away),
    )


def _coverage(ledger, asof, **kwargs):
    return known_window_coverage(ledger, "trial", REGISTERED, REVIEW, asof, **kwargs)


def _observe(ledger, game, start, seen):
    run = ledger.execute(
        "INSERT INTO paper_capture_runs (trial_id, observed_at) VALUES ('trial', ?)",
        (seen.isoformat(),),
    )
    ledger.execute(
        "INSERT INTO paper_fixture_observations VALUES (?, ?, ?)",
        (run.lastrowid, game, start.isoformat()),
    )


def test_record_known_schedules_filters_fixture_state_and_preserves_first_seen(ledger):
    start = REGISTERED + timedelta(hours=1)
    ledger.executemany(
        "INSERT INTO games VALUES (?, ?, ?, 1, 2, ?)",
        [
            ("known", 1, start.isoformat(), "scheduled"),
            ("past", 1, REGISTERED.isoformat(), "scheduled"),
            ("live", 1, start.isoformat(), "live"),
            ("scored", 1, start.isoformat(), "scheduled"),
            ("other-league", 2, start.isoformat(), "scheduled"),
            ("too-late", 1, (REVIEW + timedelta(hours=12)).isoformat(), "scheduled"),
        ],
    )
    ledger.execute("INSERT INTO game_results VALUES ('scored', NULL, 0)")
    record_known_fixtures(ledger, "trial", "NBA", REGISTERED, REVIEW)
    record_known_fixtures(ledger, "trial", "NBA", REGISTERED + timedelta(minutes=1), REVIEW)
    rows = ledger.execute("SELECT * FROM paper_fixture_schedule").fetchall()
    assert len(rows) == 1
    assert rows[0]["game_id"] == "known"
    assert rows[0]["observed_at"] == REGISTERED.isoformat()
    ledger.execute(
        "UPDATE games SET start_time_utc = ? WHERE game_id = 'known'",
        ((start + timedelta(minutes=5)).isoformat(),),
    )
    record_known_fixtures(ledger, "trial", "NBA", REGISTERED + timedelta(minutes=2), REVIEW)
    assert ledger.execute("SELECT COUNT(*) FROM paper_fixture_schedule").fetchone()[0] == 2


def test_known_future_becomes_a_missed_window_without_any_capture_run(ledger):
    start = REGISTERED + timedelta(hours=1)
    _known(ledger, "known", start)
    boundary = start - timedelta(minutes=15)
    assert _coverage(ledger, boundary)["known_future_fixtures"] == 1
    coverage = _coverage(ledger, boundary + timedelta(microseconds=1))
    assert coverage["known_fixtures"] == coverage["passed_window_fixtures"] == 1
    assert coverage["missed_known_window_fixtures"] == 1
    assert coverage["observed_passed_window_fixtures"] == 0
    assert coverage["known_future_fixtures"] == 0


def test_missing_capture_tables_still_allow_definitive_missed_window(ledger):
    ledger.execute("DROP TABLE paper_fixture_observations")
    ledger.execute("DROP TABLE paper_capture_runs")
    _known(ledger, "known", REGISTERED + timedelta(hours=1))
    assert _coverage(ledger, REGISTERED + timedelta(hours=2))["missed_known_window_fixtures"] == 1


def test_first_discovery_after_window_and_unavailable_schedule_observations_are_ignored(ledger):
    start = REGISTERED + timedelta(hours=1)
    _known(ledger, "late", start, seen=start - timedelta(minutes=14))
    _known(ledger, "before-registration", start, seen=REGISTERED - timedelta(seconds=1))
    _known(ledger, "future-observation", start, seen=REGISTERED + timedelta(minutes=10))
    _known(ledger, "other-trial", start, trial="other")
    assert _coverage(ledger, REGISTERED)["known_fixtures"] == 0
    assert _coverage(ledger, REGISTERED + timedelta(hours=2))["known_fixtures"] == 1


def test_any_in_window_observation_counts_even_if_no_decision_was_selected(ledger):
    start = REGISTERED + timedelta(hours=2)
    _known(ledger, "known", start)
    _observe(ledger, "known", start, REGISTERED + timedelta(minutes=30))
    _observe(ledger, "other", start, REGISTERED + timedelta(minutes=30))
    coverage = _coverage(ledger, start)
    assert coverage["passed_window_fixtures"] == coverage["observed_passed_window_fixtures"] == 1
    assert coverage["missed_known_window_fixtures"] == 0


@pytest.mark.parametrize("kind", ["rescheduled", "home-changed", "away-changed"])
def test_changed_schedules_are_ambiguous_instead_of_missed(ledger, kind):
    start = REGISTERED + timedelta(hours=2)
    _known(ledger, "known", start)
    changed_start = start + timedelta(minutes=11) if kind == "rescheduled" else start
    _known(
        ledger,
        "known",
        changed_start,
        seen=REGISTERED + timedelta(minutes=1),
        home=3 if kind == "home-changed" else 1,
        away=3 if kind == "away-changed" else 2,
    )
    coverage = _coverage(ledger, start + timedelta(hours=1))
    assert coverage["known_fixtures"] == coverage["ambiguous_schedule_fixtures"] == 1
    assert coverage["passed_window_fixtures"] == coverage["missed_known_window_fixtures"] == 0
    assert _coverage(ledger, REGISTERED)["ambiguous_schedule_fixtures"] == 0


def test_small_start_correction_uses_latest_known_schedule(ledger):
    start = REGISTERED + timedelta(hours=1)
    _known(ledger, "known", start)
    _known(ledger, "known", start + timedelta(minutes=10), seen=REGISTERED + timedelta(minutes=1))
    coverage = _coverage(ledger, start - timedelta(minutes=10))
    assert coverage["known_future_fixtures"] == 1
    assert coverage["ambiguous_schedule_fixtures"] == 0
    assert _coverage(ledger, start)["missed_known_window_fixtures"] == 1


def test_review_expires_remaining_known_windows_and_freezes_later_evidence(ledger):
    start = REVIEW + timedelta(hours=6)
    _known(ledger, "known", start)
    assert _coverage(ledger, REVIEW - timedelta(microseconds=1))["known_future_fixtures"] == 1
    at_review = _coverage(ledger, REVIEW)
    assert at_review["known_future_fixtures"] == 0
    assert at_review["missed_known_window_fixtures"] == 1
    _known(ledger, "known", start + timedelta(hours=1), seen=REVIEW + timedelta(seconds=1))
    _observe(ledger, "known", start, REVIEW + timedelta(seconds=1))
    assert _coverage(ledger, REVIEW + timedelta(days=30)) == at_review
    record_known_fixtures(ledger, "trial", "NBA", REVIEW, REVIEW)


def test_custom_window_bounds_are_used_for_recording_and_attempt_validation(ledger):
    start = REVIEW + timedelta(hours=18)
    ledger.execute("INSERT INTO games VALUES ('known', 1, ?, 1, 2, 'scheduled')", (start.isoformat(),))
    record_known_fixtures(ledger, "trial", "NBA", REGISTERED, REVIEW, max_lead_hours=24)
    _observe(ledger, "known", start, REVIEW - timedelta(hours=1))
    assert _coverage(ledger, REVIEW, max_lead_hours=24)["observed_passed_window_fixtures"] == 1
    assert _coverage(ledger, REVIEW, max_lead_hours=12)["missed_known_window_fixtures"] == 1

    other_start = REGISTERED + timedelta(hours=2)
    _known(ledger, "short-lead", other_start)
    _observe(ledger, "short-lead", other_start, other_start - timedelta(minutes=45))
    assert _coverage(ledger, other_start, min_lead_minutes=60)["missed_known_window_fixtures"] == 1


def test_missing_schedule_schema_is_distinct_from_no_known_fixtures(ledger):
    ledger.execute("DROP TABLE paper_fixture_schedule")
    assert _coverage(ledger, REGISTERED) == {"available": False}


def test_naive_observation_times_are_rejected(ledger):
    with pytest.raises(ValueError, match="time zone"):
        record_known_fixtures(ledger, "trial", "NBA", REGISTERED.replace(tzinfo=None), REVIEW)
    with pytest.raises(ValueError, match="time zone"):
        _coverage(ledger, REGISTERED.replace(tzinfo=None))
