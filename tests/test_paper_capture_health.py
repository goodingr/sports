"""Check the daily capture independently of supplemental task exit codes."""

import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.paper_capture_health import assess_capture_health
from src.predict.paper_ledger import load_trial

ROOT = Path(__file__).resolve().parents[1]
TICK = datetime.fromisoformat("2026-11-07T18:04:00-05:00")
FETCHED = TICK + timedelta(minutes=2)
CAPTURED = FETCHED + timedelta(seconds=4)
AFTER_GRACE = TICK + timedelta(minutes=31)
START = TICK + timedelta(hours=2)
REGISTERED = TICK.replace(hour=0, minute=30)


@pytest.fixture
def warehouse():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE paper_trials (trial_id, registered_at, specification);
        CREATE TABLE paper_capture_runs (run_id INTEGER PRIMARY KEY, trial_id, observed_at, status);
        CREATE TABLE paper_fixture_observations (run_id, game_id, outcome, snapshot_id);
        CREATE TABLE sports (sport_id, league);
        CREATE TABLE games (game_id, sport_id, odds_api_id, home_team_id, away_team_id);
        CREATE TABLE paper_fixture_schedule (
            trial_id, game_id, observed_at, start_time_utc, home_team_id, away_team_id
        );
        CREATE TABLE odds_snapshots (snapshot_id, sport_id, source, fetched_at_utc, raw_path);
        INSERT INTO sports VALUES (1, 'NBA');
        INSERT INTO games VALUES ('game', 1, 'nba-event', 1, 2);
        """
    )
    trial = load_trial(ROOT / "config/paper_trial.yml")
    conn.execute(
        "INSERT INTO paper_trials VALUES (?, ?, ?)",
        (trial.trial_id, REGISTERED.isoformat(), trial.specification),
    )
    yield conn, trial
    conn.close()


def _event(event_id="nba-event", start=START):
    return {"id": event_id, "sport_key": "basketball_nba", "commence_time": start.isoformat()}


def _archive(raw_dir, *, events=None, fetched=FETCHED, changes=None):
    payload = {
        "fetched_at": fetched.isoformat(),
        "request": {
            "url": "https://api.the-odds-api.com/v4/sports/basketball_nba/odds",
            "params": {"regions": "us", "markets": "h2h,totals", "oddsFormat": "american"},
        },
        "results": [_event()] if events is None else events,
    }
    if changes:
        changes(payload)
    path = raw_dir / fetched.astimezone(timezone.utc).strftime("odds_%Y-%m-%dT%H-%M-%SZ.json")
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _known(
    warehouse, *, game_id="game", seen=FETCHED - timedelta(hours=1), start=START, teams=(1, 2)
):
    conn, trial = warehouse
    conn.execute(
        "INSERT INTO paper_fixture_schedule VALUES (?, ?, ?, ?, ?, ?)",
        (trial.trial_id, game_id, seen.isoformat(), start.isoformat(), *teams),
    )


def _run(warehouse, *, at=CAPTURED, status="evaluated"):
    conn, trial = warehouse
    return conn.execute(
        "INSERT INTO paper_capture_runs (trial_id, observed_at, status) VALUES (?, ?, ?)",
        (trial.trial_id, at.isoformat(), status),
    ).lastrowid


def _observe(
    warehouse, path, run_id, *, source="the-odds-api", fetched=FETCHED,
    outcome="below_edge_threshold", filename=None, snapshot_id="observed", game_id="game",
):
    conn, _ = warehouse
    conn.execute(
        "INSERT INTO odds_snapshots VALUES (?, 1, ?, ?, ?)",
        (snapshot_id, source, fetched.isoformat(), rf"C:\original\archive\{filename or path.name}"),
    )
    conn.execute(
        "INSERT INTO paper_fixture_observations VALUES (?, ?, ?, ?)",
        (run_id, game_id, outcome, snapshot_id),
    )


def _assess(warehouse, raw_dir, *, now=AFTER_GRACE, **kwargs):
    conn, trial = warehouse
    return assess_capture_health(conn, trial, raw_dir, now, **kwargs)


@pytest.mark.parametrize("status", ["failed", "evaluated", "no_predictions"])
def test_unrelated_later_run_cannot_hide_verified_daily_capture(warehouse, tmp_path, status):
    path = _archive(tmp_path)
    _observe(warehouse, path, _run(warehouse))
    later_at = CAPTURED + timedelta(minutes=4)
    later_id = _run(warehouse, at=later_at, status=status)
    if status == "evaluated":
        _observe(
            warehouse, path, later_id, fetched=later_at - timedelta(seconds=2),
            filename="paper_entry.json", snapshot_id="later",
        )

    result = _assess(warehouse, tmp_path)

    assert result["status"] == "healthy"
    assert datetime.fromisoformat(result["capture_at"]) == CAPTURED
    assert datetime.fromisoformat(result["latest_attempt_at"]) == later_at
    assert result["latest_attempt_status"] == status


def test_two_partial_captures_do_not_prove_one_complete_daily_run(warehouse, tmp_path):
    conn, _ = warehouse
    conn.execute("INSERT INTO games VALUES ('other-game', 1, 'other-event', 3, 4)")
    path = _archive(tmp_path, events=[_event(), _event(event_id="other-event")])
    _observe(warehouse, path, _run(warehouse))
    _observe(
        warehouse, path, _run(warehouse, at=CAPTURED + timedelta(seconds=1)),
        snapshot_id="second", game_id="other-game",
    )

    result = _assess(warehouse, tmp_path)

    assert result["status"] == "capture_incomplete"
    assert result["evaluated_fixtures"] == 1


def test_first_daily_tick_waits_until_grace_elapses(warehouse, tmp_path):
    assert _assess(warehouse, tmp_path, now=TICK + timedelta(minutes=29))["status"] == "waiting"
    elapsed = _assess(warehouse, tmp_path, now=TICK + timedelta(minutes=30))
    assert elapsed["status"] == "receipt_missing"
    assert elapsed["expected_at"] == "2026-11-07T23:04:00+00:00"


def test_clock_uses_eastern_daylight_time_and_last_elapsed_tick(warehouse, tmp_path):
    now = datetime.fromisoformat("2027-04-01T18:20:00-04:00")
    result = _assess(warehouse, tmp_path, now=now)
    assert result["expected_at"] == "2027-03-31T22:04:00+00:00"
    assert result["status"] == "receipt_missing"
    shorter_grace = _assess(warehouse, tmp_path, now=now, grace_minutes=15)
    assert shorter_grace["expected_at"] == "2027-04-01T22:04:00+00:00"


def test_exact_windows_trigger_accepts_a_receipt_just_before_nominal_clock(warehouse, tmp_path):
    exact_tick = TICK - timedelta(seconds=3)
    fetched = exact_tick + timedelta(seconds=1)
    path = _archive(tmp_path, fetched=fetched)
    _observe(warehouse, path, _run(warehouse, at=fetched + timedelta(seconds=1)), fetched=fetched)
    assert _assess(warehouse, tmp_path)["status"] == "receipt_missing"
    exact = _assess(warehouse, tmp_path, expected_time=exact_tick.time())
    assert exact["status"] == "healthy"
    assert exact["expected_at"] == "2026-11-07T23:03:57+00:00"


def test_registered_spec_mismatch_fails_closed(warehouse, tmp_path):
    conn, _ = warehouse
    conn.execute("UPDATE paper_trials SET specification = '{}'")
    assert _assess(warehouse, tmp_path)["status"] == "trial_mismatch"


def test_supplemental_archive_does_not_satisfy_daily_heartbeat(warehouse, tmp_path):
    supplemental = tmp_path / "paper_entry"
    supplemental.mkdir()
    _archive(supplemental)
    _run(warehouse)
    assert _assess(warehouse, tmp_path)["status"] == "receipt_missing"


@pytest.mark.parametrize("problem", ["wrong_route", "targeted", "wrong_market", "wrong_sport", "404"])
def test_wrong_or_failed_archive_is_not_an_empty_slate(warehouse, tmp_path, problem):
    def change(payload):
        if problem == "wrong_route":
            payload["request"]["url"] = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"
        elif problem == "targeted":
            payload["request"]["params"]["eventIds"] = "nba-event"
        elif problem == "wrong_market":
            payload["request"]["params"]["markets"] = "totals"
        elif problem == "wrong_sport":
            payload["results"][0]["sport_key"] = "basketball_ncaab"
        else:
            payload["error"] = "404 Not Found"
            payload["results"] = []

    _archive(tmp_path, changes=change)
    _run(warehouse)
    expected = "receipt_error" if problem == "404" else "receipt_invalid"
    assert _assess(warehouse, tmp_path)["status"] == expected


def test_eligible_fixture_requires_a_capture_after_receipt(warehouse, tmp_path):
    _archive(tmp_path)
    _run(warehouse, at=FETCHED - timedelta(microseconds=1))
    assert _assess(warehouse, tmp_path)["status"] == "capture_missing"
    _run(warehouse, status="failed")
    assert _assess(warehouse, tmp_path)["status"] == "capture_failed"


@pytest.mark.parametrize("events", [[], [_event(start=START + timedelta(hours=14))]])
def test_successful_receipt_without_an_eligible_slate_is_unverified(warehouse, tmp_path, events):
    _archive(tmp_path, events=events)
    _run(warehouse, status="no_predictions")
    result = _assess(warehouse, tmp_path)
    assert result["status"] == "empty_slate_unverified"
    assert result["eligible_fixtures"] == 0


def test_all_below_edge_is_healthy_with_exact_receipt_lineage(warehouse, tmp_path):
    path = _archive(tmp_path)
    _known(warehouse)
    _observe(warehouse, path, _run(warehouse))
    result = _assess(warehouse, tmp_path)
    assert result["status"] == "healthy"
    assert result["known_eligible_fixtures"] == 1
    assert result["known_missing_fixture_ids"] == result["known_unresolved_fixture_ids"] == []
    assert result["eligible_fixtures"] == result["evaluated_fixtures"] == result["below_edge_fixtures"] == 1


def test_evaluated_strict_subset_is_incomplete_when_known_fixture_is_omitted(warehouse, tmp_path):
    conn, _ = warehouse
    conn.execute("INSERT INTO games VALUES ('omitted', 1, 'missing-event', 3, 4)")
    _known(warehouse)
    _known(warehouse, game_id="omitted", teams=(3, 4))
    path = _archive(tmp_path)
    _observe(warehouse, path, _run(warehouse))
    result = _assess(warehouse, tmp_path)
    assert result["status"] == "capture_incomplete"
    assert result["known_eligible_fixtures"] == 2
    assert result["known_missing_fixture_ids"] == ["omitted"]
    assert result["known_unresolved_fixture_ids"] == []
    assert result["eligible_fixtures"] == result["evaluated_fixtures"] == 1


@pytest.mark.parametrize(
    ("seen", "lead", "missing"),
    [
        (REGISTERED - timedelta(microseconds=1), timedelta(hours=2), False),
        (REGISTERED, timedelta(hours=2), True),
        (FETCHED, timedelta(hours=2), True),
        (FETCHED + timedelta(microseconds=1), timedelta(hours=2), False),
        (REGISTERED, timedelta(minutes=15) - timedelta(microseconds=1), False),
        (REGISTERED, timedelta(minutes=15), True),
        (REGISTERED, timedelta(hours=12), True),
        (REGISTERED, timedelta(hours=12, microseconds=1), False),
    ],
)
def test_known_slate_uses_as_of_observation_and_registered_lead_bounds(
    warehouse, tmp_path, seen, lead, missing
):
    _archive(tmp_path, events=[])
    _run(warehouse, status="no_predictions")
    _known(warehouse, seen=seen, start=FETCHED + lead)
    result = _assess(warehouse, tmp_path)
    assert result["status"] == ("capture_incomplete" if missing else "empty_slate_unverified")
    assert result["known_missing_fixture_ids"] == (["game"] if missing else [])


@pytest.mark.parametrize("conflict", ["duplicate_id", "unknown_id", "changed_identity", "changed_start"])
def test_uncertain_known_fixture_is_incomplete_without_claiming_it_was_omitted(
    warehouse, tmp_path, conflict
):
    conn, _ = warehouse
    _known(warehouse)
    _archive(tmp_path, events=[])
    _run(warehouse, status="no_predictions")
    if conflict == "duplicate_id":
        conn.execute("INSERT INTO games VALUES ('duplicate', 1, 'nba-event', 3, 4)")
    elif conflict == "unknown_id":
        conn.execute("UPDATE games SET odds_api_id = NULL")
    elif conflict == "changed_identity":
        _known(warehouse, seen=FETCHED, teams=(3, 4))
    else:
        _known(warehouse, seen=FETCHED, start=START + timedelta(minutes=11))
    result = _assess(warehouse, tmp_path)
    assert result["status"] == "capture_incomplete"
    assert result["known_missing_fixture_ids"] == []
    assert result["known_unresolved_fixture_ids"] == ["game"]


def test_schedule_revision_after_receipt_does_not_change_known_slate(warehouse, tmp_path):
    _known(warehouse)
    _known(warehouse, seen=CAPTURED, start=START + timedelta(hours=1))
    path = _archive(tmp_path)
    _observe(warehouse, path, _run(warehouse))
    result = _assess(warehouse, tmp_path)
    assert result["status"] == "healthy"
    assert result["known_eligible_fixtures"] == 1


@pytest.mark.parametrize("problem", ["wrong_time", "wrong_source", "wrong_filename", "ambiguous", "unknown", "missing"])
def test_other_observations_cannot_mask_incomplete_capture(warehouse, tmp_path, problem):
    conn, _ = warehouse
    path = _archive(tmp_path)
    if problem == "ambiguous":
        conn.execute("INSERT INTO games VALUES ('duplicate', 1, 'nba-event', 3, 4)")
    elif problem == "unknown":
        conn.execute("UPDATE games SET odds_api_id = 'different-event'")
    _observe(
        warehouse, path, _run(warehouse),
        fetched=FETCHED - timedelta(seconds=1) if problem == "wrong_time" else FETCHED,
        source="other-source" if problem == "wrong_source" else "the-odds-api",
        filename="unrelated.json" if problem == "wrong_filename" else None,
        outcome="prediction_missing" if problem == "missing" else "below_edge_threshold",
    )
    result = _assess(warehouse, tmp_path)
    assert result["status"] == "capture_incomplete"
    assert result["evaluated_fixtures"] == 0


@pytest.mark.parametrize(
    ("mode", "exit_code", "status"),
    [
        ("healthy", 0, "healthy"),
        ("missing", 1, "receipt_missing"),
        ("empty", 2, "empty_slate_unverified"),
        ("known_omission", 1, "capture_incomplete"),
    ],
)
def test_cli_returns_json_exit_code_and_keeps_inputs_read_only(warehouse, tmp_path, mode, exit_code, status):
    conn, _ = warehouse
    raw_dir = tmp_path / "nba"
    raw_dir.mkdir()
    if mode == "healthy":
        path = _archive(raw_dir)
        _observe(warehouse, path, _run(warehouse))
    elif mode in {"empty", "known_omission"}:
        _archive(raw_dir, events=[])
        _run(warehouse, status="no_predictions")
        if mode == "known_omission":
            _known(warehouse)
    db = tmp_path / "warehouse.sqlite"
    conn.commit()
    with sqlite3.connect(db) as disk:
        conn.backup(disk)
    before = db.read_bytes()
    run = subprocess.run(
        [sys.executable, "-B", "-m", "scripts.paper_capture_health", "--db", str(db),
         "--raw-dir", str(raw_dir), "--as-of", AFTER_GRACE.isoformat()],
        cwd=ROOT, capture_output=True, text=True, timeout=30, check=False,
    )
    assert run.returncode == exit_code, run.stderr
    assert json.loads(run.stdout)["status"] == status
    assert db.read_bytes() == before
