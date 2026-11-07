import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.db.core import SCHEMA_PATH
from src.predict.paper_settlement import settle_open_trials, unreconciled_grades
from src.predict.paper_settlement_history import grades_as_of

START = datetime(2026, 11, 7, 2, tzinfo=timezone.utc)
CAPTURE = START - timedelta(hours=2)


@pytest.fixture
def ledger():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.executescript(
        """
        CREATE TABLE paper_decisions (
            trial_id TEXT, game_id TEXT, start_time_utc TEXT, captured_at TEXT,
            odds_source TEXT, book_id INTEGER, side TEXT, line REAL, price REAL,
            PRIMARY KEY (trial_id, game_id)
        );
        CREATE TABLE paper_settlements (
            trial_id TEXT, game_id TEXT, settled_at TEXT, outcome TEXT, profit_units REAL,
            home_score INTEGER, away_score INTEGER, close_snapshot_id TEXT, close_fetched_at TEXT,
            close_line REAL, close_price REAL, clv_points REAL, PRIMARY KEY (trial_id, game_id)
        );
        INSERT INTO sports (sport_id, name, league, default_market) VALUES (1, 'Basketball', 'NBA', 'totals');
        INSERT INTO books (book_id, name) VALUES (1, 'DraftKings'), (2, 'Other');
        """
    )
    try:
        yield conn
    finally:
        conn.close()


def _decision(
    conn, game, *, side="over", line=220.5, status="final", scores=(116, 107), start=START
):
    conn.execute(
        """
        INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, status)
        VALUES (?, 1, ?, 1, 2, ?)
        """,
        (game, start.isoformat(), status),
    )
    conn.execute(
        "INSERT INTO game_results (game_id, home_score, away_score) VALUES (?, ?, ?)",
        (game, *scores),
    )
    conn.execute(
        """
        INSERT INTO paper_decisions (
            trial_id, game_id, start_time_utc, captured_at, odds_source, book_id, side, line, price
        ) VALUES ('pilot', ?, ?, ?, 'the-odds-api', 1, ?, ?, -110)
        """,
        (game, START.isoformat(), CAPTURE.isoformat(), side, line),
    )


def _quote(
    conn,
    game,
    snapshot,
    offset,
    *,
    book=1,
    source="the-odds-api",
    market="totals",
    lines=(222, 222),
):
    fetched = START + offset
    conn.execute(
        "INSERT INTO odds_snapshots (snapshot_id, fetched_at_utc, sport_id, source) VALUES (?, ?, 1, ?)",
        (snapshot, fetched.isoformat(), source),
    )
    conn.executemany(
        """
        INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (snapshot, game, book, market, side, price, line)
            for (side, price), line in zip((("Over", -108), ("Under", -112)), lines)
        ],
    )


def test_grades_original_price_and_keeps_latest_verified_close(ledger):
    _decision(ledger, "game", start=START.astimezone(timezone(timedelta(hours=-4))))
    _quote(ledger, "game", "valid", timedelta(minutes=-10))
    _quote(ledger, "game", "older", timedelta(minutes=-20), lines=(221.5, 221.5))
    _quote(ledger, "game", "wrong_book", timedelta(minutes=-1), book=2)
    _quote(ledger, "game", "wrong_source", timedelta(minutes=-2), source="other")
    _quote(ledger, "game", "wrong_market", timedelta(minutes=-3), market="alternate_totals")
    _quote(ledger, "game", "mismatched_pair", timedelta(minutes=-4), lines=(221.5, 222.5))
    _quote(ledger, "game", "at_start", timedelta())
    _quote(ledger, "game", "in_play", timedelta(minutes=1))
    assert settle_open_trials(ledger, START) == 0
    assert settle_open_trials(ledger, START + timedelta(hours=3)) == 1
    grade = ledger.execute("SELECT * FROM paper_settlements").fetchone()
    assert grade["outcome"] == "win"
    assert grade["profit_units"] == pytest.approx(100 / 110)
    assert grade["close_snapshot_id"] == "valid"
    assert grade["close_line"] == 222
    assert grade["close_price"] == -108
    assert grade["clv_points"] == 1.5
    ledger.execute("UPDATE game_results SET home_score = 90 WHERE game_id = 'game'")
    assert unreconciled_grades(ledger, "pilot", START + timedelta(days=1)) == 1
    assert settle_open_trials(ledger, START + timedelta(days=1)) == 1
    corrected = ledger.execute("SELECT * FROM paper_settlements").fetchone()
    assert corrected["home_score"] == 90
    assert corrected["outcome"] == "loss"
    assert corrected["profit_units"] == -1.0
    assert corrected["close_snapshot_id"] == "valid"
    assert settle_open_trials(ledger, START + timedelta(days=2)) == 0
    history = ledger.execute(
        "SELECT outcome, home_score FROM paper_settlement_history ORDER BY revision_id"
    ).fetchall()
    assert [tuple(row) for row in history] == [("win", 116), ("loss", 90)]
    assert grades_as_of(ledger, "pilot", START + timedelta(hours=3))["game"]["outcome"] == "win"
    assert grades_as_of(ledger, "pilot", START + timedelta(days=1))["game"]["outcome"] == "loss"
    assert unreconciled_grades(ledger, "pilot", START + timedelta(days=2)) == 0


def test_push_loss_void_and_pending_are_distinct(ledger):
    _decision(ledger, "push", line=223)
    _quote(ledger, "push", "outside_window", timedelta(minutes=-91))
    _decision(ledger, "loss", side="under")
    _quote(ledger, "loss", "under_close", timedelta(minutes=-8), lines=(220, 220))
    _decision(ledger, "canceled", status="cancelled", scores=(None, None))
    _decision(ledger, "rescheduled", start=START + timedelta(days=1))
    _decision(ledger, "postponed", status="postponed", scores=(None, None))
    _decision(ledger, "missing", scores=(None, None))
    _decision(ledger, "tie", scores=(110, 110))
    _decision(ledger, "closed", status="closed", scores=(None, None))

    assert settle_open_trials(ledger, START + timedelta(hours=3)) == 3
    assert settle_open_trials(ledger, START + timedelta(days=2)) == 1
    grades = {
        row["game_id"]: (row["outcome"], row["profit_units"], row["clv_points"])
        for row in ledger.execute("SELECT * FROM paper_settlements")
    }
    assert grades == {
        "push": ("push", 0.0, None),
        "loss": ("loss", -1.0, 0.5),
        "canceled": ("void", 0.0, None),
        "rescheduled": ("void", 0.0, None),
    }


@pytest.mark.parametrize("change", ["nonfinal", "missing_scores", "tied_scores", "missing_result"])
def test_retracts_invalid_final_to_pending_and_can_restore_a_corrected_grade(ledger, change):
    _decision(ledger, "game")
    _quote(ledger, "game", "close", timedelta(minutes=-5))
    first = START + timedelta(hours=3)
    assert settle_open_trials(ledger, first) == 1
    if change == "nonfinal":
        ledger.execute("UPDATE games SET status = 'in_progress' WHERE game_id = 'game'")
    elif change == "missing_scores":
        ledger.execute("UPDATE game_results SET home_score = NULL WHERE game_id = 'game'")
    elif change == "tied_scores":
        ledger.execute("UPDATE game_results SET home_score = away_score WHERE game_id = 'game'")
    else:
        ledger.execute("DELETE FROM game_results WHERE game_id = 'game'")
    retracted_at = first + timedelta(hours=1)
    changes_before_read = ledger.total_changes
    assert unreconciled_grades(ledger, "pilot", retracted_at) == 1
    assert ledger.total_changes == changes_before_read
    assert settle_open_trials(ledger, retracted_at) == 1
    assert ledger.execute("SELECT COUNT(*) FROM paper_settlements").fetchone()[0] == 0
    retracted = grades_as_of(ledger, "pilot", retracted_at)["game"]
    assert retracted["outcome"] == "pending"
    assert retracted["profit_units"] == 0.0
    assert retracted["close_snapshot_id"] is None
    assert grades_as_of(ledger, "pilot", first)["game"]["outcome"] == "win"
    assert settle_open_trials(ledger, retracted_at + timedelta(minutes=1)) == 0
    ledger.execute("UPDATE games SET status = 'final' WHERE game_id = 'game'")
    ledger.execute(
        "INSERT OR REPLACE INTO game_results (game_id, home_score, away_score) VALUES ('game', 90, 107)"
    )
    restored_at = retracted_at + timedelta(hours=1)
    assert settle_open_trials(ledger, restored_at) == 1
    assert grades_as_of(ledger, "pilot", restored_at)["game"]["outcome"] == "loss"
    assert ledger.execute("SELECT COUNT(*) FROM paper_settlement_history").fetchone()[0] == 3
    decision = ledger.execute("SELECT line, price FROM paper_decisions").fetchone()
    assert tuple(decision) == (220.5, -110.0)


def test_late_close_adds_a_revision_and_backdated_observation_cannot_replace_it(ledger):
    _decision(ledger, "game")
    first = START + timedelta(hours=3)
    assert settle_open_trials(ledger, first) == 1
    _quote(ledger, "game", "close", timedelta(minutes=-5))
    later = first + timedelta(hours=1)
    assert unreconciled_grades(ledger, "pilot", later) == 1
    assert settle_open_trials(ledger, later) == 1
    assert grades_as_of(ledger, "pilot", first)["game"]["close_snapshot_id"] is None
    assert grades_as_of(ledger, "pilot", later)["game"]["close_snapshot_id"] == "close"
    ledger.execute("UPDATE game_results SET home_score = 90 WHERE game_id = 'game'")
    assert settle_open_trials(ledger, first + timedelta(minutes=30)) == 0
    assert ledger.execute("SELECT outcome FROM paper_settlements").fetchone()[0] == "win"
    assert ledger.execute("SELECT COUNT(*) FROM paper_settlement_history").fetchone()[0] == 2
    assert settle_open_trials(ledger, later + timedelta(hours=1)) == 1
    assert ledger.execute("SELECT outcome FROM paper_settlements").fetchone()[0] == "loss"


def test_corrected_future_start_cannot_be_graded_before_tipoff(ledger):
    _decision(ledger, "game", start=START + timedelta(minutes=5))
    assert settle_open_trials(ledger, START + timedelta(minutes=1)) == 0
    assert ledger.execute("SELECT COUNT(*) FROM paper_settlement_history").fetchone()[0] == 0
    assert settle_open_trials(ledger, START + timedelta(hours=3)) == 1
    assert ledger.execute("SELECT outcome FROM paper_settlements").fetchone()[0] == "win"


def test_identity_correction_voids_the_original_fixture_when_identity_was_captured(ledger):
    ledger.execute("ALTER TABLE paper_decisions ADD COLUMN home_team_id INTEGER")
    ledger.execute("ALTER TABLE paper_decisions ADD COLUMN away_team_id INTEGER")
    _decision(ledger, "game")
    ledger.execute("UPDATE paper_decisions SET home_team_id = 1, away_team_id = 2")
    first = START + timedelta(hours=3)
    assert settle_open_trials(ledger, first) == 1
    ledger.execute("UPDATE games SET home_team_id = 3 WHERE game_id = 'game'")
    assert settle_open_trials(ledger, first + timedelta(hours=1)) == 1
    assert ledger.execute("SELECT outcome FROM paper_settlements").fetchone()[0] == "void"
    assert grades_as_of(ledger, "pilot", first)["game"]["outcome"] == "win"


def test_corrected_start_uses_the_frozen_trial_minimum_lead(ledger):
    ledger.execute("CREATE TABLE paper_trials (trial_id TEXT, specification TEXT)")
    ledger.execute(
        "INSERT INTO paper_trials VALUES ('pilot', ?)",
        (json.dumps({"min_minutes_before_start": 30}),),
    )
    _decision(ledger, "game")
    ledger.execute(
        "UPDATE paper_decisions SET captured_at = ?",
        ((START - timedelta(minutes=35)).isoformat(),),
    )
    first = START + timedelta(hours=3)
    assert settle_open_trials(ledger, first) == 1
    ledger.execute(
        "UPDATE games SET start_time_utc = ?",
        ((START - timedelta(minutes=10)).isoformat(),),
    )
    assert unreconciled_grades(ledger, "pilot", first + timedelta(hours=1)) == 1
    assert settle_open_trials(ledger, first + timedelta(hours=1)) == 1
    assert ledger.execute("SELECT outcome FROM paper_settlements").fetchone()[0] == "void"
