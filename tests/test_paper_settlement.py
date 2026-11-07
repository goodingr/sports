import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.db.core import SCHEMA_PATH
from src.predict.paper_settlement import settle_open_trials

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
        "INSERT INTO paper_decisions VALUES ('pilot', ?, ?, ?, 'the-odds-api', 1, ?, ?, -110)",
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
    assert settle_open_trials(ledger, START + timedelta(days=1)) == 0
    assert ledger.execute("SELECT home_score FROM paper_settlements").fetchone()[0] == 116


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
