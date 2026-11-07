from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.predict.paper_settlement_history import (
    SETTLEMENT_FIELDS,
    ensure_settlement_history,
    grades_as_of,
)

CUTOFF = datetime(2027, 2, 8, 5, tzinfo=timezone.utc)


@pytest.fixture
def ledger():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE paper_settlements (
            trial_id TEXT, game_id TEXT, settled_at TEXT, outcome TEXT, profit_units REAL,
            clv_points REAL
        )
        """
    )
    yield conn
    conn.close()


def _current(conn, game_id="g", *, settled_at=CUTOFF, outcome="win", trial_id="pilot"):
    conn.execute(
        "INSERT INTO paper_settlements VALUES (?, ?, ?, ?, ?, ?)",
        (trial_id, game_id, settled_at.isoformat(), outcome, 1.0, 0.5),
    )


def _revision(conn, *, settled_at, outcome="loss", trial_id="pilot", game_id="g"):
    record = {
        "trial_id": trial_id,
        "game_id": game_id,
        "settled_at": settled_at.isoformat(),
        "outcome": outcome,
        "profit_units": -1.0 if outcome == "loss" else 0.0,
    }
    conn.execute(
        f"""
        INSERT INTO paper_settlement_history ({", ".join(SETTLEMENT_FIELDS)})
        VALUES ({", ".join("?" for _ in SETTLEMENT_FIELDS)})
        """,
        [record.get(field) for field in SETTLEMENT_FIELDS],
    )


def test_legacy_cache_is_read_without_creating_history(ledger):
    _current(ledger, settled_at=CUTOFF - timedelta(hours=1))
    _current(ledger, "future", settled_at=CUTOFF + timedelta(microseconds=1))
    _current(ledger, "other", trial_id="other")
    before = ledger.total_changes
    assert set(grades_as_of(ledger, "pilot", CUTOFF)) == {"g"}
    assert grades_as_of(ledger, "pilot", CUTOFF)["g"]["home_score"] is None
    assert ledger.total_changes == before
    assert (
        ledger.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'paper_settlement_history'"
        ).fetchone()
        is None
    )


def test_history_seed_is_idempotent_preserves_time_and_does_not_commit(ledger):
    original = CUTOFF.astimezone(timezone(timedelta(hours=-5)))
    _current(ledger, settled_at=original)
    ledger.commit()
    ledger.execute("BEGIN")
    ensure_settlement_history(ledger)
    ensure_settlement_history(ledger)
    assert ledger.in_transaction
    rows = ledger.execute("SELECT * FROM paper_settlement_history").fetchall()
    assert len(rows) == 1
    assert rows[0]["settled_at"] == original.isoformat()
    assert rows[0]["outcome"] == "win"
    ledger.rollback()
    assert (
        ledger.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'paper_settlement_history'"
        ).fetchone()
        is None
    )


def test_history_exact_cutoff_and_equivalent_time_revision_tie(ledger):
    _current(ledger, settled_at=CUTOFF - timedelta(hours=1))
    ensure_settlement_history(ledger)
    _revision(ledger, settled_at=CUTOFF, outcome="loss")
    _revision(ledger, settled_at=CUTOFF.astimezone(timezone(timedelta(hours=3))), outcome="pending")
    _revision(ledger, settled_at=CUTOFF + timedelta(microseconds=1), outcome="win")
    assert (
        grades_as_of(ledger, "pilot", CUTOFF - timedelta(microseconds=1))["g"]["outcome"] == "win"
    )
    selected = grades_as_of(ledger, "pilot", CUTOFF)["g"]
    assert selected["outcome"] == "pending"
    assert selected["profit_units"] == 0.0
    assert (
        grades_as_of(ledger, "pilot", CUTOFF + timedelta(microseconds=1))["g"]["outcome"] == "win"
    )


def test_future_history_prevents_falling_back_to_latest_cache(ledger):
    _current(ledger, settled_at=CUTOFF - timedelta(days=1))
    ensure_settlement_history(ledger)
    ledger.execute("DELETE FROM paper_settlement_history")
    _revision(ledger, settled_at=CUTOFF + timedelta(days=1), outcome="loss")
    assert grades_as_of(ledger, "pilot", CUTOFF) == {}
    _current(ledger, "legacy_without_history", settled_at=CUTOFF - timedelta(hours=1))
    assert set(grades_as_of(ledger, "pilot", CUTOFF)) == {"legacy_without_history"}


def test_selector_orders_by_timestamp_before_revision_id(ledger):
    _current(ledger, settled_at=CUTOFF - timedelta(hours=1))
    ensure_settlement_history(ledger)
    _revision(ledger, settled_at=CUTOFF, outcome="loss")
    _revision(ledger, settled_at=CUTOFF - timedelta(minutes=1), outcome="pending")
    assert grades_as_of(ledger, "pilot", CUTOFF)["g"]["outcome"] == "loss"


def test_selector_requires_aware_cutoff_and_excludes_invalid_legacy_time(ledger):
    _current(ledger)
    ledger.execute("UPDATE paper_settlements SET settled_at = '2027-02-08T04:00:00'")
    assert grades_as_of(ledger, "pilot", CUTOFF) == {}
    with pytest.raises(ValueError, match="time zone"):
        grades_as_of(ledger, "pilot", CUTOFF.replace(tzinfo=None))
