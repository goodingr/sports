from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.dashboard import app as dashboard
from src.predict.paper_ledger import SCHEMA_PATH

NOW = datetime(2026, 11, 7, 4, 0, tzinfo=timezone.utc)


class FixedClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz)


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    path = tmp_path / "paper.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO paper_trials VALUES (?, ?, ?)",
            (
                "nba",
                (NOW - timedelta(days=1)).isoformat(),
                json.dumps(
                    {
                        "league": "NBA",
                        "market": "totals",
                        "book": "draftkings",
                        "review_at": (NOW + timedelta(days=30)).isoformat(),
                        "min_decisions": 150,
                        "min_roi": 0.05,
                        "min_clv_coverage": 0.8,
                    }
                ),
            ),
        )
    monkeypatch.setattr(dashboard, "resolve_db_path", lambda: path)
    monkeypatch.setattr(dashboard, "datetime", FixedClock)
    return path


def _text(component):
    if isinstance(component, (list, tuple)):
        return " ".join(_text(child) for child in component)
    if component is None:
        return ""
    if hasattr(component, "children"):
        return _text(component.children)
    return str(component)


def _record_run(path, fixtures=(), *, status="evaluated"):
    with sqlite3.connect(path) as conn:
        cursor = conn.execute(
            "INSERT INTO paper_capture_runs (trial_id, observed_at, status) VALUES (?, ?, ?)",
            ("nba", (NOW - timedelta(minutes=5)).isoformat(), status),
        )
        conn.executemany(
            "INSERT INTO paper_fixture_observations VALUES (?, ?, ?, ?, ?)",
            [
                (cursor.lastrowid, game_id, (NOW + timedelta(hours=1)).isoformat(), outcome, None)
                for game_id, outcome in fixtures
            ],
        )


def test_trial_card_shows_distinct_coverage_and_attempt_reasons(ledger):
    _record_run(
        ledger,
        [
            ("first", "below_edge_threshold"),
            ("second", "recorded"),
            ("third", "stale_quote"),
        ],
    )
    _record_run(ledger, [("first", "below_edge_threshold")])
    text = _text(dashboard.show_paper_trials(None))
    assert "Last capture attempt Nov 06, 2026 10:55 PM EST" in text
    assert "evaluated · Capture attempts 2" in text
    assert "Fully evaluated 2 / 3 observed fixtures · 1 below the edge threshold" in text
    assert "Below edge threshold 2" in text
    assert "Stale quote 1" in text
    assert "Recorded 1" in text


def test_trial_card_distinguishes_missing_history_from_empty_capture_window(ledger):
    _record_run(ledger, status="no_predictions")
    text = _text(dashboard.show_paper_trials(None))
    assert "no predictions · Capture attempts 1" in text
    assert "No fixtures were observed inside the capture window." in text
    assert "No capture history is available." not in text

    with sqlite3.connect(ledger) as conn:
        conn.execute("DROP TABLE paper_fixture_observations")
        conn.execute("DROP TABLE paper_capture_runs")
    text = _text(dashboard.show_paper_trials(None))
    assert "No capture history is available." in text
    assert "No fixtures were observed" not in text
    assert "Fully evaluated 0 / 0" not in text


def test_trial_card_shows_no_attempts_when_coverage_is_initialized(ledger):
    text = _text(dashboard.show_paper_trials(None))
    assert "No capture attempts have been recorded." in text
    assert "No fixtures were observed" not in text
    assert "No capture history is available." not in text
