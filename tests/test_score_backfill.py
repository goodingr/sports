"""Regression tests for the ESPN score backfill utility."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from src.data import score_backfill

SCHEMA = """
CREATE TABLE sports (
    sport_id INTEGER PRIMARY KEY,
    league TEXT NOT NULL UNIQUE
);
CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    sport_id INTEGER NOT NULL,
    code TEXT NOT NULL
);
CREATE TABLE games (
    game_id TEXT PRIMARY KEY,
    sport_id INTEGER NOT NULL,
    start_time_utc TEXT,
    home_team_id INTEGER,
    away_team_id INTEGER,
    status TEXT,
    espn_id TEXT
);
CREATE TABLE game_results (
    game_id TEXT PRIMARY KEY,
    home_score INTEGER,
    away_score INTEGER,
    source_version TEXT
);
"""


@pytest.fixture
def db_with_one_game(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "scores.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.execute("INSERT INTO sports (sport_id, league) VALUES (1, 'NBA')")
    conn.execute("INSERT INTO teams (team_id, sport_id, code) VALUES (10, 1, 'LAL'), (11, 1, 'BOS')")
    conn.execute(
        "INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, status) "
        "VALUES ('NBA_20260427_LAL_BOS', 1, '2026-04-27T01:40:00+00:00', 10, 11, 'scheduled')"
    )
    conn.commit()
    conn.close()

    @contextmanager
    def fake_connect() -> Iterator[sqlite3.Connection]:
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()

    monkeypatch.setattr(score_backfill, "connect", fake_connect)
    return db_path


def test_update_scores_in_db_matches_by_team_codes_and_date(db_with_one_game: Path) -> None:
    parsed = [
        {
            "league": "NBA",
            "home_team": "LAL",
            "away_team": "BOS",
            "home_score": 110,
            "away_score": 105,
            "date": "2026-04-27T01:40:00Z",
            "espn_id": "401584321",
        }
    ]
    updated = score_backfill.update_scores_in_db(parsed, "NBA")
    assert updated == 1

    with sqlite3.connect(db_with_one_game) as conn:
        conn.row_factory = sqlite3.Row
        result = conn.execute(
            "SELECT home_score, away_score, source_version FROM game_results WHERE game_id = ?",
            ("NBA_20260427_LAL_BOS",),
        ).fetchone()
        game = conn.execute(
            "SELECT status, espn_id FROM games WHERE game_id = ?",
            ("NBA_20260427_LAL_BOS",),
        ).fetchone()

    assert result["home_score"] == 110
    assert result["away_score"] == 105
    assert result["source_version"] == "espn_scoreboard"
    assert game["status"] == "final"
    assert game["espn_id"] == "401584321"


def test_update_scores_in_db_warns_on_unmatched_games(
    db_with_one_game: Path, caplog: pytest.LogCaptureFixture
) -> None:
    parsed = [
        {
            "league": "NBA",
            "home_team": "GSW",  # team that does not exist in DB
            "away_team": "DEN",
            "home_score": 120,
            "away_score": 100,
            "date": "2026-04-27T03:00:00Z",
            "espn_id": "x",
        }
    ]
    with caplog.at_level(logging.WARNING, logger=score_backfill.LOGGER.name):
        updated = score_backfill.update_scores_in_db(parsed, "NBA")
    assert updated == 0
    assert any("had no matching games row" in record.getMessage() for record in caplog.records)


def test_parse_espn_event_warns_when_team_unmappable(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(score_backfill, "normalize_team_code", lambda *_: None)
    event = {
        "date": "2026-04-27T01:00:00Z",
        "id": "espn-1",
        "status": {"type": {"completed": True, "state": "post"}},
        "competitions": [
            {
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Mystery Home"}, "score": "100"},
                    {"homeAway": "away", "team": {"displayName": "Mystery Away"}, "score": "98"},
                ],
                "status": {"type": {"completed": True, "state": "post"}},
            }
        ],
    }
    with caplog.at_level(logging.WARNING, logger=score_backfill.LOGGER.name):
        result = score_backfill.parse_espn_event(event, "NBA")
    assert result is None
    assert any("Could not normalize ESPN teams" in record.getMessage() for record in caplog.records)


def test_parse_espn_event_skips_in_progress_games() -> None:
    event = {
        "id": "live-1",
        "status": {"type": {"state": "in"}},
        "competitions": [
            {
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "A"}, "score": "5"},
                    {"homeAway": "away", "team": {"displayName": "B"}, "score": "4"},
                ],
            }
        ],
    }
    assert score_backfill.parse_espn_event(event, "NBA") is None
