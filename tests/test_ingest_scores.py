import sqlite3
from contextlib import contextmanager

import pytest

from src.data import ingest_scores


@pytest.mark.parametrize("status, expected_updates", [("scheduled", 1), ("voided", 0)])
def test_score_ingestion_preserves_voided_fixture(tmp_path, monkeypatch, status, expected_updates):
    db_path = tmp_path / "scores.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE games (game_id TEXT PRIMARY KEY, odds_api_id TEXT, status TEXT);
            CREATE TABLE game_results (game_id TEXT PRIMARY KEY, home_score INTEGER, away_score INTEGER);
            """
        )
        conn.execute("INSERT INTO games VALUES ('fixture', 'provider-id', ?)", (status,))

    @contextmanager
    def connect():
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            yield conn

    monkeypatch.setattr(ingest_scores, "connect", connect)
    event = {
        "id": "provider-id", "completed": True, "home_team": "Arsenal", "away_team": "Chelsea",
        "scores": [{"name": "Arsenal", "score": "0"}, {"name": "Chelsea", "score": "0"}],
    }
    assert ingest_scores.update_database([event], "EPL") == expected_updates
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT status FROM games").fetchone()[0] == ("final" if expected_updates else "voided")
        assert conn.execute("SELECT count(*) FROM game_results").fetchone()[0] == expected_updates
