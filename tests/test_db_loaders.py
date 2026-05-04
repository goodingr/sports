"""Tests for database loader helpers."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from src.db import loaders


def _sqlite_context(db_path: Path):
    @contextmanager
    def _inner(path: Path | None = None) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    return _inner


def test_has_successful_source_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE data_sources (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_key TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE source_runs (
            run_id TEXT PRIMARY KEY,
            source_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT
        )
        """
    )
    conn.execute("INSERT INTO data_sources (source_key) VALUES ('demo_source')")
    conn.execute(
        "INSERT INTO source_runs (run_id, source_id, status) VALUES ('run1', 1, 'success')"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(loaders, "connect", _sqlite_context(db_path))

    assert loaders.has_successful_source_run("demo_source") is True
    assert loaders.has_successful_source_run("missing") is False


def test_load_odds_snapshot_persists_total_close_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "odds.db"
    with sqlite3.connect(db_path) as conn:
        schema = Path("src/db/schema.sql").read_text(encoding="utf-8")
        conn.executescript(schema)

    monkeypatch.setattr(loaders, "connect", _sqlite_context(db_path))
    payload = {
        "fetched_at": "2026-01-01T18:00:00+00:00",
        "source": "the-odds-api",
        "results": [
            {
                "id": "odds-game-1",
                "sport_title": "NBA",
                "commence_time": "2026-01-01T20:00:00Z",
                "home_team": "Los Angeles Lakers",
                "away_team": "Boston Celtics",
                "bookmakers": [
                    {
                        "title": "DraftKings",
                        "markets": [
                            {
                                "key": "totals",
                                "outcomes": [
                                    {"name": "Over", "price": -110, "point": 221.5},
                                    {"name": "Under", "price": -110, "point": 221.5},
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    loaders.load_odds_snapshot(payload, raw_path="raw.json", sport_key="basketball_nba")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                total_close,
                total_close_snapshot_id,
                total_close_snapshot_time_utc,
                total_close_book_id,
                total_close_book,
                total_close_source
            FROM game_results
            """
        ).fetchone()

    assert row["total_close"] == 221.5
    assert row["total_close_snapshot_id"]
    assert row["total_close_snapshot_time_utc"] == "2026-01-01T18:00:00+00:00"
    assert row["total_close_book_id"] == 1
    assert row["total_close_book"] == "DraftKings"
    assert row["total_close_source"] == "the-odds-api"


def test_load_odds_snapshot_keeps_latest_pregame_total_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "odds.db"
    with sqlite3.connect(db_path) as conn:
        schema = Path("src/db/schema.sql").read_text(encoding="utf-8")
        conn.executescript(schema)

    monkeypatch.setattr(loaders, "connect", _sqlite_context(db_path))

    def payload(fetched_at: str, line: float) -> dict:
        return {
            "fetched_at": fetched_at,
            "source": "the-odds-api",
            "results": [
                {
                    "id": "odds-game-1",
                    "sport_title": "NBA",
                    "commence_time": "2026-01-01T20:00:00Z",
                    "home_team": "Los Angeles Lakers",
                    "away_team": "Boston Celtics",
                    "bookmakers": [
                        {
                            "title": "DraftKings",
                            "markets": [
                                {
                                    "key": "totals",
                                    "outcomes": [
                                        {"name": "Over", "price": -110, "point": line},
                                        {"name": "Under", "price": -110, "point": line},
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
        }

    loaders.load_odds_snapshot(
        payload("2026-01-01T18:00:00+00:00", 221.5),
        raw_path="open.json",
        sport_key="basketball_nba",
    )
    loaders.load_odds_snapshot(
        payload("2026-01-01T19:00:00+00:00", 222.5),
        raw_path="close.json",
        sport_key="basketball_nba",
    )
    loaders.load_odds_snapshot(
        payload("2026-01-01T21:00:00+00:00", 230.5),
        raw_path="after.json",
        sport_key="basketball_nba",
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT total_close, total_close_snapshot_time_utc, total_close_book
            FROM game_results
            """
        ).fetchone()

    assert row["total_close"] == 222.5
    assert row["total_close_snapshot_time_utc"] == "2026-01-01T19:00:00+00:00"
    assert row["total_close_book"] == "DraftKings"
