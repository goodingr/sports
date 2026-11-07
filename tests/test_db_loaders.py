"""Tests for database loader helpers."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd
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


def test_espn_schedule_refresh_preserves_stored_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "schedules.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(Path("src/db/schema.sql").read_text(encoding="utf-8"))
    monkeypatch.setattr(loaders, "connect", _sqlite_context(db_path))
    fixture = {
        "game_id": "EPL_123",
        "season": 2026,
        "gameday": "2026-01-01",
        "gametime": "18:00",
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_score": 2,
        "away_score": 0,
    }
    loaders.load_schedules(pd.DataFrame([fixture]), source_version="espn_scoreboard", league="EPL")
    schedule = {**fixture, "home_score": None, "away_score": None}
    loaders.load_schedules(pd.DataFrame([schedule]), source_version="espn_api", league="EPL")

    with sqlite3.connect(db_path) as conn:
        result = conn.execute(
            "SELECT status, home_score, away_score, source_version FROM games JOIN game_results USING (game_id)"
        ).fetchone()
    assert result == ("final", 2, 0, "espn_scoreboard")


def test_schedule_replay_preserves_voided_game(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "schedules.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(Path("src/db/schema.sql").read_text(encoding="utf-8"))
    monkeypatch.setattr(loaders, "connect", _sqlite_context(db_path))
    fixture = {
        "game_id": "EPL_123", "season": 2026, "gameday": "2026-01-01", "gametime": "18:00",
        "home_team": "Arsenal", "away_team": "Chelsea", "home_score": None, "away_score": None,
    }
    loaders.load_schedules(pd.DataFrame([fixture]), source_version="espn_api", league="EPL")
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE games SET status = 'voided'")
    fixture.update({"home_score": 0, "away_score": 0})
    loaders.load_schedules(pd.DataFrame([fixture]), source_version="espn_api", league="EPL")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT status, home_score, away_score FROM games JOIN game_results USING (game_id)"
        ).fetchone() == ("voided", None, None)


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

    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE games SET status = 'final'")
    archive = payload("2026-01-01T19:30:00+00:00", 223.5)
    archive["source"] = "the-odds-api-history"
    archive["results"][0]["commence_time"] = "2026-01-01T20:05:00Z"
    archive["results"].append({**archive["results"][0], "commence_time": "2026-01-02T20:00:00Z"})

    for _ in range(2):
        loaders.load_odds_snapshot(archive, raw_path="archive.json", sport_key="basketball_nba")

    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT start_time_utc, status FROM games"
        ).fetchall() == [("2026-01-01T20:00:00+00:00", "final")]
        assert conn.execute("SELECT total_close FROM game_results").fetchone() == (222.5,)
        assert conn.execute(
            """
            SELECT DISTINCT fetched_at_utc, line FROM odds JOIN odds_snapshots USING (snapshot_id)
            WHERE source = 'the-odds-api-history'
            """
        ).fetchall() == [("2026-01-01T19:30:00+00:00", 223.5)]
        assert conn.execute(
            "SELECT COUNT(*) FROM odds_snapshots WHERE source = 'the-odds-api-history'"
        ).fetchone() == (1,)


def _nba_schedule_snapshot(
    fetched_at: str, start: str, event_id: str, *, line: float | None = 220.5
) -> dict:
    bookmakers = []
    if line is not None:
        bookmakers = [
            {
                "title": "DraftKings",
                "markets": [
                    {
                        "key": "totals",
                        "outcomes": [
                            {"name": side, "price": -110, "point": line}
                            for side in ("Over", "Under")
                        ],
                    }
                ],
            }
        ]
    return {
        "source": "the-odds-api",
        "fetched_at": fetched_at,
        "results": [
            {
                "id": event_id,
                "commence_time": start,
                "home_team": "Boston Celtics",
                "away_team": "Philadelphia 76ers",
                "bookmakers": bookmakers,
            }
        ],
    }


@pytest.mark.parametrize("newer_line", [221.5, None])
def test_delayed_snapshot_keeps_newer_fixture_schedule_and_records_quotes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, newer_line: float | None
) -> None:
    db_path = tmp_path / "odds.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(Path("src/db/schema.sql").read_text(encoding="utf-8"))
    monkeypatch.setattr(loaders, "connect", _sqlite_context(db_path))

    newer = _nba_schedule_snapshot(
        "2026-11-07T18:01:00.000002-05:00", "2026-11-08T03:10:00Z", "new-id", line=newer_line
    )
    older = _nba_schedule_snapshot("2026-11-07T23:01:00.000001Z", "2026-11-08T02:40:00Z", "old-id")
    loaders.load_odds_snapshot(newer, sport_key="basketball_nba")
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE games SET status = 'in_progress'")
    loaders.load_odds_snapshot(older, sport_key="basketball_nba")

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT start_time_utc, odds_api_id, status FROM games").fetchall() == [
            ("2026-11-08T03:10:00+00:00", "new-id", "in_progress")
        ]
        assert conn.execute(
            """
            SELECT outcome, line FROM odds JOIN odds_snapshots USING (snapshot_id)
            WHERE fetched_at_utc = '2026-11-07T23:01:00.000001+00:00'
            ORDER BY outcome
            """
        ).fetchall() == [("Over", 220.5), ("Under", 220.5)]


@pytest.mark.parametrize("suffix", ["", "_01ab2345cd6789ef01ab2345cd6789ef"])
def test_existing_fixture_bootstraps_schedule_clock_from_ordinary_archive_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, suffix: str
) -> None:
    db_path = tmp_path / "odds.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(Path("src/db/schema.sql").read_text(encoding="utf-8"))
    monkeypatch.setattr(loaders, "connect", _sqlite_context(db_path))

    ordinary = _nba_schedule_snapshot("2026-11-07T23:01:00Z", "2026-11-08T03:10:00Z", "new-id")
    loaders.load_odds_snapshot(
        ordinary,
        raw_path=rf"C:\sports\data\raw\odds\basketball_nba\odds_2026-11-07T23-01-00Z{suffix}.json",
        sport_key="basketball_nba",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE odds_api_schedule_clock")
        conn.execute("UPDATE games SET status = 'voided'")
        conn.execute(
            """
            INSERT INTO odds_snapshots
                (snapshot_id, fetched_at_utc, sport_id, source, raw_path)
            VALUES ('closing', '2026-11-07T23:10:00+00:00', 1, 'the-odds-api',
                    'data/raw/odds/basketball_nba/paper_closing/odds_2026-11-07T23-10-00Z.json'),
                   ('legacy_closing', '2026-11-07T23:11:00+00:00', 1, 'the-odds-api', NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
            SELECT s.snapshot_id, g.game_id, 1, 'totals', 'Over', -110, 220.5
            FROM odds_snapshots s CROSS JOIN games g
            WHERE s.snapshot_id IN ('closing', 'legacy_closing')
            """
        )

    older = _nba_schedule_snapshot("2026-11-07T23:00:00Z", "2026-11-08T02:40:00Z", "old-id")
    loaders.load_odds_snapshot(older, sport_key="basketball_nba")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT start_time_utc, odds_api_id, status FROM games").fetchall() == [
            ("2026-11-08T03:10:00+00:00", "new-id", "voided")
        ]

    correction = _nba_schedule_snapshot("2026-11-07T23:05:00Z", "2026-11-08T03:15:00Z", "fixed-id")
    loaders.load_odds_snapshot(correction, sport_key="basketball_nba")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT start_time_utc, odds_api_id, status FROM games").fetchall() == [
            ("2026-11-08T03:15:00+00:00", "fixed-id", "voided")
        ]
