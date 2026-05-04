from __future__ import annotations

import sqlite3
from pathlib import Path

from src.data.backfill_total_close_provenance import build_backfill_report


def _create_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE sports (
                sport_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                league TEXT NOT NULL,
                default_market TEXT NOT NULL
            );
            CREATE TABLE teams (
                team_id INTEGER PRIMARY KEY,
                sport_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL
            );
            CREATE TABLE games (
                game_id TEXT PRIMARY KEY,
                sport_id INTEGER NOT NULL,
                start_time_utc TEXT NOT NULL,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL
            );
            CREATE TABLE game_results (
                game_id TEXT PRIMARY KEY,
                home_score INTEGER,
                away_score INTEGER,
                total_close REAL
            );
            CREATE TABLE books (
                book_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE odds_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                fetched_at_utc TEXT NOT NULL,
                sport_id INTEGER NOT NULL,
                source TEXT,
                raw_path TEXT
            );
            CREATE TABLE odds (
                snapshot_id TEXT NOT NULL,
                game_id TEXT NOT NULL,
                book_id INTEGER NOT NULL,
                market TEXT NOT NULL,
                outcome TEXT NOT NULL,
                price_american REAL,
                line REAL
            );
            INSERT INTO sports VALUES (1, 'Basketball', 'NBA', 'totals');
            INSERT INTO teams VALUES (1, 1, 'LAL', 'Lakers');
            INSERT INTO teams VALUES (2, 1, 'BOS', 'Celtics');
            INSERT INTO games VALUES ('EXACT', 1, '2026-01-01T20:00:00+00:00', 1, 2);
            INSERT INTO games VALUES ('AMBIG', 1, '2026-01-02T20:00:00+00:00', 1, 2);
            INSERT INTO games VALUES ('MISS', 1, '2026-01-03T20:00:00+00:00', 1, 2);
            INSERT INTO game_results VALUES ('EXACT', 100, 90, 221.5);
            INSERT INTO game_results VALUES ('AMBIG', 100, 90, 210.5);
            INSERT INTO game_results VALUES ('MISS', 100, 90, 205.5);
            INSERT INTO books VALUES (1, 'DraftKings');
            INSERT INTO books VALUES (2, 'FanDuel');
            INSERT INTO odds_snapshots VALUES ('EXACT_CLOSE', '2026-01-01T19:00:00+00:00', 1, 'odds', NULL);
            INSERT INTO odds_snapshots VALUES ('AMBIG_CLOSE', '2026-01-02T19:00:00+00:00', 1, 'odds', NULL);
            """
        )
        conn.executemany(
            """
            INSERT INTO odds (
                snapshot_id, game_id, book_id, market, outcome, price_american, line
            ) VALUES (?, ?, ?, 'totals', ?, ?, ?)
            """,
            [
                ("EXACT_CLOSE", "EXACT", 1, "Over", -110, 221.5),
                ("EXACT_CLOSE", "EXACT", 1, "Under", -110, 221.5),
                ("AMBIG_CLOSE", "AMBIG", 1, "Over", -110, 210.5),
                ("AMBIG_CLOSE", "AMBIG", 1, "Under", -110, 210.5),
                ("AMBIG_CLOSE", "AMBIG", 2, "Over", -105, 210.5),
                ("AMBIG_CLOSE", "AMBIG", 2, "Under", -115, 210.5),
            ],
        )


def test_total_close_provenance_backfill_dry_run_does_not_write(tmp_path: Path) -> None:
    db_path = tmp_path / "lineage.db"
    _create_db(db_path)

    report = build_backfill_report(db_path=db_path, leagues=["NBA"], write=False)

    assert report["target_count"] == 3
    assert report["resolved_count"] == 1
    assert report["ambiguous_count"] == 1
    assert report["unresolved_count"] == 1
    with sqlite3.connect(db_path) as conn:
        value = conn.execute(
            "SELECT total_close_snapshot_id FROM game_results WHERE game_id = 'EXACT'"
        ).fetchone()[0]
    assert value is None


def test_total_close_provenance_backfill_write_updates_only_resolved(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "lineage.db"
    _create_db(db_path)

    report = build_backfill_report(db_path=db_path, leagues=["NBA"], write=True)

    assert report["updated_rows"] == 1
    with sqlite3.connect(db_path) as conn:
        exact = conn.execute(
            """
            SELECT total_close_snapshot_id, total_close_book, total_close_source
            FROM game_results WHERE game_id = 'EXACT'
            """
        ).fetchone()
        ambiguous = conn.execute(
            "SELECT total_close_snapshot_id FROM game_results WHERE game_id = 'AMBIG'"
        ).fetchone()[0]
    assert exact == ("EXACT_CLOSE", "DraftKings", "odds")
    assert ambiguous is None


def test_total_close_provenance_latest_pregame_strategy_rebuilds_close(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "lineage.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            INSERT INTO games VALUES ('REBUILD', 1, '2026-01-04T20:00:00+00:00', 1, 2);
            INSERT INTO game_results VALUES ('REBUILD', 100, 90, 200.5);
            INSERT INTO odds_snapshots VALUES ('REBUILD_OPEN', '2026-01-04T12:00:00+00:00', 1, 'odds', NULL);
            INSERT INTO odds_snapshots VALUES ('REBUILD_CLOSE', '2026-01-04T19:00:00+00:00', 1, 'odds', NULL);
            INSERT INTO odds_snapshots VALUES ('REBUILD_AFTER', '2026-01-04T21:00:00+00:00', 1, 'odds', NULL);
            """
        )
        conn.executemany(
            """
            INSERT INTO odds (
                snapshot_id, game_id, book_id, market, outcome, price_american, line
            ) VALUES (?, 'REBUILD', 1, 'totals', ?, ?, ?)
            """,
            [
                ("REBUILD_OPEN", "Over", -110, 201.5),
                ("REBUILD_OPEN", "Under", -110, 201.5),
                ("REBUILD_CLOSE", "Over", -110, 202.5),
                ("REBUILD_CLOSE", "Under", -110, 202.5),
                ("REBUILD_AFTER", "Over", -110, 230.5),
                ("REBUILD_AFTER", "Under", -110, 230.5),
            ],
        )

    dry_run = build_backfill_report(
        db_path=db_path,
        leagues=["NBA"],
        strategy="latest-pregame",
        write=False,
    )
    rebuild = next(row for row in dry_run["resolved"] if row["game_id"] == "REBUILD")

    assert dry_run["strategy"] == "latest_pregame"
    assert rebuild["candidate"]["snapshot_id"] == "REBUILD_CLOSE"
    assert rebuild["candidate"]["line"] == 202.5
    assert rebuild["would_change_total_close"] is True
    with sqlite3.connect(db_path) as conn:
        total_close = conn.execute(
            "SELECT total_close FROM game_results WHERE game_id = 'REBUILD'"
        ).fetchone()[0]
    assert total_close == 200.5

    write_report = build_backfill_report(
        db_path=db_path,
        leagues=["NBA"],
        strategy="latest-pregame",
        write=True,
    )

    assert write_report["updated_rows"] >= 1
    with sqlite3.connect(db_path) as conn:
        rebuilt = conn.execute(
            """
            SELECT total_close, total_close_snapshot_id, total_close_book
            FROM game_results WHERE game_id = 'REBUILD'
            """
        ).fetchone()
    assert rebuilt == (202.5, "REBUILD_CLOSE", "DraftKings")
