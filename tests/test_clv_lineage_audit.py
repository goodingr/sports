from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from src.data.clv_lineage_audit import build_clv_lineage_report, main


def _create_clv_lineage_db(path: Path) -> None:
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
                away_team_id INTEGER NOT NULL,
                status TEXT
            );
            CREATE TABLE game_results (
                game_id TEXT PRIMARY KEY,
                home_score INTEGER,
                away_score INTEGER,
                total_close REAL,
                source_version TEXT
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
                price_decimal REAL,
                implied_prob REAL,
                line REAL
            );

            INSERT INTO sports VALUES (1, 'Basketball', 'NBA', 'totals');
            INSERT INTO teams VALUES (1, 1, 'LAL', 'Lakers');
            INSERT INTO teams VALUES (2, 1, 'BOS', 'Celtics');
            INSERT INTO games VALUES ('MATCHED', 1, '2026-01-10T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('UNOBSERVED', 1, '2026-01-11T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('MISSING_CLOSE', 1, '2026-01-12T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('OLDER_CLOSE', 1, '2026-01-13T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('NO_ODDS', 1, '2026-01-14T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO game_results VALUES ('MATCHED', 112, 108, 221.5, 'result-feed');
            INSERT INTO game_results VALUES ('UNOBSERVED', 104, 101, 210.5, 'result-feed');
            INSERT INTO game_results VALUES ('MISSING_CLOSE', 99, 97, NULL, 'result-feed');
            INSERT INTO game_results VALUES ('OLDER_CLOSE', 120, 118, 219.5, 'result-feed');
            INSERT INTO game_results VALUES ('NO_ODDS', 101, 100, 203.5, 'result-feed');
            INSERT INTO books VALUES (1, 'DraftKings');
            INSERT INTO books VALUES (2, 'FanDuel');

            INSERT INTO odds_snapshots VALUES (
                'MATCHED_CURRENT', '2026-01-10T18:00:00+00:00', 1, 'the-odds-api', 'matched.json'
            );
            INSERT INTO odds_snapshots VALUES (
                'UNOBSERVED_STALE', '2026-01-07T18:00:00+00:00', 1, 'the-odds-api', 'stale.json'
            );
            INSERT INTO odds_snapshots VALUES (
                'MISSING_CURRENT', '2026-01-12T18:00:00+00:00', 1, 'the-odds-api', 'missing.json'
            );
            INSERT INTO odds_snapshots VALUES (
                'OLDER_CLOSE_OPEN', '2026-01-12T12:00:00+00:00', 1, 'the-odds-api', 'older-open.json'
            );
            INSERT INTO odds_snapshots VALUES (
                'OLDER_CLOSE_CURRENT', '2026-01-13T18:00:00+00:00', 1, 'the-odds-api', 'older-current.json'
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO odds (
                snapshot_id, game_id, book_id, market, outcome, price_american, line
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("MATCHED_CURRENT", "MATCHED", 1, "totals", "Over", -105, 221.5),
                ("MATCHED_CURRENT", "MATCHED", 1, "totals", "Under", -115, 221.5),
                ("MATCHED_CURRENT", "MATCHED", 2, "totals", "Over", -100, 221.5),
                ("MATCHED_CURRENT", "MATCHED", 2, "totals", "Under", -110, 221.5),
                ("UNOBSERVED_STALE", "UNOBSERVED", 1, "totals", "Over", -110, 209.5),
                ("UNOBSERVED_STALE", "UNOBSERVED", 1, "totals", "Under", -110, 209.5),
                ("MISSING_CURRENT", "MISSING_CLOSE", 1, "totals", "Over", -110, 200.5),
                ("MISSING_CURRENT", "MISSING_CLOSE", 1, "totals", "Under", -110, 200.5),
                ("OLDER_CLOSE_OPEN", "OLDER_CLOSE", 1, "totals", "Over", -110, 219.5),
                ("OLDER_CLOSE_OPEN", "OLDER_CLOSE", 1, "totals", "Under", -110, 219.5),
                ("OLDER_CLOSE_CURRENT", "OLDER_CLOSE", 1, "totals", "Over", -108, 222.5),
                ("OLDER_CLOSE_CURRENT", "OLDER_CLOSE", 1, "totals", "Under", -112, 222.5),
            ],
        )


def _game(report: dict, game_id: str) -> dict:
    return next(row for row in report["game_lineage"] if row["game_id"] == game_id)


def test_clv_lineage_detects_exact_match_and_best_book_difference(tmp_path: Path) -> None:
    db_path = tmp_path / "lineage.db"
    _create_clv_lineage_db(db_path)

    report = build_clv_lineage_report(db_path=db_path, leagues=["NBA"])
    row = _game(report, "MATCHED")

    assert row["total_close_observed_in_pregame_odds"] is True
    assert row["selected_book_matches_total_close"] is True
    assert row["best_book_differs_any_side"] is True
    assert row["line_delta_to_total_close"] == 0.0
    assert "total_close_not_observed_in_pregame_odds" not in row["issues"]
    assert "close_provenance_not_stored" in row["issues"]


def test_clv_lineage_flags_stale_and_unobserved_close(tmp_path: Path) -> None:
    db_path = tmp_path / "lineage.db"
    _create_clv_lineage_db(db_path)

    report = build_clv_lineage_report(db_path=db_path, leagues=["NBA"])
    row = _game(report, "UNOBSERVED")

    assert row["is_stale_current_snapshot"] is True
    assert row["total_close_observed_in_pregame_odds"] is False
    assert row["hours_before_start"] == 98.0
    assert "stale_current_snapshot" in row["issues"]
    assert "total_close_not_observed_in_pregame_odds" in row["issues"]


def test_clv_lineage_flags_missing_close_and_no_odds(tmp_path: Path) -> None:
    db_path = tmp_path / "lineage.db"
    _create_clv_lineage_db(db_path)

    report = build_clv_lineage_report(db_path=db_path, leagues=["NBA"])

    assert "missing_total_close" in _game(report, "MISSING_CLOSE")["issues"]
    no_odds = _game(report, "NO_ODDS")
    assert no_odds["has_pregame_totals_pair"] is False
    assert "no_pregame_totals_pair" in no_odds["issues"]


def test_clv_lineage_flags_close_candidate_older_than_current(tmp_path: Path) -> None:
    db_path = tmp_path / "lineage.db"
    _create_clv_lineage_db(db_path)

    report = build_clv_lineage_report(db_path=db_path, leagues=["NBA"])
    row = _game(report, "OLDER_CLOSE")

    assert row["total_close_observed_in_pregame_odds"] is True
    assert row["selected_book_matches_total_close"] is True
    assert row["close_candidate_older_than_current"] is True
    assert row["line_delta_to_total_close"] == -3.0
    assert "close_candidate_older_than_current" in row["issues"]


def test_clv_lineage_recommendation_allows_historical_no_odds_gaps(tmp_path: Path) -> None:
    db_path = tmp_path / "lineage.db"
    _create_clv_lineage_db(db_path)
    with sqlite3.connect(db_path) as conn:
        for column, ddl in {
            "total_close_snapshot_id": "TEXT",
            "total_close_snapshot_time_utc": "TEXT",
            "total_close_book_id": "INTEGER",
            "total_close_book": "TEXT",
            "total_close_source": "TEXT",
        }.items():
            conn.execute(f"ALTER TABLE game_results ADD COLUMN {column} {ddl}")
        conn.execute(
            """
            UPDATE game_results
            SET total_close_snapshot_id = 'MATCHED_CURRENT',
                total_close_snapshot_time_utc = '2026-01-10T18:00:00+00:00',
                total_close_book_id = 1,
                total_close_book = 'DraftKings',
                total_close_source = 'the-odds-api'
            WHERE game_id = 'MATCHED'
            """
        )
        conn.execute(
            """
            DELETE FROM game_results
            WHERE game_id IN ('UNOBSERVED', 'MISSING_CLOSE', 'OLDER_CLOSE')
            """
        )
        conn.execute(
            """
            DELETE FROM games
            WHERE game_id IN ('UNOBSERVED', 'MISSING_CLOSE', 'OLDER_CLOSE')
            """
        )

    report = build_clv_lineage_report(db_path=db_path, leagues=["NBA"])

    assert report["recommendation"] == "lineage_usable_for_odds_backed_rows_historical_gaps_remain"
    assert report["summary"]["odds_backed_settled_games"] == 1
    assert report["summary"]["historical_gap_games_without_pregame_totals_pair"] == 1
    assert report["summary"]["odds_backed_issue_counts"] == []


def test_clv_lineage_cli_writes_json(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "lineage.db"
    output_path = tmp_path / "reports" / "lineage.json"
    _create_clv_lineage_db(db_path)

    exit_code = main(
        [
            "--db",
            str(db_path),
            "--league",
            "NBA",
            "--output",
            str(output_path),
        ]
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "CLV lineage audit" in captured
    assert "JSON written to" in captured
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["summary"]["settled_games"] == 5
    assert payload["summary"]["stale_current_snapshot_games"] == 1
    assert payload["recommendation"] == "fix_close_line_lineage_before_trusting_clv"
