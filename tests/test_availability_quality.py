from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.data import availability_quality

SCHEMA = """
CREATE TABLE sports (
    sport_id INTEGER PRIMARY KEY,
    league TEXT NOT NULL UNIQUE
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
CREATE TABLE injury_reports (
    injury_id INTEGER PRIMARY KEY,
    league TEXT NOT NULL,
    sport_id INTEGER,
    team_id INTEGER,
    team_code TEXT,
    player_name TEXT,
    player_id TEXT,
    position TEXT,
    status TEXT,
    practice_status TEXT,
    report_date TEXT,
    game_date TEXT,
    detail TEXT,
    source_key TEXT NOT NULL,
    created_at TEXT
);
"""

AS_OF = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def _create_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA)
        conn.execute("INSERT INTO sports VALUES (1, 'NBA')")
        conn.executemany(
            "INSERT INTO teams VALUES (?, 1, ?, ?)",
            [
                (1, "LAL", "Lakers"),
                (2, "BOS", "Celtics"),
                (3, "NYK", "Knicks"),
            ],
        )
        conn.execute(
            """
            INSERT INTO games
            VALUES ('NBA_20260102_LAL_BOS', 1, '2026-01-02T00:00:00+00:00', 1, 2, 'scheduled')
            """
        )


def _insert_injury(
    conn: sqlite3.Connection,
    *,
    team_id: int | None,
    team_code: str | None,
    player_name: str,
    player_id: str | None,
    report_date: str,
) -> None:
    conn.execute(
        """
        INSERT INTO injury_reports (
            league, sport_id, team_id, team_code, player_name, player_id,
            position, status, report_date, source_key
        ) VALUES ('NBA', 1, ?, ?, ?, ?, 'G', 'Out', ?, 'test')
        """,
        (team_id, team_code, player_name, player_id, report_date),
    )


def test_good_availability_coverage_passes(tmp_path: Path) -> None:
    db_path = tmp_path / "availability.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            team_id=1,
            team_code="LAL",
            player_name="Laker Out",
            player_id="100",
            report_date="2026-01-01T10:00:00+00:00",
        )
        _insert_injury(
            conn,
            team_id=2,
            team_code="BOS",
            player_name="Celtic Out",
            player_id="200",
            report_date="2026-01-01T10:30:00+00:00",
        )

    report = availability_quality.build_availability_report(
        db_path=db_path,
        league="NBA",
        lookahead_days=7,
        max_stale_days=3,
        min_coverage=0.8,
        as_of=AS_OF,
    )

    assert report["passes_min_coverage"] is True
    assert report["upcoming_games"]["with_full_availability_rows"] == 1
    assert report["upcoming_games"]["coverage_percentage"] == 1.0
    assert report["coverage_by_date"][0]["coverage_percentage"] == 1.0
    assert report["missing_mappings"]["team_rows_missing_mapping"] == 0
    assert report["missing_mappings"]["player_rows_missing_mapping"] == 0


def test_stale_availability_rows_warn_and_do_not_count_as_coverage(tmp_path: Path) -> None:
    db_path = tmp_path / "availability.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            team_id=1,
            team_code="LAL",
            player_name="Old Laker",
            player_id="100",
            report_date="2025-12-20T10:00:00+00:00",
        )
        _insert_injury(
            conn,
            team_id=2,
            team_code="BOS",
            player_name="Old Celtic",
            player_id="200",
            report_date="2025-12-20T10:00:00+00:00",
        )

    report = availability_quality.build_availability_report(
        db_path=db_path,
        league="NBA",
        lookahead_days=7,
        max_stale_days=3,
        min_coverage=0.8,
        as_of=AS_OF,
    )

    assert report["passes_min_coverage"] is False
    assert report["upcoming_games"]["coverage_percentage"] == 0.0
    assert len(report["stale_availability_rows"]) == 2
    assert any("stale availability" in warning for warning in report["warnings"])


def test_missing_coverage_and_mapping_gaps_are_reported(tmp_path: Path) -> None:
    db_path = tmp_path / "availability.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            team_id=None,
            team_code="UNKNOWN TEAM",
            player_name="Unmapped Player",
            player_id=None,
            report_date="2026-01-01T10:00:00+00:00",
        )

    report = availability_quality.build_availability_report(
        db_path=db_path,
        league="NBA",
        lookahead_days=7,
        max_stale_days=3,
        min_coverage=0.8,
        as_of=AS_OF,
    )

    assert report["upcoming_games"]["covered_team_slots"] == 0
    assert report["missing_mappings"]["team_rows_missing_mapping"] == 1
    assert report["missing_mappings"]["player_rows_missing_mapping"] == 1
    assert report["coverage_by_date_team"][0]["has_availability_rows"] is False


def test_cli_exits_nonzero_only_when_enforced(tmp_path: Path) -> None:
    db_path = tmp_path / "availability.db"
    output_dir = tmp_path / "reports"
    _create_db(db_path)

    common_args = [
        "--db-path",
        str(db_path),
        "--league",
        "NBA",
        "--lookahead-days",
        "7",
        "--max-stale-days",
        "3",
        "--min-coverage",
        "0.8",
        "--as-of",
        AS_OF.isoformat(),
        "--output-dir",
        str(output_dir),
    ]

    assert availability_quality.main(common_args) == 0
    assert availability_quality.main([*common_args, "--enforce"]) == 1
