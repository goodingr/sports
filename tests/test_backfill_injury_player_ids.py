from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.data import availability_quality
from src.data import backfill_injury_player_ids as backfill

AS_OF = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)

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
CREATE TABLE player_stats (
    stat_id INTEGER PRIMARY KEY,
    game_id TEXT NOT NULL,
    team_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    player_name TEXT,
    min REAL,
    pts INTEGER,
    reb INTEGER,
    ast INTEGER,
    stl INTEGER,
    blk INTEGER,
    tov INTEGER,
    pf INTEGER,
    plus_minus INTEGER
);
"""


def _create_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA)
        conn.execute("INSERT INTO sports VALUES (1, 'NBA')")
        conn.executemany(
            "INSERT INTO teams VALUES (?, 1, ?, ?)",
            [
                (1, "LAL", "Lakers"),
                (2, "BOS", "Celtics"),
            ],
        )
        conn.execute(
            """
            INSERT INTO games
            VALUES ('NBA_20260102_LAL_BOS', 1, '2026-01-02T00:00:00+00:00', 1, 2, 'scheduled')
            """
        )


def _create_db_without_player_id_column(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA.replace("    player_id TEXT,\n", ""))
        conn.execute("INSERT INTO sports VALUES (1, 'NBA')")
        conn.executemany(
            "INSERT INTO teams VALUES (?, 1, ?, ?)",
            [
                (1, "LAL", "Lakers"),
                (2, "BOS", "Celtics"),
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
    injury_id: int,
    team_id: int,
    team_code: str,
    player_name: str,
    player_id: str | None,
    source_key: str = "nba_injuries_espn",
) -> None:
    conn.execute(
        """
        INSERT INTO injury_reports (
            injury_id, league, sport_id, team_id, team_code, player_name, player_id,
            position, status, report_date, source_key
        ) VALUES (?, 'NBA', 1, ?, ?, ?, ?, 'G', 'Out', '2026-01-01T10:00:00+00:00', ?)
        """,
        (injury_id, team_id, team_code, player_name, player_id, source_key),
    )


def _insert_player_stat(
    conn: sqlite3.Connection,
    *,
    team_id: int,
    player_name: str,
    player_id: int,
) -> None:
    conn.execute(
        """
        INSERT INTO player_stats (
            game_id, team_id, player_id, player_name, min, pts, reb, ast, stl, blk, tov, pf, plus_minus
        ) VALUES ('NBA_20260102_LAL_BOS', ?, ?, ?, 10, 1, 1, 1, 0, 0, 0, 0, 0)
        """,
        (team_id, player_id, player_name),
    )


def _player_id(db_path: Path, injury_id: int) -> str | None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT player_id FROM injury_reports WHERE injury_id = ?",
            (injury_id,),
        ).fetchone()
    return row[0]


def test_exact_player_stats_match_writes_and_reduces_availability_mapping_gap(tmp_path: Path) -> None:
    db_path = tmp_path / "injuries.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            injury_id=1,
            team_id=1,
            team_code="LAL",
            player_name="Exact Player",
            player_id=None,
        )
        _insert_player_stat(conn, team_id=1, player_name="Exact Player", player_id=12345)

    before = availability_quality.build_availability_report(
        db_path=db_path,
        league="NBA",
        as_of=AS_OF,
    )
    assert before["missing_mappings"]["player_rows_missing_mapping"] == 1

    report = backfill.build_backfill_report(
        db_path=db_path,
        league="NBA",
        write=True,
        fetch_espn=False,
    )

    assert report["missing_rows"] == 1
    assert report["resolvable_rows"] == 1
    assert report["updated_rows"] == 1
    assert _player_id(db_path, 1) == "12345"

    after = availability_quality.build_availability_report(
        db_path=db_path,
        league="NBA",
        as_of=AS_OF,
    )
    assert after["missing_mappings"]["player_rows_missing_mapping"] == 0


def test_dry_run_reports_match_without_writing(tmp_path: Path) -> None:
    db_path = tmp_path / "injuries.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            injury_id=1,
            team_id=1,
            team_code="LAL",
            player_name="Dry Run Player",
            player_id=None,
        )
        _insert_player_stat(conn, team_id=1, player_name="Dry Run Player", player_id=222)

    report = backfill.build_backfill_report(
        db_path=db_path,
        league="NBA",
        write=False,
        fetch_espn=False,
    )

    assert report["dry_run"] is True
    assert report["resolvable_rows"] == 1
    assert report["updated_rows"] == 0
    assert _player_id(db_path, 1) is None


def test_ambiguous_name_team_match_is_not_written(tmp_path: Path) -> None:
    db_path = tmp_path / "injuries.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            injury_id=1,
            team_id=1,
            team_code="LAL",
            player_name="Ambiguous Player",
            player_id=None,
        )
        _insert_player_stat(conn, team_id=1, player_name="Ambiguous Player", player_id=111)
        _insert_player_stat(conn, team_id=1, player_name="Ambiguous Player", player_id=222)

    report = backfill.build_backfill_report(
        db_path=db_path,
        league="NBA",
        write=True,
        fetch_espn=False,
    )

    assert report["resolvable_rows"] == 0
    assert report["ambiguous_rows"] == 1
    assert report["updated_rows"] == 0
    assert _player_id(db_path, 1) is None
    assert {item["player_id"] for item in report["ambiguous"][0]["candidates"]} == {"111", "222"}


def test_unresolved_row_is_reported(tmp_path: Path) -> None:
    db_path = tmp_path / "injuries.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            injury_id=1,
            team_id=1,
            team_code="LAL",
            player_name="Missing Player",
            player_id=None,
        )

    report = backfill.build_backfill_report(
        db_path=db_path,
        league="NBA",
        write=True,
        fetch_espn=False,
    )

    assert report["resolvable_rows"] == 0
    assert report["unresolved_rows"] == 1
    assert report["unresolved"][0]["reason"] == "no_candidate"
    assert report["updated_rows"] == 0
    assert _player_id(db_path, 1) is None


def test_espn_active_injury_candidate_can_resolve_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "injuries.db"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            injury_id=1,
            team_id=1,
            team_code="LAL",
            player_name="ESPN Player",
            player_id=None,
        )

    def fake_espn_candidates(*, league: str, timeout: int):
        return (
            backfill._candidate_index_from_rows(
                [
                    {
                        "team_code": "LAL",
                        "player_name": "ESPN Player",
                        "player_id": "espn-777",
                    }
                ],
                league=league,
                source="espn_active_injuries",
            ),
            {"enabled": True, "source_rows": 1, "error": None},
        )

    monkeypatch.setattr(backfill, "_load_espn_active_injury_candidates", fake_espn_candidates)

    report = backfill.build_backfill_report(db_path=db_path, league="NBA", write=False)

    assert report["resolvable_rows"] == 1
    assert report["resolutions"][0]["player_id"] == "espn-777"
    assert report["espn_candidate_source"]["source_rows"] == 1


def test_cli_dry_run_writes_report_without_updating(tmp_path: Path) -> None:
    db_path = tmp_path / "injuries.db"
    output_path = tmp_path / "reports" / "backfill.json"
    _create_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _insert_injury(
            conn,
            injury_id=1,
            team_id=1,
            team_code="LAL",
            player_name="Cli Player",
            player_id=None,
        )
        _insert_player_stat(conn, team_id=1, player_name="Cli Player", player_id=333)

    exit_code = backfill.main(
        [
            "--db-path",
            str(db_path),
            "--league",
            "NBA",
            "--dry-run",
            "--skip-espn-fetch",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert output_path.exists()
    assert _player_id(db_path, 1) is None


def test_write_adds_missing_player_id_column_before_backfill(tmp_path: Path) -> None:
    db_path = tmp_path / "injuries.db"
    _create_db_without_player_id_column(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO injury_reports (
                injury_id, league, sport_id, team_id, team_code, player_name,
                position, status, report_date, source_key
            ) VALUES (
                1, 'NBA', 1, 1, 'LAL', 'Legacy Player',
                'G', 'Out', '2026-01-01T10:00:00+00:00', 'nba_injuries_espn'
            )
            """
        )
        _insert_player_stat(conn, team_id=1, player_name="Legacy Player", player_id=444)

    dry_run = backfill.build_backfill_report(
        db_path=db_path,
        league="NBA",
        write=False,
        fetch_espn=False,
    )
    assert dry_run["player_id_column_present_before"] is False
    assert dry_run["resolvable_rows"] == 1

    report = backfill.build_backfill_report(
        db_path=db_path,
        league="NBA",
        write=True,
        fetch_espn=False,
    )

    assert report["player_id_column_present_before"] is False
    assert report["schema_migration_applied"] is True
    assert report["updated_rows"] == 1
    assert _player_id(db_path, 1) == "444"
