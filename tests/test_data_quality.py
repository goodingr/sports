"""Regression tests for data-quality checks and orphan pruning.

These tests build a tiny synthetic SQLite database that mirrors the schema the
production checks query, so they are independent of the live betting.db state.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.data import quality

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
    odds_api_id TEXT,
    espn_id TEXT
);
CREATE TABLE game_results (
    game_id TEXT PRIMARY KEY,
    home_score INTEGER,
    away_score INTEGER,
    source_version TEXT
);
CREATE TABLE odds_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    fetched_at_utc TEXT NOT NULL,
    sport_id INTEGER NOT NULL
);
CREATE TABLE odds (
    snapshot_id TEXT,
    game_id TEXT,
    market TEXT,
    price_american REAL
);
CREATE TABLE predictions (
    prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    model_type TEXT,
    predicted_at TEXT
);
"""


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "test_quality.db"
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)

    sports = [(1, "NBA"), (2, "NFL"), (3, "EPL")]
    conn.executemany("INSERT INTO sports (sport_id, league) VALUES (?, ?)", sports)
    conn.executemany(
        "INSERT INTO teams (team_id, sport_id, code) VALUES (?, ?, ?)",
        [(10, 1, "LAL"), (11, 1, "BOS"), (20, 2, "DAL"), (21, 2, "PHI"), (30, 3, "ARS"), (31, 3, "CHE")],
    )
    conn.commit()
    conn.close()
    return path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Orphan checks
# ---------------------------------------------------------------------------


def _seed_orphans(db_path: Path) -> None:
    """Insert orphan game_results across leagues and a legacy bare-id row."""
    with _connect(db_path) as conn:
        # NBA-prefixed orphan (release league) — should fail when scoping NBA.
        conn.execute(
            "INSERT INTO game_results (game_id, home_score, away_score, source_version) "
            "VALUES ('NBA_20260101_LAL_BOS', 100, 99, 'espn_scoreboard')"
        )
        # NFL-prefixed orphan — should NOT count when scope is NBA-only.
        conn.execute(
            "INSERT INTO game_results (game_id, home_score, away_score, source_version) "
            "VALUES ('NFL_abc123', 21, 14, 'the-odds-api')"
        )
        # Bare-id legacy orphan (no league prefix) — excluded from scoped runs.
        conn.execute(
            "INSERT INTO game_results (game_id, home_score, away_score, source_version) "
            "VALUES ('aa01b4552005af5ec228f330135f870c', 82, 77, NULL)"
        )
        conn.commit()


def test_orphan_results_unscoped_counts_everything(db_path: Path) -> None:
    _seed_orphans(db_path)
    with _connect(db_path) as conn:
        result = quality.check_orphan_results(conn)
    assert result.name == "orphan_results"
    assert result.count == 3
    assert result.passed is False
    # Detail must include per-prefix breakdown so operators see where orphans live.
    assert "NBA" in result.detail and "NFL" in result.detail


def test_orphan_results_scoped_to_league_excludes_other_prefixes(db_path: Path) -> None:
    _seed_orphans(db_path)
    with _connect(db_path) as conn:
        result = quality.check_orphan_results(conn, leagues=["NBA"])
    # Only the NBA-prefixed orphan; NFL and bare-hex are out of scope.
    assert result.count == 1
    assert result.passed is False


def test_orphan_results_passes_when_no_release_league_orphans(db_path: Path) -> None:
    """NFL/bare-hex orphans must not fail readiness for an EPL/NBA-only launch."""
    _seed_orphans(db_path)
    with _connect(db_path) as conn:
        # Delete the NBA orphan — only NFL + bare-hex remain.
        conn.execute("DELETE FROM game_results WHERE game_id = 'NBA_20260101_LAL_BOS'")
        conn.commit()
        result = quality.check_orphan_results(conn, leagues=["NBA", "EPL"])
    assert result.count == 0
    assert result.passed is True


def test_orphan_predictions_scoped_filter(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO predictions (game_id, model_type, predicted_at) "
            "VALUES ('NBA_orphan_pred', 'ensemble', '2026-04-27T00:00:00+00:00')"
        )
        conn.execute(
            "INSERT INTO predictions (game_id, model_type, predicted_at) "
            "VALUES ('NFL_orphan_pred', 'ensemble', '2026-04-27T00:00:00+00:00')"
        )
        conn.commit()
        scoped = quality.check_orphan_predictions(conn, leagues=["NBA"])
        unscoped = quality.check_orphan_predictions(conn)
    assert scoped.count == 1
    assert unscoped.count == 2


# ---------------------------------------------------------------------------
# Stale games & missing scores
# ---------------------------------------------------------------------------


def _insert_game(
    conn: sqlite3.Connection,
    *,
    game_id: str,
    sport_id: int,
    start_time: datetime,
    status: str = "scheduled",
    home_team_id: int | None = None,
    away_team_id: int | None = None,
) -> None:
    conn.execute(
        "INSERT INTO games (game_id, sport_id, start_time_utc, status, home_team_id, away_team_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (game_id, sport_id, _iso(start_time), status, home_team_id, away_team_id),
    )


def test_stale_games_flags_only_release_leagues(db_path: Path) -> None:
    now = datetime.now(timezone.utc)
    with _connect(db_path) as conn:
        _insert_game(conn, game_id="NBA_old_unfinal", sport_id=1, start_time=now - timedelta(hours=12))
        _insert_game(conn, game_id="NBA_recent", sport_id=1, start_time=now - timedelta(hours=2))
        _insert_game(conn, game_id="NBA_old_final", sport_id=1, start_time=now - timedelta(hours=12), status="final")
        _insert_game(conn, game_id="NFL_old_unfinal", sport_id=2, start_time=now - timedelta(hours=12))
        conn.commit()

        scoped = quality.check_stale_games(conn, leagues=["NBA"], stale_hours=6)
        unscoped = quality.check_stale_games(conn, leagues=None, stale_hours=6)

    assert scoped.count == 1  # only NBA_old_unfinal
    assert scoped.passed is False
    assert "NBA=1" in scoped.detail
    assert unscoped.count == 2


def test_missing_scores_only_counts_old_games_without_scores(db_path: Path) -> None:
    now = datetime.now(timezone.utc)
    with _connect(db_path) as conn:
        _insert_game(conn, game_id="NBA_with_score", sport_id=1, start_time=now - timedelta(hours=12), status="final")
        _insert_game(conn, game_id="NBA_no_score", sport_id=1, start_time=now - timedelta(hours=12))
        _insert_game(conn, game_id="NBA_recent_no_score", sport_id=1, start_time=now - timedelta(hours=1))
        conn.execute(
            "INSERT INTO game_results (game_id, home_score, away_score) VALUES ('NBA_with_score', 110, 105)"
        )
        conn.commit()

        result = quality.check_missing_scores(conn, leagues=["NBA"], stale_hours=6)

    assert result.count == 1  # only NBA_no_score (recent one is < 6h stale_hours)
    assert "NBA=1" in result.detail


def test_finalize_scored_games_marks_stale_scored_rows_final(db_path: Path) -> None:
    now = datetime.now(timezone.utc)
    with _connect(db_path) as conn:
        _insert_game(conn, game_id="NBA_scored_scheduled", sport_id=1, start_time=now - timedelta(hours=12))
        conn.execute(
            "INSERT INTO game_results (game_id, home_score, away_score) VALUES ('NBA_scored_scheduled', 110, 105)"
        )
        conn.commit()

    updated = quality.finalize_scored_games(db_path, leagues=["NBA"], stale_hours=6)

    with _connect(db_path) as conn:
        status = conn.execute(
            "SELECT status FROM games WHERE game_id = 'NBA_scored_scheduled'"
        ).fetchone()["status"]
    assert updated == 1
    assert status == "final"


def test_close_unresolved_stale_games_excludes_rows_from_readiness(db_path: Path) -> None:
    now = datetime.now(timezone.utc)
    with _connect(db_path) as conn:
        _insert_game(conn, game_id="NBA_unresolved_old", sport_id=1, start_time=now - timedelta(hours=12))
        conn.commit()

    before = quality.run_checks(db_path, leagues=["NBA"], stale_hours=6)
    assert any(result.name == "missing_scores" and result.count == 1 for result in before)

    updated = quality.close_unresolved_stale_games(db_path, leagues=["NBA"], stale_hours=6)
    after = quality.run_checks(db_path, leagues=["NBA"], stale_hours=6)

    with _connect(db_path) as conn:
        status = conn.execute(
            "SELECT status FROM games WHERE game_id = 'NBA_unresolved_old'"
        ).fetchone()["status"]
    assert updated == 1
    assert status == "closed_missing_score"
    assert all(result.count == 0 for result in after if result.name in {"stale_games", "missing_scores"})


# ---------------------------------------------------------------------------
# Odds freshness
# ---------------------------------------------------------------------------


def test_odds_freshness_passes_when_no_future_games(db_path: Path) -> None:
    """A league that has no upcoming games shouldn't fail freshness."""
    now = datetime.now(timezone.utc)
    with _connect(db_path) as conn:
        # Past-only NBA games => no future games => freshness check skips this league.
        _insert_game(conn, game_id="NBA_past", sport_id=1, start_time=now - timedelta(days=1), status="final")
        conn.commit()
        result = quality.check_odds_freshness(conn, leagues=["NBA"], max_age_hours=12)
    assert result.passed is True
    assert result.count == 0


def test_odds_freshness_fails_when_snapshots_too_old(db_path: Path) -> None:
    now = datetime.now(timezone.utc)
    with _connect(db_path) as conn:
        _insert_game(conn, game_id="NBA_future", sport_id=1, start_time=now + timedelta(days=1))
        conn.execute(
            "INSERT INTO odds_snapshots (snapshot_id, fetched_at_utc, sport_id) VALUES ('s1', ?, 1)",
            (_iso(now - timedelta(hours=24)),),
        )
        conn.commit()
        result = quality.check_odds_freshness(conn, leagues=["NBA"], max_age_hours=12)
    assert result.passed is False
    assert "NBA" in result.detail


def test_odds_freshness_passes_when_snapshot_recent(db_path: Path) -> None:
    now = datetime.now(timezone.utc)
    with _connect(db_path) as conn:
        _insert_game(conn, game_id="NBA_future", sport_id=1, start_time=now + timedelta(days=1))
        conn.execute(
            "INSERT INTO odds_snapshots (snapshot_id, fetched_at_utc, sport_id) VALUES ('s1', ?, 1)",
            (_iso(now - timedelta(hours=2)),),
        )
        conn.commit()
        result = quality.check_odds_freshness(conn, leagues=["NBA"], max_age_hours=12)
    assert result.passed is True


# ---------------------------------------------------------------------------
# Future-games-without-odds
# ---------------------------------------------------------------------------


def test_future_games_without_odds(db_path: Path) -> None:
    now = datetime.now(timezone.utc)
    with _connect(db_path) as conn:
        _insert_game(conn, game_id="NBA_with_odds", sport_id=1, start_time=now + timedelta(days=1))
        _insert_game(conn, game_id="NBA_no_odds", sport_id=1, start_time=now + timedelta(days=2))
        conn.execute(
            "INSERT INTO odds (snapshot_id, game_id, market, price_american) VALUES ('s1', 'NBA_with_odds', 'h2h', -110)"
        )
        conn.commit()
        result = quality.check_future_games_without_odds(conn, leagues=["NBA"], window_days=14)
    assert result.count == 1
    assert result.passed is False


# ---------------------------------------------------------------------------
# prune_orphan_results
# ---------------------------------------------------------------------------


def test_prune_orphan_results_unscoped_removes_everything(db_path: Path) -> None:
    _seed_orphans(db_path)
    # Add a non-orphan to ensure it is preserved.
    with _connect(db_path) as conn:
        _insert_game(
            conn,
            game_id="NBA_real",
            sport_id=1,
            start_time=datetime.now(timezone.utc) - timedelta(days=1),
            status="final",
        )
        conn.execute(
            "INSERT INTO game_results (game_id, home_score, away_score) VALUES ('NBA_real', 110, 100)"
        )
        conn.commit()

    deleted = quality.prune_orphan_results(db_path)
    assert deleted == 3

    with _connect(db_path) as conn:
        remaining = conn.execute("SELECT game_id FROM game_results ORDER BY game_id").fetchall()
    assert [row["game_id"] for row in remaining] == ["NBA_real"]


def test_prune_orphan_results_scoped_only_targets_release_leagues(db_path: Path) -> None:
    _seed_orphans(db_path)

    deleted = quality.prune_orphan_results(db_path, leagues=["NBA"])
    assert deleted == 1  # only NBA-prefixed orphan removed

    with _connect(db_path) as conn:
        remaining_ids = {
            row["game_id"]
            for row in conn.execute("SELECT game_id FROM game_results").fetchall()
        }
    # NFL orphan and bare-hex orphan must be left alone — they are out of scope.
    assert "NBA_20260101_LAL_BOS" not in remaining_ids
    assert "NFL_abc123" in remaining_ids
    assert "aa01b4552005af5ec228f330135f870c" in remaining_ids


# ---------------------------------------------------------------------------
# run_checks integration
# ---------------------------------------------------------------------------


def test_run_checks_returns_seven_named_results(db_path: Path) -> None:
    results = quality.run_checks(db_path, leagues=["NBA"])
    names = [r.name for r in results]
    assert names == [
        "orphan_results",
        "orphan_predictions",
        "duplicate_games",
        "stale_games",
        "missing_scores",
        "odds_freshness",
        "future_games_without_odds",
    ]


def test_format_summary_marks_pass_or_fail(db_path: Path) -> None:
    _seed_orphans(db_path)
    results = quality.run_checks(db_path, leagues=["NBA"])
    summary = quality.format_summary(results)
    assert "DATA QUALITY SUMMARY" in summary
    assert "FAIL orphan_results" in summary
