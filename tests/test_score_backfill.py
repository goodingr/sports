"""Regression tests for the ESPN score backfill utility."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

import pytest

from src.data import score_backfill
from src.db.core import connect, initialize
from src.predict.published_ledger import publish_release, published_record

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
    source_version TEXT,
    total_close REAL,
    total_close_snapshot_id TEXT
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


@pytest.mark.parametrize("unbound_id", [None, ""])
def test_update_scores_in_db_matches_by_team_codes_and_date(
    db_with_one_game: Path, unbound_id: str | None,
) -> None:
    with connect(db_with_one_game) as conn:
        conn.execute("UPDATE games SET espn_id = ?", (unbound_id,))
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


def test_score_backfill_preserves_voided_game(db_with_one_game: Path) -> None:
    with sqlite3.connect(db_with_one_game) as conn:
        conn.execute("UPDATE games SET status = 'voided'")
    parsed = [{
        "home_team": "LAL", "away_team": "BOS", "home_score": 0, "away_score": 0,
        "date": "2026-04-27T01:40:00Z", "espn_id": "401584321",
    }]
    assert score_backfill.update_scores_in_db(parsed, "NBA") == 0
    with sqlite3.connect(db_with_one_game) as conn:
        assert conn.execute("SELECT status FROM games").fetchone()[0] == "voided"
        assert conn.execute("SELECT count(*) FROM game_results").fetchone()[0] == 0


def _score_event(**overrides):
    return {
        "home_team": "LAL", "away_team": "BOS", "home_score": 115, "away_score": 110,
        "date": "2026-04-27T01:40:00Z", "espn_id": "espn-fixture", **overrides,
    }


def test_exact_espn_id_wins_over_a_closer_unbound_candidate(db_with_one_game: Path) -> None:
    with connect(db_with_one_game) as conn:
        conn.execute("UPDATE games SET espn_id = 'espn-fixture', start_time_utc = '2026-04-27T01:49:00Z'")
        conn.execute("INSERT INTO games VALUES ('unbound', 1, '2026-04-27T01:40:00Z', 10, 11, 'scheduled', NULL)")

    assert score_backfill.update_scores_in_db([_score_event()], "NBA") == 1
    with connect(db_with_one_game) as conn:
        assert conn.execute("SELECT game_id FROM game_results").fetchone()[0] == "NBA_20260427_LAL_BOS"


def test_conflicting_exact_espn_id_does_not_fall_back_to_an_unbound_row(db_with_one_game: Path) -> None:
    with connect(db_with_one_game) as conn:
        conn.execute("UPDATE games SET espn_id = 'espn-fixture', home_team_id = 11, away_team_id = 10")
        conn.execute("INSERT INTO games VALUES ('unbound', 1, '2026-04-27T01:40:00Z', 10, 11, 'scheduled', NULL)")

    assert score_backfill.update_scores_in_db([_score_event()], "NBA") == 0
    with connect(db_with_one_game) as conn:
        assert conn.execute("SELECT count(*) FROM game_results").fetchone()[0] == 0


@pytest.mark.parametrize("alias_id", [None, "espn-fixture"])
def test_ambiguous_warehouse_fixture_is_never_selected(db_with_one_game: Path, alias_id) -> None:
    with connect(db_with_one_game) as conn:
        conn.execute("UPDATE games SET espn_id = ?", (alias_id,))
        conn.execute(
            "INSERT INTO games VALUES ('alias', 1, '2026-04-27T01:41:00Z', 10, 11, 'scheduled', ?)",
            (alias_id,),
        )

    assert score_backfill.update_scores_in_db([_score_event()], "NBA") == 0
    with connect(db_with_one_game) as conn:
        assert conn.execute("SELECT count(*) FROM game_results").fetchone()[0] == 0


def test_two_provider_ids_cannot_claim_one_unbound_fixture(db_with_one_game: Path) -> None:
    assert score_backfill.update_scores_in_db(
        [_score_event(), _score_event(espn_id="other", home_score=101)], "NBA",
    ) == 0
    with connect(db_with_one_game) as conn:
        assert conn.execute("SELECT espn_id FROM games").fetchone()[0] is None


def test_conflicting_espn_id_cannot_replace_existing_binding(db_with_one_game: Path) -> None:
    with connect(db_with_one_game) as conn:
        conn.execute("UPDATE games SET espn_id = 'original'")
    assert score_backfill.update_scores_in_db([_score_event()], "NBA") == 0
    with connect(db_with_one_game) as conn:
        assert conn.execute("SELECT espn_id, status FROM games").fetchone()[:] == ("original", "scheduled")


@pytest.mark.parametrize(
    ("game_start", "event_start", "updates"),
    [
        ("2026-04-27 01:50:00", "2026-04-26T21:40:00-04:00", 1),
        ("2026-04-27T01:50:00.000001Z", "2026-04-27T01:40:00Z", 0),
        ("2026-04-27", "2026-04-27T00:00:00Z", 0),
        ("2026-04-27T01:40:00Z", "2026-04-27T01:40:00", 0),
    ],
)
def test_short_exact_time_window_supports_legacy_utc_starts(
    db_with_one_game: Path, game_start: str, event_start: str, updates: int,
) -> None:
    with connect(db_with_one_game) as conn:
        conn.execute("UPDATE games SET start_time_utc = ?", (game_start,))
    assert score_backfill.update_scores_in_db([_score_event(date=event_start)], "NBA") == updates


def test_later_fixture_cannot_grade_an_issued_pick(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "issued.db"
    initialize(path)
    start = datetime(2026, 4, 27, 1, 40, tzinfo=timezone.utc)
    published_at = start - timedelta(hours=2)
    with connect(path) as conn:
        conn.execute("INSERT INTO sports (sport_id, name, league, default_market) VALUES (1,'NBA','NBA','totals')")
        conn.execute("INSERT INTO teams (team_id, sport_id, code, name) VALUES (1,1,'LAL','Lakers'),(2,1,'BOS','Celtics')")
        conn.execute(
            """
            INSERT INTO games (game_id, sport_id, home_team_id, away_team_id, start_time_utc, espn_id)
            VALUES ('issued', 1, 1, 2, ?, 'espn-original')
            """,
            (start.isoformat(),),
        )
        conn.execute("INSERT INTO books (book_id, name) VALUES (1,'DraftKings')")
        conn.execute("INSERT INTO odds_snapshots VALUES ('quote',?,1,'the-odds-api',NULL)", (published_at.isoformat(),))
        conn.execute(
            """
            INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
            VALUES ('quote','issued',1,'totals','Over',100,220), ('quote','issued',1,'totals','Under',100,220)
            """
        )
    release = publish_release(
        {
            "generated_at": published_at.isoformat(), "passing_approved_rule_ids": ["fixture"],
            "bets": [{
                "rule_id": "fixture", "game_id": "issued", "market": "totals",
                "start_time_utc": start.isoformat(), "predicted_at": published_at.isoformat(),
                "side": "over", "total_line": 220.0, "moneyline": 100, "model_probability": 0.6,
                "quote_snapshot_id": "quote", "quote_book_id": 1, "quality_summary": {"passes_launch_gate": True},
            }],
        },
        path, commit_at=published_at,
    )
    assert release["bet_count"] == 1

    monkeypatch.setattr(score_backfill, "connect", lambda: connect(path))
    later_score = _score_event(date=(start + timedelta(hours=18)).isoformat(), espn_id="espn-later")
    assert score_backfill.update_scores_in_db([later_score], "NBA", only_missing=True) == 0
    review = start + timedelta(hours=23)
    publish_release({"generated_at": review.isoformat(), "bets": []}, path, commit_at=review)
    record = published_record(path)
    assert (record["pending_picks"], record["wins"], record["profit_units"], record["roi_percent"]) == (1, 0, 0, None)
    with connect(path) as conn:
        assert conn.execute("SELECT espn_id FROM games").fetchone()[0] == "espn-original"


def test_stale_score_resolution_preserves_existing_and_concurrent_finals(
    db_with_one_game: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    existing = "NBA_20260427_LAL_BOS"
    tip = (datetime.now(timezone.utc) - timedelta(days=2)).replace(
        hour=0, minute=40, second=0, microsecond=0
    )
    target_date = tip.astimezone(ZoneInfo("America/New_York")).date()
    with sqlite3.connect(db_with_one_game) as conn:
        conn.execute(
            "UPDATE games SET status = 'final', espn_id = 'old-espn', start_time_utc = ?",
            (tip.isoformat(),),
        )
        conn.execute(
            "INSERT INTO teams VALUES (12, 1, 'CLE'), (13, 1, 'NYK'), (14, 1, 'ATL'), (15, 1, 'MIA')"
        )
        conn.executemany(
            "INSERT INTO games VALUES (?, 1, ?, ?, ?, 'scheduled', NULL)",
            [
                ("missing", tip.isoformat(), 12, 13),
                ("completed_during_fetch", tip.isoformat(), 14, 15),
            ],
        )
        conn.executemany(
            "INSERT INTO game_results VALUES (?, ?, ?, ?, ?, ?)",
            [(existing, 120, 110, "trusted", 221.5, "before"), ("missing", 98, None, "partial", 204.5, "other")],
        )

    def fetch(league, day):
        assert (league, day) == ("NBA", target_date)
        with sqlite3.connect(db_with_one_game) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO game_results VALUES ('completed_during_fetch', 130, 100, 'other-feed', 219.5, 'new')"
            )
            conn.execute(
                "UPDATE games SET status = 'final', espn_id = 'other-espn' WHERE game_id = 'completed_during_fetch'"
            )
        teams_and_scores = [
            ("Los Angeles Lakers", "Boston Celtics", "115", "100"),
            ("Cleveland Cavaliers", "New York Knicks", "108", "101"),
            ("Atlanta Hawks", "Miami Heat", "112", "105"),
        ]
        return {"events": [
            {
                "id": ("old-espn", "espn-1", "other-espn")[index],
                "date": tip.isoformat(),
                "status": {"type": {"completed": True, "state": "post"}},
                "competitions": [{"competitors": [
                    {"homeAway": "home", "team": {"displayName": home}, "score": home_score},
                    {"homeAway": "away", "team": {"displayName": away}, "score": away_score},
                ]}],
            }
            for index, (home, away, home_score, away_score) in enumerate(teams_and_scores)
        ]}

    monkeypatch.setattr(score_backfill, "fetch_espn_scoreboard", fetch)
    assert score_backfill.resolve_stale_scores(["NBA"]) == 1
    with sqlite3.connect(db_with_one_game) as conn:
        rows = conn.execute(
            """
            SELECT g.game_id, r.home_score, r.away_score, r.source_version,
                   r.total_close, r.total_close_snapshot_id, g.status, g.espn_id
            FROM games g JOIN game_results r USING(game_id)
            ORDER BY g.game_id
            """
        ).fetchall()
    assert rows == [
        (existing, 120, 110, "trusted", 221.5, "before", "final", "old-espn"),
        ("completed_during_fetch", 130, 100, "other-feed", 219.5, "new", "final", "other-espn"),
        ("missing", 108, 101, "espn_scoreboard", 204.5, "other", "final", "espn-1"),
    ]

    with sqlite3.connect(db_with_one_game) as conn:
        conn.execute("UPDATE games SET status = 'scheduled' WHERE game_id = ?", (existing,))
    assert score_backfill.stale_score_targets(["NBA"]) == []

    assert score_backfill.backfill_scores(["NBA"], target_date, target_date, pause_seconds=0) == 3
    with sqlite3.connect(db_with_one_game) as conn:
        row = conn.execute(
            """
            SELECT home_score, away_score, source_version, total_close, total_close_snapshot_id
            FROM game_results WHERE game_id = ?
            """,
            (existing,),
        ).fetchone()
    assert row == (115, 100, "espn_scoreboard", 221.5, "before")


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


@pytest.mark.parametrize("status", ["STATUS_ABANDONED", "STATUS_AWARDED", "STATUS_POSTPONED", "STATUS_SUSPENDED"])
def test_parse_espn_event_rejects_nonstandard_completed_game(status: str) -> None:
    event = {
        "status": {"type": {"completed": True, "state": "post", "name": "STATUS_FINAL"}},
        "competitions": [{
            "status": {"type": {"completed": True, "state": "post", "name": status}},
            "competitors": [
                {"homeAway": "home", "team": {"displayName": "Arsenal"}, "score": "0"},
                {"homeAway": "away", "team": {"displayName": "Chelsea"}, "score": "0"},
            ],
        }],
    }
    assert score_backfill.parse_espn_event(event, "EPL") is None
