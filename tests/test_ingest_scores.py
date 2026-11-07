import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
import requests

from src.data import ingest_scores, score_backfill
from src.db.core import connect, initialize
from src.predict.paper_ledger import SCHEMA_PATH as PAPER_SCHEMA_PATH
from src.predict.paper_settlement import settle_open_trials, unreconciled_grades


def test_score_fetch_logs_status_without_credentials(monkeypatch, caplog):
    keys = ["first-test-credential", "second-test-credential"]
    active = keys.copy()
    manager = ingest_scores.OddsAPIKeyManager
    monkeypatch.setattr(ingest_scores.OddsAPISettings, "from_env", lambda _: ingest_scores.OddsAPISettings(keys[0]))
    monkeypatch.setattr(manager, "get_available_keys", lambda: keys)
    monkeypatch.setattr(manager, "get_current_key", lambda: active[0])
    monkeypatch.setattr(manager, "rotate_key", lambda: active.pop(0))

    def get(url, *, params, timeout):
        response = requests.Response()
        response.status_code = 401 if params["apiKey"] == keys[0] else 503
        response.url = f"{url}?apiKey={params['apiKey']}"
        return response

    monkeypatch.setattr(ingest_scores.requests, "get", get)

    assert ingest_scores.fetch_scores("NBA") == []
    assert "401 Unauthorized" in caplog.text
    assert "status=503" in caplog.text
    assert "credential" not in caplog.text


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


TIPOFF = datetime(2026, 11, 5, 1, tzinfo=timezone.utc)
FINAL_AT = TIPOFF + timedelta(hours=4)
PROVIDER_ID = "05200821a78e4f6db8131cff1652550f"
HOME, AWAY = "Washington Wizards", "Atlanta Hawks"


def _nba_event(at=FINAL_AT, *, home_score=105, away_score=120, completed=True, **overrides):
    return {
        "id": PROVIDER_ID,
        "sport_key": "basketball_nba",
        "commence_time": TIPOFF.isoformat(),
        "home_team": HOME,
        "away_team": AWAY,
        "completed": completed,
        "last_update": at.isoformat(),
        "scores": [
            {"name": HOME, "score": str(home_score)},
            {"name": AWAY, "score": str(away_score)},
        ],
        **overrides,
    }


@pytest.fixture
def nba_warehouse(tmp_path, monkeypatch):
    path = tmp_path / "nba.db"
    initialize(path)
    monkeypatch.setenv("SPORTS_DB_PATH", str(path))
    with connect(path) as conn:
        conn.executescript(PAPER_SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sports (sport_id, name, league, default_market)
            VALUES (1, 'Basketball', 'NBA', 'totals');
            INSERT INTO teams (team_id, sport_id, code, name)
            VALUES (1, 1, 'WAS', 'Washington Wizards'), (2, 1, 'ATL', 'Atlanta Hawks');
            INSERT INTO books (book_id, name) VALUES (1, 'DraftKings');
            """
        )
        conn.execute(
            """
            INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id,
                               status, odds_api_id)
            VALUES ('fixture', 1, ?, 1, 2, 'scheduled', ?)
            """,
            (TIPOFF.isoformat(), PROVIDER_ID),
        )
        conn.execute(
            """
            INSERT INTO game_results (game_id, total_close, total_close_snapshot_id,
                                      total_close_source, source_version)
            VALUES ('fixture', 226.5, 'close-snapshot', 'the-odds-api', 'the-odds-api')
            """
        )
        captured = (TIPOFF - timedelta(hours=1)).isoformat()
        conn.execute("INSERT INTO paper_trials VALUES ('trial', ?, '{}')", (captured,))
        conn.execute(
            """
            INSERT INTO paper_decisions VALUES (
                'trial', 'fixture', 1, 2, ?, ?, ?, 'the-odds-api', 'entry', ?,
                1, 'DraftKings', 'over', 225.5, -110, -110, 0.75, 0.5, 0.25
            )
            """,
            (TIPOFF.isoformat(), captured, captured, captured),
        )
    return path


def _nba_result(conn):
    return tuple(
        conn.execute(
            """
        SELECT g.status, r.home_score, r.away_score, r.source_version
        FROM games g JOIN game_results r USING (game_id) WHERE g.game_id = 'fixture'
        """
        ).fetchone()
    )


def test_nba_provider_clock_keeps_corrections_retractions_and_recovery(nba_warehouse):
    early = _nba_event()
    correction = _nba_event(FINAL_AT + timedelta(minutes=1), home_score=106)
    conflicting = {
        **correction,
        "scores": early["scores"],
        "last_update": correction["last_update"].replace("+00:00", "Z"),
    }
    corroborated_at = FINAL_AT + timedelta(minutes=1, seconds=30)
    retraction = _nba_event(FINAL_AT + timedelta(minutes=2), completed=False, home_score=99)
    recovered = _nba_event(FINAL_AT + timedelta(minutes=3), home_score=107)
    review = FINAL_AT + timedelta(hours=1)

    assert ingest_scores.update_database([early], "NBA") == 1
    with connect(nba_warehouse) as conn:
        assert settle_open_trials(conn, review) == 1
    assert ingest_scores.update_database([correction], "NBA") == 1
    assert ingest_scores.update_database([early], "NBA") == 0
    assert ingest_scores.update_database([conflicting], "NBA") == 0
    assert ingest_scores.update_database([_nba_event(corroborated_at, home_score=106)], "NBA") == 0
    assert (
        ingest_scores.update_database(
            [_nba_event(corroborated_at - timedelta(seconds=1), home_score=103)], "NBA"
        )
        == 0
    )
    with connect(nba_warehouse) as conn:
        assert _nba_result(conn) == ("final", 106, 120, "the-odds-api-scores")
        assert settle_open_trials(conn, review + timedelta(minutes=1)) == 1
    assert ingest_scores.update_database([retraction], "NBA") == 1
    assert ingest_scores.update_database([correction], "NBA") == 0
    with connect(nba_warehouse) as conn:
        assert _nba_result(conn) == ("in_progress", None, None, "the-odds-api-scores")
        assert unreconciled_grades(conn, "trial", review + timedelta(minutes=2)) == 1
        assert settle_open_trials(conn, review + timedelta(minutes=2)) == 1
        assert conn.execute("SELECT COUNT(*) FROM paper_settlements").fetchone()[0] == 0
    assert ingest_scores.update_database([recovered], "NBA") == 1
    assert ingest_scores.update_database([recovered], "NBA") == 0
    with connect(nba_warehouse) as conn:
        assert _nba_result(conn) == ("final", 107, 120, "the-odds-api-scores")
        assert settle_open_trials(conn, review + timedelta(minutes=3)) == 1
        assert [
            row["outcome"]
            for row in conn.execute(
                "SELECT outcome FROM paper_settlement_history ORDER BY revision_id"
            )
        ] == ["loss", "win", "pending", "win"]
        state = conn.execute("SELECT * FROM nba_odds_api_score_state").fetchone()
        assert (
            state["last_update_utc"],
            state["completed"],
            state["home_score"],
            state["applied"],
        ) == ((FINAL_AT + timedelta(minutes=3)).isoformat(timespec="microseconds"), 1, 107, 1)
        assert tuple(
            conn.execute(
                "SELECT total_close, total_close_snapshot_id, total_close_source FROM game_results"
            ).fetchone()
        ) == (226.5, "close-snapshot", "the-odds-api")


@pytest.mark.parametrize(
    ("source", "away_score"),
    [
        (None, 120),
        ("the-odds-api", 120),
        ("nba_api", 120),
        ("espn_scoreboard", 120),
        ("nba_api", None),
    ],
)
def test_nba_clock_never_claims_legacy_or_independent_scores(nba_warehouse, source, away_score):
    with connect(nba_warehouse) as conn:
        conn.execute("UPDATE games SET status = 'final'")
        conn.execute(
            "UPDATE game_results SET home_score = 105, away_score = ?, source_version = ?",
            (away_score, source),
        )
    assert ingest_scores.update_database([_nba_event()], "NBA") == 0
    assert (
        ingest_scores.update_database(
            [_nba_event(FINAL_AT + timedelta(minutes=1), home_score=106)], "NBA"
        )
        == 0
    )
    assert (
        ingest_scores.update_database(
            [_nba_event(FINAL_AT + timedelta(minutes=2), completed=False, scores=None)], "NBA"
        )
        == 0
    )
    with connect(nba_warehouse) as conn:
        assert _nba_result(conn) == ("final", 105, away_score, source)
        assert tuple(
            conn.execute("SELECT completed, applied FROM nba_odds_api_score_state").fetchone()
        ) == (0, 0)


def test_nba_provider_does_not_retract_final_after_espn_takes_ownership(nba_warehouse):
    assert ingest_scores.update_database([_nba_event()], "NBA") == 1
    assert (
        score_backfill.update_scores_in_db(
            [
                {
                    "home_team": "WAS",
                    "away_team": "ATL",
                    "home_score": 105,
                    "away_score": 120,
                    "date": TIPOFF.isoformat(),
                    "espn_id": "espn-fixture",
                }
            ],
            "NBA",
        )
        == 1
    )
    assert (
        ingest_scores.update_database(
            [_nba_event(FINAL_AT + timedelta(minutes=1), completed=False, scores=None)], "NBA"
        )
        == 0
    )
    with connect(nba_warehouse) as conn:
        assert _nba_result(conn) == ("final", 105, 120, "espn_scoreboard")
        assert conn.execute("SELECT applied FROM nba_odds_api_score_state").fetchone()[0] == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"sport_key": "basketball_ncaab"},
        {"home_team": AWAY, "away_team": HOME},
        {"home_team": "Wrong Asset"},  # Heuristic normalization alone could yield WAS.
        {"last_update": "2026-11-05T05:00:00"},
        {"last_update": None},
        {"last_update": "2999-01-01T00:00:00+00:00"},
        {"last_update": TIPOFF.isoformat()},
        {"commence_time": None},
        {"completed": None},
        {"completed": 1},
        {"scores": None},
    ],
)
def test_nba_updater_requires_valid_clock_state_and_ordered_identity(nba_warehouse, overrides):
    assert ingest_scores.update_database([_nba_event(**overrides)], "NBA") == 0
    with connect(nba_warehouse) as conn:
        assert _nba_result(conn) == ("scheduled", None, None, "the-odds-api")
        assert conn.execute("SELECT COUNT(*) FROM nba_odds_api_score_state").fetchone()[0] == 0


def test_nba_updater_rejects_ambiguous_event_ids(nba_warehouse):
    event = _nba_event()
    assert ingest_scores.update_database([event, event], "NBA") == 0
    with connect(nba_warehouse) as conn:
        conn.execute(
            """
            INSERT INTO games (game_id, sport_id, home_team_id, away_team_id, odds_api_id)
            VALUES ('alias', 1, 1, 2, ?)
            """,
            (PROVIDER_ID,),
        )
    assert ingest_scores.update_database([event], "NBA") == 0
    with connect(nba_warehouse) as conn:
        assert _nba_result(conn) == ("scheduled", None, None, "the-odds-api")
        assert conn.execute("SELECT COUNT(*) FROM nba_odds_api_score_state").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("kickoff_offset", "update_offset"),
    [
        (timedelta(hours=-4), timedelta(minutes=-1)),
        (timedelta(hours=-4), timedelta(hours=4)),
        (timedelta(minutes=-10), timedelta(0)),
        (timedelta(minutes=10, microseconds=1), timedelta(hours=4)),
    ],
)
def test_nba_final_score_requires_current_fixture_clock(nba_warehouse, kickoff_offset, update_offset):
    stale = _nba_event(
        TIPOFF + update_offset,
        home_score=110,
        commence_time=(TIPOFF + kickoff_offset).isoformat(),
    )
    assert ingest_scores.update_database([stale], "NBA") == 0
    with connect(nba_warehouse) as conn:
        assert _nba_result(conn) == ("scheduled", None, None, "the-odds-api")
        assert conn.execute("SELECT COUNT(*) FROM nba_odds_api_score_state").fetchone()[0] == 0
        assert settle_open_trials(conn, FINAL_AT + timedelta(minutes=10)) == 0

    final = _nba_event(
        FINAL_AT - timedelta(minutes=1),
        commence_time=(TIPOFF - timedelta(minutes=10)).isoformat(),
    )
    assert ingest_scores.update_database([final], "NBA") == 1
    with connect(nba_warehouse) as conn:
        assert conn.execute("SELECT start_time_utc FROM games").fetchone()[0] == TIPOFF.isoformat()
        assert _nba_result(conn) == ("final", 105, 120, "the-odds-api-scores")
        assert settle_open_trials(conn, FINAL_AT + timedelta(minutes=11)) == 1
        assert conn.execute("SELECT outcome FROM paper_settlements").fetchone()[0] == "loss"
