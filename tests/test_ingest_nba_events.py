"""Validate the free NBA schedule discovery with no authenticated requests."""

import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event

import pytest
import requests

from src.data import ingest_nba_events as discovery
from src.db.core import connect, initialize
from src.db.loaders import _generate_internal_id
from src.predict import paper_ledger

NOW = datetime(2026, 11, 7, 14, 5, tzinfo=timezone.utc)
TEAMS = [
    (1, "IND", "Indiana Pacers"),
    (2, "DEN", "Denver Nuggets"),
    (3, "ATL", "Atlanta Hawks"),
    (4, "BKN", "Brooklyn Nets"),
    (5, "BOS", "Boston Celtics"),
    (6, "NYK", "New York Knicks"),
    (7, "MIA", "Miami Heat"),
    (8, "ORL", "Orlando Magic"),
]
NAMES = {team_id: name for team_id, _, name in TEAMS}


def _id(index):
    return f"{index:032x}"


def _event(index, start, home_id, away_id):
    return {
        "id": _id(index),
        "sport_key": "basketball_nba",
        "sport_title": "NBA",
        "commence_time": start.isoformat(),
        "home_team": NAMES[home_id],
        "away_team": NAMES[away_id],
    }


def _game(conn, game_id, start, home_id, away_id, *, status="scheduled", provider_id=None):
    conn.execute(
        """
        INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, status,
                           odds_api_id, venue, espn_id)
        VALUES (?, 1, ?, ?, ?, ?, ?, 'Original arena', 'original-espn')
        """,
        (game_id, start.isoformat(), home_id, away_id, status, provider_id),
    )


def _response(payload, *, status=200, credits="0"):
    result = requests.Response()
    result.status_code = status
    if credits is not None:
        result.headers["x-requests-last"] = credits
    result.headers["x-requests-remaining"] = "127"
    result._content = json.dumps(payload).encode("utf-8")
    return result


def _unexpected(*args, **kwargs):
    pytest.fail("No real provider calls are allowed")


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    class Clock(datetime):
        current = NOW

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz)

    path = tmp_path / "sports.db"
    raw = tmp_path / "raw"
    monkeypatch.setenv("SPORTS_DB_PATH", str(path))
    monkeypatch.setattr(discovery, "RAW_DATA_DIR", raw)
    monkeypatch.setattr(discovery, "datetime", Clock)
    monkeypatch.setattr(discovery, "load_env", lambda: None)
    monkeypatch.setattr(discovery.OddsAPIKeyManager, "get_available_keys", lambda: ["test-key"])
    monkeypatch.setattr(discovery.OddsAPIKeyManager, "get_current_index", lambda: 0)
    monkeypatch.setattr(discovery.OddsAPIKeyManager, "rotate_key", _unexpected)
    monkeypatch.setattr(requests, "get", _unexpected)
    initialize(path)
    with connect(path) as conn:
        conn.execute(
            "INSERT INTO sports (sport_id, name, league, default_market) VALUES (1, 'Basketball', 'NBA', 'moneyline')"
        )
        conn.executemany(
            "INSERT INTO teams (team_id, sport_id, code, name) VALUES (?, 1, ?, ?)", TEAMS
        )
    return path, raw, Clock


def test_preview_reads_no_keys_and_creates_no_records(warehouse, monkeypatch):
    path, raw, _ = warehouse
    monkeypatch.setattr(discovery.OddsAPIKeyManager, "get_available_keys", _unexpected)
    assert discovery.discover_events() == {"status": "due"}
    assert not raw.exists()
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM source_runs").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM data_sources").fetchone()[0] == 0


def test_scheduled_observation_retries_during_cooldown(warehouse, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["ingest_nba_events", "--collect", "--observe-paper"])
    calls = []
    monkeypatch.setattr(discovery, "discover_events", lambda *, collect: {"status": "cooldown"})
    monkeypatch.setattr(paper_ledger, "register_trial", calls.append)

    discovery.main()

    assert calls == [paper_ledger.load_trial()]
    assert json.loads(capsys.readouterr().out) == {"status": "cooldown"}


def test_scheduled_observation_stops_at_capture_deadline(warehouse, monkeypatch, capsys):
    _, _, clock = warehouse
    clock.current = datetime.fromisoformat(paper_ledger.load_trial().review_at)
    monkeypatch.setattr(sys, "argv", ["ingest_nba_events", "--collect", "--observe-paper"])
    monkeypatch.setattr(discovery, "discover_events", _unexpected)
    monkeypatch.setattr(paper_ledger, "register_trial", _unexpected)

    discovery.main()

    assert json.loads(capsys.readouterr().out) == {"status": "trial_inactive"}


def test_archives_free_events_and_applies_only_fixture_links_or_inserts(warehouse, monkeypatch):
    path, raw, clock = warehouse
    known_start = NOW + timedelta(hours=7)
    provider_start = known_start + timedelta(minutes=10)
    new_start = NOW + timedelta(hours=30)
    payload = [_event(1, provider_start, 1, 2), _event(2, new_start, 3, 4)]
    with connect(path) as conn:
        _game(conn, "known", known_start, 1, 2)
        conn.execute(
            "INSERT INTO game_results (game_id, total_close, source_version) VALUES ('known', 224.5, 'original')"
        )
        before_game = dict(conn.execute("SELECT * FROM games").fetchone())
        before_results = [tuple(row) for row in conn.execute("SELECT * FROM game_results")]

    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return _response(payload)

    monkeypatch.setattr(requests, "get", get)
    assert discovery.discover_events(collect=True) == {
        "status": "discovered",
        "linked": 1,
        "created": 1,
        "skipped": 0,
    }
    assert discovery.discover_events(collect=True) == {"status": "cooldown"}
    assert len(calls) == 1
    url, call = calls[0]
    assert url == "https://api.the-odds-api.com/v4/sports/basketball_nba/events"
    assert call == {
        "params": {
            "apiKey": "test-key",
            "dateFormat": "iso",
            "commenceTimeFrom": "2026-11-07T14:05:00Z",
            "commenceTimeTo": "2026-11-09T14:05:00Z",
        },
        "timeout": 10,
        "allow_redirects": False,
    }
    new_id = _generate_internal_id("NBA", new_start.isoformat(), NAMES[3], NAMES[4])
    with connect(path) as conn:
        assert dict(conn.execute("SELECT * FROM games WHERE game_id = 'known'").fetchone()) == {
            **before_game,
            "odds_api_id": _id(1),
        }
        created = conn.execute(
            "SELECT start_time_utc, status, odds_api_id, home_team_id, away_team_id FROM games WHERE game_id = ?",
            (new_id,),
        ).fetchone()
        assert tuple(created) == (new_start.isoformat(), "scheduled", _id(2), 3, 4)
        assert [tuple(row) for row in conn.execute("SELECT * FROM game_results")] == before_results
        assert conn.execute("SELECT count(*) FROM odds_snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM odds").fetchone()[0] == 0
        clock_rows = conn.execute(
            "SELECT game_id, fetched_at_utc FROM odds_api_schedule_clock"
        ).fetchall()
        assert [tuple(row) for row in clock_rows] == [
            (new_id, NOW.isoformat(timespec="microseconds"))
        ]
        run = conn.execute("SELECT * FROM source_runs").fetchone()
        evidence = [
            tuple(row)
            for row in conn.execute(
                """
                SELECT provider_event_id, game_id, run_id, provider_start_utc, fixture_start_utc,
                       home_team_id, away_team_id, action
                FROM nba_event_discovery_links ORDER BY provider_event_id
                """
            )
        ]
        source_file = conn.execute("SELECT * FROM source_files").fetchone()
    assert evidence == [
        (
            _id(1),
            "known",
            run["run_id"],
            provider_start.isoformat(),
            known_start.isoformat(),
            1,
            2,
            "linked",
        ),
        (
            _id(2),
            new_id,
            run["run_id"],
            new_start.isoformat(),
            new_start.isoformat(),
            3,
            4,
            "created",
        ),
    ]
    assert run["status"] == "success" and run["records_ingested"] == 2
    archive = Path(run["raw_path"])
    assert archive.parent == raw / "odds" / "basketball_nba" / "events"
    archived = archive.read_bytes()
    assert b"test-key" not in archived
    assert json.loads(archived)["results"] == payload
    assert json.loads(archived)["request"]["params"] == {
        key: value for key, value in call["params"].items() if key != "apiKey"
    }
    assert source_file["storage_path"] == str(archive)
    assert source_file["hash"] == hashlib.sha256(archived).hexdigest()

    clock.current += timedelta(hours=1)
    assert discovery.discover_events(collect=True) == {
        "status": "discovered",
        "linked": 0,
        "created": 0,
        "skipped": 2,
    }
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM games").fetchone()[0] == 2
        assert [
            row[0] for row in conn.execute("SELECT DISTINCT run_id FROM nba_event_discovery_links")
        ] == [run["run_id"]]


@pytest.mark.parametrize(
    "conflict",
    ["shift", "reverse", "multiple", "scored", "final", "foreign_id", "existing_id", "date_only"],
)
def test_existing_conflicts_are_never_replaced_or_duplicated(warehouse, monkeypatch, conflict):
    path, _, _ = warehouse
    start = NOW + timedelta(hours=7)
    with connect(path) as conn:
        if conflict == "shift":
            _game(conn, "first", start - timedelta(hours=3), 1, 2)
        elif conflict == "reverse":
            _game(conn, "first", start, 2, 1)
        elif conflict == "foreign_id":
            _game(conn, "other", start, 3, 4, provider_id=_id(1))
        elif conflict == "existing_id":
            _game(conn, "first", start, 1, 2, provider_id=_id(8))
        elif conflict == "date_only":
            _game(conn, "first", start.replace(hour=0, minute=0), 1, 2)
        else:
            _game(
                conn, "first", start, 1, 2, status="final" if conflict == "final" else "scheduled"
            )
            if conflict == "multiple":
                _game(conn, "alias", start + timedelta(minutes=1), 1, 2)
            elif conflict == "scored":
                conn.execute("INSERT INTO game_results (game_id, away_score) VALUES ('first', 0)")
        original = [tuple(row) for row in conn.execute("SELECT * FROM games ORDER BY game_id")]
    monkeypatch.setattr(
        requests, "get", lambda *args, **kwargs: _response([_event(1, start, 1, 2)])
    )
    assert discovery.discover_events(collect=True) == {
        "status": "discovered",
        "linked": 0,
        "created": 0,
        "skipped": 1,
    }
    with connect(path) as conn:
        assert [
            tuple(row) for row in conn.execute("SELECT * FROM games ORDER BY game_id")
        ] == original
        assert conn.execute("SELECT count(*) FROM nba_event_discovery_links").fetchone()[0] == 0


def test_malformed_live_and_ambiguous_provider_events_remain_raw_only(warehouse, monkeypatch):
    path, raw, _ = warehouse
    first = _event(1, NOW + timedelta(hours=1), 1, 2)
    pair_a = _event(2, NOW + timedelta(hours=2), 3, 4)
    pair_b = _event(3, NOW + timedelta(hours=5), 4, 3)
    in_play = _event(4, NOW - timedelta(minutes=5), 5, 6)
    apparent_future_alias = _event(5, NOW + timedelta(minutes=15), 5, 6)
    safe = _event(6, NOW + timedelta(hours=6), 7, 8)
    payload = [
        first,
        first,
        pair_a,
        pair_b,
        in_play,
        apparent_future_alias,
        safe,
        {**safe, "id": _id(7), "sport_key": "basketball_ncaab"},
        {**safe, "id": _id(8), "home_team": "Unrecognized NBA team"},
        {**safe, "id": _id(9), "commence_time": "2026-11-07T22:00:00"},
        {**safe, "id": "bad-id"},
        _event(10, NOW + timedelta(hours=49), 7, 8),
    ]
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: _response(payload))
    assert discovery.discover_events(collect=True) == {
        "status": "discovered",
        "linked": 0,
        "created": 1,
        "skipped": len(payload) - 1,
    }
    with connect(path) as conn:
        assert [row[0] for row in conn.execute("SELECT odds_api_id FROM games")] == [_id(6)]
    [archive] = raw.rglob("*.json")
    assert json.loads(archive.read_text(encoding="utf-8"))["results"] == payload


def test_real_back_to_back_on_another_eastern_day_is_not_an_alias(warehouse, monkeypatch):
    path, _, _ = warehouse
    first_start = datetime(2026, 11, 8, tzinfo=timezone.utc)
    second_start = first_start + timedelta(hours=23)
    with connect(path) as conn:
        _game(conn, "yesterday", first_start, 1, 2, provider_id=_id(1))
    events = [_event(1, first_start, 1, 2), _event(2, second_start, 2, 1)]
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: _response(events))
    assert discovery.discover_events(collect=True) == {
        "status": "discovered",
        "linked": 0,
        "created": 1,
        "skipped": 1,
    }


@pytest.mark.parametrize("status, credits", [(429, "0"), (200, "1"), (200, None)])
def test_provider_errors_still_reserve_one_attempt_for_an_hour(
    warehouse, monkeypatch, status, credits
):
    path, raw, clock = warehouse
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        return _response({"detail": "test-key"}, status=status, credits=credits)

    monkeypatch.setattr(requests, "get", get)
    with pytest.raises(RuntimeError, match="^Odds API events") as error:
        discovery.discover_events(collect=True)
    assert "test-key" not in str(error.value)
    clock.current += timedelta(hours=1, seconds=-1)
    assert discovery.discover_events(collect=True) == {"status": "cooldown"}
    clock.current += timedelta(seconds=1)
    assert discovery.discover_events() == {"status": "due"}
    assert len(calls) == 1 and calls[0].endswith("/events")
    assert not raw.exists()
    with connect(path) as conn:
        assert conn.execute("SELECT status FROM source_runs").fetchone()[0] == "failed"


def test_concurrent_collectors_share_a_single_hourly_reservation(warehouse, monkeypatch):
    _, _, _ = warehouse
    in_request = Event()
    release = Event()
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        in_request.set()
        assert release.wait(timeout=5)
        return _response([])

    monkeypatch.setattr(requests, "get", get)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(discovery.discover_events, collect=True)
        assert in_request.wait(timeout=5)
        try:
            assert pool.submit(discovery.discover_events, collect=True).result(timeout=5) == {
                "status": "cooldown"
            }
        finally:
            release.set()
        assert first.result(timeout=5)["status"] == "discovered"
    assert len(calls) == 1


def test_failed_append_rolls_back_fixture_and_provenance_changes(warehouse, monkeypatch):
    path, raw, _ = warehouse
    start = NOW + timedelta(hours=5)
    with connect(path) as conn:
        _game(conn, "known", start, 1, 2)
        conn.execute(
            """
            CREATE TRIGGER fail_discovery BEFORE INSERT ON games
            BEGIN SELECT RAISE(ABORT, 'simulated persistence failure'); END
            """
        )
    payload = [_event(1, start, 1, 2), _event(2, start, 3, 4)]
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: _response(payload))
    with pytest.raises(RuntimeError, match="Could not persist the NBA event discovery batch"):
        discovery.discover_events(collect=True)
    with connect(path) as conn:
        assert tuple(conn.execute("SELECT game_id, odds_api_id FROM games").fetchone()) == (
            "known",
            None,
        )
        assert conn.execute("SELECT count(*) FROM games").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM source_files").fetchone()[0] == 0
        assert conn.execute("SELECT status FROM source_runs").fetchone()[0] == "failed"
        assert (
            conn.execute(
                "SELECT count(*) FROM sqlite_master WHERE name = 'nba_event_discovery_links'"
            ).fetchone()[0]
            == 0
        )
    assert len(list(raw.rglob("*.json"))) == 1
