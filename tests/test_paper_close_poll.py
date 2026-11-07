"""Quota and timing regression tests for the optional closing collector."""

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier

import pytest
import requests

from src.data import ingest_odds
from src.db.core import connect, initialize
from src.predict import paper_close_poll
from src.predict.paper_ledger import SCHEMA_PATH

NOW = datetime(2026, 11, 7, 0, 20, tzinfo=timezone.utc)


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz)


class FakeResponse:
    status_code = 200

    def __init__(self, quota=None, payload=None, status=200):
        self.headers = {} if quota is None else {"x-requests-remaining": quota}
        self.payload = payload
        self.status_code = status

    def json(self):
        return self.payload


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    path = tmp_path / "paper.db"
    monkeypatch.setenv("SPORTS_DB_PATH", str(path))
    monkeypatch.setattr(paper_close_poll, "datetime", FrozenDateTime)
    monkeypatch.setattr(ingest_odds, "datetime", FrozenDateTime)
    monkeypatch.setattr(paper_close_poll, "RAW_DATA_DIR", tmp_path / "raw")
    monkeypatch.setattr(paper_close_poll, "load_env", lambda: None)
    monkeypatch.setattr(
        paper_close_poll.OddsAPIKeyManager, "get_available_keys", lambda: ["test-key"]
    )
    monkeypatch.setattr(paper_close_poll.OddsAPIKeyManager, "get_current_index", lambda: 0)
    initialize(path)
    with connect(path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO sports (sport_id, name, league, default_market) VALUES (1, 'Basketball', 'NBA', 'moneyline')"
        )
        conn.executemany(
            "INSERT INTO teams (team_id, sport_id, code, name) VALUES (?, 1, ?, ?)",
            [(1, "BOS", "Boston Celtics"), (2, "NYK", "New York Knicks")],
        )
        conn.execute("INSERT INTO books VALUES (1, 'DraftKings', 'us')")
        start = NOW + timedelta(minutes=30)
        conn.execute(
            """
            INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, odds_api_id)
            VALUES ('game', 1, ?, 1, 2, 'event')
            """,
            (start.isoformat(),),
        )
        captured = NOW - timedelta(hours=2)
        conn.execute("INSERT INTO paper_trials VALUES ('trial', ?, '{}')", (captured.isoformat(),))
        conn.execute(
            """
            INSERT INTO paper_decisions VALUES (
                'trial', 'game', 1, 2, ?, ?, ?, 'the-odds-api', 'decision-quote', ?, 1, 'DraftKings',
                'under', 225.5, -110, -110, 0.75, 0.50, 0.25
            )
            """,
            (start.isoformat(), captured.isoformat(), captured.isoformat(), captured.isoformat()),
        )
    return path


def _unexpected_request(*args, **kwargs):
    pytest.fail("A local inspection must not access the provider")


def test_dry_run_and_no_due_are_network_free(warehouse, monkeypatch):
    monkeypatch.setattr(requests, "get", _unexpected_request)
    assert paper_close_poll.poll_closes() == {"status": "due", "due_events": 1}
    with connect(warehouse) as conn:
        assert not paper_close_poll._has_table(conn, "paper_close_requests")
        conn.execute(
            "UPDATE games SET start_time_utc = ?", ((NOW + timedelta(minutes=5)).isoformat(),)
        )
    assert paper_close_poll.poll_closes(collect=True) == {"status": "no_close_due", "due_events": 0}


def test_existing_close_skips_provider(warehouse, monkeypatch):
    monkeypatch.setattr(requests, "get", _unexpected_request)
    with connect(warehouse) as conn:
        conn.execute(
            "INSERT INTO odds_snapshots VALUES ('close', ?, 1, 'the-odds-api', NULL)",
            ((NOW - timedelta(minutes=30)).isoformat(),),
        )
        conn.executemany(
            """
            INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
            VALUES ('close', 'game', 1, 'totals', ?, -110, 226)
            """,
            [("Over",), ("Under",)],
        )
    assert paper_close_poll.poll_closes(collect=True)["status"] == "no_close_due"


def test_scheduled_fixture_with_one_score_skips_provider(warehouse, monkeypatch):
    monkeypatch.setattr(requests, "get", _unexpected_request)
    with connect(warehouse) as conn:
        conn.execute("INSERT INTO game_results (game_id, away_score) VALUES ('game', 0)")
    assert paper_close_poll.poll_closes(collect=True)["status"] == "no_close_due"


@pytest.mark.parametrize("quota", [None, "invalid", "200", "100"])
def test_quota_reserve_fails_closed(warehouse, monkeypatch, quota):
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        return FakeResponse(quota)

    monkeypatch.setattr(requests, "get", get)
    assert paper_close_poll.poll_closes(collect=True)["status"] == "quota_reserve"
    assert calls == [f"{paper_close_poll._BASE_URL}/events"]
    with connect(warehouse) as conn:
        assert not paper_close_poll._has_table(conn, "paper_close_requests")


def test_low_active_key_uses_another_budget_without_changing_active_index(warehouse, monkeypatch):
    keys = ["healthy-first", "low-active", "later-healthy"]
    monkeypatch.setattr(paper_close_poll.OddsAPIKeyManager, "get_available_keys", lambda: keys)
    monkeypatch.setattr(paper_close_poll.OddsAPIKeyManager, "get_current_index", lambda: 1)
    monkeypatch.setattr(
        paper_close_poll.OddsAPIKeyManager, "set_current_index", _unexpected_request
    )
    monkeypatch.setattr(paper_close_poll.OddsAPIKeyManager, "rotate_key", _unexpected_request)
    calls = []

    def get(url, **kwargs):
        key = kwargs["params"]["apiKey"]
        calls.append((url.rsplit("/", 1)[1], key))
        return FakeResponse("200" if key == "low-active" else "201", [])

    monkeypatch.setattr(requests, "get", get)
    assert paper_close_poll.poll_closes(collect=True)["status"] == "no_closing_quotes"
    assert calls == [
        ("events", "low-active"),
        ("events", "healthy-first"),
        ("odds", "healthy-first"),
    ]


def test_all_low_distinct_keys_are_probed_once(warehouse, monkeypatch):
    monkeypatch.setattr(
        paper_close_poll.OddsAPIKeyManager,
        "get_available_keys",
        lambda: ["duplicate", "other-low", "duplicate"],
    )
    monkeypatch.setattr(paper_close_poll.OddsAPIKeyManager, "get_current_index", lambda: 2)
    calls = []

    def get(url, **kwargs):
        calls.append((url.rsplit("/", 1)[1], kwargs["params"]["apiKey"]))
        return FakeResponse("200")

    monkeypatch.setattr(requests, "get", get)
    assert paper_close_poll.poll_closes(collect=True)["status"] == "quota_reserve"
    assert calls == [("events", "duplicate"), ("events", "other-low")]
    with connect(warehouse) as conn:
        assert not paper_close_poll._has_table(conn, "paper_close_requests")


@pytest.mark.parametrize("quota", [None, "invalid", "-1"])
def test_unknown_quota_stops_before_trying_another_key(warehouse, monkeypatch, quota):
    monkeypatch.setattr(
        paper_close_poll.OddsAPIKeyManager, "get_available_keys", lambda: ["unknown", "healthy"]
    )
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs["params"]["apiKey"])
        return FakeResponse(quota if calls[-1] == "unknown" else "500")

    monkeypatch.setattr(requests, "get", get)
    assert paper_close_poll.poll_closes(collect=True)["status"] == "quota_reserve"
    assert calls == ["unknown"]


@pytest.mark.parametrize("code", ["INVALID_KEY", "OUT_OF_USAGE_CREDITS"])
def test_confirmed_key_rejection_allows_a_free_probe_of_next_key(warehouse, monkeypatch, code):
    monkeypatch.setattr(
        paper_close_poll.OddsAPIKeyManager, "get_available_keys", lambda: ["rejected", "healthy"]
    )
    calls = []

    def get(url, **kwargs):
        key = kwargs["params"]["apiKey"]
        calls.append((url.rsplit("/", 1)[1], key))
        if key == "rejected":
            return FakeResponse(payload={"error_code": code}, status=401)
        return FakeResponse("201", [])

    monkeypatch.setattr(requests, "get", get)
    assert paper_close_poll.poll_closes(collect=True)["status"] == "no_closing_quotes"
    assert calls == [("events", "rejected"), ("events", "healthy"), ("odds", "healthy")]


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (429, {}),
        (500, {}),
        (422, {}),
        (401, {}),
        (401, {"error_code": "UNKNOWN"}),
        (403, {"error_code": "INVALID_KEY"}),
    ],
)
def test_other_provider_errors_stop_without_key_fallback(warehouse, monkeypatch, status, body):
    monkeypatch.setattr(
        paper_close_poll.OddsAPIKeyManager, "get_available_keys", lambda: ["first", "healthy"]
    )
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs["params"]["apiKey"])
        return FakeResponse("500", payload=body, status=status)

    monkeypatch.setattr(requests, "get", get)
    with pytest.raises(RuntimeError, match=f"Odds API events returned HTTP {status}"):
        paper_close_poll.poll_closes(collect=True)
    assert calls == ["first"]
    with connect(warehouse) as conn:
        assert not paper_close_poll._has_table(conn, "paper_close_requests")


def test_transport_error_stops_before_probing_another_key(warehouse, monkeypatch):
    monkeypatch.setattr(
        paper_close_poll.OddsAPIKeyManager, "get_available_keys", lambda: ["first", "healthy"]
    )
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs["params"]["apiKey"])
        raise requests.Timeout("raw request details")

    monkeypatch.setattr(requests, "get", get)
    with pytest.raises(RuntimeError, match="^Odds API events request failed$"):
        paper_close_poll.poll_closes(collect=True)
    assert calls == ["first"]


def _provider_event(event_id, start):
    return {
        "id": event_id,
        "sport_key": "basketball_nba",
        "sport_title": "NBA",
        "commence_time": start.isoformat(),
        "home_team": "Boston Celtics",
        "away_team": "New York Knicks",
        "bookmakers": [
            {
                "key": "draftkings",
                "title": "DraftKings",
                "markets": [
                    {
                        "key": "totals",
                        "outcomes": [
                            {"name": "Over", "price": -110, "point": 224.5},
                            {"name": "Under", "price": -110, "point": 224.5},
                        ],
                    },
                    {"key": "h2h", "outcomes": [{"name": "Boston Celtics", "price": -110}]},
                ],
            },
            {"key": "unrelated", "title": "Unrelated book"},
        ],
    }


def test_collects_and_loads_only_a_due_draftkings_totals_batch(warehouse, monkeypatch):
    calls = []
    event = _provider_event("event", NOW + timedelta(minutes=30))
    payload = [event, {**event, "id": "unexpected"}]
    with connect(warehouse) as conn:
        conn.execute("UPDATE games SET season = 2026, venue = 'Original arena', espn_id = 'espn'")
        conn.execute(
            """
            INSERT INTO game_results (game_id, total_close, source_version)
            VALUES ('game', 222.5, 'original-source')
            """
        )
        before_game = tuple(conn.execute("SELECT * FROM games").fetchone())
        before_result = tuple(conn.execute("SELECT * FROM game_results").fetchone())

    def get(url, **kwargs):
        calls.append((url.rsplit("/", 1)[1], kwargs["params"]))
        return FakeResponse("201", payload)

    monkeypatch.setattr(requests, "get", get)
    assert paper_close_poll.poll_closes(collect=True) == {
        "status": "collected",
        "due_events": 1,
        "events_loaded": 1,
    }
    assert [name for name, _ in calls] == ["events", "odds"]
    assert calls[1][1] == {
        "apiKey": "test-key",
        "regions": "us",
        "markets": "totals",
        "bookmakers": "draftkings",
        "eventIds": "event",
        "oddsFormat": "american",
    }
    with connect(warehouse) as conn:
        assert tuple(conn.execute("SELECT * FROM games").fetchone()) == before_game
        assert tuple(conn.execute("SELECT * FROM game_results").fetchone()) == before_result
        snapshot = conn.execute("SELECT * FROM odds_snapshots").fetchone()
        assert snapshot["source"] == "the-odds-api"
        assert snapshot["fetched_at_utc"] == NOW.isoformat()
        assert [
            tuple(row)
            for row in conn.execute(
                """
            SELECT game_id, book_id, market, outcome, line, price_american, price_decimal, implied_prob
            FROM odds WHERE snapshot_id = ? ORDER BY outcome
            """,
                (snapshot["snapshot_id"],),
            )
        ] == [
            ("game", 1, "totals", side, 224.5, -110, 1 + 100 / 110, 110 / 210)
            for side in ("Over", "Under")
        ]
        assert conn.execute("SELECT COUNT(*) FROM paper_close_requests").fetchone()[0] == 1
    raw_path = Path(snapshot["raw_path"])
    raw_dir = warehouse.parent / "raw" / "odds" / "basketball_nba" / "paper_closing"
    assert raw_path.is_relative_to(raw_dir)
    assert raw_path.parent.name == snapshot["snapshot_id"]
    archived = raw_path.read_text(encoding="utf-8")
    assert "test-key" not in archived
    assert json.loads(archived) == {
        "source": "the-odds-api",
        "fetched_at": NOW.isoformat(),
        "request": {
            "url": f"{paper_close_poll._BASE_URL}/odds",
            "params": {name: value for name, value in calls[1][1].items() if name != "apiKey"},
        },
        "results": payload,
        "rate_limit_remaining": "201",
        "rate_limit_reset": None,
    }
    assert paper_close_poll.poll_closes(collect=True)["status"] == "no_close_due"
    assert len(calls) == 2


@pytest.mark.parametrize(
    ("event_overrides", "under_overrides", "repeat"),
    [
        ({"sport_key": "basketball_ncaab"}, {}, False),
        ({"home_team": "New York Knicks", "away_team": "Boston Celtics"}, {}, False),
        ({"home_team": "Baskets Osiris"}, {}, False),
        ({"commence_time": (NOW + timedelta(minutes=41)).isoformat()}, {}, False),
        ({}, {"name": "Over"}, False),
        ({}, {"point": 223.5}, False),
        ({}, {"price": 0}, False),
        ({}, {}, True),
    ],
)
def test_rejected_provider_rows_are_archived_without_loading(
    warehouse, monkeypatch, event_overrides, under_overrides, repeat
):
    event = _provider_event("event", NOW + timedelta(minutes=30))
    event["bookmakers"][0]["markets"][0]["outcomes"][1].update(under_overrides)
    event.update(event_overrides)
    payload = [event, event] if repeat else [event]
    monkeypatch.setattr(requests, "get", lambda url, **kwargs: FakeResponse("201", payload))
    assert paper_close_poll.poll_closes(collect=True)["status"] == "no_closing_quotes"
    with connect(warehouse) as conn:
        assert conn.execute("SELECT COUNT(*) FROM odds_snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM odds").fetchone()[0] == 0
    [raw_path] = (warehouse.parent / "raw").rglob("odds_*.json")
    assert json.loads(raw_path.read_text(encoding="utf-8"))["results"] == payload


@pytest.mark.parametrize(
    "fixture_change",
    [
        "UPDATE games SET status = 'in_progress'",
        "UPDATE games SET odds_api_id = 'reassigned'",
        "INSERT INTO game_results (game_id, home_score) VALUES ('game', 0)",
        """
        INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, odds_api_id)
        SELECT 'duplicate', sport_id, start_time_utc, home_team_id, away_team_id, odds_api_id
        FROM games WHERE game_id = 'game'
        """,
    ],
)
def test_loading_rechecks_reserved_fixture_identity(warehouse, monkeypatch, fixture_change):
    def get(url, **kwargs):
        if url.endswith("/events"):
            return FakeResponse("201")
        with connect(warehouse) as conn:
            conn.execute(fixture_change)
        return FakeResponse("201", [_provider_event("event", NOW + timedelta(minutes=30))])

    monkeypatch.setattr(requests, "get", get)
    assert paper_close_poll.poll_closes(collect=True)["status"] == "no_closing_quotes"
    with connect(warehouse) as conn:
        assert conn.execute("SELECT COUNT(*) FROM odds_snapshots").fetchone()[0] == 0


def test_snapshot_and_paired_outcomes_roll_back_together(warehouse, monkeypatch):
    with connect(warehouse) as conn:
        conn.execute(
            """
            CREATE TRIGGER fail_under BEFORE INSERT ON odds WHEN NEW.outcome = 'Under'
            BEGIN SELECT RAISE(ABORT, 'pair insert failed'); END
            """
        )
    payload = [_provider_event("event", NOW + timedelta(minutes=30))]
    monkeypatch.setattr(requests, "get", lambda url, **kwargs: FakeResponse("201", payload))
    with pytest.raises(sqlite3.IntegrityError, match="pair insert failed"):
        paper_close_poll.poll_closes(collect=True)
    with connect(warehouse) as conn:
        assert conn.execute("SELECT COUNT(*) FROM odds_snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM odds").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM paper_close_requests").fetchone()[0] == 1
    assert len(list((warehouse.parent / "raw").rglob("odds_*.json"))) == 1


def test_archive_failure_keeps_request_count_and_skips_load(warehouse, monkeypatch):
    payload = [_provider_event("event", NOW + timedelta(minutes=30))]
    monkeypatch.setattr(requests, "get", lambda url, **kwargs: FakeResponse("201", payload))

    def fail_archive(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(paper_close_poll, "write_snapshot", fail_archive)
    with pytest.raises(RuntimeError, match="Could not archive the closing odds response"):
        paper_close_poll.poll_closes(collect=True)
    with connect(warehouse) as conn:
        assert conn.execute("SELECT COUNT(*) FROM odds_snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM paper_close_requests").fetchone()[0] == 1


def test_daily_cap_and_recent_failed_attempt_are_network_free(warehouse, monkeypatch):
    monkeypatch.setattr(requests, "get", _unexpected_request)
    with connect(warehouse) as conn:
        conn.execute("CREATE TABLE paper_close_requests (requested_at TEXT PRIMARY KEY)")
        conn.executemany(
            "INSERT INTO paper_close_requests VALUES (?)",
            [((NOW - timedelta(minutes=minutes)).isoformat(),) for minutes in (4, 8, 12)],
        )
    assert paper_close_poll.poll_closes(collect=True)["status"] == "daily_cap"
    with connect(warehouse) as conn:
        conn.execute(
            "DELETE FROM paper_close_requests WHERE requested_at = ?",
            ((NOW - timedelta(minutes=4)).isoformat(),),
        )
    assert paper_close_poll.poll_closes(collect=True)["status"] == "cooldown"


def test_paid_request_failure_is_sanitized_and_counts_toward_limit(warehouse, monkeypatch):
    monkeypatch.setattr(
        paper_close_poll.OddsAPIKeyManager, "get_available_keys", lambda: ["test-key", "spare"]
    )
    calls = []

    def get(url, **kwargs):
        calls.append((url.rsplit("/", 1)[1], kwargs["params"]["apiKey"]))
        if url.endswith("/odds"):
            raise requests.Timeout("raw request with key test-key")
        return FakeResponse("201")

    monkeypatch.setattr(requests, "get", get)
    with pytest.raises(RuntimeError, match="^Odds API odds request failed$") as error:
        paper_close_poll.poll_closes(collect=True)
    assert error.value.__suppress_context__
    assert calls == [("events", "test-key"), ("odds", "test-key")]
    with connect(warehouse) as conn:
        assert conn.execute("SELECT COUNT(*) FROM paper_close_requests").fetchone()[0] == 1


def test_concurrent_collectors_share_one_attempt(warehouse, monkeypatch):
    quota_barrier = Barrier(2)
    calls = []

    def get(url, **kwargs):
        calls.append(url.rsplit("/", 1)[1])
        if url.endswith("/events"):
            quota_barrier.wait(timeout=5)
        return FakeResponse("201", [])

    monkeypatch.setattr(requests, "get", get)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(paper_close_poll.poll_closes, collect=True) for _ in range(2)]
        statuses = [future.result(timeout=10)["status"] for future in futures]
    assert sorted(statuses) == ["cooldown", "no_closing_quotes"]
    assert sorted(calls) == ["events", "events", "odds"]


def test_scheduled_batch_waits_with_room_for_a_delayed_next_poll(warehouse, monkeypatch):
    current = NOW
    monkeypatch.setattr(
        FrozenDateTime, "now", classmethod(lambda cls, tz=None: current.astimezone(tz))
    )
    start = NOW + timedelta(minutes=28, seconds=1)
    with connect(warehouse) as conn:
        conn.execute("UPDATE games SET start_time_utc = ?", (start.isoformat(),))
        conn.execute("UPDATE paper_decisions SET start_time_utc = ?", (start.isoformat(),))
    monkeypatch.setattr(requests, "get", _unexpected_request)
    assert paper_close_poll.poll_closes(collect=True, batch_scheduled=True) == {
        "status": "waiting_for_batch",
        "due_events": 1,
    }
    with connect(warehouse) as conn:
        assert not paper_close_poll._has_table(conn, "paper_close_requests")

    current += timedelta(minutes=18)
    assert start - current == timedelta(minutes=10, seconds=1)

    def get(url, **kwargs):
        return FakeResponse("201", [_provider_event("event", start)])

    monkeypatch.setattr(requests, "get", get)
    assert paper_close_poll.poll_closes(collect=True, batch_scheduled=True)["status"] == "collected"


@pytest.mark.parametrize("batch_scheduled", [False, True], ids=["immediate", "batched"])
def test_saturday_slate_preserves_late_closes_under_daily_cap(
    warehouse, monkeypatch, batch_scheduled
):
    midnight = datetime(2026, 11, 8, tzinfo=timezone.utc)
    starts = {
        f"event-{index}": midnight + timedelta(minutes=minutes)
        for index, minutes in enumerate((40, 70, 70, 100, 130, 190))
    }
    captured = (midnight - timedelta(minutes=112)).isoformat()
    with connect(warehouse) as conn:
        conn.execute("DELETE FROM paper_decisions")
        conn.execute("DELETE FROM games")
        conn.executemany(
            """
            INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, odds_api_id)
            VALUES (?, 1, ?, 1, 2, ?)
            """,
            [(event_id, start.isoformat(), event_id) for event_id, start in starts.items()],
        )
        conn.executemany(
            """
            INSERT INTO paper_decisions VALUES (
                'trial', ?, 1, 2, ?, ?, ?, 'the-odds-api', 'decision-quote', ?, 1, 'DraftKings',
                'under', 225.5, -110, -110, 0.75, 0.50, 0.25
            )
            """,
            [
                (event_id, start.isoformat(), captured, captured, captured)
                for event_id, start in starts.items()
            ],
        )
    current = midnight - timedelta(minutes=45)
    monkeypatch.setattr(
        FrozenDateTime, "now", classmethod(lambda cls, tz=None: current.astimezone(tz))
    )
    attempts = []

    def get(url, **kwargs):
        if url.endswith("/events"):
            return FakeResponse("201")
        event_ids = kwargs["params"]["eventIds"].split(",")
        attempts.append((int((current - midnight).total_seconds() / 60), len(event_ids)))
        return FakeResponse(
            "201", [_provider_event(event_id, starts[event_id]) for event_id in event_ids]
        )

    monkeypatch.setattr(requests, "get", get)
    while current <= midnight + timedelta(hours=3):
        paper_close_poll.poll_closes(collect=True, batch_scheduled=batch_scheduled)
        current += timedelta(minutes=15)

    with connect(warehouse) as conn:
        covered = {row[0] for row in conn.execute("SELECT DISTINCT game_id FROM odds")}
    if batch_scheduled:
        assert attempts == [(15, 3), (75, 2), (165, 1)]
        assert covered == set(starts)
    else:
        assert attempts == [(-15, 1), (15, 2), (45, 1), (75, 1)]
        assert covered == set(starts) - {"event-5"}
