"""Exercise the paid close boundary with a fake clock and transport."""

import json
from datetime import datetime, timedelta, timezone

import pytest
import requests

from src.db.core import connect, initialize
from src.predict import paper_close_poll as closing

RESERVED_AT = datetime(2026, 11, 7, 23, 59, 30, tzinfo=timezone.utc)


@pytest.fixture
def batch(tmp_path, monkeypatch):
    class Clock(datetime):
        current = RESERVED_AT

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz)

    path = tmp_path / "betting.db"
    initialize(path)
    fixtures = {}
    with connect(path) as conn:
        conn.execute(
            "INSERT INTO sports(sport_id, name, league, default_market) "
            "VALUES (1, 'Basketball', 'NBA', 'moneyline')"
        )
        conn.execute("INSERT INTO books VALUES (1, 'DraftKings', 'us')")
        conn.executemany(
            "INSERT INTO teams(team_id, sport_id, code, name) VALUES (?, 1, ?, ?)",
            [(1, "BOS", "Boston Celtics"), (2, "NYK", "New York Knicks")],
        )
        for event_id, lead in [("early", 10), ("later", 25)]:
            start = RESERVED_AT + timedelta(minutes=lead)
            conn.execute(
                """
                INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, odds_api_id)
                VALUES (?, 1, ?, 1, 2, ?)
                """,
                (event_id, start.isoformat(), event_id),
            )
            fixtures[event_id] = closing._ClosingFixture(event_id, 1, 1, 2, 1, start, start)

    monkeypatch.setattr(closing, "datetime", Clock)
    monkeypatch.setattr(closing, "load_env", lambda: None)
    monkeypatch.setattr(closing, "resolve_db_path", lambda: path)
    monkeypatch.setattr(closing, "RAW_DATA_DIR", tmp_path / "data" / "raw")
    monkeypatch.setattr(closing.OddsAPIKeyManager, "get_available_keys", lambda: ["test-key"])
    monkeypatch.setattr(closing.OddsAPIKeyManager, "get_current_index", lambda: 0)
    # Focus on dispatch after the normal planner has selected an eligible batch.
    monkeypatch.setattr(closing, "_plan", lambda *args, **kwargs: ("due", fixtures))
    return path, Clock, fixtures


def _resume_after_reserve(monkeypatch, clock, dispatch_at):
    reserve = closing._reserve_attempt

    def delayed(*args, **kwargs):
        result = reserve(*args, **kwargs)
        clock.current = dispatch_at
        return result

    monkeypatch.setattr(closing, "_reserve_attempt", delayed)


def _fake_transport(monkeypatch, payload=(), *, timeout=False):
    calls = []

    def get(url, **kwargs):
        endpoint = url.rsplit("/", 1)[-1]
        calls.append((endpoint, kwargs["params"]))
        if endpoint == "odds" and timeout:
            raise requests.Timeout("fake provider timeout")
        response = requests.Response()
        response.status_code = 200
        response.headers["x-requests-remaining"] = "201"
        response._content = json.dumps(payload if endpoint == "odds" else []).encode()
        return response

    monkeypatch.setattr(requests, "get", get)
    return calls


def _quote(event_id, fixture):
    return {
        "id": event_id,
        "sport_key": "basketball_nba",
        "commence_time": fixture.start.isoformat(),
        "home_team": "Boston Celtics",
        "away_team": "New York Knicks",
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {
                        "key": "totals",
                        "outcomes": [
                            {"name": side, "price": -110, "point": 225.5}
                            for side in ("Over", "Under")
                        ],
                    }
                ],
            }
        ],
    }


def _assert_original_reservation(path):
    with connect(path) as conn:
        assert [
            row[0] for row in conn.execute("SELECT requested_at FROM paper_close_requests")
        ] == [RESERVED_AT.isoformat()]


@pytest.mark.parametrize(
    ("delay", "status"),
    [
        (timedelta(seconds=1), "closing_lead_elapsed"),
        (timedelta(minutes=1), "closing_date_changed"),
    ],
)
def test_elapsed_lead_or_utc_date_stops_paid_dispatch(batch, monkeypatch, delay, status):
    path, clock, fixtures = batch
    if status == "closing_lead_elapsed":
        del fixtures["later"]
    calls = _fake_transport(monkeypatch)
    _resume_after_reserve(monkeypatch, clock, RESERVED_AT + delay)
    assert closing.poll_closes(collect=True, batch_scheduled=True) == {
        "status": status,
        "due_events": 0,
    }
    assert [endpoint for endpoint, _ in calls] == ["events"]
    _assert_original_reservation(path)


@pytest.mark.parametrize("timeout", [False, True])
def test_partial_batch_requests_only_remaining_fixture_and_keeps_reservation(
    batch, monkeypatch, timeout
):
    path, clock, fixtures = batch
    _resume_after_reserve(monkeypatch, clock, RESERVED_AT + timedelta(seconds=1))
    calls = _fake_transport(
        monkeypatch,
        [_quote(event_id, fixture) for event_id, fixture in fixtures.items()],
        timeout=timeout,
    )
    if timeout:
        with pytest.raises(RuntimeError, match="^Odds API odds request failed$"):
            closing.poll_closes(collect=True, batch_scheduled=True)
    else:
        assert closing.poll_closes(collect=True, batch_scheduled=True) == {
            "status": "collected",
            "due_events": 1,
            "events_loaded": 1,
        }
    assert [endpoint for endpoint, _ in calls] == ["events", "odds"]
    assert calls[1][1]["eventIds"] == "later"
    with connect(path) as conn:
        normalized = [row[0] for row in conn.execute("SELECT DISTINCT game_id FROM odds")]
        assert normalized == ([] if timeout else ["later"])
    _assert_original_reservation(path)
