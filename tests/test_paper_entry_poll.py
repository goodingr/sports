"""Exercise the morning NBA collector with a frozen clock and no live requests."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import requests

from src.db.core import connect, initialize
from src.db.loaders import load_odds_snapshot
from src.predict import paper_entry_poll, paper_ledger
from src.predict.paper_close_poll import OddsAPIKeyManager

NOW = datetime(2026, 11, 7, 21, tzinfo=timezone.utc)


def _unexpected(*args, **kwargs):
    pytest.fail("This path must not call the provider")


def _event(event_id, start, home, away):
    return {
        "id": event_id,
        "sport_key": "basketball_nba",
        "commence_time": start.isoformat(),
        "home_team": home,
        "away_team": away,
        "bookmakers": [
            {
                "key": "draftkings",
                "title": "DraftKings",
                "markets": [
                    {
                        "key": "h2h",
                        "outcomes": [{"name": home, "price": -120}, {"name": away, "price": 110}],
                    },
                    {
                        "key": "totals",
                        "outcomes": [
                            {"name": "Over", "point": 225.5, "price": -110},
                            {"name": "Under", "point": 225.5, "price": -110},
                        ],
                    },
                ],
            },
        ],
    }


def _response(payload, *, quota="202"):
    response = requests.Response()
    response.status_code = 200
    response.headers["x-requests-remaining"] = quota
    response._content = json.dumps(payload).encode("utf-8")
    return response


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    class ClockType(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class Clock(datetime, metaclass=ClockType):
        current = NOW

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz)

    path = tmp_path / "paper.db"
    raw = tmp_path / "raw"
    trial = paper_ledger.load_trial()
    monkeypatch.setenv("SPORTS_DB_PATH", str(path))
    monkeypatch.setattr(paper_entry_poll, "RAW_DATA_DIR", raw)
    monkeypatch.setattr(paper_entry_poll, "datetime", Clock)
    monkeypatch.setattr(paper_ledger, "datetime", Clock)
    monkeypatch.setattr(paper_entry_poll, "load_env", lambda: None)
    monkeypatch.setattr(OddsAPIKeyManager, "get_available_keys", lambda: ["test-key"])
    monkeypatch.setattr(OddsAPIKeyManager, "get_current_index", lambda: 0)
    monkeypatch.setattr(requests, "get", _unexpected)
    events = [
        _event("early-one", NOW + timedelta(minutes=65), "Boston Celtics", "New York Knicks"),
        _event("early-two", NOW + timedelta(minutes=130), "Miami Heat", "Atlanta Hawks"),
        _event("regular", NOW + timedelta(minutes=150), "Orlando Magic", "Chicago Bulls"),
    ]
    initialize(path)
    with connect(path) as conn:
        conn.executescript(paper_ledger.SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO sports (sport_id, name, league, default_market) VALUES (1, 'Basketball', 'NBA', 'moneyline')"
        )
        conn.execute("INSERT INTO books VALUES (1, 'DraftKings', 'us')")
        conn.executemany(
            "INSERT INTO teams (team_id, sport_id, code, name) VALUES (?, 1, ?, ?)",
            [
                (1, "BOS", "Boston Celtics"),
                (2, "NYK", "New York Knicks"),
                (3, "MIA", "Miami Heat"),
                (4, "ATL", "Atlanta Hawks"),
                (5, "ORL", "Orlando Magic"),
                (6, "CHI", "Chicago Bulls"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO games (game_id, sport_id, start_time_utc, home_team_id, away_team_id, odds_api_id)
            VALUES (?, 1, ?, ?, ?, ?)
            """,
            [
                (e["id"], e["commence_time"], idx * 2 + 1, idx * 2 + 2, e["id"])
                for idx, e in enumerate(events)
            ],
        )
        conn.execute(
            "INSERT INTO game_results (game_id, total_close, source_version) VALUES ('early-one', 221.5, 'prior')"
        )
        conn.execute(
            "INSERT INTO paper_trials VALUES (?, ?, ?)",
            (trial.trial_id, (NOW - timedelta(days=1)).isoformat(), trial.specification),
        )

    predictions_requested = []

    class Engine:
        def __init__(self, *, model_type):
            assert model_type == trial.model_type

        def load_model(self, league, *, trace_totals):
            assert league == "NBA" and trace_totals
            self.totals_model = object()
            self.totals_std = 10
            self.totals_model_sha256 = trial.model_sha256
            return True

        def predict(self, league, *, days_ahead, trace_totals, fixture_ids):
            assert (league, days_ahead, trace_totals) == ("NBA", 1, True)
            predictions_requested.append(fixture_ids)
            with connect(path) as conn:
                selected = conn.execute(
                    """
                    SELECT DISTINCT g.game_id, g.start_time_utc, o.snapshot_id
                    FROM games g JOIN odds o ON o.game_id = g.game_id WHERE o.market = 'totals'
                    """
                ).fetchall()
            return pd.DataFrame(
                [
                    {
                        "game_id": game["game_id"],
                        "commence_time": game["start_time_utc"],
                        "predicted_at": Clock.current.isoformat(),
                        "selected_totals_snapshot_id": game["snapshot_id"],
                        "selected_totals_book_id": 1,
                        "selected_h2h_snapshot_id": game["snapshot_id"],
                        "selected_h2h_book_id": 1,
                        "home_moneyline": -120,
                        "away_moneyline": 110,
                        "total_line": 225.5,
                        "over_moneyline": -110,
                        "under_moneyline": -110,
                        "over_predicted_prob": 0.8,
                        "under_predicted_prob": 0.2,
                    }
                    for game in selected
                ]
            )

    monkeypatch.setattr(paper_entry_poll, "PredictionEngine", Engine)
    return path, raw, events, Clock, predictions_requested


def _seed_quotes(events, *, fetched=NOW - timedelta(minutes=3)):
    load_odds_snapshot(
        {"source": "the-odds-api", "fetched_at": fetched.isoformat(), "results": events},
        region="us",
        sport_key="basketball_nba",
    )


def test_read_only_trigger_limits_and_evening_boundary(warehouse):
    path, _, _, clock, predictions = warehouse
    clock.current = NOW - timedelta(minutes=11)
    assert paper_entry_poll.poll_entry() == {"status": "no_entry_due", "due_events": 0}
    clock.current += timedelta(minutes=1)
    assert paper_entry_poll.poll_entry() == {"status": "due", "due_events": 2}
    clock.current = NOW + timedelta(minutes=35)
    assert paper_entry_poll.poll_entry() == {"status": "due", "due_events": 2}
    clock.current += timedelta(seconds=1)
    assert paper_entry_poll.poll_entry() == {"status": "no_entry_due", "due_events": 0}
    assert not predictions
    with connect(path) as conn:
        assert not paper_entry_poll._has_table(conn, "paper_entry_requests")


def test_collects_exact_batch_and_preserves_existing_fixtures(warehouse, monkeypatch):
    path, raw, events, _, predictions = warehouse
    _seed_quotes(events[2:])  # The model double returns this valid evening prediction too.
    with connect(path) as conn:
        old_games = [tuple(row) for row in conn.execute("SELECT * FROM games ORDER BY game_id")]
        old_results = [
            tuple(row) for row in conn.execute("SELECT * FROM game_results ORDER BY game_id")
        ]
    quota = []
    calls = []

    def budget(*, request_credits):
        quota.append(request_credits)
        return "test-key"

    def provider(endpoint, params):
        calls.append((endpoint, params))
        return _response(events)

    monkeypatch.setattr(paper_entry_poll, "_budgeted_key", budget)
    monkeypatch.setattr(paper_entry_poll, "_provider_get", provider)
    assert paper_entry_poll.poll_entry(collect=True) == {
        "status": "collected",
        "due_events": 2,
        "events_loaded": 2,
        "decisions_recorded": 2,
    }
    assert quota == [2]
    [(endpoint, params)] = calls
    assert endpoint == "odds"
    assert params["markets"] == "h2h,totals"
    assert params["bookmakers"] == "draftkings"
    assert params["eventIds"] == "early-one,early-two"
    assert predictions == [{"early-one", "early-two"}]
    with connect(path) as conn:
        assert [
            tuple(row) for row in conn.execute("SELECT * FROM games ORDER BY game_id")
        ] == old_games
        assert [
            tuple(row) for row in conn.execute("SELECT * FROM game_results ORDER BY game_id")
        ] == old_results
        assert {row[0] for row in conn.execute("SELECT game_id FROM paper_decisions")} == {
            "early-one",
            "early-two",
        }
        assert {
            row[0] for row in conn.execute("SELECT game_id FROM paper_fixture_observations")
        } == {"early-one", "early-two"}
        snapshot = conn.execute(
            "SELECT * FROM odds_snapshots WHERE raw_path IS NOT NULL"
        ).fetchone()
        assert (
            conn.execute(
                "SELECT count(*) FROM odds WHERE snapshot_id = ?", (snapshot["snapshot_id"],)
            ).fetchone()[0]
            == 8
        )
    archived = Path(snapshot["raw_path"])
    assert archived.is_relative_to(raw)
    raw_json = archived.read_text(encoding="utf-8")
    assert "test-key" not in raw_json
    assert json.loads(raw_json)["results"] == events


def test_fresh_exact_pairs_reuse_without_quota_or_daily_reservation(warehouse):
    path, _, events, _, predictions = warehouse
    _seed_quotes(events)
    assert paper_entry_poll.poll_entry(collect=True) == {
        "status": "reused",
        "due_events": 2,
        "decisions_recorded": 2,
    }
    assert predictions == [{"early-one", "early-two"}]
    with connect(path) as conn:
        assert not paper_entry_poll._has_table(conn, "paper_entry_requests")


def test_one_available_credit_above_reserve_cannot_pay_for_two_markets(warehouse, monkeypatch):
    path, _, _, _, _ = warehouse
    calls = []

    def get(url, **kwargs):
        calls.append(url.rsplit("/", 1)[-1])
        return _response([], quota="201")

    monkeypatch.setattr(requests, "get", get)
    assert paper_entry_poll.poll_entry(collect=True)["status"] == "quota_reserve"
    assert calls == ["events"]
    with connect(path) as conn:
        assert not paper_entry_poll._has_table(conn, "paper_entry_requests")


def test_failed_paid_request_consumes_eastern_date_cap(warehouse, monkeypatch):
    path, _, _, _, predictions = warehouse
    calls = []

    def get(url, **kwargs):
        endpoint = url.rsplit("/", 1)[-1]
        calls.append(endpoint)
        if endpoint == "odds":
            raise requests.Timeout("raw transport includes test-key")
        return _response([])

    monkeypatch.setattr(requests, "get", get)
    with pytest.raises(RuntimeError, match="^Odds API odds request failed$"):
        paper_entry_poll.poll_entry(collect=True)
    assert paper_entry_poll.poll_entry(collect=True)["status"] == "daily_cap"
    assert calls == ["events", "odds"]
    assert not predictions
    with connect(path) as conn:
        assert (
            conn.execute("SELECT entry_date_et FROM paper_entry_requests").fetchone()[0]
            == "2026-11-07"
        )


def test_malformed_and_duplicate_event_stay_in_raw_archive_only(warehouse, monkeypatch):
    path, raw, events, _, predictions = warehouse
    events[1]["bookmakers"][0]["markets"][0]["outcomes"].pop()
    payload = [events[0], events[0], events[1], events[2]]
    monkeypatch.setattr(paper_entry_poll, "_budgeted_key", lambda **kwargs: "test-key")
    monkeypatch.setattr(
        paper_entry_poll, "_provider_get", lambda endpoint, params: _response(payload)
    )
    result = paper_entry_poll.poll_entry(collect=True)
    assert result["status"] == "no_entry_quotes"
    assert result["events_loaded"] == 0
    assert not predictions
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM odds_snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM paper_decisions").fetchone()[0] == 0
    [archived] = raw.rglob("odds_*.json")
    assert json.loads(archived.read_text(encoding="utf-8"))["results"] == payload


def test_partial_batch_drops_quote_inside_provider_lead(warehouse, monkeypatch):
    path, _, events, _, predictions = warehouse
    with connect(path) as conn:
        conn.execute(
            "UPDATE games SET start_time_utc = ? WHERE game_id = 'early-two'",
            ((NOW + timedelta(minutes=20)).isoformat(),),
        )
    events[1]["commence_time"] = (NOW + timedelta(minutes=14)).isoformat()
    monkeypatch.setattr(paper_entry_poll, "_budgeted_key", lambda **kwargs: "test-key")
    monkeypatch.setattr(
        paper_entry_poll, "_provider_get", lambda endpoint, params: _response(events)
    )
    result = paper_entry_poll.poll_entry(collect=True)
    assert (result["due_events"], result["events_loaded"], result["decisions_recorded"]) == (
        2,
        1,
        1,
    )
    assert predictions == [{"early-one"}]
    with connect(path) as conn:
        assert [row[0] for row in conn.execute("SELECT DISTINCT game_id FROM odds")] == [
            "early-one"
        ]
        assert dict(conn.execute("SELECT game_id, outcome FROM paper_fixture_observations")) == {
            "early-one": "recorded",
            "early-two": "prediction_missing",
        }


def test_changed_rules_are_rejected_in_preview_before_model_or_provider(warehouse):
    path, _, _, _, predictions = warehouse
    with connect(path) as conn:
        conn.execute("UPDATE paper_trials SET specification = '{}' ")
    with pytest.raises(RuntimeError, match="differs from its registered rules"):
        paper_entry_poll.poll_entry(collect=True)
    assert not predictions
