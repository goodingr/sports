"""Exercise the morning NBA collector with a frozen clock and no live requests."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import requests

from src.data import audit_nba_schedule, ingest_nba_events
from src.db.core import connect, initialize
from src.db.loaders import _generate_internal_id, load_odds_snapshot
from src.predict import paper_entry_poll, paper_ledger
from src.predict.paper_close_poll import OddsAPIKeyManager
from src.predict.storage import load_games_from_database

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
    raw = tmp_path / "data" / "raw"
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

        def load_pinned_totals(self, league, model_sha256):
            assert (league, model_sha256) == ("NBA", trial.model_sha256)
            self.totals_model_sha256 = model_sha256

        def predict_pinned_totals(self, league, model_sha256, *, days_ahead, fixture_ids):
            assert (league, days_ahead, model_sha256) == ("NBA", 1, self.totals_model_sha256)
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
    payload = {
        "source": "the-odds-api",
        "fetched_at": fetched.isoformat(),
        "request": {"url": f"{paper_entry_poll._BASE_URL}/odds", "params": {}},
        "results": events,
    }
    raw_path = paper_entry_poll.write_snapshot(
        payload, paper_entry_poll.RAW_DATA_DIR / "odds" / "basketball_nba"
    )
    load_odds_snapshot(
        payload,
        raw_path=str(raw_path),
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
    events[0]["commence_time"] = (NOW + timedelta(minutes=69)).isoformat()
    _seed_quotes(events[2:])  # The model double returns this valid evening prediction too.
    with connect(path) as conn:
        old_games = [tuple(row) for row in conn.execute("SELECT * FROM games ORDER BY game_id")]
        old_results = [
            tuple(row) for row in conn.execute("SELECT * FROM game_results ORDER BY game_id")
        ]
    quota = []
    calls = []

    def budget(*, request_credits, on_free_events):
        assert callable(on_free_events)
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
            """
            SELECT s.* FROM odds_snapshots s
            JOIN paper_decisions d ON d.snapshot_id = s.snapshot_id
            WHERE d.game_id = 'early-one'
            """
        ).fetchone()
        assert dict(
            conn.execute(
                """
                SELECT game_id, observed_start_utc FROM odds_snapshot_game_starts
                WHERE snapshot_id = ?
                """,
                (snapshot["snapshot_id"],),
            )
        ) == {event["id"]: event["commence_time"] for event in events[:2]}
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


@pytest.mark.parametrize("probe_cost", ["0", "1", None])
def test_winning_entry_probe_can_add_a_fixture_without_another_call(
    warehouse, monkeypatch, probe_cost
):
    path, raw, events, clock, predictions = warehouse
    zero_credit = probe_cost == "0"
    monkeypatch.setattr(ingest_nba_events, "RAW_DATA_DIR", raw)
    monkeypatch.setattr(ingest_nba_events, "datetime", clock)
    monkeypatch.setattr(OddsAPIKeyManager, "get_available_keys", lambda: ["low-key", "test-key"])
    original, new, regular = events
    original["id"], new["id"], regular["id"] = "a" * 32, "b" * 32, "c" * 32
    regular["commence_time"] = (NOW + timedelta(minutes=145)).isoformat()
    future = {
        **regular,
        "id": "d" * 32,
        "commence_time": (NOW + timedelta(days=1)).isoformat(),
    }
    only_low_key = {
        **regular,
        "id": "e" * 32,
        "commence_time": (NOW + timedelta(minutes=40)).isoformat(),
    }
    with connect(path) as conn:
        conn.execute(
            "UPDATE games SET odds_api_id = ? WHERE game_id = 'early-one'", (original["id"],)
        )
        conn.execute("DELETE FROM games WHERE game_id = 'early-two'")
        conn.execute("UPDATE games SET odds_api_id = NULL WHERE game_id = 'regular'")
        conn.execute(
            "INSERT INTO data_sources (source_id, source_key, name, category) VALUES (10, ?, 'Schedule', 'schedule')",
            (ingest_nba_events._SOURCE,),
        )
        conn.execute(
            "INSERT INTO source_runs (run_id, source_id, started_at, status) VALUES ('schedule-run', 10, ?, 'success')",
            ((NOW - timedelta(minutes=59)).isoformat(),),
        )
    calls = []

    def get(url, *, params, **kwargs):
        endpoint = url.rsplit("/", 1)[-1]
        calls.append((endpoint, params.copy()))
        if endpoint == "odds":
            return _response([original, new])
        if params["apiKey"] == "low-key":
            result = _response([only_low_key], quota="201")
            result.headers["x-requests-last"] = "0"
            return result
        result = _response([original, new, regular, future])
        if probe_cost is not None:
            result.headers["x-requests-last"] = probe_cost
        return result

    monkeypatch.setattr(requests, "get", get)
    result = paper_entry_poll.poll_entry(collect=True)
    count = 2 if zero_credit else 1
    assert result == {
        "status": "collected",
        "due_events": count,
        "events_loaded": count,
        "decisions_recorded": count,
    }
    assert [call[0] for call in calls] == ["events", "events", "odds"]
    assert [call[1]["apiKey"] for call in calls] == ["low-key", "test-key", "test-key"]
    assert calls[-1][1]["eventIds"].split(",") == (
        [original["id"], new["id"]] if zero_credit else [original["id"]]
    )
    generated = _generate_internal_id(
        "NBA", new["commence_time"], new["home_team"], new["away_team"]
    )
    assert predictions == [{"early-one", generated} if zero_credit else {"early-one"}]
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1
        assert {row[0] for row in conn.execute("SELECT odds_api_id FROM games")} == (
            {original["id"], new["id"], None} if zero_credit else {original["id"], None}
        )
        assert not ingest_nba_events._recent_attempt(conn, NOW + timedelta(minutes=1))
        probe = conn.execute(
            """
            SELECT r.raw_path, f.hash, r.records_ingested
            FROM source_runs r JOIN data_sources s USING (source_id)
            JOIN source_files f ON f.storage_path = r.raw_path
            WHERE s.source_key = ?
            """,
            (ingest_nba_events._ENTRY_PROBE_SOURCE,),
        ).fetchone()
        if not zero_credit:
            assert probe is None
            return
        assert probe is not None and probe["records_ingested"] == 1
        assert (
            conn.execute("SELECT provider_event_id FROM nba_event_discovery_links").fetchone()[0]
            == new["id"]
        )
        retained, coverage = audit_nba_schedule._provider_evidence(
            conn, path.parent, NOW - timedelta(hours=1), NOW
        )
        assert coverage["archives_unavailable_or_invalid"] == 0
        assert any(
            event.source == "entry_probe" and event.event_id == new["id"]
            for event in retained[("MIA", "ATL")]
        )
    archive = Path(probe["raw_path"])
    assert archive.is_relative_to(raw)
    assert "test-key" not in archive.read_text(encoding="utf-8")
    payload = json.loads(archive.read_bytes())
    assert payload["request"] == {"url": ingest_nba_events._URL, "params": {}}
    assert payload["results"] == [original, new, regular, future]


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


@pytest.mark.parametrize("book", ["draftkings", "fanduel"])
def test_paper_inputs_ignore_newer_moneyline_without_the_selected_totals(warehouse, book):
    path, _, events, _, predictions = warehouse
    _seed_quotes(events)
    first = events[0]
    h2h = first["bookmakers"][0]["markets"][0]
    title = "DraftKings" if book == "draftkings" else "FanDuel"
    newer = {
        **first,
        "bookmakers": [{"key": book, "title": title, "markets": [h2h]}],
    }
    _seed_quotes([newer], fetched=NOW - timedelta(minutes=1))

    assert paper_entry_poll.poll_entry(collect=True) == {
        "status": "reused", "due_events": 2, "decisions_recorded": 2,
    }
    assert predictions == [{"early-one", "early-two"}]
    with connect(path) as conn:
        assert not paper_entry_poll._has_table(conn, "paper_entry_requests")
        conn.execute(
            "UPDATE games SET start_time_utc = ? WHERE game_id = 'early-one'",
            ((datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),),
        )
    ordinary = load_games_from_database("NBA", fixture_ids={"early-one"}).iloc[0]
    assert ordinary.selected_h2h_snapshot_id != ordinary.selected_totals_snapshot_id
    paper = load_games_from_database("NBA", paper_quotes=True, fixture_ids={"early-one"}).iloc[0]
    assert paper.selected_h2h_snapshot_id == paper.selected_totals_snapshot_id
    assert paper.selected_h2h_book_id == paper.selected_totals_book_id == 1


def test_newer_unpaired_trial_totals_still_block_reuse_of_older_quotes(warehouse):
    path, _, events, _, _ = warehouse
    _seed_quotes(events)
    first = events[0]
    total = first["bookmakers"][0]["markets"][1]
    newer = {
        **first,
        "bookmakers": [{
            "key": "draftkings", "title": "DraftKings",
            "markets": [{**total, "outcomes": total["outcomes"][:1]}],
        }],
    }
    _seed_quotes([newer], fetched=NOW - timedelta(minutes=1))

    assert paper_entry_poll.poll_entry() == {"status": "due", "due_events": 2}
    with connect(path) as conn:
        conn.execute(
            "UPDATE games SET start_time_utc = ? WHERE game_id = 'early-one'",
            ((datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),),
        )
    paper = load_games_from_database("NBA", paper_quotes=True, fixture_ids={"early-one"}).iloc[0]
    assert pd.isna(paper.selected_totals_snapshot_id)


@pytest.mark.parametrize(
    ("fetch_offset", "status"),
    [
        (timedelta(minutes=-10), "reusable"),
        (timedelta(minutes=-10, microseconds=-1), "due"),
        (timedelta(microseconds=1), "due"),
    ],
)
def test_reuse_requires_the_exact_freshness_window(warehouse, fetch_offset, status):
    _, _, events, _, _ = warehouse
    _seed_quotes(events, fetched=NOW + fetch_offset)
    assert paper_entry_poll.poll_entry() == {"status": status, "due_events": 2}


@pytest.mark.parametrize(
    ("provider_lead", "outcome"),
    [(12, "outside_decision_window"), (15, "recorded")],
)
def test_ordinary_capture_uses_quote_tipoff_after_no_quote_schedule_update(
    warehouse, provider_lead, outcome
):
    path, _, events, _, _ = warehouse
    trial = paper_ledger.load_trial()
    provider_start = NOW + timedelta(minutes=provider_lead)
    current_start = NOW + timedelta(minutes=20)
    quote_event = {**events[0], "commence_time": provider_start.isoformat()}
    schedule_event = {**quote_event, "commence_time": current_start.isoformat(), "bookmakers": []}
    _seed_quotes([quote_event])
    _seed_quotes([schedule_event], fetched=NOW - timedelta(minutes=1))

    engine = paper_entry_poll.PredictionEngine(model_type=trial.model_type)
    engine.load_pinned_totals(trial.league, trial.model_sha256)
    predictions = engine.predict_pinned_totals(
        trial.league, trial.model_sha256, days_ahead=1, fixture_ids={"early-one"}
    )
    [(snapshot_id, start)] = list(
        predictions[["selected_totals_snapshot_id", "commence_time"]].itertuples(
            index=False, name=None
        )
    )
    assert start == current_start.isoformat()
    with connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM odds_snapshots").fetchone()[0] == 2
        proof = conn.execute(
            """
            SELECT observed_start_utc FROM odds_snapshot_game_starts
            WHERE snapshot_id = ? AND game_id = 'early-one'
            """,
            (snapshot_id,),
        ).fetchone()
        assert proof["observed_start_utc"] == provider_start.isoformat()

    assert paper_ledger.capture_predictions(trial, predictions, trial.model_sha256, path) == {
        outcome: 1,
        "prediction_missing": 2,
    }
    with connect(path) as conn:
        decision = conn.execute(
            "SELECT snapshot_id, start_time_utc FROM paper_decisions"
        ).fetchone()
    if outcome == "recorded":
        assert tuple(decision) == (snapshot_id, provider_start.isoformat())
    else:
        assert decision is None


@pytest.mark.parametrize("elapsed_minutes", [4, 5])
def test_retry_uses_original_verified_tipoff(warehouse, monkeypatch, elapsed_minutes):
    path, _, events, clock, predictions = warehouse
    canonical_start = NOW + timedelta(minutes=27)
    provider_start = NOW + timedelta(minutes=19)
    events[1]["commence_time"] = provider_start.isoformat()
    with connect(path) as conn:
        conn.execute(
            "UPDATE games SET start_time_utc = ? WHERE game_id = 'early-two'",
            (canonical_start.isoformat(),),
        )

    def interrupted(*args, **kwargs):
        raise RuntimeError("interrupted after appending quotes")

    with monkeypatch.context() as first_run:
        first_run.setattr(paper_entry_poll, "_budgeted_key", lambda **kwargs: "test-key")
        first_run.setattr(
            paper_entry_poll, "_provider_get", lambda endpoint, params: _response(events)
        )
        first_run.setattr(paper_entry_poll, "_capture", interrupted)
        with pytest.raises(RuntimeError, match="interrupted after appending quotes"):
            paper_entry_poll.poll_entry(collect=True)

    clock.current += timedelta(minutes=elapsed_minutes)
    accepted = {"early-one", "early-two"} if elapsed_minutes == 4 else {"early-one"}
    assert paper_entry_poll.poll_entry(collect=True) == {
        "status": "reused",
        "due_events": 2,
        "decisions_recorded": len(accepted),
    }
    assert predictions == [accepted]
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1
        decisions = dict(conn.execute("SELECT game_id, start_time_utc FROM paper_decisions"))
        assert set(decisions) == accepted
        evidence = dict(
            conn.execute(
                """
                SELECT e.game_id, e.earliest_start_utc
                FROM paper_entry_quote_starts e JOIN odds_snapshots s USING (snapshot_id)
                """
            )
        )
        assert evidence["early-two"] == provider_start.isoformat()
        if "early-two" in accepted:
            assert decisions["early-two"] == provider_start.isoformat()


def test_legacy_entry_cache_without_verified_start_fails_closed(warehouse):
    path, _, events, _, predictions = warehouse
    _seed_quotes(events)
    with connect(path) as conn:
        conn.execute(
            "UPDATE odds_snapshots SET raw_path = ?",
            (r"D:\previous\data\raw\odds\basketball_nba\paper_entry\legacy\odds.json",),
        )
        conn.execute("CREATE TABLE paper_entry_requests (entry_date_et TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO paper_entry_requests VALUES ('2026-11-07')")
    assert paper_entry_poll.poll_entry(collect=True) == {"status": "daily_cap", "due_events": 2}
    assert not predictions
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_decisions").fetchone()[0] == 0


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
        assert conn.execute("SELECT count(*) FROM odds_snapshot_game_starts").fetchone()[0] == 0
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
