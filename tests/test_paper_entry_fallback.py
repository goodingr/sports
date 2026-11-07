"""Offline proof for the official-slate paper-entry contingency."""

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier

import pandas as pd
import pytest
import requests

from src.data import ingest_nba_events
from src.db.core import connect, initialize
from src.predict import paper_entry_fallback as fallback
from src.predict import paper_entry_poll as entry
from src.predict import paper_ledger
from src.predict.paper_close_poll import OddsAPIKeyManager
from src.predict.paper_ledger import SCHEMA_PATH, load_trial

NOW = datetime(2026, 11, 8, 19, 50, tzinfo=timezone.utc)  # 14:50 Eastern
PHI_START = datetime(2026, 11, 8, 20, 30, tzinfo=timezone.utc)
DET_START = datetime(2026, 11, 8, 23, 0, tzinfo=timezone.utc)
TEAMS = {
    "PHI": (1, 1610612755, "Philadelphia 76ers"),
    "CHI": (2, 1610612741, "Chicago Bulls"),
    "LAL": (3, 1610612747, "Los Angeles Lakers"),
    "DET": (4, 1610612765, "Detroit Pistons"),
    "UTA": (5, 1610612762, "Utah Jazz"),
    "MIN": (6, 1610612750, "Minnesota Timberwolves"),
    "BKN": (7, 1610612751, "Brooklyn Nets"),
    "DAL": (8, 1610612742, "Dallas Mavericks"),
    "BOS": (9, 1610612738, "Boston Celtics"),
    "MIL": (10, 1610612749, "Milwaukee Bucks"),
    "NOP": (11, 1610612740, "New Orleans Pelicans"),
    "LAC": (12, 1610612746, "LA Clippers"),
    "MIA": (13, 1610612748, "Miami Heat"),
    "SAC": (14, 1610612758, "Sacramento Kings"),
}


def _official():
    def card(game_id, start, home, away):
        return {
            "cardType": "game",
            "cardData": {
                "gameId": game_id,
                "leagueId": "00",
                "seasonType": "Regular Season",
                "gameStatus": 1,
                "gameTimeUtc": start.isoformat(),
                "homeTeam": {"teamTricode": home, "teamId": TEAMS[home][1]},
                "awayTeam": {"teamTricode": away, "teamId": TEAMS[away][1]},
            },
        }

    return json.dumps(
        {
            "pageProps": {
                "selectedDate": "2026-11-08",
                "hasError": False,
                "allGamesInCurrentYear": {"2026": {"2026-11-08": 7}},
                "gameCardFeed": {
                    "modules": [
                        {
                            "cards": [
                                card("0022600196", PHI_START, "CHI", "PHI"),
                                card("0022600197", DET_START, "DET", "LAL"),
                                card("0022600198", DET_START + timedelta(hours=1), "MIN", "UTA"),
                                card("0022600199", DET_START + timedelta(minutes=90), "DAL", "BKN"),
                                card("0022600200", DET_START + timedelta(hours=2), "MIL", "BOS"),
                                card("0022600201", DET_START + timedelta(hours=3), "LAC", "NOP"),
                                card("0022600202", DET_START + timedelta(hours=3), "SAC", "MIA"),
                            ]
                        }
                    ]
                },
            }
        }
    ).encode()


def _quote(provider_id, start, home, away):
    home_name, away_name = TEAMS[home][2], TEAMS[away][2]
    return {
        "id": provider_id,
        "sport_key": "basketball_nba",
        "commence_time": start.isoformat(),
        "home_team": home_name,
        "away_team": away_name,
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {
                        "key": "h2h",
                        "outcomes": [
                            {"name": home_name, "price": -120},
                            {"name": away_name, "price": 110},
                        ],
                    },
                    {
                        "key": "totals",
                        "outcomes": [
                            {"name": "Over", "price": -110, "point": 225.5},
                            {"name": "Under", "price": -108, "point": 225.5},
                        ],
                    },
                ],
            }
        ],
    }


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("The proof never uses the network")

    monkeypatch.setattr(requests, "get", unexpected)
    path = tmp_path / "sports.db"
    initialize(path)
    trial = load_trial()
    with connect(path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO sports (sport_id,name,league,default_market) VALUES (1,'Basketball','NBA','moneyline')"
        )
        conn.execute("INSERT INTO books VALUES (1, 'DraftKings', 'us')")
        conn.executemany(
            "INSERT INTO teams (team_id, sport_id, code, name) VALUES (?, 1, ?, ?)",
            [(team_id, code, name) for code, (team_id, _, name) in TEAMS.items()],
        )
        conn.execute(
            "INSERT INTO paper_trials VALUES (?, ?, ?)",
            (trial.trial_id, (NOW - timedelta(days=1)).isoformat(), trial.specification),
        )
    return path, trial


def _game(conn, game_id, start, home, away, provider_id=None):
    conn.execute(
        """
        INSERT INTO games (game_id,sport_id,start_time_utc,home_team_id,away_team_id,odds_api_id)
        VALUES (?,1,?,?,?,?)
        """,
        (game_id, start.isoformat(), TEAMS[home][0], TEAMS[away][0], provider_id),
    )


def _plan_window(conn, trial, raw, observed, now):
    status, _, _, window = entry._plan_entry_path(conn, trial, now, (raw, observed))
    return status, window


def test_exact_november_8_request_and_pairing_preserve_earliest_verified_start(warehouse):
    path, trial = warehouse
    earlier = PHI_START - timedelta(minutes=5)
    with connect(path) as conn:
        _game(conn, "canonical-phi", earlier, "CHI", "PHI")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        before = list(conn.iterdump())
        raw = _official()
        observed = NOW - timedelta(minutes=5)
        status, plan = _plan_window(conn, trial, raw, observed, NOW)
        assert status == "window_due"
        assert plan is not None
        assert plan.eastern_date.isoformat() == "2026-11-08"
        assert plan.official_sha256 == hashlib.sha256(raw).hexdigest()
        assert plan.official_observed_at == observed
        assert [target.official.game_id for target in plan.targets] == ["0022600196", "0022600197"]
        assert plan.odds_window() == {
            "dateFormat": "iso",
            "commenceTimeFrom": "2026-11-08T20:20:00Z",
            "commenceTimeTo": "2026-11-08T23:10:00Z",
        }
        fixtures = fallback.match_window_fixtures(
            conn,
            trial,
            plan,
            [
                _quote("a" * 32, PHI_START + timedelta(minutes=5), "CHI", "PHI"),
                _quote("b" * 32, DET_START + timedelta(minutes=10), "DET", "LAL"),
                _quote("c" * 32, DET_START + timedelta(hours=1), "MIN", "UTA"),
            ],
            NOW + timedelta(seconds=1),
            1,
        )
        assert set(fixtures) == {"a" * 32, "b" * 32}
        phi, det = fixtures["a" * 32], fixtures["b" * 32]
        assert phi.fixture.game_id == "canonical-phi"
        assert phi.fixture.start == earlier
        assert phi.action == "linked"
        assert det.provider.start == DET_START + timedelta(minutes=10)
        assert det.fixture.start == DET_START
        assert det.action == "created"
        assert list(conn.iterdump()) == before


@pytest.mark.parametrize(
    ("stage", "expected"),
    [
        ("too_early", "waiting_for_last_window"),
        ("stale", "stale_official_observation"),
        ("previous_reservation", "daily_cap"),
        ("reversed", "fixture_conflict"),
        ("duplicate", "fixture_conflict"),
        ("scored", "fixture_conflict"),
    ],
)
def test_fallback_gate_preserves_normal_route_and_one_eastern_date_slot(warehouse, stage, expected):
    path, trial = warehouse
    now, observed = NOW, NOW
    with connect(path) as conn:
        if stage == "too_early":
            now -= timedelta(minutes=5)
            observed = now
        elif stage == "stale":
            observed -= timedelta(minutes=31)
        elif stage == "previous_reservation":
            conn.execute(
                "CREATE TABLE paper_entry_requests (entry_date_et TEXT PRIMARY KEY, requested_at_utc TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO paper_entry_requests VALUES ('2026-11-08', ?)", (NOW.isoformat(),)
            )
        elif stage == "reversed":
            _game(conn, "reversed", PHI_START, "PHI", "CHI")
        else:
            _game(conn, "canonical", PHI_START, "CHI", "PHI")
            if stage == "duplicate":
                _game(conn, "other", PHI_START + timedelta(minutes=1), "CHI", "PHI")
            else:
                conn.execute(
                    "INSERT INTO game_results (game_id, home_score) VALUES ('canonical',0)"
                )
        assert _plan_window(conn, trial, _official(), observed, now) == (
            expected,
            None,
        )


@pytest.mark.parametrize("defect", ["duplicate", "reversed", "foreign_id", "schedule_span"])
def test_matcher_rejects_unverified_fixtures_and_quotes(warehouse, defect):
    path, trial = warehouse
    with connect(path) as conn:
        if defect == "schedule_span":
            _game(conn, "early-source", PHI_START - timedelta(minutes=10), "CHI", "PHI")
        status, plan = _plan_window(conn, trial, _official(), NOW, NOW)
        assert status == "window_due" and plan is not None
        quote = _quote("a" * 32, PHI_START + timedelta(minutes=10), "CHI", "PHI")
        payload = [quote]
        if defect == "duplicate":
            payload.append(quote)
        elif defect == "reversed":
            payload = [_quote("a" * 32, PHI_START, "PHI", "CHI")]
        elif defect == "foreign_id":
            _game(conn, "different", DET_START, "MIN", "UTA", "a" * 32)
        assert fallback.match_window_fixtures(conn, trial, plan, payload, NOW, 1) == {}


def test_match_keeps_earlier_pre_request_canonical_clock_after_schedule_refresh(warehouse):
    path, trial = warehouse
    initial = PHI_START - timedelta(minutes=5)
    with connect(path) as conn:
        _game(conn, "same-fixture", initial, "CHI", "PHI")
        status, plan = _plan_window(conn, trial, _official(), NOW, NOW)
        assert status == "window_due" and plan is not None
        conn.execute("UPDATE games SET start_time_utc = ?", (PHI_START.isoformat(),))
        [match] = fallback.match_window_fixtures(
            conn,
            trial,
            plan,
            [
                _quote("a" * 32, PHI_START + timedelta(minutes=5), "CHI", "PHI"),
            ],
            NOW,
            1,
        ).values()
        assert match.fixture.start == initial
        assert match.fixture.latest_start == PHI_START + timedelta(minutes=5)


@pytest.fixture
def entry_runtime(warehouse, tmp_path, monkeypatch):
    path, trial = warehouse
    raw = tmp_path / "data" / "raw"

    class ClockType(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class Clock(datetime, metaclass=ClockType):
        current = NOW

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz)

    predictions = []

    class PinnedModel:
        def __init__(self, *, model_type):
            assert model_type == trial.model_type

        def load_pinned_totals(self, league, model_sha256):
            assert (league, model_sha256) == ("NBA", trial.model_sha256)
            self.totals_model_sha256 = model_sha256

        def predict_pinned_totals(self, league, model_sha256, *, days_ahead, fixture_ids):
            assert (league, model_sha256, days_ahead) == ("NBA", trial.model_sha256, 1)
            predictions.append(fixture_ids)
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
                        "game_id": row["game_id"],
                        "commence_time": row["start_time_utc"],
                        "predicted_at": Clock.now(timezone.utc).isoformat(),
                        "selected_totals_snapshot_id": row["snapshot_id"],
                        "selected_totals_book_id": 1,
                        "selected_h2h_snapshot_id": row["snapshot_id"],
                        "selected_h2h_book_id": 1,
                        "home_moneyline": -120,
                        "away_moneyline": 110,
                        "total_line": 225.5,
                        "over_moneyline": -110,
                        "under_moneyline": -108,
                        "over_predicted_prob": 0.8,
                        "under_predicted_prob": 0.2,
                    }
                    for row in selected
                ]
            )

    monkeypatch.setenv("SPORTS_DB_PATH", str(path))
    monkeypatch.setattr(entry, "load_env", lambda: None)
    monkeypatch.setattr(entry, "RAW_DATA_DIR", raw)
    monkeypatch.setattr(ingest_nba_events, "RAW_DATA_DIR", raw)
    for module in (entry, fallback, ingest_nba_events, paper_ledger):
        monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(OddsAPIKeyManager, "get_available_keys", lambda: ["test-key"])
    monkeypatch.setattr(OddsAPIKeyManager, "get_current_index", lambda: 0)
    monkeypatch.setattr(entry, "PredictionEngine", PinnedModel)
    return path, raw, Clock, predictions


def _mock_sources(
    monkeypatch, paid, *, events=None, free_cost="0", remaining="202", barrier=None, official=None
):
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs.get("params", {})))
        response = requests.Response()
        response.status_code = 200
        if url == "https://www.nba.com/games?date=2026-11-08":
            assert "params" not in kwargs
            response._content = _official() if official is None else official
        elif url == f"{entry._BASE_URL}/events":
            assert kwargs["params"] == {"apiKey": "test-key"}
            response.headers.update(
                {"x-requests-last": free_cost, "x-requests-remaining": remaining}
            )
            if barrier is not None:
                barrier.wait(timeout=10)
            response._content = json.dumps(events if events is not None else []).encode()
        elif url == f"{entry._BASE_URL}/odds":
            payload = paid() if callable(paid) else paid
            response.headers["x-requests-remaining"] = "200"
            response._content = (
                payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            )
        else:
            pytest.fail(f"Unexpected endpoint: {url}")
        return response

    monkeypatch.setattr(requests, "get", get)
    return calls


def _collect_window():
    return entry.poll_entry(collect=True, official_window_fallback=True)


def _resume_after_reservation(monkeypatch, clock, dispatch_at):
    reserve = entry._reserve

    def delayed(*args, **kwargs):
        result = reserve(*args, **kwargs)
        assert result[0] in {"due", "window_due"}
        clock.current = dispatch_at
        return result

    monkeypatch.setattr(entry, "_reserve", delayed)


@pytest.mark.parametrize(
    ("dispatch_at", "status"),
    [
        (PHI_START - timedelta(minutes=30), "no_entry_quotes"),
        (PHI_START - timedelta(minutes=30) + timedelta(microseconds=1), "waiting_for_last_window"),
        (NOW + timedelta(minutes=20), "waiting_for_last_window"),
        (NOW + timedelta(minutes=20, microseconds=1), "no_entry_quotes"),
        (NOW + timedelta(minutes=30), "no_entry_quotes"),
        (NOW + timedelta(minutes=30, microseconds=1), "stale_official_observation"),
        (NOW.replace(hour=23, minute=15), "outside_entry_window"),
        (NOW + timedelta(hours=10), "entry_date_changed"),
        (NOW - timedelta(microseconds=1), "entry_clock_reversed"),
    ],
)
def test_fallback_rechecks_clocks_at_paid_dispatch(entry_runtime, monkeypatch, dispatch_at, status):
    path, _, clock, _ = entry_runtime
    # The second fixture is exactly 45 minutes away 20 minutes after reservation.
    second_start = NOW + timedelta(minutes=65)
    official = _official().replace(
        DET_START.isoformat().encode(), second_start.isoformat().encode(), 1
    )
    calls = _mock_sources(monkeypatch, [], official=official)
    _resume_after_reservation(monkeypatch, clock, dispatch_at)

    assert _collect_window()["status"] == status

    assert len(calls) == (3 if status == "no_entry_quotes" else 2)
    with connect(path) as conn:
        assert conn.execute("SELECT entry_date_et FROM paper_entry_requests").fetchone()[0] == (
            "2026-11-08"
        )
    clock.current = NOW
    assert _collect_window()["status"] == "daily_cap"
    assert len(calls) == (3 if status == "no_entry_quotes" else 2)


def test_fallback_dispatch_uses_earlier_locked_canonical_start(entry_runtime, monkeypatch):
    path, _, clock, _ = entry_runtime
    with connect(path) as conn:
        _game(conn, "existing", PHI_START - timedelta(minutes=5), "CHI", "PHI")
    calls = _mock_sources(monkeypatch, [])
    _resume_after_reservation(monkeypatch, clock, PHI_START - timedelta(minutes=30))

    assert _collect_window()["status"] == "waiting_for_last_window"
    assert len(calls) == 2


@pytest.mark.parametrize(
    ("late_by", "second_due", "status"),
    [(0, False, "no_entry_quotes"), (1, False, "entry_lead_elapsed"), (1, True, "no_entry_quotes")],
)
def test_ordinary_paid_batch_keeps_any_actionable_fixture(
    entry_runtime, monkeypatch, late_by, second_due, status
):
    path, _, clock, _ = entry_runtime
    with connect(path) as conn:
        _game(conn, "phi", PHI_START, "CHI", "PHI", "a" * 32)
        if second_due:
            _game(conn, "det", DET_START, "DET", "LAL", "b" * 32)
    calls = _mock_sources(monkeypatch, [])
    _resume_after_reservation(
        monkeypatch, clock, PHI_START - timedelta(minutes=15) + timedelta(microseconds=late_by)
    )

    assert entry.poll_entry(collect=True)["status"] == status

    assert len(calls) == (2 if status == "no_entry_quotes" else 1)
    if second_due:
        assert calls[-1][1]["eventIds"] == f"{'a' * 32},{'b' * 32}"
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1


@pytest.mark.parametrize("use_fallback", [False, True])
def test_review_cutoff_prevents_reserved_paid_dispatch(entry_runtime, monkeypatch, use_fallback):
    path, _, clock, _ = entry_runtime
    review = NOW + timedelta(minutes=1)
    trial = replace(load_trial(), review_at=review.isoformat())
    with connect(path) as conn:
        conn.execute("UPDATE paper_trials SET specification = ?", (trial.specification,))
        if not use_fallback:
            _game(conn, "phi", PHI_START, "CHI", "PHI", "a" * 32)
    monkeypatch.setattr(entry, "load_trial", lambda *_: trial)
    calls = _mock_sources(monkeypatch, [])
    _resume_after_reservation(monkeypatch, clock, review)

    assert entry.poll_entry(collect=True, official_window_fallback=use_fallback)["status"] == (
        "trial_inactive"
    )
    assert len(calls) == (2 if use_fallback else 1)


def test_window_binds_only_raw_pairs_and_captures_the_earliest_verified_clocks(
    entry_runtime, monkeypatch
):
    path, _, _, predictions = entry_runtime
    canonical_start = PHI_START - timedelta(minutes=5)
    phi_provider = PHI_START + timedelta(minutes=5)
    det_provider = DET_START + timedelta(minutes=10)
    with connect(path) as conn:
        _game(conn, "existing-phi", canonical_start, "CHI", "PHI")
        conn.execute(
            """
            INSERT INTO game_results (game_id, total_close, source_version)
            VALUES ('existing-phi', 221.5, 'prior')
            """
        )
        previous_game = dict(conn.execute("SELECT * FROM games").fetchone())
        previous_result = tuple(conn.execute("SELECT * FROM game_results").fetchone())
    payload = [
        _quote("a" * 32, phi_provider, "CHI", "PHI"),
        _quote("b" * 32, det_provider, "DET", "LAL"),
        _quote("c" * 32, DET_START + timedelta(hours=1), "MIN", "UTA"),
    ]
    calls = _mock_sources(monkeypatch, payload)
    assert _collect_window() == {
        "status": "collected",
        "due_events": 2,
        "events_loaded": 2,
        "decisions_recorded": 2,
        "mode": "official_window",
    }
    assert [url.rsplit("/", 1)[-1] for url, _ in calls] == [
        "games?date=2026-11-08",
        "events",
        "odds",
    ]
    _, params = calls[-1]
    assert params == {
        "apiKey": "test-key",
        "regions": "us",
        "bookmakers": "draftkings",
        "markets": "h2h,totals",
        "dateFormat": "iso",
        "oddsFormat": "american",
        "commenceTimeFrom": "2026-11-08T20:20:00Z",
        "commenceTimeTo": "2026-11-08T23:10:00Z",
    }
    with connect(path) as conn:
        games = {row["odds_api_id"]: dict(row) for row in conn.execute("SELECT * FROM games")}
        assert set(games) == {"a" * 32, "b" * 32}
        assert games["a" * 32] == {**previous_game, "odds_api_id": "a" * 32}
        assert tuple(conn.execute("SELECT * FROM game_results").fetchone()) == previous_result
        det = games["b" * 32]
        assert det["start_time_utc"] == det_provider.isoformat()
        assert predictions == [{"existing-phi", det["game_id"]}]
        for table, col, expected_phi, expected_det in (
            ("odds_snapshot_game_starts", "observed_start_utc", phi_provider, det_provider),
            ("paper_entry_quote_starts", "earliest_start_utc", canonical_start, DET_START),
            ("paper_decisions", "start_time_utc", canonical_start, DET_START),
        ):
            assert dict(conn.execute(f"SELECT game_id, {col} FROM {table}")) == {
                "existing-phi": expected_phi.isoformat(),
                det["game_id"]: expected_det.isoformat(),
            }
        assert conn.execute("SELECT count(*) FROM odds").fetchone()[0] == 8
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1
        raw_path = Path(conn.execute("SELECT raw_path FROM odds_snapshots").fetchone()[0])
    contents = raw_path.read_text(encoding="utf-8")
    envelope = json.loads(contents)
    assert "test-key" not in contents
    assert envelope["results"] == payload
    assert envelope["provider_events_probe"] == {
        "url": f"{entry._BASE_URL}/events",
        "observed_at": NOW.isoformat(),
        "request_credits": 0,
        "results": [],
    }
    official = envelope["official_schedule"]
    assert official["observed_at"] == NOW.isoformat()
    assert official["nba_game_ids"] == ["0022600196", "0022600197"]
    archived = raw_path.with_name(official["filename"]).read_bytes()
    assert archived == _official()
    assert official["sha256"] == hashlib.sha256(archived).hexdigest()


@pytest.mark.parametrize(
    "defect", ["reversed", "line", "missing_h2h", "future_market", "malformed_market"]
)
def test_no_match_archives_response_without_binding_and_consumes_only_one_slot(
    entry_runtime, monkeypatch, defect
):
    path, raw, _, predictions = entry_runtime
    event = _quote("a" * 32, PHI_START, "CHI", "PHI")
    markets = event["bookmakers"][0]["markets"]
    if defect == "reversed":
        event = _quote("a" * 32, PHI_START, "PHI", "CHI")
    elif defect == "line":
        markets[1]["outcomes"][1]["point"] = 226.5
    elif defect == "missing_h2h":
        markets.pop(0)
    elif defect == "future_market":
        markets[1]["last_update"] = (NOW + timedelta(seconds=1)).isoformat()
    else:
        markets[0]["last_update"] = "broken"
    calls = _mock_sources(monkeypatch, [event])
    assert _collect_window()["status"] == "no_entry_quotes"
    assert _collect_window() == {"status": "daily_cap", "due_events": 0}
    assert len(calls) == 3
    assert not predictions
    assert len(list(raw.rglob("official_schedule.html"))) == 1
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1
        for table in ("games", "odds_snapshots", "paper_decisions"):
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0


def test_winning_free_probe_routes_fully_linked_slate_to_normal_entry(entry_runtime, monkeypatch):
    path, raw, _, _ = entry_runtime
    paid = [
        _quote("a" * 32, PHI_START, "CHI", "PHI"),
        _quote("b" * 32, DET_START, "DET", "LAL"),
    ]
    calls = _mock_sources(monkeypatch, paid, events=paid)
    assert _collect_window() == {
        "status": "collected",
        "due_events": 2,
        "events_loaded": 2,
        "decisions_recorded": 2,
    }
    assert len(calls) == 3
    assert calls[-1][1]["eventIds"] == f'{"a" * 32},{"b" * 32}'
    assert "commenceTimeFrom" not in calls[-1][1]
    assert not list(raw.rglob("official_schedule.html"))
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM nba_event_discovery_links").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1


def test_concurrent_fallbacks_share_one_paid_request(entry_runtime, monkeypatch):
    path, _, _, _ = entry_runtime
    paid = [_quote("a" * 32, PHI_START, "CHI", "PHI")]
    calls = _mock_sources(monkeypatch, paid, barrier=Barrier(2))
    with ThreadPoolExecutor(max_workers=2) as pool:
        runs = [pool.submit(_collect_window) for _ in range(2)]
        statuses = sorted(run.result(timeout=20)["status"] for run in runs)
    # The loser may see the winner's already-written quotes and reuse them for free.
    assert statuses in (["collected", "daily_cap"], ["collected", "reused"])
    assert sum(url.endswith("/odds") for url, _ in calls) == 1
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (b"broken", "invalid entry odds JSON"),
        ({"unexpected": []}, "unexpected entry odds response"),
        ("timeout", "Odds API odds request failed"),
    ],
)
def test_failed_paid_attempt_cannot_request_twice(entry_runtime, monkeypatch, failure, message):
    path, _, _, predictions = entry_runtime

    def paid():
        if failure == "timeout":
            raise requests.Timeout("sensitive URL")
        return failure

    calls = _mock_sources(monkeypatch, paid)
    with pytest.raises(RuntimeError, match=message) as error:
        _collect_window()
    assert "sensitive URL" not in str(error.value)
    assert _collect_window()["status"] == "daily_cap"
    assert len(calls) == 3
    assert not predictions
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM games").fetchone()[0] == 0


def test_paid_response_after_official_deadline_cannot_bind_provider_fixture(
    entry_runtime, monkeypatch
):
    path, _, clock, predictions = entry_runtime

    def paid():
        clock.current = PHI_START - timedelta(minutes=15) + timedelta(seconds=1)
        return [_quote("a" * 32, PHI_START + timedelta(minutes=10), "CHI", "PHI")]

    calls = _mock_sources(monkeypatch, paid)
    assert _collect_window()["status"] == "no_entry_quotes"
    assert len(calls) == 3
    assert not predictions
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM games").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("free_cost", "remaining", "status"),
    [("1", "202", "unverified_zero_credit_probe"), ("0", "201", "quota_reserve")],
)
def test_fallback_requires_verified_probe_and_two_credits_above_reserve(
    entry_runtime, monkeypatch, free_cost, remaining, status
):
    path, _, _, _ = entry_runtime
    calls = _mock_sources(monkeypatch, [], free_cost=free_cost, remaining=remaining)
    assert _collect_window()["status"] == status
    assert len(calls) == 2
    with connect(path) as conn:
        assert not entry._has_table(conn, "paper_entry_requests")


def test_ordinary_default_does_not_call_official_source(entry_runtime):
    assert entry.poll_entry(collect=True) == {"status": "no_entry_due", "due_events": 0}


def test_duplicate_official_team_aborts_before_budget(entry_runtime, monkeypatch):
    path, _, _, _ = entry_runtime
    slate = json.loads(_official())
    cards = slate["pageProps"]["gameCardFeed"]["modules"][0]["cards"]
    cards[1]["cardData"]["awayTeam"] = cards[0]["cardData"]["awayTeam"]
    monkeypatch.setattr(
        fallback, "fetch_official_slate", lambda day: (json.dumps(slate).encode(), NOW)
    )
    assert _collect_window() == {"status": "fixture_conflict", "due_events": 0}
    with connect(path) as conn:
        assert not entry._has_table(conn, "paper_entry_requests")


def test_invalid_probe_does_not_trigger_paid_fallback(entry_runtime, monkeypatch):
    path, _, _, _ = entry_runtime
    calls = _mock_sources(monkeypatch, [], events={"unexpected": []})
    assert _collect_window()["status"] == "invalid_zero_credit_probe"
    assert len(calls) == 2
    with connect(path) as conn:
        assert not entry._has_table(conn, "paper_entry_requests")


def test_failed_quote_write_rolls_back_fixture_binding_and_preserves_paid_slot(
    entry_runtime, monkeypatch
):
    path, _, _, _ = entry_runtime
    with connect(path) as conn:
        _game(conn, "existing", PHI_START, "CHI", "PHI")
        conn.executescript(
            """
            CREATE TRIGGER reject_quote BEFORE INSERT ON odds
            BEGIN SELECT RAISE(ABORT, 'test quote write failure'); END;
            """
        )
    calls = _mock_sources(
        monkeypatch,
        [_quote("a" * 32, PHI_START, "CHI", "PHI"), _quote("b" * 32, DET_START, "DET", "LAL")],
    )
    with pytest.raises(sqlite3.IntegrityError, match="test quote write failure"):
        _collect_window()
    assert _collect_window()["status"] == "daily_cap"
    assert len(calls) == 3
    with connect(path) as conn:
        assert [tuple(row) for row in conn.execute("SELECT game_id, odds_api_id FROM games")] == [
            ("existing", None)
        ]
        assert conn.execute("SELECT count(*) FROM odds_snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1


@pytest.mark.parametrize(
    "defect", ["duplicate_id", "cross_pair_id", "competing_id", "reversed", "clock", "malformed"]
)
def test_free_probe_ambiguity_cannot_trigger_paid_fallback(entry_runtime, monkeypatch, defect):
    path, _, _, _ = entry_runtime
    valid = _quote("a" * 32, PHI_START, "CHI", "PHI")
    probe = {
        "duplicate_id": [valid, valid],
        "cross_pair_id": [valid, _quote("a" * 32, DET_START, "DET", "LAL")],
        "competing_id": [valid, _quote("b" * 32, PHI_START, "CHI", "PHI")],
        "reversed": [_quote("a" * 32, PHI_START, "PHI", "CHI")],
        "clock": [_quote("a" * 32, PHI_START + timedelta(minutes=11), "CHI", "PHI")],
        "malformed": [42],
    }[defect]
    calls = _mock_sources(monkeypatch, [valid], events=probe)
    assert _collect_window()["status"] == "provider_probe_conflict"
    assert len(calls) == 2
    with connect(path) as conn:
        assert not entry._has_table(conn, "paper_entry_requests")
        assert conn.execute("SELECT count(*) FROM games").fetchone()[0] == 0


def test_unrelated_well_formed_free_events_do_not_block_window(entry_runtime, monkeypatch):
    paid = [_quote("a" * 32, PHI_START, "CHI", "PHI")]
    events = [_quote("c" * 32, DET_START + timedelta(hours=1), "MIN", "UTA")]
    calls = _mock_sources(monkeypatch, paid, events=events)
    assert _collect_window()["status"] == "collected"
    assert len(calls) == 3


def test_ordinary_entry_keeps_existing_market_clock_policy(entry_runtime, monkeypatch):
    path, _, _, _ = entry_runtime
    with connect(path) as conn:
        _game(conn, "regular", PHI_START, "CHI", "PHI", "a" * 32)
    event = _quote("a" * 32, PHI_START, "CHI", "PHI")
    event["bookmakers"][0]["markets"][1]["last_update"] = (NOW + timedelta(seconds=1)).isoformat()
    calls = _mock_sources(monkeypatch, [event])
    assert entry.poll_entry(collect=True)["status"] == "collected"
    assert [url.rsplit("/", 1)[-1] for url, _ in calls] == ["events", "odds"]
    assert calls[-1][1]["eventIds"] == "a" * 32


@pytest.mark.parametrize("state", ["early_known", "late_known", "partially_linked"])
def test_mixed_window_keeps_verified_known_fixtures_and_the_single_slot(
    entry_runtime, monkeypatch, state
):
    path, raw, clock, predictions = entry_runtime
    known_phi = state != "late_known"
    known_id = "a" * 32 if known_phi else "b" * 32
    start = PHI_START if known_phi else DET_START
    home, away = ("CHI", "PHI") if known_phi else ("DET", "LAL")
    earlier_probe = start - timedelta(minutes=4)
    if state == "early_known":
        clock.current = NOW - timedelta(minutes=30)
    if state != "partially_linked":
        with connect(path) as conn:
            _game(conn, "known", start, home, away, known_id)
    paid = [
        _quote("a" * 32, PHI_START + timedelta(minutes=2), "CHI", "PHI"),
        _quote("b" * 32, DET_START + timedelta(minutes=2), "DET", "LAL"),
    ]
    calls = _mock_sources(monkeypatch, paid, events=[_quote(known_id, earlier_probe, home, away)])
    assert _collect_window() == {
        "status": "collected",
        "due_events": 2,
        "events_loaded": 2,
        "decisions_recorded": 2,
        "mode": "official_window",
    }
    assert len(calls) == 3 and "eventIds" not in calls[-1][1]
    assert calls[-1][1]["commenceTimeFrom"] == "2026-11-08T20:20:00Z"
    assert calls[-1][1]["commenceTimeTo"] == "2026-11-08T23:10:00Z"
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1
        assert (
            dict(
                conn.execute(
                    """
            SELECT g.odds_api_id, d.start_time_utc
            FROM paper_decisions d JOIN games g USING(game_id)
            """
                )
            )[known_id]
            == earlier_probe.isoformat()
        )
        if state != "partially_linked":
            assert (
                conn.execute("SELECT start_time_utc FROM games WHERE game_id='known'").fetchone()[0]
                == start.isoformat()
            )
    assert len(predictions[0]) == 2
    assert len(list(raw.rglob("official_schedule.html"))) == 1
    clock.current = DET_START - timedelta(hours=1)
    assert _collect_window()["status"] == "daily_cap"
    assert len(calls) == 3


@pytest.mark.parametrize(
    "defect",
    [
        "official_unavailable",
        "official_invalid",
        "known_absent",
        "free_unverified",
        "free_conflict",
        "ordinary_uncovered",
    ],
)
def test_inconclusive_mixed_evidence_preserves_every_due_targeted_fixture(
    entry_runtime, monkeypatch, defect
):
    path, _, clock, _ = entry_runtime
    clock.current = NOW - timedelta(minutes=30)
    early = _quote("a" * 32, PHI_START, "CHI", "PHI")
    free = [early]
    paid = [early]
    with connect(path) as conn:
        _game(conn, "known", PHI_START, "CHI", "PHI", "a" * 32)
        if defect == "ordinary_uncovered":
            extra_start = PHI_START + timedelta(minutes=10)
            _game(conn, "other", extra_start, "MIN", "UTA", "c" * 32)
            extra = _quote("c" * 32, extra_start, "MIN", "UTA")
            free.append(extra)
            paid.append(extra)
    if defect == "official_unavailable":

        def unavailable(day):
            raise RuntimeError("offline official schedule")

        monkeypatch.setattr(fallback, "fetch_official_slate", unavailable)
    elif defect == "official_invalid":
        monkeypatch.setattr(
            fallback, "fetch_official_slate", lambda day: (b"broken", clock.current)
        )
    elif defect == "known_absent":
        free.clear()
    elif defect == "free_conflict":
        free.append(_quote("b" * 32, PHI_START, "PHI", "CHI"))
    calls = _mock_sources(
        monkeypatch, paid, events=free, free_cost="1" if defect == "free_unverified" else "0"
    )
    result = _collect_window()
    assert result["status"] == "collected" and "mode" not in result
    assert result["events_loaded"] == len(paid)
    assert calls[-1][1]["eventIds"] == ",".join(sorted(e["id"] for e in paid))
    assert sum(url.endswith("/odds") for url, _ in calls) == 1
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM games").fetchone()[0] == len(paid)
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1


def test_mixed_paid_response_cannot_replace_known_provider_identity(entry_runtime, monkeypatch):
    path, _, _, _ = entry_runtime
    with connect(path) as conn:
        _game(conn, "known", PHI_START, "CHI", "PHI", "a" * 32)
    calls = _mock_sources(
        monkeypatch,
        [_quote("f" * 32, PHI_START, "CHI", "PHI"), _quote("b" * 32, DET_START, "DET", "LAL")],
        events=[_quote("a" * 32, PHI_START, "CHI", "PHI")],
    )
    assert _collect_window()["events_loaded"] == 1
    assert len(calls) == 3
    with connect(path) as conn:
        assert (
            conn.execute("SELECT odds_api_id FROM games WHERE game_id='known'").fetchone()[0]
            == "a" * 32
        )
        assert conn.execute("SELECT count(*) FROM odds WHERE game_id='known'").fetchone()[0] == 0


@pytest.mark.parametrize("earlier_source", ["official", "free"])
@pytest.mark.parametrize(("opt_in", "gap"), [(True, 10), (True, 11), (False, 11)])
def test_targeted_downgrade_still_uses_an_earlier_verified_clock(
    entry_runtime, monkeypatch, earlier_source, opt_in, gap
):
    path, raw, clock, _ = entry_runtime
    clock.current = PHI_START - timedelta(minutes=10)
    first = PHI_START + timedelta(minutes=gap)
    second = PHI_START + timedelta(minutes=30)
    third = DET_START + timedelta(hours=1)
    cutoff_source = PHI_START if earlier_source == "official" else PHI_START + timedelta(minutes=5)
    official = _official().replace(
        PHI_START.isoformat().encode(), cutoff_source.isoformat().encode(), 1
    )
    official = official.replace(DET_START.isoformat().encode(), second.isoformat().encode(), 1)
    official = official.replace(third.isoformat().encode(), DET_START.isoformat().encode(), 1)
    with connect(path) as conn:
        _game(conn, "too-late", first, "CHI", "PHI", "a" * 32)
        _game(conn, "on-time", second, "DET", "LAL", "b" * 32)
    paid = [_quote("a" * 32, first, "CHI", "PHI"), _quote("b" * 32, second, "DET", "LAL")]
    free_first = PHI_START if earlier_source == "free" else first
    free = [_quote("a" * 32, free_first, "CHI", "PHI"), paid[1]]
    calls = _mock_sources(monkeypatch, paid, events=free, official=official)
    blocked = opt_in and gap > 10
    assert entry.poll_entry(collect=True, official_window_fallback=opt_in) == {
        "status": "collected",
        "due_events": 1 if blocked else 2,
        "events_loaded": 1 if opt_in else 2,
        "decisions_recorded": 1 if opt_in else 2,
    }
    assert calls[-1][1]["eventIds"] == ("b" * 32 if blocked else f'{"a" * 32},{"b" * 32}')
    assert any(url.startswith("https://www.nba.com") for url, _ in calls) is opt_in
    with connect(path) as conn:
        decisions = {row[0] for row in conn.execute("SELECT game_id FROM paper_decisions")}
        assert decisions == ({"on-time"} if opt_in else {"on-time", "too-late"})
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1
    if opt_in and gap == 10:
        [archive] = raw.rglob("official_schedule.html")
        assert archive.read_bytes() == official


def test_mixed_dispatch_rechecks_the_earlier_verified_free_clock(entry_runtime, monkeypatch):
    path, _, clock, _ = entry_runtime
    nominal = PHI_START - timedelta(minutes=15)
    canonical = nominal + timedelta(minutes=5)
    free = nominal - timedelta(minutes=5)
    official = _official().replace(PHI_START.isoformat().encode(), nominal.isoformat().encode(), 1)
    with connect(path) as conn:
        _game(conn, "known", canonical, "CHI", "PHI", "a" * 32)
    calls = _mock_sources(
        monkeypatch, [], events=[_quote("a" * 32, free, "CHI", "PHI")], official=official
    )
    _resume_after_reservation(
        monkeypatch, clock, free - timedelta(minutes=15) + timedelta(microseconds=1)
    )
    assert _collect_window()["status"] == "entry_lead_elapsed"
    assert len(calls) == 2
    with connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM paper_entry_requests").fetchone()[0] == 1


@pytest.mark.parametrize("earlier_source", ["official", "free"])
def test_only_clock_conflict_does_not_reserve_paid_slot(entry_runtime, monkeypatch, earlier_source):
    path, _, clock, _ = entry_runtime
    clock.current = PHI_START - timedelta(minutes=20)
    canonical = PHI_START + timedelta(minutes=11)
    official_start = PHI_START if earlier_source == "official" else PHI_START + timedelta(minutes=5)
    official = _official().replace(
        PHI_START.isoformat().encode(), official_start.isoformat().encode(), 1
    )
    with connect(path) as conn:
        _game(conn, "known", canonical, "CHI", "PHI", "a" * 32)
    paid = [_quote("a" * 32, canonical, "CHI", "PHI")]
    free = [_quote("a" * 32, PHI_START, "CHI", "PHI")]
    calls = _mock_sources(monkeypatch, paid, events=free, official=official)
    assert _collect_window() == {"status": "fixture_clock_conflict", "due_events": 0}
    assert len(calls) == (1 if earlier_source == "official" else 2)
    assert not any(url.endswith("/odds") for url, _ in calls)
    with connect(path) as conn:
        assert not entry._has_table(conn, "paper_entry_requests")
        assert conn.execute("SELECT count(*) FROM paper_decisions").fetchone()[0] == 0


@pytest.mark.parametrize("gap", [10, 11])
def test_free_clock_still_applies_if_the_official_page_is_unavailable(
    entry_runtime, monkeypatch, gap
):
    path, _, clock, _ = entry_runtime
    clock.current = PHI_START - timedelta(minutes=25)
    canonical = PHI_START + timedelta(minutes=gap)
    with connect(path) as conn:
        _game(conn, "known", canonical, "CHI", "PHI", "a" * 32)

    def unavailable(day):
        raise RuntimeError("offline official schedule")

    monkeypatch.setattr(fallback, "fetch_official_slate", unavailable)
    paid = [_quote("a" * 32, canonical, "CHI", "PHI")]
    free = [_quote("a" * 32, PHI_START, "CHI", "PHI")]
    calls = _mock_sources(monkeypatch, paid, events=free)
    result = _collect_window()
    with connect(path) as conn:
        if gap == 11:
            assert result == {"status": "fixture_clock_conflict", "due_events": 0}
            assert [url.rsplit("/", 1)[-1] for url, _ in calls] == ["events"]
            assert not entry._has_table(conn, "paper_entry_requests")
        else:
            assert result["decisions_recorded"] == 1
            assert (
                conn.execute("SELECT start_time_utc FROM paper_decisions").fetchone()[0]
                == PHI_START.isoformat()
            )
            [raw_path] = conn.execute("SELECT raw_path FROM odds_snapshots").fetchone()
            receipt = json.loads(Path(raw_path).read_text(encoding="utf-8"))
            assert receipt["provider_events_probe"]["results"] == free


@pytest.mark.parametrize("source", ["official", "free"])
@pytest.mark.parametrize(("gap", "cached_offset"), [(10, None), (11, None), (8, 0), (8, -1)])
def test_racing_cache_does_not_bypass_observed_earlier_clock(
    warehouse, entry_runtime, tmp_path, monkeypatch, source, gap, cached_offset
):
    path, trial = warehouse
    _, _, clock, _ = entry_runtime
    clock.current = PHI_START - timedelta(minutes=25 if cached_offset is not None else 10)
    first = PHI_START + timedelta(minutes=gap)
    second = PHI_START + timedelta(minutes=30)
    official_start = PHI_START if source == "official" else PHI_START + timedelta(minutes=5)
    official = _official().replace(
        PHI_START.isoformat().encode(), official_start.isoformat().encode(), 1
    )
    official = official.replace(DET_START.isoformat().encode(), second.isoformat().encode(), 1)
    with connect(path) as conn:
        _game(conn, "too-late", first, "CHI", "PHI", "a" * 32)
        _game(conn, "on-time", second, "DET", "LAL", "b" * 32)
        _, ordinary_due, _ = entry._plan(conn, trial, clock.current)
    if cached_offset is not None:
        provider_id = "a" * 32
        ordinary_due[provider_id] = replace(
            ordinary_due[provider_id], start=PHI_START + timedelta(minutes=cached_offset)
        )
    paid = [_quote("a" * 32, first, "CHI", "PHI"), _quote("b" * 32, second, "DET", "LAL")]
    free_start = PHI_START if source == "free" else first
    free = [_quote("a" * 32, free_start, "CHI", "PHI"), paid[1]]
    calls = _mock_sources(monkeypatch, paid, events=free, official=official)
    fake_request = requests.get
    cache_dir = entry.RAW_DATA_DIR / "odds" / "basketball_nba" / "paper_entry" / "racing-cache"
    cache_receipt = cache_dir / "receipt.json"
    cache_receipt.parent.mkdir(parents=True)
    fetched = clock.current - timedelta(seconds=1)
    raw = json.dumps(
        {
            "source": trial.odds_source,
            "fetched_at": fetched.isoformat(),
            "request": {"url": f"{entry._BASE_URL}/odds", "params": {}},
            "results": paid,
            "provider_events_probe": {
                "results": [
                    _quote("a" * 32, PHI_START + timedelta(minutes=cached_offset), "CHI", "PHI")
                ]
                if cached_offset is not None
                else []
            },
        }
    ).encode()
    cache_receipt.write_bytes(raw)

    def publish_cache_before_probe_returns(url, **kwargs):
        if url.endswith("/events"):
            entry._append_entry_quotes(
                path,
                trial,
                paid,
                ordinary_due,
                fetched,
                "racing-cache",
                cache_receipt,
            )
        return fake_request(url, **kwargs)

    monkeypatch.setattr(requests, "get", publish_cache_before_probe_returns)
    expected = 1 if cached_offset is None else 2
    assert _collect_window() == {
        "status": "reused",
        "due_events": expected,
        "decisions_recorded": expected,
    }
    assert len(calls) == 2
    assert cache_receipt.read_bytes() == raw
    with connect(path) as conn:
        assert not entry._has_table(conn, "paper_entry_requests")
        decisions = dict(conn.execute("SELECT game_id, start_time_utc FROM paper_decisions"))
        assert set(decisions) == ({"on-time"} if cached_offset is None else {"on-time", "too-late"})
        if cached_offset is not None:
            assert (
                decisions["too-late"] == (PHI_START + timedelta(minutes=cached_offset)).isoformat()
            )
