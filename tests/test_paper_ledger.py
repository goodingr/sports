import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from sklearn.ensemble import RandomForestRegressor

from scripts.backup_evidence import EVIDENCE_ZIP, create_evidence_zip
from src.db.core import connect, initialize
from src.predict import paper_ledger
from src.predict.engine import PredictionEngine
from src.predict.paper_ledger import (
    capture_predictions,
    load_trial,
    record_capture_failure,
    register_trial,
    update,
)
from src.predict.runner import run_predictions


def _original_event(provider_id, start, *, line=222.5, moneylines=(-130, 110)):
    home, away = "San Antonio Spurs", "Sacramento Kings"
    return {
        "id": provider_id, "sport_key": "basketball_nba", "commence_time": start,
        "home_team": home, "away_team": away,
        "bookmakers": [{"key": "draftkings", "markets": [
            {"key": "h2h", "outcomes": [
                {"name": home, "price": moneylines[0]}, {"name": away, "price": moneylines[1]},
            ]},
            {"key": "totals", "outcomes": [
                {"name": "Over", "point": line, "price": -110},
                {"name": "Under", "point": line, "price": -110},
            ]},
        ]}],
    }


def _write_original(path, fetched, events):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "fetched_at": fetched.isoformat(),
        "request": {"url": "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"},
        "results": events,
    }), encoding="utf-8")


def _set_original_provider_start(conn, game_id, start):
    row = conn.execute(
        """
        SELECT s.raw_path, g.odds_api_id FROM games g JOIN odds o USING (game_id)
        JOIN odds_snapshots s USING (snapshot_id) WHERE g.game_id = ? LIMIT 1
        """,
        (game_id,),
    ).fetchone()
    path = Path(row["raw_path"])
    payload = json.loads(path.read_text(encoding="utf-8"))
    for event in payload["results"]:
        if event["id"] == row["odds_api_id"]:
            event["commence_time"] = start
    path.write_text(json.dumps(payload), encoding="utf-8")


def _set_fixture_start(conn, game_id, start):
    conn.execute("UPDATE games SET start_time_utc = ? WHERE game_id = ?", (start, game_id))
    conn.execute(
        "UPDATE odds_snapshot_game_starts SET observed_start_utc = ? WHERE game_id = ?",
        (start, game_id),
    )
    _set_original_provider_start(conn, game_id, start)


@pytest.fixture
def warehouse(tmp_path: Path):
    db = tmp_path / "trial.db"
    initialize(db)
    now = datetime.now(timezone.utc)
    trial = replace(load_trial(), review_at=(now + timedelta(days=2)).isoformat())
    register_trial(trial, db)
    with connect(db) as conn:
        conn.execute("INSERT INTO sports VALUES (1, 'Basketball', 'NBA', 'totals', 'today')")
        conn.execute(
            """
            INSERT INTO teams (team_id, sport_id, code, name)
            VALUES (1, 1, 'SAS', 'San Antonio Spurs'), (2, 1, 'SAC', 'Sacramento Kings')
            """
        )
        conn.execute("INSERT INTO books VALUES (1, 'DraftKings', 'us')")
        start = (now + timedelta(hours=2)).isoformat()
        conn.execute(
            """
            INSERT INTO games (game_id, sport_id, home_team_id, away_team_id, start_time_utc, odds_api_id)
            VALUES ('game', 1, 1, 2, ?, 'provider-game'), ('duplicate', 1, 1, 2, ?, 'provider-duplicate')
            """,
            (start, (now + timedelta(days=3)).isoformat()),
        )
        conn.execute(
            "INSERT INTO odds_snapshots VALUES ('snapshot', ?, 1, 'the-odds-api', NULL)",
            (now.isoformat(),),
        )
        conn.execute(
            """
            INSERT INTO odds_snapshot_game_starts (snapshot_id, game_id, observed_start_utc)
            SELECT 'snapshot', game_id, start_time_utc FROM games
            """
        )
        conn.execute(
            """
            INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
            VALUES ('snapshot', 'game', 1, 'totals', 'Over', -110, 222.5),
                   ('snapshot', 'game', 1, 'totals', 'Under', -110, 222.5),
                   ('snapshot', 'duplicate', 1, 'totals', 'Over', -110, 222.5),
                   ('snapshot', 'duplicate', 1, 'totals', 'Under', -110, 222.5),
                   ('snapshot', 'game', 1, 'h2h', 'home', -130, NULL),
                   ('snapshot', 'game', 1, 'h2h', 'away', 110, NULL),
                   ('snapshot', 'duplicate', 1, 'h2h', 'home', -130, NULL),
                   ('snapshot', 'duplicate', 1, 'h2h', 'away', 110, NULL)
            """
        )
        raw = tmp_path / "data" / "raw" / "odds" / "basketball_nba" / "snapshot.json"
        _write_original(raw, now, [
            _original_event("provider-game", start),
            _original_event("provider-duplicate", (now + timedelta(days=3)).isoformat()),
        ])
        conn.execute("UPDATE odds_snapshots SET raw_path = ?", (str(raw),))
    predictions = pd.DataFrame(
        [
            {
                "game_id": "game",
                "commence_time": start,
                "predicted_at": datetime.now(timezone.utc),
                "selected_totals_snapshot_id": "snapshot",
                "selected_totals_book_id": 1,
                "selected_h2h_snapshot_id": "snapshot",
                "selected_h2h_book_id": 1,
                "total_line": 222.5,
                "over_moneyline": -110,
                "under_moneyline": -110,
                "home_moneyline": -130,
                "away_moneyline": 110,
                "over_predicted_prob": 0.75,
                "under_predicted_prob": 0.25,
            }
        ]
    )
    return db, trial, predictions, now


def test_capture_freezes_first_eligible_decision(warehouse):
    db, trial, predictions, _ = warehouse
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {"recorded": 1}
    predictions["over_predicted_prob"] = 0.20
    predictions["under_predicted_prob"] = 0.80
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {
        "already_recorded": 1
    }
    with connect(db) as conn:
        _set_fixture_start(conn, "duplicate", predictions.iloc[0]["commence_time"])
    duplicate = predictions.assign(game_id="duplicate")
    assert capture_predictions(
        trial, pd.concat([predictions, duplicate]), trial.model_sha256, db
    ) == {"already_recorded": 2}
    with connect(db) as conn:
        decision = conn.execute("SELECT * FROM paper_decisions").fetchone()
    assert decision["side"] == "over"
    assert decision["model_probability"] == 0.75
    assert decision["price"] == -110
    assert decision["snapshot_id"] == "snapshot"
    assert update(trial, db)["trials"][0]["metrics"]["pending"] == 1
    with pytest.raises(ValueError, match="use a new trial ID"):
        register_trial(replace(trial, min_edge=0.21), db)


def test_scoped_capture_excludes_other_eligible_fixtures(warehouse):
    db, trial, predictions, now = warehouse
    second_start = (now + timedelta(hours=5)).isoformat()
    with connect(db) as conn:
        _set_fixture_start(conn, "duplicate", second_start)
    second = predictions.assign(game_id="duplicate", commence_time=second_start)
    assert capture_predictions(
        trial,
        pd.concat([predictions, second]),
        trial.model_sha256,
        db,
        fixture_ids={"game"},
    ) == {"recorded": 1}
    record_capture_failure(trial, db, fixture_ids={"duplicate"})
    with connect(db) as conn:
        decisions = conn.execute("SELECT game_id FROM paper_decisions").fetchall()
        observations = conn.execute(
            "SELECT game_id, outcome FROM paper_fixture_observations ORDER BY run_id, game_id"
        ).fetchall()
    assert [row["game_id"] for row in decisions] == ["game"]
    assert [tuple(row) for row in observations] == [
        ("game", "recorded"),
        ("duplicate", "prediction_missing"),
    ]


def test_scoped_capture_checks_earlier_start_at_ledger_write(warehouse):
    db, trial, predictions, now = warehouse
    canonical_start = now + timedelta(minutes=20)
    earlier_start = canonical_start - timedelta(minutes=8)
    predictions["commence_time"] = canonical_start.isoformat()
    with connect(db) as conn:
        _set_fixture_start(conn, "game", canonical_start.isoformat())
    assert capture_predictions(
        trial, predictions, trial.model_sha256, db, fixture_start_limits={"game": earlier_start}
    ) == {"outside_decision_window": 1}
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {"recorded": 1}


@pytest.mark.parametrize(
    ("boundary", "expected"),
    [
        ("late_start", "outside_decision_window"),
        ("minimum_lead", "recorded"),
        ("expired_quote", "stale_quote"),
        ("maximum_quote_age", "recorded"),
        ("review_deadline", "trial_closed"),
        ("clock_regression", "invalid_time"),
    ],
)
@pytest.mark.parametrize("over_probability", [0.75, 0.50], ids=["positive_edge", "below_threshold"])
def test_capture_rechecks_wall_clock_after_original_quote_verification(
    warehouse, monkeypatch, boundary, expected, over_probability
):
    db, trial, predictions, fetched_at = warehouse
    verification_duration = timedelta(seconds=2)
    attempt_at = fetched_at + timedelta(seconds=1)
    start = fetched_at + timedelta(hours=2)
    if boundary in {"late_start", "minimum_lead"}:
        lead_slack = 1 if boundary == "late_start" else 2
        start = attempt_at + timedelta(minutes=trial.min_minutes_before_start, seconds=lead_slack)
    elif boundary in {"expired_quote", "maximum_quote_age"}:
        age_slack = 1 if boundary == "expired_quote" else 2
        attempt_at = fetched_at + timedelta(
            minutes=trial.max_quote_age_minutes, seconds=-age_slack
        )
    elif boundary == "review_deadline":
        trial = replace(
            trial,
            trial_id=f"{trial.trial_id}_clock_test",
            review_at=(attempt_at + verification_duration).isoformat(),
        )
    elif boundary == "clock_regression":
        verification_duration = timedelta(seconds=-1)
    predictions = predictions.assign(
        commence_time=start.isoformat(),
        predicted_at=attempt_at.isoformat(),
        over_predicted_prob=over_probability,
        under_predicted_prob=1 - over_probability,
    )
    with connect(db) as conn:
        _set_fixture_start(conn, "game", start.isoformat())

    class Clock(datetime):
        current = attempt_at

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz)

    verify_original_quote = paper_ledger._quote_source_start

    def verify_with_clock_change(*args, **kwargs):
        provider_start = verify_original_quote(*args, **kwargs)
        assert provider_start is not None
        Clock.current += verification_duration
        return provider_start

    monkeypatch.setattr(paper_ledger, "datetime", Clock)
    monkeypatch.setattr(paper_ledger, "_quote_source_start", verify_with_clock_change)

    if expected == "recorded" and over_probability == 0.50:
        expected = "below_edge_threshold"
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {expected: 1}
    with connect(db) as conn:
        decision = conn.execute("SELECT captured_at FROM paper_decisions").fetchone()
        attempt = conn.execute("SELECT observed_at FROM paper_capture_runs").fetchone()
        observation = conn.execute("SELECT outcome FROM paper_fixture_observations").fetchone()
    assert attempt["observed_at"] == attempt_at.isoformat()
    assert observation["outcome"] == expected
    if expected == "recorded":
        assert decision["captured_at"] == Clock.current.isoformat()
    else:
        assert decision is None


def test_scoped_capture_checks_clock_span_across_all_three_sources(warehouse):
    db, trial, predictions, now = warehouse
    canonical_start = now + timedelta(hours=2)
    provider_start = canonical_start + timedelta(minutes=6)
    scoped_start = canonical_start - timedelta(minutes=5)
    with connect(db) as conn:
        conn.execute(
            "UPDATE odds_snapshot_game_starts SET observed_start_utc = ? WHERE game_id = 'game'",
            (provider_start.isoformat(),),
        )
        _set_original_provider_start(conn, "game", provider_start.isoformat())
    assert capture_predictions(
        trial, predictions, trial.model_sha256, db, fixture_start_limits={"game": scoped_start}
    ) == {"game_schedule_changed": 1}
    with connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM paper_decisions").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("proof", "expected"),
    [
        ("legacy_row", "recorded"),
        ("legacy_table", "recorded"),
        ("legacy_zip", "recorded"),
        ("missing", "unverified_original_quote"),
        ("missing_original", "unverified_original_quote"),
        ("invalid_normalized", "unverified_original_quote"),
        ("contradictory_clock", "unverified_original_quote"),
        ("contradictory_totals", "unverified_original_quote"),
    ],
)
def test_capture_accepts_only_verified_provider_tipoff(warehouse, tmp_path, proof, expected):
    db, trial, predictions, now = warehouse
    provider_start = now + timedelta(hours=1, minutes=52)
    with connect(db) as conn:
        if proof == "legacy_table":
            conn.execute("DROP TABLE odds_snapshot_game_starts")
        elif proof == "invalid_normalized":
            conn.execute(
                """
                UPDATE odds_snapshot_game_starts SET observed_start_utc = '2026-11-07T22:00:00'
                WHERE game_id = 'game'
                """
            )
        elif proof in {"legacy_row", "legacy_zip", "missing"}:
            conn.execute("DELETE FROM odds_snapshot_game_starts WHERE game_id = 'game'")
        conn.execute(
            "UPDATE odds_snapshots SET raw_path = ? WHERE snapshot_id = 'snapshot'",
            (r"Z:\old_checkout\data\raw\odds\basketball_nba\odds_entry.json",),
        )
    if proof not in {"missing", "missing_original"}:
        raw_root = (tmp_path / "bundle" if proof == "legacy_zip" else tmp_path) / "raw"
        raw = raw_root / "odds" / "basketball_nba" / "odds_entry.json"
        if proof == "contradictory_totals":
            event = _original_event("provider-game", predictions.iloc[0].commence_time, line=223.5)
        else:
            event = _original_event("provider-game", provider_start.isoformat())
        _write_original(raw, now, [event])
        if proof == "legacy_zip":
            create_evidence_zip(tmp_path / EVIDENCE_ZIP, [("raw", raw_root)])

    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {expected: 1}
    with connect(db) as conn:
        decision = conn.execute("SELECT start_time_utc FROM paper_decisions").fetchone()
    if expected == "recorded":
        assert decision["start_time_utc"] == provider_start.isoformat()
    else:
        assert decision is None


def test_capture_rejects_missing_provenance_wrong_model_and_invalid_market(warehouse):
    db, trial, predictions, _ = warehouse
    with pytest.raises(ValueError, match="pinned digest"):
        capture_predictions(trial, predictions, "wrong", db)
    assert capture_predictions(
        trial, predictions.assign(selected_totals_snapshot_id=None), trial.model_sha256, db
    ) == {"no_paired_quote": 1}
    assert capture_predictions(
        trial, predictions.assign(over_moneyline=-105), trial.model_sha256, db
    ) == {"invalid_quote_or_prediction": 1}
    assert capture_predictions(
        trial, predictions.assign(home_moneyline=-135), trial.model_sha256, db
    ) == {"unverified_model_inputs": 1}
    assert capture_predictions(
        trial,
        predictions.assign(over_predicted_prob=0.60, under_predicted_prob=0.40),
        trial.model_sha256,
        db,
    ) == {"below_edge_threshold": 1}
    with connect(db) as conn:
        conn.execute(
            "UPDATE games SET start_time_utc = ? WHERE game_id = 'duplicate'",
            (predictions.iloc[0]["commence_time"],),
        )
        conn.execute("INSERT INTO game_results (game_id, home_score) VALUES ('duplicate', 0)")
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {
        "game_unavailable": 1,
        "prediction_missing": 1,
    }


@pytest.mark.parametrize(
    "provenance",
    [
        {"selected_h2h_snapshot_id": "different-snapshot"},
        {"selected_h2h_book_id": 2},
        {"selected_h2h_snapshot_id": None},
    ],
)
def test_capture_rejects_matching_prices_with_different_provenance(warehouse, provenance):
    db, trial, predictions, _ = warehouse
    assert capture_predictions(trial, predictions.assign(**provenance), trial.model_sha256, db) == {
        "unverified_model_inputs": 1
    }


@pytest.mark.parametrize(
    ("market", "outcome", "line", "failure"),
    [
        ("h2h", "HOME", None, "unverified_model_inputs"),
        ("totals", "OVER", 222.5, "quote_not_current_at_trial_book"),
    ],
)
def test_capture_rejects_ambiguous_quote_pairs(warehouse, market, outcome, line, failure):
    db, trial, predictions, _ = warehouse
    with connect(db) as conn:
        conn.execute(
            """
            INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
            VALUES ('snapshot', 'game', 1, ?, ?, -130, ?)
            """,
            (market, outcome, line),
        )
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {failure: 1}


def test_capture_records_empty_and_failed_runs(warehouse, monkeypatch):
    db, trial, _, _ = warehouse
    assert capture_predictions(trial, None, None, db) == {"prediction_missing": 1}
    with connect(db) as conn:
        run = conn.execute("SELECT * FROM paper_capture_runs").fetchone()
        observation = conn.execute("SELECT * FROM paper_fixture_observations").fetchone()
    assert run["status"] == "no_predictions"
    assert observation["game_id"] == "game"
    assert observation["outcome"] == "prediction_missing"
    assert observation["snapshot_id"] is None

    class FailedEngine:
        def __init__(self, model_type):
            pass

        def predict_pinned_totals(self, league, model_sha256):
            raise ValueError("synthetic inference failure")

    monkeypatch.setenv("SPORTS_DB_PATH", str(db))
    monkeypatch.setattr("src.predict.runner.PredictionEngine", FailedEngine)
    monkeypatch.setattr("src.predict.runner.load_trial", lambda _: trial)
    with pytest.raises(ValueError, match="synthetic inference failure"):
        run_predictions(["NBA"], "random_forest", paper_rules=Path("unused"))
    with connect(db) as conn:
        rows = conn.execute("SELECT * FROM paper_capture_runs ORDER BY run_id").fetchall()
        observations = conn.execute("SELECT * FROM paper_fixture_observations").fetchall()
    assert [row["status"] for row in rows] == ["no_predictions", "failed"]
    assert len(observations) == 2


def test_runner_signals_empty_predictions_when_a_fixture_is_in_window(warehouse, monkeypatch):
    db, trial, _, now = warehouse

    class EmptyEngine:
        totals_model_sha256 = None

        def __init__(self, model_type):
            pass

        def predict_pinned_totals(self, league, model_sha256):
            return None

        def predict(self, league, *, trace_totals):
            return None

    monkeypatch.setenv("SPORTS_DB_PATH", str(db))
    monkeypatch.setattr("src.predict.runner.PredictionEngine", EmptyEngine)
    monkeypatch.setattr("src.predict.runner.load_trial", lambda _: trial)
    with pytest.raises(
        RuntimeError, match="produced no predictions during an active capture window"
    ):
        run_predictions(["NBA"], "random_forest", paper_rules=Path("unused"))

    with connect(db) as conn:
        statuses = conn.execute("SELECT status FROM paper_capture_runs").fetchall()
        conn.execute(
            "UPDATE games SET start_time_utc = ?", ((now + timedelta(days=1)).isoformat(),)
        )
    assert [row["status"] for row in statuses] == ["no_predictions"]
    run_predictions(["NBA"], "random_forest", paper_rules=Path("unused"))


@pytest.mark.parametrize("paper_only", [True, False])
def test_runner_captures_pinned_totals_before_optional_ordinary_refresh(
    warehouse, monkeypatch, paper_only
):
    db, trial, predictions, _ = warehouse
    calls = []

    class Engine:
        def __init__(self, model_type):
            self.totals_model_sha256 = trial.model_sha256

        def predict_pinned_totals(self, league, model_sha256):
            assert (league, model_sha256) == ("NBA", trial.model_sha256)
            calls.append("paper")
            return predictions

        def predict(self, league, *, trace_totals):
            assert league == "NBA" and trace_totals
            with connect(db) as conn:
                assert conn.execute("SELECT COUNT(*) FROM paper_decisions").fetchone()[0] == 1
            calls.append("ordinary")

    monkeypatch.setenv("SPORTS_DB_PATH", str(db))
    monkeypatch.setattr("src.predict.runner.PredictionEngine", Engine)
    monkeypatch.setattr("src.predict.runner.load_trial", lambda _: trial)
    run_predictions(
        ["NBA"], "random_forest", paper_rules=Path("unused"), paper_only=paper_only
    )
    assert calls == (["paper"] if paper_only else ["paper", "ordinary"])


def test_capture_rejects_stale_superseded_and_future_predictions(warehouse):
    db, trial, predictions, now = warehouse
    assert capture_predictions(
        trial, predictions.assign(predicted_at=now + timedelta(seconds=10)), trial.model_sha256, db
    ) == {"outside_decision_window": 1}
    with connect(db) as conn:
        conn.execute(
            "UPDATE odds_snapshots SET fetched_at_utc = ? WHERE snapshot_id = 'snapshot'",
            ((now - timedelta(minutes=11)).isoformat(),),
        )
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {"stale_quote": 1}
    with connect(db) as conn:
        conn.execute(
            "INSERT INTO odds_snapshots VALUES ('newer', ?, 1, 'the-odds-api', NULL)",
            (now.isoformat(),),
        )
        conn.execute(
            """
            INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
            VALUES ('newer', 'game', 1, 'totals', 'Over', -120, 223.5)
            """
        )
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {
        "quote_not_current_at_trial_book": 1
    }


@pytest.mark.parametrize(
    ("clock", "complete", "new_outcome"),
    [
        ("utc", True, "recorded"),
        ("offset", True, "recorded"),
        ("exact_tie", True, "quote_not_current_at_trial_book"),
        ("utc", False, "quote_not_current_at_trial_book"),
        ("naive", False, "quote_not_current_at_trial_book"),
        ("naive", True, "invalid_time"),
    ],
)
def test_paper_quote_order_preserves_submillisecond_and_legacy_clocks(
    warehouse, clock, complete, new_outcome
):
    db, trial, predictions, now = warehouse
    base = (now - timedelta(seconds=1)).replace(microsecond=0)
    older = base + timedelta(microseconds=100)
    newer = base + timedelta(microseconds=200)
    if clock == "offset":
        newer = newer.astimezone(timezone(timedelta(hours=-5)))
    elif clock == "exact_tie":
        newer = older.astimezone(timezone(timedelta(hours=-5)))
    elif clock == "naive":
        newer = newer.replace(tzinfo=None)
    with connect(db) as conn:
        conn.execute(
            "UPDATE odds_snapshots SET fetched_at_utc = ? WHERE snapshot_id = 'snapshot'",
            (older.isoformat(),),
        )
        conn.execute(
            "INSERT INTO odds_snapshots VALUES ('a-new', ?, 1, 'the-odds-api', NULL)",
            (newer.isoformat(),),
        )
        conn.execute(
            """
            INSERT INTO odds_snapshot_game_starts
            SELECT 'a-new', game_id, observed_start_utc FROM odds_snapshot_game_starts
            WHERE snapshot_id = 'snapshot' AND game_id = 'game'
            """
        )
        conn.execute(
            """
            INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome, price_american, line)
            SELECT 'a-new', game_id, book_id, market, outcome, price_american,
                   CASE WHEN market = 'totals' THEN 229.5 ELSE line END
            FROM odds WHERE snapshot_id = 'snapshot' AND game_id = 'game'
              AND (? OR (market = 'totals' AND outcome = 'Over'))
            """,
            (complete,),
        )
    assert capture_predictions(trial, predictions, trial.model_sha256, db) == {
        "quote_not_current_at_trial_book": 1
    }
    if new_outcome == "recorded":
        raw = db.parent / "data" / "raw" / "odds" / "basketball_nba" / "new.json"
        _write_original(
            raw, newer, [_original_event("provider-game", predictions.iloc[0].commence_time, line=229.5)]
        )
        with connect(db) as conn:
            conn.execute("UPDATE odds_snapshots SET raw_path = ? WHERE snapshot_id = 'a-new'", (str(raw),))
    latest = predictions.assign(
        selected_totals_snapshot_id="a-new", selected_h2h_snapshot_id="a-new", total_line=229.5
    )
    assert capture_predictions(trial, latest, trial.model_sha256, db) == {new_outcome: 1}
    with connect(db) as conn:
        decision = conn.execute("SELECT snapshot_id, line FROM paper_decisions").fetchone()
    if new_outcome == "recorded":
        assert tuple(decision) == ("a-new", 229.5)
    else:
        assert decision is None


@pytest.mark.parametrize("original_agrees", [False, True])
def test_real_forest_threshold_crossing_requires_exact_original_inputs(warehouse, original_agrees):
    db, trial, predictions, fetched = warehouse
    line = 220.5
    raw_prices = (-130, 110)
    normalized_prices = (200, -250)
    inputs = pd.DataFrame(
        [
            [line, 0.0, *raw_prices],
            [line, 0.0, *normalized_prices],
        ],
        columns=["total_close", "spread_close", "home_moneyline_close", "away_moneyline_close"],
    )
    engine = PredictionEngine(model_type=trial.model_type)
    engine.totals_model = RandomForestRegressor(n_estimators=3, bootstrap=False, random_state=42).fit(
        inputs, [line, line - 20]
    )
    engine.totals_features = inputs.columns.tolist()
    engine.totals_std = 10.0
    normalized = predictions.assign(
        total_line=line, spread_line=0.0,
        home_moneyline=normalized_prices[0], away_moneyline=normalized_prices[1],
    )
    archived = normalized.assign(home_moneyline=raw_prices[0], away_moneyline=raw_prices[1])
    engine._score_totals(normalized)
    engine._score_totals(archived)
    assert abs(archived.iloc[0].under_predicted_prob - 0.5) < trial.min_edge
    assert normalized.iloc[0].under_predicted_prob - 0.5 > trial.min_edge

    with connect(db) as conn:
        conn.execute("UPDATE odds SET line = ? WHERE game_id = 'game' AND market = 'totals'", (line,))
        conn.executemany(
            "UPDATE odds SET price_american = ? WHERE game_id = 'game' AND market = 'h2h' AND outcome = ?",
            list(zip(normalized_prices, ("home", "away"))),
        )
        path = Path(conn.execute("SELECT raw_path FROM odds_snapshots").fetchone()[0])
    _write_original(path, fetched, [_original_event(
        "provider-game", predictions.iloc[0].commence_time, line=line,
        moneylines=normalized_prices if original_agrees else raw_prices,
    )])

    expected = "recorded" if original_agrees else "unverified_original_quote"
    assert capture_predictions(trial, normalized, trial.model_sha256, db) == {expected: 1}
    with connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM paper_decisions").fetchone()[0] == int(original_agrees)
