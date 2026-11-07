from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.db.core import connect, initialize
from src.predict.paper_ledger import capture_predictions, load_trial, register_trial, update
from src.predict.runner import run_predictions


@pytest.fixture
def warehouse(tmp_path: Path):
    db = tmp_path / "trial.db"
    initialize(db)
    now = datetime.now(timezone.utc)
    trial = replace(load_trial(), review_at=(now + timedelta(days=2)).isoformat())
    register_trial(trial, db)
    with connect(db) as conn:
        conn.execute("INSERT INTO sports VALUES (1, 'Basketball', 'NBA', 'totals', 'today')")
        conn.execute("INSERT INTO teams (team_id, sport_id, code, name) VALUES (1, 1, 'H', 'Home')")
        conn.execute("INSERT INTO teams (team_id, sport_id, code, name) VALUES (2, 1, 'A', 'Away')")
        conn.execute("INSERT INTO books VALUES (1, 'DraftKings', 'us')")
        start = (now + timedelta(hours=2)).isoformat()
        conn.execute(
            """
            INSERT INTO games (game_id, sport_id, home_team_id, away_team_id, start_time_utc)
            VALUES ('game', 1, 1, 2, ?), ('duplicate', 1, 1, 2, ?)
            """,
            (start, (now + timedelta(days=3)).isoformat()),
        )
        conn.execute(
            "INSERT INTO odds_snapshots VALUES ('snapshot', ?, 1, 'the-odds-api', NULL)",
            (now.isoformat(),),
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
        conn.execute(
            "UPDATE games SET start_time_utc = ? WHERE game_id = 'duplicate'",
            (predictions.iloc[0]["commence_time"],),
        )
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

        def predict(self, league, *, trace_totals):
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
