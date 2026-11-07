"""Exercise the live paid publisher without ordinary forecasts or outcome data."""

import hashlib
import json
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone

import joblib
import numpy as np
import pytest
import yaml

from src.db.core import connect, initialize, store_odds_snapshot_game_starts
from src.predict import engine, publishable_bets, storage
from src.predict.paper_approval import rule_for_trial
from src.predict.paper_ledger import load_trial


class QuoteTotals:
    def predict(self, features):
        assert features["home_moneyline_close"].tolist() == [-120.0]
        assert features["away_moneyline_close"].tolist() == [110.0]
        return np.full(len(features), 242.0)


@pytest.fixture
def prospective_release(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    fetched = now - timedelta(minutes=2)
    start = now + timedelta(hours=1)
    data = tmp_path / "data"
    raw = data / "raw" / "odds" / "basketball_nba"
    raw.mkdir(parents=True)
    warehouse = data / "betting.db"
    wrong_db = tmp_path / "wrong-default.db"
    monkeypatch.setenv("SPORTS_DB_PATH", str(wrong_db))
    initialize(warehouse)
    provider_id = "a" * 32
    original = raw / fetched.strftime("odds_%Y-%m-%dT%H-%M-%SZ.json")
    original.write_text(
        json.dumps(
            {
                "source": "the-odds-api",
                "fetched_at": fetched.isoformat(),
                "request": {"url": "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"},
                "results": [
                    {
                        "id": provider_id,
                        "sport_key": "basketball_nba",
                        "commence_time": start.isoformat(),
                        "home_team": "Boston Celtics",
                        "away_team": "New York Knicks",
                        "bookmakers": [
                            {
                                "key": "draftkings",
                                "markets": [
                                    {
                                        "key": "totals",
                                        "outcomes": [
                                            {"name": side, "price": -110, "point": 220.5}
                                            for side in ("Over", "Under")
                                        ],
                                    },
                                    {
                                        "key": "h2h",
                                        "outcomes": [
                                            {"name": "Boston Celtics", "price": -120},
                                            {"name": "New York Knicks", "price": 110},
                                        ],
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with connect(warehouse) as conn:
        conn.executescript("""
            INSERT INTO sports (sport_id, name, league, default_market)
            VALUES (1, 'Basketball', 'NBA', 'moneyline');
            INSERT INTO books VALUES (1, 'DraftKings', 'us');
            INSERT INTO teams (team_id, sport_id, code, name)
            VALUES (1, 1, 'BOS', 'Boston Celtics'), (2, 1, 'NYK', 'New York Knicks');
        """)
        conn.execute(
            """
            INSERT INTO games (game_id,sport_id,start_time_utc,home_team_id,away_team_id,odds_api_id)
            VALUES ('game',1,?,1,2,?)
        """,
            (start.isoformat(), provider_id),
        )
        conn.executemany(
            "INSERT INTO odds_snapshots VALUES (?, ?, 1, 'the-odds-api', ?)",
            [
                ("paired", fetched.isoformat(), str(original)),
                ("newer-h2h", (fetched + timedelta(minutes=1)).isoformat(), None),
            ],
        )
        store_odds_snapshot_game_starts(conn, "paired", {"game": start.isoformat()})
        conn.executemany(
            """
            INSERT INTO odds (snapshot_id,game_id,book_id,market,outcome,price_american,line)
            VALUES (?,'game',1,?,?,?,?)
        """,
            [
                ("paired", "h2h", "home", -120, None),
                ("paired", "h2h", "away", 110, None),
                ("paired", "totals", "Over", -110, 220.5),
                ("paired", "totals", "Under", -110, 220.5),
                ("newer-h2h", "h2h", "home", -155, None),
                ("newer-h2h", "h2h", "away", 135, None),
            ],
        )

    model_registry = tmp_path / "models"
    model_registry.mkdir()
    monkeypatch.setattr(engine, "MODEL_REGISTRY_PATH", model_registry)
    model = {
        "regressor": QuoteTotals(),
        "residual_std": 10.0,
        "feature_names": ["total_close", "home_moneyline_close", "away_moneyline_close"],
    }
    alias = model_registry / "nba_totals_random_forest.pkl"
    joblib.dump(model, alias)
    digest = hashlib.sha256(alias.read_bytes()).hexdigest()
    archived = alias.with_name(f"{alias.stem}.{digest}{alias.suffix}")
    alias.rename(archived)
    trial = replace(load_trial(), model_sha256=digest)
    review = {
        "specification": asdict(trial),
        "review_status": "complete",
        "review_verdict": "pass",
        "evidence_cutoff": (now - timedelta(days=1)).isoformat(),
        "metrics": {
            "graded_decisions": 150,
            "wins": 100,
            "losses": 50,
            "roi": 0.2,
            "profit_units": 30,
            "bootstrap_roi_interval": {"low": 0.05},
            "model_brier": 0.18,
            "market_brier": 0.25,
            "clv_count": 150,
            "clv_coverage": 1.0,
            "avg_clv_points": 0.4,
            "clv_win_rate": 0.6,
        },
    }
    monkeypatch.setattr(
        publishable_bets, "paper_reviews", lambda db, at, **kwargs: {trial.trial_id: review}
    )
    rule = {**rule_for_trial(review), "status": "approved"}
    rules = tmp_path / "rules.yml"
    rules.write_text(yaml.safe_dump({"approved_rules": [rule]}), encoding="utf-8")
    return warehouse, wrong_db, rules, rule, review, original, alias, archived, model


def _publish(tmp_path, release, **kwargs):
    warehouse, _, rules, *_ = release
    return publishable_bets.publishable_bets(
        db_path=warehouse,
        rules_path=rules,
        output_path=tmp_path / "paid.json",
        quality_output_path=tmp_path / "quality.json",
        **kwargs,
    )


@pytest.mark.parametrize(
    "rotated", [False, True], ids=["ordinary-model-absent", "ordinary-model-rotated"]
)
def test_live_publisher_scores_the_paired_pinned_receipt_without_ordinary_rows(
    prospective_release, tmp_path, rotated
):
    warehouse, wrong_db, _, rule, _, _, alias, archived, model = prospective_release
    if rotated:
        joblib.dump({**model, "different_registry_version": True}, alias)
        assert hashlib.sha256(alias.read_bytes()).hexdigest() != rule["model_sha256"]
    else:
        assert not alias.exists()
    ordinary = storage.load_games_from_database("NBA", db_path=warehouse).iloc[0]
    assert ordinary["selected_totals_snapshot_id"] == "paired"
    assert ordinary["selected_h2h_snapshot_id"] == "newer-h2h"

    result = _publish(tmp_path, prospective_release)
    [pick] = result["bets"]
    assert pick["model_sha256"] == rule["model_sha256"]
    assert pick["quote_snapshot_id"] == pick["input_h2h_snapshot_id"] == "paired"
    assert pick["input_home_moneyline"] == -120 and pick["input_away_moneyline"] == 110
    evaluated = datetime.fromisoformat(result["generated_at"])
    scored = datetime.fromisoformat(pick["predicted_at"])
    published = datetime.fromisoformat(result["published_at"])
    assert evaluated < scored <= published
    assert hashlib.sha256(archived.read_bytes()).hexdigest() == rule["model_sha256"]
    assert not wrong_db.exists()
    with connect(warehouse) as conn:
        assert conn.execute("SELECT count(*) FROM predictions").fetchone()[0] == 0


def test_live_pinned_lane_still_requires_the_exact_original_receipt(prospective_release, tmp_path):
    _, _, _, _, _, original, *_ = prospective_release
    payload = json.loads(original.read_text(encoding="utf-8"))
    payload["results"][0]["bookmakers"][0]["markets"][1]["outcomes"][0]["price"] = -121
    original.write_text(json.dumps(payload), encoding="utf-8")
    result = _publish(tmp_path, prospective_release)
    assert result["bets"] == []
    assert result["reason"] == "no_current_bets_matching_original_quotes"


@pytest.mark.parametrize(
    ("scenario", "reason"),
    [
        ("unapproved", "no_passing_approved_rule"),
        ("review-failed", "no_passing_approved_rule"),
        ("historical-clock", "no_current_prediction_bets"),
        ("excluded-league", "no_current_prediction_bets"),
    ],
)
def test_publisher_does_not_score_outside_approved_live_nba_lane(
    prospective_release, tmp_path, monkeypatch, scenario, reason
):
    _, _, rules, rule, review, *_ = prospective_release
    if scenario == "unapproved":
        rules.write_text(yaml.safe_dump({"candidate_rules": [rule]}), encoding="utf-8")
    elif scenario == "review-failed":
        review["review_verdict"] = "fail"

    def unexpected(*args, **kwargs):
        pytest.fail("This release must not run fresh inference")

    monkeypatch.setattr(engine, "PredictionEngine", unexpected)
    kwargs = {}
    if scenario == "historical-clock":
        kwargs["now"] = datetime.now(timezone.utc)
    elif scenario == "excluded-league":
        kwargs["leagues"] = ["NFL"]
    result = _publish(tmp_path, prospective_release, **kwargs)
    assert result["bets"] == []
    assert result["reason"] == reason
