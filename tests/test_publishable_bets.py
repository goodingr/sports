from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import yaml

import src.predict.publishable_bets as publisher
from src.models.prediction_quality import QualityGate
from src.predict.paper_approval import (
    paper_reviews,
    paper_rule_result,
    promote_paper_trial,
    rule_for_trial,
)
from src.predict.publishable_bets import lock_candidate_rule, promote_candidate_rule
from src.predict.publishable_bets import main as publish_main


def _seed_paper_evidence(db_path, trial_id="nba_totals_fixture"):
    specification = {
        "trial_id": trial_id, "league": "NBA", "market": "totals", "model_type": "ensemble",
        "model_sha256": "a" * 64, "book": "draftkings", "odds_source": "the-odds-api",
        "min_edge": 0.05, "max_quote_age_minutes": 10, "min_minutes_before_start": 15,
        "max_hours_before_start": 72, "min_decisions": 150, "min_roi": 0.05,
        "min_clv_coverage": 0.8, "review_at": "2026-01-20T00:00:00+00:00",
    }
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE paper_trials (trial_id TEXT, registered_at TEXT, specification TEXT);
            CREATE TABLE paper_decisions (
                trial_id TEXT, game_id TEXT, start_time_utc TEXT, captured_at TEXT,
                model_probability REAL, market_probability REAL
            );
            CREATE TABLE paper_settlements (
                trial_id TEXT, game_id TEXT, settled_at TEXT, outcome TEXT,
                profit_units REAL, clv_points REAL
            );
        """)
        conn.execute(
            "INSERT INTO paper_trials VALUES (?, '2026-01-01T00:00:00+00:00', ?)",
            (trial_id, json.dumps(specification)),
        )
        for index in range(150):
            captured = datetime(2026, 1, 2, 18, tzinfo=timezone.utc) + timedelta(days=index % 10)
            conn.execute(
                "INSERT INTO paper_decisions VALUES (?, ?, ?, ?, 0.8, 0.5)",
                (trial_id, f"forward{index}", (captured + timedelta(hours=1)).isoformat(), captured.isoformat()),
            )
            won = index < 140
            conn.execute(
                "INSERT INTO paper_settlements VALUES (?, ?, ?, ?, ?, 0.5)",
                (trial_id, f"forward{index}", (captured + timedelta(hours=5)).isoformat(),
                 "win" if won else "loss", 1.0 if won else -1.0),
            )


def _create_publish_db(path, *, include_history: bool) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE sports (
                sport_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                league TEXT NOT NULL,
                default_market TEXT NOT NULL
            );
            CREATE TABLE teams (
                team_id INTEGER PRIMARY KEY,
                sport_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL
            );
            CREATE TABLE games (
                game_id TEXT PRIMARY KEY,
                sport_id INTEGER NOT NULL,
                start_time_utc TEXT NOT NULL,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL,
                status TEXT
            );
            CREATE TABLE game_results (
                game_id TEXT PRIMARY KEY,
                home_score INTEGER,
                away_score INTEGER,
                home_moneyline_close REAL,
                away_moneyline_close REAL,
                total_close REAL
            );
            CREATE TABLE predictions (
                prediction_id INTEGER PRIMARY KEY,
                game_id TEXT NOT NULL,
                model_type TEXT NOT NULL,
                predicted_at TEXT NOT NULL,
                home_prob REAL,
                away_prob REAL,
                home_moneyline REAL,
                away_moneyline REAL,
                home_edge REAL,
                away_edge REAL,
                home_implied_prob REAL,
                away_implied_prob REAL,
                total_line REAL,
                over_prob REAL,
                under_prob REAL,
                over_moneyline REAL,
                under_moneyline REAL,
                over_edge REAL,
                under_edge REAL,
                over_implied_prob REAL,
                under_implied_prob REAL,
                predicted_total_points REAL
            );
            INSERT INTO sports VALUES (1, 'Basketball', 'NBA', 'totals');
            INSERT INTO teams VALUES (1, 1, 'HOME', 'Home Team');
            INSERT INTO teams VALUES (2, 1, 'AWAY', 'Away Team');
            """
        )
        prediction_id = 1
        if include_history:
            history = [
                ("HIST1", "2026-01-01T20:00:00+00:00", 115, 106),
                ("HIST2", "2026-01-02T20:00:00+00:00", 112, 107),
                ("HIST3", "2026-01-03T20:00:00+00:00", 118, 112),
                ("HIST4", "2026-01-04T20:00:00+00:00", 90, 90),
            ]
            for game_id, start_time, home_score, away_score in history:
                conn.execute(
                    "INSERT INTO games VALUES (?, 1, ?, 1, 2, 'final')",
                    (game_id, start_time),
                )
                conn.execute(
                    "INSERT INTO game_results VALUES (?, ?, ?, -120, 110, 201.5)",
                    (game_id, home_score, away_score),
                )
                conn.execute(
                    """
                    INSERT INTO predictions (
                        prediction_id, game_id, model_type, predicted_at,
                        total_line, over_prob, under_prob, over_moneyline, under_moneyline,
                        over_edge, under_edge, over_implied_prob, under_implied_prob,
                        predicted_total_points
                    ) VALUES (?, ?, 'ensemble', '2026-01-01T18:00:00+00:00',
                              200.5, 0.80, 0.20, 200, -110, 0.40, -0.40,
                              0.40, 0.60, 225.0)
                    """,
                    (prediction_id, game_id),
                )
                prediction_id += 1

        conn.execute(
            """
            INSERT INTO games VALUES (
                'CUR1', 1, '2026-02-02T20:00:00+00:00', 1, 2, 'scheduled'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO predictions (
                prediction_id, game_id, model_type, predicted_at,
                total_line, over_prob, under_prob, over_moneyline, under_moneyline,
                over_edge, under_edge, over_implied_prob, under_implied_prob,
                predicted_total_points
            ) VALUES (?, 'CUR1', 'ensemble', '2026-02-01T10:00:00+00:00',
                      210.5, 0.72, 0.28, 100, 100, 0.22, -0.22,
                      0.50, 0.50, 225.0)
            """,
            (prediction_id,),
        )
        conn.executescript(
            """
            ALTER TABLE predictions ADD COLUMN selected_totals_snapshot_id TEXT;
            ALTER TABLE predictions ADD COLUMN selected_totals_book_id INTEGER;
            ALTER TABLE predictions ADD COLUMN selected_h2h_snapshot_id TEXT;
            ALTER TABLE predictions ADD COLUMN selected_h2h_book_id INTEGER;
            ALTER TABLE predictions ADD COLUMN totals_model_sha256 TEXT;
            UPDATE predictions SET totals_model_sha256 =
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
            CREATE TABLE books (book_id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO books VALUES (1, 'DraftKings');
            CREATE TABLE odds_snapshots (
                snapshot_id TEXT PRIMARY KEY, fetched_at_utc TEXT, sport_id INTEGER, source TEXT
            );
            INSERT INTO odds_snapshots VALUES ('current', '2026-02-01T11:55:00+00:00', 1, 'the-odds-api');
            CREATE TABLE odds (
                snapshot_id TEXT, game_id TEXT, book_id INTEGER, market TEXT,
                outcome TEXT, price_american REAL, line REAL
            );
            INSERT INTO odds VALUES ('current','CUR1',1,'totals','Over',100,210.5);
            INSERT INTO odds VALUES ('current','CUR1',1,'totals','Under',100,210.5);
            INSERT INTO odds VALUES ('current','CUR1',1,'h2h','home',-110,NULL);
            INSERT INTO odds VALUES ('current','CUR1',1,'h2h','away',-110,NULL);
            UPDATE predictions SET selected_totals_snapshot_id = 'current',
                selected_totals_book_id = 1, selected_h2h_snapshot_id = 'current',
                selected_h2h_book_id = 1, home_moneyline = -110, away_moneyline = -110,
                predicted_at = '2026-02-01T11:56:00+00:00'
            WHERE game_id = 'CUR1';
            """
        )
    if include_history:
        _seed_paper_evidence(path)


def _write_rules(path) -> None:
    path.write_text(
        """
launch_gate:
  min_bets_narrow: 4
  min_bets_multi_league: 4
  min_roi: 0.5
  min_bootstrap_roi_low: -0.5
  max_hours_before_start: null
  bootstrap_samples: 300
  random_seed: 42
approved_rules:
  - id: nba_totals_fixture
    market: totals
    league: NBA
    model_type: ensemble
    model_sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    paper_trial_id: nba_totals_fixture
    validation: prospective_paper
    book: draftkings
    odds_source: the-odds-api
    max_quote_age_minutes: 10
    min_minutes_before_start: 15
    max_hours_before_start: 72
    same_snapshot_moneyline_inputs: true
    side: both
    min_edge: 0.05
candidate_rules: []
""",
        encoding="utf-8",
    )


def _write_candidate_rules(path) -> None:
    path.write_text(
        """
launch_gate:
  min_bets_narrow: 4
  min_bets_multi_league: 4
  min_roi: 0.5
  min_bootstrap_roi_low: -0.5
  max_hours_before_start: null
  bootstrap_samples: 300
  random_seed: 42
approved_rules: []
candidate_rules:
  - id: nba_totals_candidate
    market: totals
    league: NBA
    model_type: ensemble
    model_sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    side: over
    min_edge: 0.05
""",
        encoding="utf-8",
    )


def _write_metadata_approved_rules(path) -> None:
    path.write_text(
        """
launch_gate:
  min_bets_narrow: 1
  min_roi: -1.0
  min_bootstrap_roi_low: -1.0
  max_hours_before_start: null
approved_rules:
  - id: nba_totals_metadata_rule
    market: totals
    league: NBA
    model_type: logistic
    prediction_variant: market_residual_shrink_050_sigmoid
    feature_variant: current_features
    price_mode: selected_book
    timing_bucket: 1-6h
    side: over
    min_edge: 0.05
candidate_rules: []
""",
        encoding="utf-8",
    )


def test_publish_command_fails_closed_without_passing_approved_rule(tmp_path):
    db_path = tmp_path / "publish.db"
    rules_path = tmp_path / "rules.yml"
    output_path = tmp_path / "latest_publishable_bets.json"
    quality_path = tmp_path / "quality.json"
    _create_publish_db(db_path, include_history=False)
    _write_rules(rules_path)
    output_path.write_text("stale paid list", encoding="utf-8")

    exit_code = publish_main(
        [
            "--db",
            str(db_path),
            "--rules",
            str(rules_path),
            "--output",
            str(output_path),
            "--quality-output",
            str(quality_path),
            "--leagues",
            "NBA",
            "--now",
            "2026-02-01T12:00:00+00:00",
        ]
    )

    assert exit_code == 1
    assert not output_path.exists()
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    assert quality["publishable_profitable_list_exists"] is False

    allow_empty_exit = publish_main(
        [
            "--db",
            str(db_path),
            "--rules",
            str(rules_path),
            "--output",
            str(output_path),
            "--quality-output",
            str(quality_path),
            "--leagues",
            "NBA",
            "--now",
            "2026-02-01T12:00:00+00:00",
            "--allow-empty",
        ]
    )

    assert allow_empty_exit == 0
    assert not output_path.exists()


def test_publish_command_writes_expected_list_with_passing_rule(tmp_path):
    db_path = tmp_path / "publish.db"
    rules_path = tmp_path / "rules.yml"
    output_path = tmp_path / "latest_publishable_bets.json"
    quality_path = tmp_path / "quality.json"
    _create_publish_db(db_path, include_history=True)
    _write_rules(rules_path)

    exit_code = publish_main(
        [
            "--db",
            str(db_path),
            "--rules",
            str(rules_path),
            "--output",
            str(output_path),
            "--quality-output",
            str(quality_path),
            "--leagues",
            "NBA",
            "--now",
            "2026-02-01T12:00:00+00:00",
        ]
    )

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["publishable_profitable_list_exists"] is True
    assert payload["passing_approved_rule_ids"] == ["nba_totals_fixture"]
    assert len(payload["bets"]) == 1
    bet = payload["bets"][0]
    assert bet["rule_id"] == "nba_totals_fixture"
    assert bet["market"] == "totals"
    assert bet["league"] == "NBA"
    assert bet["side"] == "over"
    assert bet["odds"] == 100.0
    assert bet["edge"] == pytest.approx(0.22)
    assert bet["model_probability"] == 0.72
    assert bet["market_probability"] == 0.5
    assert bet["quality_summary"]["passes_launch_gate"] is True
    assert bet["quality_summary"]["roi"] > 0.5


def test_legacy_historical_approval_is_insufficient_to_publish(tmp_path, monkeypatch):
    rules_path = tmp_path / "rules.yml"
    output_path = tmp_path / "latest_publishable_bets.json"
    _write_metadata_approved_rules(rules_path)
    db_path = tmp_path / "publish.db"
    _create_publish_db(db_path, include_history=False)
    output_path.write_text("stale list", encoding="utf-8")
    monkeypatch.setattr(
        publisher, "load_current_prediction_output",
        lambda **_: pytest.fail("Unverified approval reached current inference"),
    )
    result = publisher.publishable_bets(
        db_path=db_path, rules_path=rules_path, output_path=output_path,
        quality_output_path=tmp_path / "quality.json", leagues=["NBA"],
        now="2026-02-01T12:00:00+00:00",
    )
    assert result["reason"] == "no_passing_approved_rule"
    assert not output_path.exists()


def test_live_matching_requires_contract_and_rule_id_on_each_pick():
    frame = pd.DataFrame(
        [
            {"game_id": "wrong-rule", "rule_id": "another", "feature_contract": "canonical"},
            {"game_id": "wrong-contract", "rule_id": "current", "feature_contract": "legacy"},
            {"game_id": "matched", "rule_id": "current", "feature_contract": "canonical"},
        ]
    )
    frame["model_sha256"] = "a" * 64

    matching, mismatches, missing_id = publisher._matching_live_rows(
        frame, {"id": "current", "feature_contract": "canonical", "model_sha256": "a" * 64}
    )

    assert matching["game_id"].tolist() == ["matched"]
    assert not mismatches
    assert not missing_id


def test_locked_rule_evidence_excludes_prelock_predictions():
    bets = pd.DataFrame({
        "game_id": ["before", "after"],
        "predicted_at": ["2026-03-01T00:00:00Z", "2026-03-03T00:00:00Z"],
    })
    rule = {"id": "locked", "locked_at": "2026-03-02T00:00:00Z"}

    assert publisher.filter_bets_for_rule(bets, rule)["game_id"].tolist() == ["after"]


def test_retrospective_candidate_cannot_be_promoted_without_a_forward_lock(tmp_path):
    rules = tmp_path / "rules.yml"
    _write_candidate_rules(rules)
    with pytest.raises(RuntimeError, match="Lock the candidate"):
        promote_candidate_rule(rule_id="nba_totals_candidate", rules_path=rules)


def test_promote_candidate_rule_requires_passing_gate(tmp_path):
    failing_db = tmp_path / "failing.db"
    passing_db = tmp_path / "passing.db"
    rules_path = tmp_path / "rules.yml"
    _create_publish_db(failing_db, include_history=False)
    _create_publish_db(passing_db, include_history=True)
    _write_candidate_rules(rules_path)
    config = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    config["locked_candidate_rules"] = [
        {**config["candidate_rules"][0], "status": "locked_candidate", "locked_at": "2026-01-01T00:00:00Z"}
    ]
    config["candidate_rules"] = []
    rules_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(RuntimeError):
        promote_candidate_rule(
            rule_id="nba_totals_candidate",
            rules_path=rules_path,
            db_path=failing_db,
            leagues=["NBA"],
        )
    failed_config = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    assert failed_config["approved_rules"] == []
    assert failed_config["locked_candidate_rules"][0]["id"] == "nba_totals_candidate"

    with pytest.raises(RuntimeError, match="completed prospective paper trial"):
        promote_candidate_rule(
            rule_id="nba_totals_candidate", rules_path=rules_path,
            db_path=passing_db, leagues=["NBA"],
        )
    assert yaml.safe_load(rules_path.read_text(encoding="utf-8"))["approved_rules"] == []


def test_lock_candidate_rule_uses_decision_time_recommendation_without_approval(tmp_path):
    rules_path = tmp_path / "rules.yml"
    report_path = tmp_path / "decision_time_benchmark.json"
    _write_candidate_rules(rules_path)
    report_path.write_text(
        json.dumps(
            {
                "locked_candidate_recommendations": [
                    {
                        "rule_id": "nba_totals_candidate",
                        "market": "totals",
                        "league": "NBA",
                        "model_type": "ensemble",
                        "prediction_variant": "market_residual_shrink_050_sigmoid",
                        "feature_variant": "current_features",
                        "price_mode": "selected_book",
                        "timing_bucket": "1-6h",
                        "side": "over",
                        "min_edge": 0.05,
                        "lock_status": "lock_recommended",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = lock_candidate_rule(
        rule_id="nba_totals_candidate",
        rules_path=rules_path,
        benchmark_report_path=report_path,
        locked_at="2026-03-01T00:00:00+00:00",
    )

    locked_config = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    assert result["locked"] is True
    assert locked_config["approved_rules"] == []
    assert locked_config["candidate_rules"] == []
    assert locked_config["locked_candidate_rules"][0]["id"] == "nba_totals_candidate"
    assert locked_config["locked_candidate_rules"][0]["status"] == "locked_candidate"
    assert locked_config["locked_candidate_rules"][0]["approval_requires"] == (
        "strict_locked_out_of_sample_results"
    )


def test_paper_promotion_waits_for_the_frozen_review_and_keeps_the_execution_contract(tmp_path):
    db_path = tmp_path / "paper.db"
    _seed_paper_evidence(db_path)
    rules_path = tmp_path / "rules.yml"
    rules_path.write_text("approved_rules: []\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="not eligible"):
        promote_paper_trial(
            "nba_totals_fixture", db_path, rules_path, QualityGate(),
            observed_at=datetime(2026, 1, 22, tzinfo=timezone.utc),
        )
    assert yaml.safe_load(rules_path.read_text(encoding="utf-8"))["approved_rules"] == []

    result = promote_paper_trial(
        "nba_totals_fixture", db_path, rules_path, QualityGate(),
        observed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )
    approved = yaml.safe_load(rules_path.read_text(encoding="utf-8"))["approved_rules"][0]
    assert result["promoted"] is True
    assert approved["model_sha256"] == "a" * 64
    assert approved["book"] == "draftkings"
    assert approved["max_quote_age_minutes"] == 10
    assert approved["min_minutes_before_start"] == 15
    review = paper_reviews(db_path, datetime(2026, 2, 1, tzinfo=timezone.utc))["nba_totals_fixture"]
    assert paper_rule_result(approved, review, QualityGate())["passes_launch_gate"]
    approved["min_edge"] = 0.01
    assert not paper_rule_result(approved, review, QualityGate())["passes_launch_gate"]


def test_paper_promotion_respects_current_stricter_launch_thresholds(tmp_path):
    db_path = tmp_path / "paper.db"
    _seed_paper_evidence(db_path)
    rules_path = tmp_path / "rules.yml"
    rules_path.write_text("approved_rules: []\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="roi_below_launch_threshold"):
        promote_paper_trial(
            "nba_totals_fixture", db_path, rules_path, QualityGate(min_roi=0.99),
            observed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize("extra", [
    {"min_moneyline": -110}, {"max_moneyline": -110},
    {"locked_at": "2026-01-06T00:00:00+00:00"},
    {"prediction_variant": "retuned"}, {"feature_variant": "retuned"},
    {"feature_contract": "retuned"}, {"price_mode": "best_price"},
    {"timing_bucket": "one_hour"}, {"future_filter": None},
])
def test_paper_approval_rejects_additional_strategy_fields(tmp_path, extra):
    db_path = tmp_path / "paper.db"
    _seed_paper_evidence(db_path)
    review = paper_reviews(db_path, datetime(2026, 2, 1, tzinfo=timezone.utc))["nba_totals_fixture"]
    canonical = rule_for_trial(review)
    assert paper_rule_result(canonical, review, QualityGate())["passes_launch_gate"]

    result = paper_rule_result({**canonical, **extra}, review, QualityGate())

    assert result["passes_launch_gate"] is False
    assert result["launch_gate_failures"] == ["paper_trial_unrecognized_rule_fields"]
    assert result["unrecognized_rule_fields"] == list(extra)


def test_filtered_subset_cannot_inherit_the_full_trial_approval(tmp_path):
    db_path = tmp_path / "paper.db"
    _seed_paper_evidence(db_path)
    review = paper_reviews(db_path, datetime(2026, 2, 1, tzinfo=timezone.utc))["nba_totals_fixture"]
    canonical = rule_for_trial(review)
    rows = pd.DataFrame({
        "game_id": ["cheaper", "dearer"], "market": ["totals"] * 2,
        "league": ["NBA"] * 2, "model_type": ["ensemble"] * 2,
        "model_sha256": ["a" * 64] * 2, "edge": [0.1] * 2,
        "moneyline": [-105, -115],
    })
    filtered = {**canonical, "max_moneyline": -110}
    assert len(publisher.filter_bets_for_rule(rows, canonical)) == 2
    assert publisher.filter_bets_for_rule(rows, filtered)["game_id"].tolist() == ["dearer"]
    assert not paper_rule_result(filtered, review, QualityGate())["passes_launch_gate"]
