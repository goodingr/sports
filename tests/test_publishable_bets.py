from __future__ import annotations

import json
import sqlite3

import pandas as pd
import pytest
import yaml

import src.predict.publishable_bets as publisher
from src.predict.publishable_bets import lock_candidate_rule, promote_candidate_rule
from src.predict.publishable_bets import main as publish_main


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
    side: over
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
    assert bet["edge"] == 0.22
    assert bet["model_probability"] == 0.72
    assert bet["market_probability"] == 0.5
    assert bet["quality_summary"]["passes_launch_gate"] is True
    assert bet["quality_summary"]["roi"] > 0.5


def test_publish_guardrail_blocks_legacy_rows_for_benchmarked_rule(
    tmp_path,
    monkeypatch,
):
    rules_path = tmp_path / "rules.yml"
    output_path = tmp_path / "latest_publishable_bets.json"
    quality_path = tmp_path / "quality.json"
    _write_metadata_approved_rules(rules_path)
    output_path.write_text("stale paid list", encoding="utf-8")

    monkeypatch.setattr(
        publisher,
        "build_quality_report",
        lambda **kwargs: {
            "rule_results": [
                {
                    "rule_id": "nba_totals_metadata_rule",
                    "passes_launch_gate": True,
                    "odds_timing_filter": {"max_hours_before_start": None},
                }
            ],
            "evaluation_sources": [],
        },
    )
    monkeypatch.setattr(
        publisher,
        "load_current_prediction_output",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "prediction_id": 1,
                    "game_id": "CUR1",
                    "model_type": "logistic",
                    "predicted_at": "2026-02-01T10:00:00+00:00",
                    "start_time_utc": "2026-02-02T20:00:00+00:00",
                    "league": "NBA",
                    "home_team": "Home",
                    "away_team": "Away",
                    "game_status": "scheduled",
                    "total_line": 210.5,
                    "over_prob": 0.72,
                    "under_prob": 0.28,
                    "over_moneyline": 100,
                    "under_moneyline": 100,
                    "over_edge": 0.22,
                    "under_edge": -0.22,
                    "over_implied_prob": 0.5,
                    "under_implied_prob": 0.5,
                }
            ]
        ),
    )

    result = publisher.publishable_bets(
        db_path=tmp_path / "unused.db",
        rules_path=rules_path,
        output_path=output_path,
        quality_output_path=quality_path,
        leagues=["NBA"],
        now="2026-02-01T12:00:00+00:00",
    )

    assert result["publishable_profitable_list_exists"] is False
    assert result["reason"] == "passing_rules_require_benchmarked_live_metadata"
    assert result["skipped_passing_rule_ids"] == ["nba_totals_metadata_rule"]
    assert not output_path.exists()


def test_promote_candidate_rule_requires_passing_gate(tmp_path):
    failing_db = tmp_path / "failing.db"
    passing_db = tmp_path / "passing.db"
    rules_path = tmp_path / "rules.yml"
    _create_publish_db(failing_db, include_history=False)
    _create_publish_db(passing_db, include_history=True)
    _write_candidate_rules(rules_path)

    with pytest.raises(RuntimeError):
        promote_candidate_rule(
            rule_id="nba_totals_candidate",
            rules_path=rules_path,
            db_path=failing_db,
            leagues=["NBA"],
        )
    failed_config = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    assert failed_config["approved_rules"] == []
    assert failed_config["candidate_rules"][0]["id"] == "nba_totals_candidate"

    result = promote_candidate_rule(
        rule_id="nba_totals_candidate",
        rules_path=rules_path,
        db_path=passing_db,
        leagues=["NBA"],
    )

    promoted_config = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    assert result["promoted"] is True
    assert promoted_config["approved_rules"][0]["id"] == "nba_totals_candidate"
    assert promoted_config["approved_rules"][0]["status"] == "approved"
    assert promoted_config["candidate_rules"] == []


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
