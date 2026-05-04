from __future__ import annotations

import sqlite3

import pandas as pd

from src.models.prediction_quality import (
    QualityGate,
    american_profit,
    build_quality_report,
    closing_line_value_summary,
    expand_moneyline_bets,
    expand_totals_bets,
    filter_bets_for_rule,
    load_totals_model_input,
    settle_total_side,
    summarize_bets,
)


def _create_quality_db(path):
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
            INSERT INTO teams VALUES (1, 1, 'LAL', 'Lakers');
            INSERT INTO teams VALUES (2, 1, 'BOS', 'Celtics');
            INSERT INTO games VALUES (
                'GAME1', 1, '2026-01-01T20:00:00+00:00', 1, 2, 'final'
            );
            INSERT INTO game_results VALUES ('GAME1', 120, 110, -115, -105, 221.5);
            """
        )
        conn.executemany(
            """
            INSERT INTO predictions (
                prediction_id, game_id, model_type, predicted_at,
                total_line, over_prob, under_prob, over_moneyline, under_moneyline,
                over_edge, under_edge, over_implied_prob, under_implied_prob,
                predicted_total_points
            ) VALUES (?, 'GAME1', 'ensemble', ?, 220.5, ?, ?, -110, -110, ?, ?, 0.5, 0.5, ?)
            """,
            [
                (1, "2026-01-01T18:00:00+00:00", 0.55, 0.45, 0.05, -0.05, 224.0),
                (2, "2026-01-01T19:00:00+00:00", 0.60, 0.40, 0.10, -0.10, 226.0),
                (3, "2026-01-01T21:00:00+00:00", 0.99, 0.01, 0.49, -0.49, 250.0),
            ],
        )


def test_total_settlement_and_american_profit():
    assert settle_total_side(221, 220.5, "over") is True
    assert settle_total_side(220, 220.5, "under") is True
    assert settle_total_side(220.5, 220.5, "over") is None
    assert round(american_profit(-110, True), 2) == 90.91
    assert american_profit(-110, False) == -100


def test_totals_model_input_uses_latest_pre_game_prediction(tmp_path):
    db_path = tmp_path / "quality.db"
    _create_quality_db(db_path)

    model_input = load_totals_model_input(db_path=db_path, leagues=["NBA"])

    assert len(model_input) == 1
    row = model_input.iloc[0]
    assert row["prediction_id"] == 2
    assert row["over_prob"] == 0.60
    assert row["predicted_total_points"] == 226.0


def test_expand_totals_bets_excludes_pushes_and_calculates_roi(tmp_path):
    db_path = tmp_path / "quality.db"
    _create_quality_db(db_path)
    bets = expand_totals_bets(load_totals_model_input(db_path=db_path, leagues=["NBA"]))

    assert set(bets["side"]) == {"over", "under"}
    over = bets[bets["side"] == "over"].iloc[0]
    under = bets[bets["side"] == "under"].iloc[0]
    assert over["won"] is True or bool(over["won"]) is True
    assert under["won"] is False or bool(under["won"]) is False

    summary = summarize_bets(bets, gate=QualityGate(min_bets_narrow=1, bootstrap_samples=200))
    assert summary["bets"] == 2
    assert summary["profit"] < 0
    assert summary["brier_score"] is not None


def test_clv_is_computed_for_totals_and_moneyline_sides(tmp_path):
    db_path = tmp_path / "quality.db"
    _create_quality_db(db_path)
    totals_bets = expand_totals_bets(load_totals_model_input(db_path=db_path, leagues=["NBA"]))

    over = totals_bets[totals_bets["side"] == "over"].iloc[0]
    under = totals_bets[totals_bets["side"] == "under"].iloc[0]
    assert over["closing_line_value"] == 1.0
    assert under["closing_line_value"] == -1.0

    moneyline_input = pd.DataFrame(
        {
            "game_id": ["GAME1"],
            "league": ["NBA"],
            "model_type": ["ensemble"],
            "start_time_utc": [pd.Timestamp("2026-01-01T20:00:00Z")],
            "home_score": [120],
            "away_score": [110],
            "home_prob": [0.55],
            "away_prob": [0.45],
            "home_moneyline": [-110],
            "away_moneyline": [120],
            "home_moneyline_close": [-120],
            "away_moneyline_close": [130],
            "home_edge": [0.03],
            "away_edge": [-0.03],
            "home_implied_prob": [0.52],
            "away_implied_prob": [0.48],
            "home_no_vig_prob": [0.51],
            "away_no_vig_prob": [0.49],
        }
    )
    moneyline_bets = expand_moneyline_bets(moneyline_input)
    home = moneyline_bets[moneyline_bets["side"] == "home"].iloc[0]
    away = moneyline_bets[moneyline_bets["side"] == "away"].iloc[0]

    assert round(home["closing_line_value"], 4) == 0.0758
    assert round(away["closing_line_value"], 4) == -0.1


def test_missing_close_lines_are_excluded_from_clv_summary():
    bets = pd.DataFrame({"closing_line_value": [None, float("nan")]})

    summary = closing_line_value_summary(bets)

    assert summary["closing_line_value_count"] == 0
    assert summary["avg_closing_line_value"] is None
    assert summary["closing_line_value_win_rate"] is None


def test_rule_filter_isolates_benchmark_variant_fields() -> None:
    bets = pd.DataFrame(
        [
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "variant_a",
                "price_mode": "selected_book",
                "timing_bucket": "1-6h",
                "feature_variant": "current_features",
                "side": "over",
                "edge": 0.06,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "variant_b",
                "price_mode": "selected_book",
                "timing_bucket": "1-6h",
                "feature_variant": "current_features",
                "side": "over",
                "edge": 0.06,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "variant_a",
                "price_mode": "best_book",
                "timing_bucket": "1-6h",
                "feature_variant": "current_features",
                "side": "over",
                "edge": 0.06,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "variant_a",
                "price_mode": "selected_book",
                "timing_bucket": "6-24h",
                "feature_variant": "current_features",
                "side": "over",
                "edge": 0.06,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "variant_a",
                "price_mode": "selected_book",
                "timing_bucket": "1-6h",
                "feature_variant": "no_availability_features",
                "side": "over",
                "edge": 0.06,
            },
        ]
    )
    rule = {
        "market": "totals",
        "league": "NBA",
        "model_type": "logistic",
        "prediction_variant": "variant_a",
        "price_mode": "selected_book",
        "timing_bucket": "1-6h",
        "feature_variant": "current_features",
        "side": "over",
        "min_edge": 0.05,
    }

    filtered = filter_bets_for_rule(bets, rule)

    assert len(filtered) == 1
    assert filtered.iloc[0]["prediction_variant"] == "variant_a"


def test_quality_report_evaluates_candidate_rule(tmp_path):
    db_path = tmp_path / "quality.db"
    _create_quality_db(db_path)
    rules_path = tmp_path / "rules.yml"
    rules_path.write_text(
        """
launch_gate:
  min_bets_narrow: 1
  min_roi: -1.0
  min_bootstrap_roi_low: -1.0
  bootstrap_samples: 200
candidate_rules:
  - id: nba_test_rule
    status: candidate
    market: totals
    league: NBA
    model_type: ensemble
    side: over
    min_edge: 0.05
approved_rules: []
""",
        encoding="utf-8",
    )

    report = build_quality_report(db_path=db_path, rules_path=rules_path, leagues=["NBA"])

    assert report["dataset_counts"]["totals_prediction_rows"] == 1
    assert report["rule_results"][0]["rule_id"] == "nba_test_rule"
    assert report["rule_results"][0]["bets"] == 1
    assert "clv_summary" in report
    assert "feature_contract_coverage" in report
    assert report["candidate_rule_rankings"][0]["rule_id"] == "nba_test_rule"


def test_quality_report_evaluates_candidate_benchmark_output_with_same_gate(tmp_path):
    db_path = tmp_path / "quality.db"
    _create_quality_db(db_path)
    benchmark_path = tmp_path / "benchmark_predictions.parquet"
    pd.DataFrame(
        {
            "league": ["NBA", "NBA", "NBA", "NBA"],
            "model_type": ["benchmark_model"] * 4,
            "start_time_utc": pd.date_range("2026-01-01", periods=4, tz="UTC"),
            "snapshot_time_utc": pd.date_range("2025-12-31", periods=4, tz="UTC"),
            "line": [200.5, 200.5, 200.5, 200.5],
            "total_close": [201.5, 201.0, 202.0, 201.5],
            "actual_total": [221, 219, 230, 180],
            "predicted_prob": [0.80, 0.80, 0.80, 0.80],
            "over_no_vig_prob": [0.40, 0.40, 0.40, 0.40],
            "under_no_vig_prob": [0.60, 0.60, 0.60, 0.60],
            "over_moneyline": [200, 200, 200, 200],
            "under_moneyline": [-110, -110, -110, -110],
        }
    ).to_parquet(benchmark_path, index=False)
    rules_path = tmp_path / "rules.yml"
    rules_path.write_text(
        """
launch_gate:
  min_bets_narrow: 4
  min_roi: 0.5
  min_bootstrap_roi_low: -0.5
  bootstrap_samples: 300
candidate_rules:
  - id: nba_benchmark_rule
    market: totals
    league: NBA
    model_type: benchmark_model
    side: over
    min_edge: 0.05
approved_rules: []
""",
        encoding="utf-8",
    )

    report = build_quality_report(
        db_path=db_path,
        rules_path=rules_path,
        leagues=["NBA"],
        benchmark_prediction_paths=[benchmark_path],
    )

    benchmark_source = report["benchmark_results"][0]
    result = benchmark_source["rule_results"][0]
    assert benchmark_source["source_type"] == "candidate_benchmark_output"
    assert result["rule_id"] == "nba_benchmark_rule"
    assert result["passes_launch_gate"] is True
    assert report["publishable_profitable_list_exists"] is False
