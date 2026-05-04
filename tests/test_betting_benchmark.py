from __future__ import annotations

import json

import pandas as pd

from src.models.betting_benchmark import (
    PredictionVariant,
    RollingConfig,
    StrictGate,
    _sort_number,
    rank_predeclared_rules,
    rolling_origin_predictions_for_variants,
    run_betting_benchmark,
)
from src.models.decision_time_benchmark import (
    PRICE_MODES,
    TIMING_BUCKETS,
    expand_moneyline_bets_for_price_mode,
    select_decision_time_training_frame,
)
from src.models.decision_time_benchmark import (
    main as decision_time_main,
)
from src.models.train_betting import FeatureContract


def test_sort_number_preserves_zero_values() -> None:
    assert _sort_number(0.0, 999.0) == 0.0
    assert _sort_number(None, 999.0) == 999.0


def _synthetic_totals_frame() -> pd.DataFrame:
    targets = [0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1]
    rows = []
    for idx, target in enumerate(targets):
        line = 220.5 + (idx % 2)
        actual_total = line + 4 if target else line - 4
        rows.append(
            {
                "game_id": f"GAME{idx}",
                "league": "NBA",
                "start_time_utc": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(days=idx),
                "snapshot_time_utc": pd.Timestamp("2025-12-31", tz="UTC") + pd.Timedelta(days=idx),
                "line": line,
                "over_moneyline": -110,
                "under_moneyline": -110,
                "best_over_moneyline": -105,
                "best_under_moneyline": -105,
                "over_no_vig_prob": 0.5,
                "under_no_vig_prob": 0.5,
                "line_movement": 0.25 if target else -0.25,
                "signal": float(target),
                "actual_total": actual_total,
                "target_over": target,
            }
        )
    return pd.DataFrame(rows)


def _synthetic_decision_totals_frame() -> pd.DataFrame:
    targets = [0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1]
    hours = [0.5, 2.0, 8.0, 36.0] * 3
    rows = []
    for idx, target in enumerate(targets):
        line = 220.5 + (idx % 2)
        rows.append(
            {
                "game_id": f"NBA{idx}",
                "league": "NBA",
                "start_time_utc": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(days=idx),
                "snapshot_time_utc": pd.Timestamp("2025-12-31", tz="UTC") + pd.Timedelta(days=idx),
                "line": line,
                "actual_total": line + 5 if target else line - 5,
                "target_over": target,
                "over_moneyline": -110,
                "under_moneyline": -110,
                "best_over_moneyline": -105,
                "best_under_moneyline": 105,
                "book": "SelectedBook",
                "best_over_book": "BestOverBook",
                "best_under_book": "BestUnderBook",
                "over_no_vig_prob": 0.50,
                "under_no_vig_prob": 0.50,
                "total_close": line + (0.5 if target else -0.5),
                "hours_before_start": hours[idx],
                "line_movement": 0.25 if target else -0.25,
                "signal": float(target),
                "home_injuries_out": float(idx % 2),
                "away_injuries_out": float((idx + 1) % 2),
            }
        )
    return pd.DataFrame(rows)


def _decision_totals_contract() -> FeatureContract:
    return FeatureContract(
        market="totals",
        target_column="target_over",
        feature_columns=[
            "line",
            "over_no_vig_prob",
            "under_no_vig_prob",
            "line_movement",
            "signal",
            "home_injuries_out",
            "away_injuries_out",
        ],
        market_probability_column="over_no_vig_prob",
    )


def test_residual_shrink_variant_is_rolling_origin_and_calibrated() -> None:
    df = _synthetic_totals_frame()
    contract = FeatureContract(
        market="totals",
        target_column="target_over",
        feature_columns=["line", "over_no_vig_prob", "line_movement", "signal"],
        market_probability_column="over_no_vig_prob",
    )
    variant = PredictionVariant(
        id="market_residual_shrink_050_sigmoid",
        residual=True,
        shrinkage=0.5,
        calibration="sigmoid",
    )

    predictions_by_variant = rolling_origin_predictions_for_variants(
        df,
        contract,
        model_type="logistic",
        variants=[variant],
        rolling=RollingConfig(
            folds=2,
            min_train_size=6,
            calibration_folds=2,
            min_calibration_size=3,
        ),
    )

    predictions = predictions_by_variant[variant.id]
    assert len(predictions) == 6
    assert predictions["validation_fold"].min() == 1
    assert predictions["predicted_prob"].between(0.0, 1.0).all()
    assert predictions["uncalibrated_prob"].between(0.0, 1.0).all()
    assert set(predictions["prediction_variant"]) == {variant.id}
    assert "residual_signal" in predictions.columns
    assert predictions["calibration_method"].notna().all()


def test_ranked_rules_include_market_baseline_and_strict_gate() -> None:
    start = pd.Timestamp("2026-01-01", tz="UTC")
    bets = pd.DataFrame(
        [
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "edge": 0.08,
                "profit": 90.91,
                "predicted_prob": 0.70,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 20.0,
                "actual_value": 90.91,
                "start_time_utc": start,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "edge": 0.07,
                "profit": 90.91,
                "predicted_prob": 0.69,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 18.0,
                "actual_value": 90.91,
                "start_time_utc": start + pd.Timedelta(days=1),
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "market_only",
                "prediction_variant": "baseline",
                "side": "over",
                "edge": 0.0,
                "profit": 90.91,
                "predicted_prob": 0.52,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 0.0,
                "actual_value": 90.91,
                "start_time_utc": start,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "market_only",
                "prediction_variant": "baseline",
                "side": "over",
                "edge": 0.0,
                "profit": -100.0,
                "predicted_prob": 0.52,
                "no_vig_market_prob": 0.52,
                "won": False,
                "expected_value": 0.0,
                "actual_value": -100.0,
                "start_time_utc": start + pd.Timedelta(days=1),
            },
        ]
    )
    rules = [
        {
            "id": "nba_totals_logistic_residual_over_edge_002",
            "status": "candidate",
            "kind": "candidate",
            "market": "totals",
            "league": "NBA",
            "model_type": "logistic",
            "prediction_variant": "market_residual_shrink_050_sigmoid",
            "side": "over",
            "min_edge": 0.02,
            "residual": True,
            "shrinkage": 0.5,
            "calibration": "sigmoid",
        },
        {
            "id": "nba_totals_market_baseline_over_edge_000",
            "status": "candidate",
            "kind": "baseline",
            "market": "totals",
            "league": "NBA",
            "model_type": "market_only",
            "prediction_variant": "baseline",
            "side": "over",
            "min_edge": 0.0,
            "residual": False,
            "shrinkage": 1.0,
            "calibration": "none",
        },
    ]

    ranked = rank_predeclared_rules(
        bets,
        rules,
        StrictGate(
            min_bets_narrow=3,
            min_roi=-1.0,
            min_bootstrap_roi_low=-1.0,
            bootstrap_samples=200,
        ),
    )
    candidate = next(row for row in ranked if row["kind"] == "candidate")

    assert all("market_baseline_comparison" in row for row in ranked)
    assert candidate["market_baseline_comparison"]["market_only"]["bets"] == 2
    assert candidate["publishable"] is False
    assert "sample_size_below_3" in candidate["strict_gate_failures"]
    assert "candidate_rule:" in candidate["candidate_rule_yaml"]


def test_ranked_rules_report_clv_slices_and_stale_odds_filter() -> None:
    start = pd.Timestamp("2026-01-01", tz="UTC")
    bets = pd.DataFrame(
        [
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "edge": 0.08,
                "profit": 90.91,
                "predicted_prob": 0.70,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 20.0,
                "actual_value": 90.91,
                "closing_line_value": 1.5,
                "book": "draftkings",
                "selected_book": "fanduel",
                "best_book": "draftkings",
                "price_source": "best_book",
                "hours_before_start": 12.0,
                "start_time_utc": start,
            },
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "edge": 0.09,
                "profit": 90.91,
                "predicted_prob": 0.71,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 21.0,
                "actual_value": 90.91,
                "closing_line_value": 2.0,
                "book": "fanduel",
                "selected_book": "fanduel",
                "best_book": "fanduel",
                "price_source": "selected_book",
                "hours_before_start": 96.0,
                "start_time_utc": start + pd.Timedelta(days=1),
            },
        ]
    )
    rules = [
        {
            "id": "nba_totals_logistic_residual_over_edge_002",
            "status": "candidate",
            "kind": "candidate",
            "market": "totals",
            "league": "NBA",
            "model_type": "logistic",
            "prediction_variant": "market_residual_shrink_050_sigmoid",
            "side": "over",
            "min_edge": 0.02,
            "residual": True,
            "shrinkage": 0.5,
            "calibration": "sigmoid",
        }
    ]

    ranked = rank_predeclared_rules(
        bets,
        rules,
        StrictGate(
            min_bets_narrow=1,
            min_roi=-1.0,
            min_bootstrap_roi_low=-1.0,
            min_clv_win_rate=0.0,
            bootstrap_samples=200,
            require_brier_beats_market=False,
            require_roi_beats_market_baseline=False,
            max_hours_before_start=72.0,
        ),
    )

    candidate = ranked[0]
    assert candidate["bets"] == 1
    assert candidate["odds_timing_filter"]["stale_odds_excluded"] == 1
    assert candidate["clv_slices"][0]["book"] == "draftkings"
    assert candidate["clv_slices"][0]["hours_bucket"] == "6-24h"
    assert candidate["clv_slices"][0]["price_source"] == "best_book"
    assert candidate["clv_slices"][0]["closing_line_value_source"] == "unknown"


def test_decision_time_selection_chooses_one_snapshot_per_game_side() -> None:
    frame = pd.DataFrame(
        [
            {
                "game_id": "NBA1",
                "side": "home",
                "start_time_utc": pd.Timestamp("2026-01-02T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-01T12:00:00Z"),
                "hours_before_start": 12.0,
                "book": "FanDuel",
                "moneyline": -110,
            },
            {
                "game_id": "NBA1",
                "side": "home",
                "start_time_utc": pd.Timestamp("2026-01-02T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-01T12:00:00Z"),
                "hours_before_start": 12.0,
                "book": "DraftKings",
                "moneyline": -105,
            },
            {
                "game_id": "NBA1",
                "side": "away",
                "start_time_utc": pd.Timestamp("2026-01-02T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-01T13:00:00Z"),
                "hours_before_start": 11.0,
                "book": "FanDuel",
                "moneyline": 110,
            },
            {
                "game_id": "NBA1",
                "side": "away",
                "start_time_utc": pd.Timestamp("2026-01-02T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-01T16:00:00Z"),
                "hours_before_start": 8.0,
                "book": "DraftKings",
                "moneyline": 115,
            },
        ]
    )

    selected = select_decision_time_training_frame(frame, "6-24h", market="moneyline")

    assert len(selected) == 2
    home = selected[selected["side"] == "home"].iloc[0]
    away = selected[selected["side"] == "away"].iloc[0]
    assert home["book"] == "DraftKings"
    assert away["book"] == "DraftKings"
    assert away["hours_before_start"] == 8.0


def test_moneyline_selected_and_best_book_price_modes_produce_distinct_roi() -> None:
    predictions = pd.DataFrame(
        [
            {
                "game_id": "NBA1",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "validation_fold": 1,
                "side": "home",
                "predicted_prob": 0.70,
                "raw_model_prob": 0.70,
                "uncalibrated_prob": 0.70,
                "calibration_method": "none",
                "no_vig_prob": 0.52,
                "moneyline": -110,
                "best_moneyline": 120,
                "book": "SelectedBook",
                "best_book": "BestBook",
                "target_win": 1,
                "close_moneyline": -100,
                "snapshot_time_utc": pd.Timestamp("2026-01-01", tz="UTC"),
                "start_time_utc": pd.Timestamp("2026-01-02", tz="UTC"),
                "hours_before_start": 24.0,
            }
        ]
    )

    selected = expand_moneyline_bets_for_price_mode(
        predictions,
        price_mode="selected_book",
        feature_variant="current_features",
    )
    best = expand_moneyline_bets_for_price_mode(
        predictions,
        price_mode="best_book",
        feature_variant="current_features",
    )

    assert selected.iloc[0]["price_source"] == "selected_book"
    assert best.iloc[0]["price_source"] == "best_book"
    assert selected.iloc[0]["profit"] != best.iloc[0]["profit"]


def test_missing_clv_cannot_pass_strict_gate() -> None:
    bets = pd.DataFrame(
        [
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "edge": 0.08,
                "profit": 100.0,
                "predicted_prob": 0.70,
                "no_vig_market_prob": 0.52,
                "won": True,
                "expected_value": 20.0,
                "actual_value": 100.0,
                "start_time_utc": pd.Timestamp("2026-01-01", tz="UTC"),
            }
        ]
    )
    rules = [
        {
            "id": "nba_totals_logistic_residual_over_edge_002",
            "status": "candidate",
            "kind": "candidate",
            "market": "totals",
            "league": "NBA",
            "model_type": "logistic",
            "prediction_variant": "market_residual_shrink_050_sigmoid",
            "side": "over",
            "min_edge": 0.02,
            "residual": True,
            "shrinkage": 0.5,
            "calibration": "sigmoid",
        }
    ]

    ranked = rank_predeclared_rules(
        bets,
        rules,
        StrictGate(
            min_bets_narrow=1,
            min_roi=-1.0,
            min_bootstrap_roi_low=-1.0,
            bootstrap_samples=20,
            require_brier_beats_market=False,
            require_roi_beats_market_baseline=False,
            require_positive_clv=True,
        ),
    )

    assert ranked[0]["passes_strict_gate"] is False
    assert "missing_clv" in ranked[0]["strict_gate_failures"]


def test_benchmark_run_writes_triage_artifact(tmp_path) -> None:
    config_path = tmp_path / "benchmark.yml"
    output_dir = tmp_path / "reports"
    config_path.write_text(
        """
sweep:
  markets:
    - totals
  leagues:
    - NBA
  baselines:
    - market_only
  candidates:
    - logistic
  min_edge_thresholds:
    - 0.0
  sides:
    totals:
      - over
  variants:
    baselines:
      - id: baseline
        residual: false
        shrinkage: 1.0
        calibration: none
    candidates:
      - id: market_residual_shrink_050_sigmoid
        residual: true
        shrinkage: 0.5
        calibration: sigmoid
rolling_origin:
  folds: 1
  min_train_size: 2
  calibration_folds: 1
  min_calibration_size: 1
strict_gate:
  min_bets_narrow: 150
  min_bets_multi_league: 300
  min_roi: 0.05
  min_bootstrap_roi_low: 0.0
  min_avg_clv: 0.0
  min_clv_win_rate: 0.5
  bootstrap_samples: 20
  require_brier_beats_market: true
  require_roi_beats_market_baseline: true
""",
        encoding="utf-8",
    )

    artifacts = run_betting_benchmark(
        db_path=tmp_path / "missing.sqlite",
        config_path=config_path,
        output_dir=output_dir,
    )

    report = json.loads(artifacts.report_path.read_text(encoding="utf-8"))
    triage = json.loads(artifacts.triage_report_path.read_text(encoding="utf-8"))

    assert report["triage_report_path"] == str(artifacts.triage_report_path)
    assert triage["source_benchmark_report"] == str(artifacts.report_path)
    assert triage["candidate_count"] == 1
    assert triage["passing_candidate_count"] == 0
    assert triage["closest_to_pass"][0]["rule_id"].startswith("nba_totals_logistic")


def test_decision_time_benchmark_cli_writes_no_auto_promotion_report(
    tmp_path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "benchmark.yml"
    output_path = tmp_path / "decision_time_benchmark_latest.json"
    config_path.write_text(
        """
sweep:
  markets:
    - totals
  leagues:
    - NBA
  baselines:
    - market_only
  candidates:
    - logistic
  min_edge_thresholds:
    - 0.0
    - 0.02
  sides:
    totals:
      - over
      - under
      - both
  variants:
    baselines:
      - id: baseline
        residual: false
        shrinkage: 1.0
        calibration: none
    candidates:
      - id: market_residual_shrink_050_sigmoid
        residual: true
        shrinkage: 0.5
        calibration: sigmoid
rolling_origin:
  folds: 2
  min_train_size: 6
  calibration_folds: 2
  min_calibration_size: 3
strict_gate:
  min_bets_narrow: 150
  min_bets_multi_league: 300
  min_roi: 0.05
  min_bootstrap_roi_low: 0.0
  min_avg_clv: 0.0
  min_clv_win_rate: 0.5
  max_hours_before_start: 72
  bootstrap_samples: 20
  require_brier_beats_market: true
  require_roi_beats_market_baseline: true
""",
        encoding="utf-8",
    )

    import src.models.decision_time_benchmark as decision

    monkeypatch.setattr(
        decision,
        "load_training_frame",
        lambda *args, **kwargs: (_synthetic_decision_totals_frame(), _decision_totals_contract()),
    )

    exit_code = decision_time_main(
        [
            "--db",
            str(tmp_path / "missing.sqlite"),
            "--config",
            str(config_path),
            "--output",
            str(output_path),
            "--markets",
            "totals",
            "--leagues",
            "NBA",
        ]
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["sweep"]["timing_buckets"] == list(TIMING_BUCKETS)
    assert payload["sweep"]["price_modes"] == list(PRICE_MODES)
    assert payload["candidate_rankings"]
    assert "strict_gate_failures" in payload["candidate_rankings"][0]
    assert "clv_slices" in payload["candidate_rankings"][0]
    assert payload["approved_candidate_count"] == 0
    assert payload["auto_promotions"] == []
    assert payload["recommendation"] in {
        "do_not_promote",
        "monitor_profitable_slices",
        "manual_locked_candidate_review",
    }
