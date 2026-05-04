from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.models.betting_benchmark import StrictGate, rank_predeclared_rules
from src.models.nba_totals_clv_sweep import (
    TIMING_BUCKETS,
    expand_totals_bets_for_price_mode,
    feature_columns_for_variant,
    filter_timing_bucket,
    main,
    select_timing_training_frame,
)
from src.models.train_betting import FeatureContract


def _synthetic_predictions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "game_id": "NBA1",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "validation_fold": 1,
                "predicted_prob": 0.70,
                "raw_model_prob": 0.70,
                "uncalibrated_prob": 0.70,
                "calibration_method": "none",
                "line": 220.5,
                "actual_total": 226.5,
                "over_moneyline": -110,
                "under_moneyline": -110,
                "best_over_moneyline": 120,
                "best_under_moneyline": -105,
                "book": "SelectedBook",
                "best_over_book": "BestBook",
                "best_under_book": "OtherBestBook",
                "over_no_vig_prob": 0.52,
                "under_no_vig_prob": 0.48,
                "total_close": 222.5,
                "snapshot_time_utc": pd.Timestamp("2026-01-01", tz="UTC"),
                "start_time_utc": pd.Timestamp("2026-01-02", tz="UTC"),
                "hours_before_start": 12.0,
            }
        ]
    )


def _synthetic_training_frame() -> pd.DataFrame:
    targets = [0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1]
    hours = [0.5, 2.0, 8.0, 36.0] * 3
    rows = []
    for idx, target in enumerate(targets):
        line = 220.5 + (idx % 2)
        rows.append(
            {
                "game_id": f"NBA{idx}",
                "league": "NBA",
                "start_time_utc": pd.Timestamp("2026-01-01", tz="UTC")
                + pd.Timedelta(days=idx),
                "snapshot_time_utc": pd.Timestamp("2025-12-31", tz="UTC")
                + pd.Timedelta(days=idx),
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
                "home_injuries_out": float(idx % 3),
                "away_injuries_out": float((idx + 1) % 3),
            }
        )
    return pd.DataFrame(rows)


def _contract() -> FeatureContract:
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


def test_feature_variant_filter_removes_only_availability_features() -> None:
    columns = [
        "line",
        "home_injuries_out",
        "away_injuries_questionable",
        "home_player_usage",
        "away_rolling_pace",
    ]

    assert feature_columns_for_variant(columns, "no_availability_features") == [
        "line",
        "home_player_usage",
        "away_rolling_pace",
    ]
    assert feature_columns_for_variant(columns, "current_features") == columns


def test_timing_bucket_filters_include_expected_hours() -> None:
    bets = pd.DataFrame({"hours_before_start": [0.5, 1.0, 5.9, 6.0, 23.9, 24.0, 72.0, 73.0]})

    assert filter_timing_bucket(bets, "<1h")["hours_before_start"].tolist() == [0.5]
    assert filter_timing_bucket(bets, "1-6h")["hours_before_start"].tolist() == [1.0, 5.9]
    assert filter_timing_bucket(bets, "6-24h")["hours_before_start"].tolist() == [6.0, 23.9]
    assert filter_timing_bucket(bets, "24-72h")["hours_before_start"].tolist() == [24.0, 72.0]
    assert filter_timing_bucket(bets, "all_0_72h")["hours_before_start"].tolist() == [
        0.5,
        1.0,
        5.9,
        6.0,
        23.9,
        24.0,
        72.0,
    ]


def test_timing_training_frame_selects_latest_as_of_row_per_game() -> None:
    frame = pd.DataFrame(
        [
            {
                "game_id": "NBA1",
                "start_time_utc": pd.Timestamp("2026-01-02T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-01T04:00:00Z"),
                "hours_before_start": 20.0,
                "book": "DraftKings",
                "line": 220.5,
            },
            {
                "game_id": "NBA1",
                "start_time_utc": pd.Timestamp("2026-01-02T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-01T14:00:00Z"),
                "hours_before_start": 10.0,
                "book": "FanDuel",
                "line": 221.0,
            },
            {
                "game_id": "NBA1",
                "start_time_utc": pd.Timestamp("2026-01-02T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-01T14:00:00Z"),
                "hours_before_start": 10.0,
                "book": "DraftKings",
                "line": 221.5,
            },
            {
                "game_id": "NBA2",
                "start_time_utc": pd.Timestamp("2026-01-03T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-01T23:00:00Z"),
                "hours_before_start": 25.0,
                "book": "DraftKings",
                "line": 218.5,
            },
            {
                "game_id": "NBA2",
                "start_time_utc": pd.Timestamp("2026-01-03T00:00:00Z"),
                "snapshot_time_utc": pd.Timestamp("2026-01-02T17:00:00Z"),
                "hours_before_start": 7.0,
                "book": "FanDuel",
                "line": 219.0,
            },
        ]
    )

    selected = select_timing_training_frame(frame, "6-24h")

    assert selected["game_id"].tolist() == ["NBA1", "NBA2"]
    nba1 = selected[selected["game_id"] == "NBA1"].iloc[0]
    assert nba1["book"] == "DraftKings"
    assert nba1["line"] == 221.5
    nba2 = selected[selected["game_id"] == "NBA2"].iloc[0]
    assert nba2["book"] == "FanDuel"
    assert nba2["line"] == 219.0


def test_selected_and_best_book_price_modes_produce_different_roi() -> None:
    selected = expand_totals_bets_for_price_mode(
        _synthetic_predictions(),
        price_mode="selected_book",
        feature_variant="current_features",
    )
    best = expand_totals_bets_for_price_mode(
        _synthetic_predictions(),
        price_mode="best_book",
        feature_variant="current_features",
    )

    selected_over = selected[selected["side"] == "over"].iloc[0]
    best_over = best[best["side"] == "over"].iloc[0]

    assert selected_over["price_source"] == "selected_book"
    assert best_over["price_source"] == "best_book"
    assert selected_over["profit"] != best_over["profit"]


def test_strict_failures_include_core_launch_gate_reasons() -> None:
    bets = pd.DataFrame(
        [
            {
                "market": "totals",
                "league": "NBA",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "edge": 0.10,
                "profit": -100.0,
                "predicted_prob": 0.90,
                "no_vig_market_prob": 0.10,
                "won": False,
                "expected_value": -5.0,
                "actual_value": -100.0,
                "closing_line_value": -1.5,
                "hours_before_start": 12.0,
                "start_time_utc": pd.Timestamp("2026-01-01", tz="UTC"),
            }
        ]
    )
    rules = [
        {
            "id": "nba_totals_logistic_over_edge_000",
            "status": "candidate",
            "kind": "candidate",
            "market": "totals",
            "league": "NBA",
            "model_type": "logistic",
            "prediction_variant": "market_residual_shrink_050_sigmoid",
            "side": "over",
            "min_edge": 0.0,
            "validation": "rolling_origin",
            "residual": True,
            "shrinkage": 0.5,
            "calibration": "sigmoid",
        }
    ]

    ranked = rank_predeclared_rules(
        bets,
        rules,
        StrictGate(
            min_bets_narrow=2,
            min_roi=0.05,
            min_bootstrap_roi_low=0.0,
            min_avg_clv=0.0,
            min_clv_win_rate=0.5,
            bootstrap_samples=20,
            require_brier_beats_market=True,
            require_roi_beats_market_baseline=False,
        ),
    )

    failures = set(ranked[0]["strict_gate_failures"])
    assert "sample_size_below_2" in failures
    assert "roi_below_0.05" in failures
    assert "bootstrap_roi_low_not_above_0.0" in failures
    assert "brier_does_not_beat_market" in failures
    assert "avg_clv_not_above_0.0" in failures
    assert "clv_win_rate_not_above_0.5" in failures


def test_nba_totals_clv_sweep_cli_writes_research_only_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "benchmark.yml"
    output_path = tmp_path / "nba_totals_clv_sweep_latest.json"
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

    import src.models.nba_totals_clv_sweep as sweep

    monkeypatch.setattr(
        sweep,
        "load_training_frame",
        lambda *args, **kwargs: (_synthetic_training_frame(), _contract()),
    )

    exit_code = main(
        [
            "--db",
            str(tmp_path / "missing.sqlite"),
            "--benchmark-config",
            str(config_path),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["dataset_rows"] == 12
    assert payload["sweep"]["feature_variants"] == ["current_features", "no_availability_features"]
    assert payload["sweep"]["price_modes"] == ["best_book", "selected_book"]
    assert payload["sweep"]["timing_buckets"] == list(TIMING_BUCKETS)
    assert {row["feature_variant"] for row in payload["candidate_rankings"]} == {
        "current_features",
        "no_availability_features",
    }
    assert {row["price_mode"] for row in payload["candidate_rankings"]} == {
        "best_book",
        "selected_book",
    }
    assert payload["recommendation"] in {
        "do_not_promote",
        "add_to_strict_benchmark_for_monitoring",
        "candidate_ready_for_manual_review",
    }
    assert not (tmp_path / "latest_publishable_bets.json").exists()
