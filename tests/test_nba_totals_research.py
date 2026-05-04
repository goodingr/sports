from __future__ import annotations

import json
from pathlib import Path

from src.models.nba_totals_research import build_nba_totals_research_report, main


def _candidate(
    *,
    rule_id: str,
    rank: int,
    roi: float,
    avg_clv: float,
    clv_win_rate: float,
    failures: list[str],
    league: str = "NBA",
    market: str = "totals",
    side: str = "under",
) -> dict:
    return {
        "rank": rank,
        "rule_id": rule_id,
        "kind": "candidate",
        "league": league,
        "market": market,
        "model_type": "random_forest",
        "prediction_variant": "market_residual_shrink_075_sigmoid",
        "side": side,
        "min_edge": 0.04,
        "bets": 120,
        "roi": roi,
        "bootstrap_roi_low": -0.04,
        "brier_delta_vs_market": -0.01,
        "model_beats_market_brier": True,
        "avg_closing_line_value": avg_clv,
        "closing_line_value_win_rate": clv_win_rate,
        "closing_line_value_count": 118,
        "passes_strict_gate": False,
        "strict_gate_failures": failures,
        "clv_slices": [
            {
                "book": "DraftKings",
                "price_source": "best_book",
                "hours_bucket": "1-6h",
                "bets": 50,
                "roi": roi,
                "avg_closing_line_value": avg_clv,
                "closing_line_value_win_rate": clv_win_rate,
            }
        ],
    }


def _benchmark() -> dict:
    return {
        "strict_gate": {"min_bets_narrow": 150},
        "dataset_counts": {"totals": {"leagues": {"NBA": 665}}},
        "candidate_rule_rankings": [
            _candidate(
                rule_id="nba_totals_under",
                rank=2,
                roi=0.10,
                avg_clv=-0.2,
                clv_win_rate=0.1,
                failures=["avg_clv_not_above_0.0", "clv_win_rate_not_above_0.5"],
            ),
            _candidate(
                rule_id="nba_totals_over",
                rank=1,
                roi=-0.02,
                avg_clv=0.1,
                clv_win_rate=0.6,
                failures=["roi_below_0.05"],
                side="over",
            ),
            _candidate(
                rule_id="nhl_totals_ignore",
                rank=1,
                roi=0.5,
                avg_clv=1.0,
                clv_win_rate=1.0,
                failures=[],
                league="NHL",
            ),
        ],
    }


def _sweep() -> dict:
    return {
        "dataset_rows": 12,
        "feature_variant_column_counts": {
            "current_features": 7,
            "no_availability_features": 5,
        },
        "availability_feature_columns": ["home_injuries_out", "away_injuries_out"],
        "recommendation": "add_to_strict_benchmark_for_monitoring",
        "candidate_rankings": [
            {
                "rank": 1,
                "rule_id": "best_positive_clv",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "over",
                "min_edge": 0.02,
                "feature_variant": "current_features",
                "price_mode": "best_book",
                "timing_bucket": "1-6h",
                "bets": 20,
                "roi": 0.05,
                "bootstrap_roi_low": -0.01,
                "brier_delta_vs_market": -0.02,
                "avg_clv": 0.2,
                "clv_win_rate": 0.6,
                "clv_count": 20,
                "passes_strict_gate": False,
                "failed_gates": ["sample_size_below_150"],
            },
            {
                "rank": 2,
                "rule_id": "selected_negative_clv",
                "model_type": "logistic",
                "prediction_variant": "market_residual_shrink_050_sigmoid",
                "side": "under",
                "min_edge": 0.02,
                "feature_variant": "no_availability_features",
                "price_mode": "selected_book",
                "timing_bucket": "1-6h",
                "bets": 20,
                "roi": -0.05,
                "bootstrap_roi_low": -0.10,
                "brier_delta_vs_market": 0.01,
                "avg_clv": -0.1,
                "clv_win_rate": 0.4,
                "clv_count": 20,
                "passes_strict_gate": False,
                "failed_gates": ["avg_clv_not_above_0.0", "clv_win_rate_not_above_0.5"],
            },
        ],
    }


def test_nba_totals_research_summarizes_failures_and_slices() -> None:
    report = build_nba_totals_research_report(_benchmark(), source_benchmark_report="bench.json")

    assert report["dataset_rows"] == 665
    assert report["candidate_count"] == 2
    assert report["positive_roi_candidate_count"] == 1
    assert report["positive_avg_clv_candidate_count"] == 1
    assert report["positive_roi_negative_avg_clv_candidate_count"] == 1
    assert report["failure_counts"][0]["failure_reason"] in {"avg_clv", "clv_win_rate", "roi"}
    assert [row["rule_id"] for row in report["top_candidates_by_benchmark_rank"]] == [
        "nba_totals_over",
        "nba_totals_under",
    ]
    assert report["clv_slice_rankings"][0]["book"] == "DraftKings"
    assert report["availability_feature_comparison"]["status"] == "not_run_in_current_artifact"


def test_nba_totals_research_reads_clv_sweep_artifact() -> None:
    report = build_nba_totals_research_report(
        _benchmark(),
        source_benchmark_report="bench.json",
        sweep_report=_sweep(),
        source_sweep_report="sweep.json",
    )

    sweep_summary = report["clv_sweep_summary"]
    assert sweep_summary["source_sweep_report"] == "sweep.json"
    assert sweep_summary["best_positive_clv_slice"]["rule_id"] == "best_positive_clv"
    assert sweep_summary["best_candidate_by_strict_gate_distance"]["rule_id"] == "best_positive_clv"
    assert sweep_summary["price_mode_comparison"][0]["price_mode"] == "best_book"
    assert report["availability_feature_comparison"]["status"] == "completed"
    assert report["availability_feature_comparison"]["feature_variant_column_counts"][
        "no_availability_features"
    ] == 5
    assert sweep_summary["worth_adding_to_predeclared_benchmark"] is True


def test_nba_totals_research_cli_writes_report(tmp_path: Path) -> None:
    benchmark_path = tmp_path / "benchmark.json"
    output_path = tmp_path / "research.json"
    benchmark_path.write_text(json.dumps(_benchmark()), encoding="utf-8")

    exit_code = main(
        [
            "--benchmark",
            str(benchmark_path),
            "--output",
            str(output_path),
            "--top-n",
            "1",
        ]
    )

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["candidate_count"] == 2
    assert len(payload["top_candidates_by_benchmark_rank"]) == 1
