from __future__ import annotations

from typing import Any

from src.models.benchmark_triage import build_benchmark_triage_report

GATE = {
    "min_bets_narrow": 150,
    "min_bets_multi_league": 300,
    "min_roi": 0.05,
    "min_bootstrap_roi_low": 0.0,
    "min_avg_clv": 0.0,
    "min_clv_win_rate": 0.5,
}


def _candidate(
    rule_id: str,
    failures: list[str],
    *,
    league: str = "NBA",
    market: str = "totals",
    model_type: str = "logistic",
    bets: int = 150,
    required_min_bets: int = 150,
    roi: float | None = 0.08,
    bootstrap_roi_low: float | None = 0.02,
    brier_delta_vs_market: float | None = -0.01,
    avg_closing_line_value: float | None = 0.02,
    closing_line_value_win_rate: float | None = 0.6,
    closing_line_value_count: int | None = None,
    stale_odds_excluded: int = 0,
    market_baseline_roi: float | None = 0.01,
    rank: int = 1,
) -> dict[str, Any]:
    clv_count = bets if closing_line_value_count is None else closing_line_value_count
    return {
        "rank": rank,
        "kind": "candidate",
        "rule_id": rule_id,
        "league": league,
        "market": market,
        "model_type": model_type,
        "prediction_variant": "market_residual_shrink_050_sigmoid",
        "side": "over",
        "min_edge": 0.04,
        "bets": bets,
        "required_min_bets": required_min_bets,
        "roi": roi,
        "bootstrap_roi_low": bootstrap_roi_low,
        "brier_delta_vs_market": brier_delta_vs_market,
        "model_beats_market_brier": bool(
            brier_delta_vs_market is not None and brier_delta_vs_market < 0
        ),
        "avg_closing_line_value": avg_closing_line_value,
        "closing_line_value_win_rate": closing_line_value_win_rate,
        "closing_line_value_count": clv_count,
        "strict_gate_failures": failures,
        "passes_strict_gate": not failures,
        "odds_timing_filter": {"stale_odds_excluded": stale_odds_excluded},
        "market_baseline_comparison": {"market_only": {"roi": market_baseline_roi}},
    }


def _find(rows: list[dict[str, Any]], **criteria: Any) -> dict[str, Any]:
    for row in rows:
        if all(row.get(key) == value for key, value in criteria.items()):
            return row
    raise AssertionError(f"row not found: {criteria}")


def test_triage_groups_failures_by_league_market_model_and_reason() -> None:
    report = build_benchmark_triage_report(
        [
            _candidate(
                "nba_totals_close_sample",
                ["sample_size_below_150", "clv_win_rate_not_above_0.5"],
                bets=140,
                closing_line_value_win_rate=0.48,
                rank=1,
            ),
            _candidate(
                "nba_totals_stale_sample",
                ["sample_size_below_150"],
                bets=12,
                closing_line_value_count=10,
                stale_odds_excluded=4,
                rank=2,
            ),
            _candidate(
                "nhl_moneyline_no_clv",
                [
                    "roi_below_0.05",
                    "brier_does_not_beat_market",
                    "missing_clv",
                    "avg_clv_not_above_0.0",
                    "clv_win_rate_not_above_0.5",
                ],
                league="NHL",
                market="moneyline",
                model_type="random_forest",
                bets=200,
                roi=-0.01,
                brier_delta_vs_market=0.03,
                avg_closing_line_value=None,
                closing_line_value_win_rate=None,
                closing_line_value_count=0,
                rank=3,
            ),
            {
                "kind": "baseline",
                "league": "NBA",
                "market": "totals",
                "model_type": "market_only",
                "strict_gate_failures": ["baseline_rules_are_not_publishable"],
            },
        ],
        strict_gate=GATE,
    )

    grouped = _find(
        report["failure_counts"],
        league="NBA",
        market="totals",
        model_type="logistic",
        failure_reason="sample_size",
        failure_id="sample_size_below_150",
    )
    assert grouped["candidate_count"] == 2

    missing_clv = _find(
        report["failure_counts_by_league_market"],
        league="NHL",
        market="moneyline",
        failure_reason="missing_clv",
        failure_id="missing_clv",
    )
    assert missing_clv["candidate_count"] == 1

    stale = _find(
        report["filter_diagnostics"],
        league="NBA",
        market="totals",
        model_type="logistic",
        diagnostic_reason="stale_odds_filtered",
    )
    assert stale["candidate_count"] == 1
    assert stale["affected_bet_count"] == 4

    no_clv_values = _find(
        report["filter_diagnostics"],
        league="NHL",
        market="moneyline",
        model_type="random_forest",
        diagnostic_reason="missing_clv_values",
    )
    assert no_clv_values["affected_bet_count"] == 200

    stale_bottleneck = _find(report["bottlenecks"], reason="stale_odds_filtered")
    assert stale_bottleneck["strict_gate_failure"] is False


def test_triage_ranks_closest_to_pass_by_gate_count_then_shortfall() -> None:
    report = build_benchmark_triage_report(
        [
            _candidate(
                "needs_one_more_bet",
                ["sample_size_below_150"],
                bets=149,
                rank=3,
            ),
            _candidate(
                "needs_fifty_more_bets",
                ["sample_size_below_150"],
                bets=100,
                rank=1,
            ),
            _candidate(
                "needs_sample_and_roi",
                ["sample_size_below_150", "roi_below_0.05"],
                bets=149,
                roi=0.01,
                rank=2,
            ),
        ],
        strict_gate=GATE,
        top_n=3,
    )

    closest = report["closest_to_pass"]
    assert [row["rule_id"] for row in closest] == [
        "needs_one_more_bet",
        "needs_fifty_more_bets",
        "needs_sample_and_roi",
    ]
    assert closest[0]["triage_rank"] == 1
    assert closest[0]["metrics"]["sample_size_shortfall"] == 1
    assert closest[0]["remaining_failed_gates"] == ["sample_size_below_150"]


def test_triage_treats_zero_metrics_as_real_values() -> None:
    report = build_benchmark_triage_report(
        [
            _candidate(
                "missing_roi_sorts_after_zero",
                ["sample_size_below_150"],
                bets=149,
                roi=None,
            ),
            _candidate(
                "zero_roi_is_not_missing",
                ["sample_size_below_150"],
                bets=149,
                roi=0.0,
            ),
        ],
        strict_gate=GATE,
        top_n=2,
    )

    assert [row["rule_id"] for row in report["closest_to_pass"]] == [
        "zero_roi_is_not_missing",
        "missing_roi_sorts_after_zero",
    ]
