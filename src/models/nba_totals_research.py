"""Focused NBA totals research summaries from strict benchmark artifacts."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from src.models.benchmark_triage import normalize_failure_reason

DEFAULT_BENCHMARK_DIR = Path("reports/betting_benchmarks")
DEFAULT_TOP_N = 25


def _latest_benchmark_path(directory: Path = DEFAULT_BENCHMARK_DIR) -> Path:
    paths = sorted(directory.glob("betting_benchmark_*.json"))
    if not paths:
        raise FileNotFoundError(f"No benchmark reports found in {directory}")
    return paths[-1]


def _latest_sweep_path(directory: Path = DEFAULT_BENCHMARK_DIR) -> Path:
    paths = sorted(directory.glob("nba_totals_clv_sweep_*.json"))
    if not paths:
        raise FileNotFoundError(f"No NBA totals CLV sweep reports found in {directory}")
    return paths[-1]


def _number(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number != number:
        return default
    return number


def _candidate_rows(
    benchmark: dict[str, Any],
    *,
    league: str = "NBA",
    market: str = "totals",
) -> list[dict[str, Any]]:
    rows = benchmark.get("candidate_rule_rankings") or benchmark.get("ranked_rules") or []
    return [
        row
        for row in rows
        if str(row.get("league", "")).upper() == league.upper()
        and str(row.get("market", "")).lower() == market.lower()
        and str(row.get("kind", "candidate")).lower() == "candidate"
    ]


def _compact_candidate(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank": row.get("rank"),
        "rule_id": row.get("rule_id"),
        "model_type": row.get("model_type"),
        "prediction_variant": row.get("prediction_variant"),
        "side": row.get("side"),
        "min_edge": row.get("min_edge"),
        "bets": row.get("bets"),
        "roi": row.get("roi"),
        "bootstrap_roi_low": row.get("bootstrap_roi_low"),
        "brier_delta_vs_market": row.get("brier_delta_vs_market"),
        "model_beats_market_brier": row.get("model_beats_market_brier"),
        "avg_closing_line_value": row.get("avg_closing_line_value"),
        "closing_line_value_win_rate": row.get("closing_line_value_win_rate"),
        "closing_line_value_count": row.get("closing_line_value_count"),
        "passes_strict_gate": row.get("passes_strict_gate"),
        "strict_gate_failures": row.get("strict_gate_failures", []),
        "odds_timing_filter": row.get("odds_timing_filter"),
    }


def _metric(row: dict[str, Any], *keys: str, default: float | None = None) -> float | None:
    for key in keys:
        value = _number(row.get(key))
        if value is not None:
            return value
    return default


def _compact_sweep_candidate(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "rank": row.get("rank"),
        "rule_id": row.get("rule_id"),
        "model_type": row.get("model_type") or row.get("model"),
        "prediction_variant": row.get("prediction_variant") or row.get("variant"),
        "side": row.get("side"),
        "min_edge": row.get("min_edge") if row.get("min_edge") is not None else row.get("edge"),
        "feature_variant": row.get("feature_variant"),
        "price_mode": row.get("price_mode"),
        "timing_bucket": row.get("timing_bucket"),
        "bets": row.get("bets"),
        "roi": row.get("roi"),
        "bootstrap_roi_low": row.get("bootstrap_roi_low"),
        "brier_delta_vs_market": row.get("brier_delta_vs_market"),
        "avg_closing_line_value": _metric(row, "avg_closing_line_value", "avg_clv"),
        "closing_line_value_win_rate": _metric(
            row,
            "closing_line_value_win_rate",
            "clv_win_rate",
        ),
        "closing_line_value_count": row.get("closing_line_value_count") or row.get("clv_count"),
        "passes_strict_gate": row.get("passes_strict_gate"),
        "strict_gate_failures": row.get("strict_gate_failures") or row.get("failed_gates") or [],
    }


def _failure_counts(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    raw_ids: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        for failure_id in row.get("strict_gate_failures") or []:
            reason = normalize_failure_reason(str(failure_id))
            counts[reason] += 1
            raw_ids[reason].add(str(failure_id))
    return [
        {
            "failure_reason": reason,
            "candidate_count": count,
            "failure_ids": sorted(raw_ids[reason]),
        }
        for reason, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _sum_int(rows: Iterable[dict[str, Any]], key: str) -> int:
    return int(sum(int(_number(row.get(key), 0) or 0) for row in rows))


def _weighted_average(
    rows: Iterable[dict[str, Any]],
    value_key: str,
    *,
    weight_key: str = "bets",
) -> float | None:
    weighted_sum = 0.0
    total_weight = 0.0
    for row in rows:
        value = _number(row.get(value_key))
        weight = _number(row.get(weight_key), 0.0) or 0.0
        if value is None or weight <= 0:
            continue
        weighted_sum += value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return float(weighted_sum / total_weight)


def _weighted_metric_average(
    rows: Iterable[dict[str, Any]],
    *value_keys: str,
    weight_key: str = "bets",
) -> float | None:
    weighted_sum = 0.0
    total_weight = 0.0
    for row in rows:
        value = _metric(row, *value_keys)
        weight = _number(row.get(weight_key), 0.0) or 0.0
        if value is None or weight <= 0:
            continue
        weighted_sum += value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return float(weighted_sum / total_weight)


def _sweep_strict_distance_key(row: dict[str, Any]) -> tuple[float, ...]:
    failures = row.get("strict_gate_failures") or row.get("failed_gates") or []
    avg_clv = _metric(row, "avg_closing_line_value", "avg_clv", default=-999.0) or -999.0
    return (
        float(len(failures)),
        -float(bool(row.get("passes_strict_gate"))),
        -float(avg_clv > 0),
        -(_metric(row, "closing_line_value_win_rate", "clv_win_rate", default=-999.0) or -999.0),
        -(_number(row.get("bootstrap_roi_low"), -999.0) or -999.0),
        -float(bool(row.get("model_beats_market_brier"))),
        -(_number(row.get("roi"), -999.0) or -999.0),
        -float(int(row.get("bets") or 0)),
    )


def _dimension_comparison(
    rows: list[dict[str, Any]],
    dimension: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(dimension) or "unknown")].append(row)
    comparisons = []
    for value, group in grouped.items():
        best = min(group, key=_sweep_strict_distance_key)
        comparisons.append(
            {
                dimension: value,
                "candidate_count": len(group),
                "passing_candidate_count": sum(
                    1 for row in group if row.get("passes_strict_gate")
                ),
                "candidate_bet_observations": _sum_int(group, "bets"),
                "weighted_roi": _weighted_metric_average(group, "roi"),
                "weighted_avg_clv": _weighted_metric_average(
                    group,
                    "avg_closing_line_value",
                    "avg_clv",
                ),
                "weighted_clv_win_rate": _weighted_metric_average(
                    group,
                    "closing_line_value_win_rate",
                    "clv_win_rate",
                ),
                "best_candidate": _compact_sweep_candidate(best),
            }
        )
    comparisons.sort(
        key=lambda row: (
            row["weighted_avg_clv"] if row["weighted_avg_clv"] is not None else -999.0,
            row["weighted_clv_win_rate"] if row["weighted_clv_win_rate"] is not None else -999.0,
            row["candidate_bet_observations"],
        ),
        reverse=True,
    )
    return comparisons


def _sweep_summary(
    sweep_report: dict[str, Any],
    *,
    source_sweep_report: Path | str | None,
) -> dict[str, Any]:
    rows = list(sweep_report.get("candidate_rankings") or [])
    positive_clv = [
        row for row in rows if (_metric(row, "avg_closing_line_value", "avg_clv") or -999.0) > 0
    ]
    best_positive = (
        max(
            positive_clv,
            key=lambda row: (
                _metric(row, "avg_closing_line_value", "avg_clv", default=-999.0) or -999.0,
                _metric(
                    row,
                    "closing_line_value_win_rate",
                    "clv_win_rate",
                    default=-999.0,
                )
                or -999.0,
                _number(row.get("roi"), -999.0) or -999.0,
                int(row.get("bets") or 0),
            ),
        )
        if positive_clv
        else None
    )
    best_by_distance = min(rows, key=_sweep_strict_distance_key) if rows else None
    recommendation = sweep_report.get("recommendation") or "do_not_promote"
    return {
        "source_sweep_report": str(source_sweep_report) if source_sweep_report else None,
        "dataset_rows": sweep_report.get("dataset_rows"),
        "candidate_count": len(rows),
        "passing_candidate_count": sum(1 for row in rows if row.get("passes_strict_gate")),
        "positive_avg_clv_candidate_count": len(positive_clv),
        "best_candidate_by_strict_gate_distance": _compact_sweep_candidate(best_by_distance),
        "best_positive_clv_slice": _compact_sweep_candidate(best_positive),
        "price_mode_comparison": _dimension_comparison(rows, "price_mode"),
        "availability_feature_comparison": {
            "status": "completed",
            "feature_variant_column_counts": sweep_report.get("feature_variant_column_counts", {}),
            "availability_feature_columns": sweep_report.get("availability_feature_columns", []),
            "comparison": _dimension_comparison(rows, "feature_variant"),
        },
        "predeclared_grid_recommendation": recommendation,
        "worth_adding_to_predeclared_benchmark": recommendation
        in {"add_to_strict_benchmark_for_monitoring", "candidate_ready_for_manual_review"},
    }


def _cohort_rankings(rows: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, float], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row.get("model_type")),
                str(row.get("side")),
                float(_number(row.get("min_edge"), 0.0) or 0.0),
            )
        ].append(row)

    cohorts = []
    for (model_type, side, min_edge), group in grouped.items():
        best_ranked = min(group, key=lambda row: int(row.get("rank") or 999999))
        positive_roi = [row for row in group if (_number(row.get("roi")) or -999.0) > 0]
        positive_clv = [
            row for row in group if (_number(row.get("avg_closing_line_value")) or -999.0) > 0
        ]
        cohorts.append(
            {
                "model_type": model_type,
                "side": side,
                "min_edge": min_edge,
                "candidate_count": len(group),
                "best_rank": best_ranked.get("rank"),
                "best_rule_id": best_ranked.get("rule_id"),
                "max_bets": max(int(row.get("bets") or 0) for row in group),
                "max_roi": max((_number(row.get("roi"), -999.0) or -999.0) for row in group),
                "max_bootstrap_roi_low": max(
                    (_number(row.get("bootstrap_roi_low"), -999.0) or -999.0)
                    for row in group
                ),
                "min_brier_delta_vs_market": min(
                    (_number(row.get("brier_delta_vs_market"), 999.0) or 999.0)
                    for row in group
                ),
                "max_avg_clv": max(
                    (_number(row.get("avg_closing_line_value"), -999.0) or -999.0)
                    for row in group
                ),
                "max_clv_win_rate": max(
                    (_number(row.get("closing_line_value_win_rate"), -999.0) or -999.0)
                    for row in group
                ),
                "positive_roi_candidates": len(positive_roi),
                "positive_avg_clv_candidates": len(positive_clv),
            }
        )

    cohorts.sort(
        key=lambda row: (
            row["positive_avg_clv_candidates"],
            row["max_avg_clv"],
            row["max_clv_win_rate"],
            row["max_roi"],
            row["max_bets"],
        ),
        reverse=True,
    )
    return cohorts[:top_n]


def _slice_rankings(rows: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for clv_slice in row.get("clv_slices") or []:
            grouped[
                (
                    str(clv_slice.get("book") or "unknown"),
                    str(clv_slice.get("price_source") or "unknown"),
                    str(clv_slice.get("hours_bucket") or "unknown"),
                    str(row.get("side") or "unknown"),
                )
            ].append({**clv_slice, "rule_id": row.get("rule_id")})

    slices = []
    for (book, price_source, hours_bucket, side), group in grouped.items():
        best = max(
            group,
            key=lambda item: (
                _number(item.get("avg_closing_line_value"), -999.0) or -999.0,
                _number(item.get("roi"), -999.0) or -999.0,
            ),
        )
        slices.append(
            {
                "book": book,
                "price_source": price_source,
                "hours_bucket": hours_bucket,
                "side": side,
                "candidate_slice_count": len(group),
                "candidate_bet_observations": _sum_int(group, "bets"),
                "weighted_roi": _weighted_average(group, "roi"),
                "weighted_avg_clv": _weighted_average(group, "avg_closing_line_value"),
                "weighted_clv_win_rate": _weighted_average(
                    group,
                    "closing_line_value_win_rate",
                ),
                "best_rule_id": best.get("rule_id"),
                "best_avg_clv": best.get("avg_closing_line_value"),
                "best_roi": best.get("roi"),
            }
        )

    slices.sort(
        key=lambda row: (
            row["weighted_avg_clv"] if row["weighted_avg_clv"] is not None else -999.0,
            row["weighted_clv_win_rate"] if row["weighted_clv_win_rate"] is not None else -999.0,
            row["candidate_bet_observations"],
        ),
        reverse=True,
    )
    return slices[:top_n]


def build_nba_totals_research_report(
    benchmark: dict[str, Any],
    *,
    source_benchmark_report: Path | str | None = None,
    sweep_report: dict[str, Any] | None = None,
    source_sweep_report: Path | str | None = None,
    top_n: int = DEFAULT_TOP_N,
) -> dict[str, Any]:
    """Build a focused NBA totals report from the strict benchmark artifact."""
    rows = _candidate_rows(benchmark, league="NBA", market="totals")
    passing = [row for row in rows if row.get("passes_strict_gate")]
    positive_roi = [row for row in rows if (_number(row.get("roi")) or -999.0) > 0]
    model_beats_market = [row for row in rows if row.get("model_beats_market_brier")]
    positive_clv = [row for row in rows if (_number(row.get("avg_closing_line_value")) or -999.0) > 0]
    clv_win_rate_pass = [
        row for row in rows if (_number(row.get("closing_line_value_win_rate")) or -999.0) > 0.5
    ]
    positive_roi_negative_clv = [
        row
        for row in rows
        if (_number(row.get("roi")) or -999.0) > 0
        and (_number(row.get("avg_closing_line_value")) or 0.0) <= 0
    ]

    dataset_rows = (
        benchmark.get("dataset_counts", {})
        .get("totals", {})
        .get("leagues", {})
        .get("NBA")
    )
    recommendation = (
        "Do not promote NBA totals. The lane has enough settled rows for research, "
        "but every candidate fails CLV and bootstrap robustness. Prioritize book/timing "
        "and price-source research before model tuning."
    )
    if positive_clv and clv_win_rate_pass:
        recommendation = (
            "Investigate positive-CLV NBA totals slices, but keep paid publishing gated "
            "until a predeclared strict rule passes all launch metrics."
        )

    sweep_research = (
        _sweep_summary(sweep_report, source_sweep_report=source_sweep_report)
        if sweep_report is not None
        else None
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_benchmark_report": str(source_benchmark_report)
        if source_benchmark_report
        else None,
        "league": "NBA",
        "market": "totals",
        "strict_gate": benchmark.get("strict_gate"),
        "dataset_rows": dataset_rows,
        "candidate_count": len(rows),
        "passing_candidate_count": len(passing),
        "positive_roi_candidate_count": len(positive_roi),
        "model_beats_market_candidate_count": len(model_beats_market),
        "positive_avg_clv_candidate_count": len(positive_clv),
        "clv_win_rate_above_half_candidate_count": len(clv_win_rate_pass),
        "positive_roi_negative_avg_clv_candidate_count": len(positive_roi_negative_clv),
        "failure_counts": _failure_counts(rows),
        "top_candidates_by_benchmark_rank": [
            _compact_candidate(row)
            for row in sorted(rows, key=lambda item: int(item.get("rank") or 999999))[:top_n]
        ],
        "candidate_cohorts": _cohort_rankings(rows, top_n=top_n),
        "clv_slice_rankings": _slice_rankings(rows, top_n=top_n),
        "availability_feature_comparison": (
            sweep_research["availability_feature_comparison"]
            if sweep_research
            else {
                "status": "not_run_in_current_artifact",
                "note": (
                    "This benchmark uses the current feature contract only. A true "
                    "with-vs-without availability comparison requires a separate dual-feature "
                    "benchmark run."
                ),
            }
        ),
        "clv_sweep_summary": sweep_research,
        "recommendation": recommendation,
    }


def write_report(report: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return output_path


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize NBA totals research signals from a strict benchmark artifact."
    )
    parser.add_argument("--benchmark", type=Path, default=None)
    parser.add_argument("--sweep", type=Path, default=None)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    benchmark_path = args.benchmark or _latest_benchmark_path()
    benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
    sweep_path = args.sweep
    if sweep_path is None:
        try:
            sweep_path = _latest_sweep_path()
        except FileNotFoundError:
            sweep_path = None
    sweep_report = (
        json.loads(sweep_path.read_text(encoding="utf-8")) if sweep_path is not None else None
    )
    report = build_nba_totals_research_report(
        benchmark,
        source_benchmark_report=benchmark_path,
        sweep_report=sweep_report,
        source_sweep_report=sweep_path,
        top_n=args.top_n,
    )
    output_path = args.output
    if output_path is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = DEFAULT_BENCHMARK_DIR / f"nba_totals_research_{timestamp}.json"
    write_report(report, output_path)
    print("NBA totals research report")
    print(f"benchmark={benchmark_path}")
    print(f"output={output_path}")
    print(f"candidate_count={report['candidate_count']}")
    print(f"passing_candidate_count={report['passing_candidate_count']}")
    print(f"positive_avg_clv_candidate_count={report['positive_avg_clv_candidate_count']}")
    if sweep_path is not None:
        print(f"sweep={sweep_path}")
    print(f"recommendation={report['recommendation']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
