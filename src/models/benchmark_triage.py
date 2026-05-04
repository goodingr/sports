"""Triage summaries for betting benchmark candidate failures."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Mapping

FAILURE_REASON_LABELS = {
    "sample_size": "Sample size below strict gate",
    "roi": "ROI below strict gate",
    "bootstrap_roi_low": "Bootstrap ROI lower bound below strict gate",
    "brier_vs_market": "Brier score does not beat no-vig market",
    "avg_clv": "Average CLV below strict gate",
    "clv_win_rate": "CLV win rate below strict gate",
    "missing_clv": "No closing-line value observations",
    "roi_vs_market_baseline": "ROI does not beat market-only baseline",
    "baseline_not_publishable": "Baseline rule is not publishable",
    "unknown": "Unclassified strict-gate failure",
}

DIAGNOSTIC_REASON_LABELS = {
    "stale_odds_filtered": "Odds timing filter removed candidate bets",
    "missing_clv_values": "Candidate bets missing CLV values",
}


def normalize_failure_reason(failure_id: str) -> str:
    """Collapse threshold-specific gate ids into stable triage categories."""
    if failure_id.startswith("sample_size_below_"):
        return "sample_size"
    if failure_id.startswith("roi_below_"):
        return "roi"
    if failure_id.startswith("bootstrap_roi_low_not_above_"):
        return "bootstrap_roi_low"
    if failure_id == "brier_does_not_beat_market":
        return "brier_vs_market"
    if failure_id.startswith("avg_clv_not_above_"):
        return "avg_clv"
    if failure_id.startswith("clv_win_rate_not_above_"):
        return "clv_win_rate"
    if failure_id == "missing_clv":
        return "missing_clv"
    if failure_id == "roi_does_not_beat_market_baseline":
        return "roi_vs_market_baseline"
    if failure_id == "baseline_rules_are_not_publishable":
        return "baseline_not_publishable"
    return "unknown"


def build_benchmark_triage_report(
    ranked_rules: Iterable[Mapping[str, Any]],
    *,
    strict_gate: Mapping[str, Any],
    generated_at: str | None = None,
    source_report_path: Path | str | None = None,
    top_n: int = 25,
) -> dict[str, Any]:
    """Build a compact triage report from ranked benchmark rule results.

    This function is intentionally read-only over already evaluated candidates.
    It does not re-score, relax, or promote rules.
    """
    generated = generated_at or datetime.now(timezone.utc).isoformat()
    candidates = [dict(row) for row in ranked_rules if row.get("kind") == "candidate"]
    passing = [row for row in candidates if _passes_strict_gate(row)]
    failing = [row for row in candidates if not _passes_strict_gate(row)]

    failure_counts = _failure_counts(candidates, include_model=True)
    failure_counts_by_league_market = _failure_counts(candidates, include_model=False)
    failure_counts_by_reason = _reason_counts(candidates)
    filter_diagnostics = _filter_diagnostics(candidates)

    closest = []
    for rank, row in enumerate(
        sorted(failing, key=lambda item: _closest_sort_key(item, strict_gate))[:top_n],
        start=1,
    ):
        payload = _closest_to_pass_payload(row, strict_gate)
        payload["triage_rank"] = rank
        closest.append(payload)

    return {
        "generated_at": generated,
        "source_benchmark_report": str(source_report_path) if source_report_path else None,
        "strict_gate": dict(strict_gate),
        "candidate_count": len(candidates),
        "passing_candidate_count": len(passing),
        "failing_candidate_count": len(failing),
        "failure_counts": failure_counts,
        "failure_counts_by_league_market": failure_counts_by_league_market,
        "failure_counts_by_reason": failure_counts_by_reason,
        "bottlenecks": _bottlenecks(candidates, failure_counts_by_reason, filter_diagnostics),
        "filter_diagnostics": filter_diagnostics,
        "closest_to_pass": closest,
    }


def _passes_strict_gate(row: Mapping[str, Any]) -> bool:
    if _strict_failures(row):
        return False
    return bool(
        row.get("passes_strict_gate") or row.get("passes_launch_gate") or row.get("publishable")
    )


def _strict_failures(row: Mapping[str, Any]) -> list[str]:
    failures = row.get("strict_gate_failures") or row.get("launch_gate_failures") or []
    return [str(failure) for failure in failures]


def _failure_reasons(row: Mapping[str, Any]) -> list[str]:
    return list(dict.fromkeys(normalize_failure_reason(item) for item in _strict_failures(row)))


def _failure_counts(
    candidates: Iterable[Mapping[str, Any]],
    *,
    include_model: bool,
) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, ...]] = Counter()
    for row in candidates:
        for failure_id in _strict_failures(row):
            reason = normalize_failure_reason(failure_id)
            if include_model:
                key = (
                    _text(row.get("league")),
                    _text(row.get("market")),
                    _text(row.get("model_type")),
                    reason,
                    failure_id,
                )
            else:
                key = (
                    _text(row.get("league")),
                    _text(row.get("market")),
                    reason,
                    failure_id,
                )
            counts[key] += 1

    rows: list[dict[str, Any]] = []
    for key, count in sorted(counts.items()):
        if include_model:
            league, market, model_type, reason, failure_id = key
            row = {
                "league": league,
                "market": market,
                "model_type": model_type,
                "failure_reason": reason,
                "failure_id": failure_id,
                "label": FAILURE_REASON_LABELS.get(reason, FAILURE_REASON_LABELS["unknown"]),
                "candidate_count": count,
            }
        else:
            league, market, reason, failure_id = key
            row = {
                "league": league,
                "market": market,
                "failure_reason": reason,
                "failure_id": failure_id,
                "label": FAILURE_REASON_LABELS.get(reason, FAILURE_REASON_LABELS["unknown"]),
                "candidate_count": count,
            }
        rows.append(row)
    return rows


def _reason_counts(candidates: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    failure_ids_by_reason: dict[str, set[str]] = defaultdict(set)
    for row in candidates:
        for failure_id in _strict_failures(row):
            reason = normalize_failure_reason(failure_id)
            counts[reason] += 1
            failure_ids_by_reason[reason].add(failure_id)
    return [
        {
            "failure_reason": reason,
            "label": FAILURE_REASON_LABELS.get(reason, FAILURE_REASON_LABELS["unknown"]),
            "candidate_count": count,
            "failure_ids": sorted(failure_ids_by_reason[reason]),
        }
        for reason, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _filter_diagnostics(candidates: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in candidates:
        league = _text(row.get("league"))
        market = _text(row.get("market"))
        model = _text(row.get("model_type"))
        stale_excluded = _stale_odds_excluded(row)
        if stale_excluded > 0:
            _increment_diagnostic(
                grouped,
                (league, market, model, "stale_odds_filtered"),
                stale_excluded,
            )
        metrics = _candidate_metrics(row, {})
        missing_clv = int(metrics["missing_clv_count"] or 0)
        if missing_clv > 0:
            _increment_diagnostic(
                grouped,
                (league, market, model, "missing_clv_values"),
                missing_clv,
            )
    return [
        {
            "league": league,
            "market": market,
            "model_type": model,
            "diagnostic_reason": reason,
            "label": DIAGNOSTIC_REASON_LABELS[reason],
            "candidate_count": values["candidate_count"],
            "affected_bet_count": values["affected_bet_count"],
        }
        for (league, market, model, reason), values in sorted(grouped.items())
    ]


def _increment_diagnostic(
    grouped: dict[tuple[str, str, str, str], dict[str, Any]],
    key: tuple[str, str, str, str],
    affected_bets: int,
) -> None:
    if key not in grouped:
        grouped[key] = {"candidate_count": 0, "affected_bet_count": 0}
    grouped[key]["candidate_count"] += 1
    grouped[key]["affected_bet_count"] += int(affected_bets)


def _bottlenecks(
    candidates: list[Mapping[str, Any]],
    failure_counts_by_reason: list[Mapping[str, Any]],
    filter_diagnostics: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidate_count = len(candidates)
    for item in failure_counts_by_reason:
        reason = str(item["failure_reason"])
        affected = [row for row in candidates if reason in _failure_reasons(row)]
        row: dict[str, Any] = {
            "reason": reason,
            "label": item["label"],
            "candidate_count": int(item["candidate_count"]),
            "share_of_candidates": _share(int(item["candidate_count"]), candidate_count),
            "strict_gate_failure": True,
        }
        row.update(_shortfall_stats(reason, affected))
        rows.append(row)

    diagnostics_by_reason: dict[str, dict[str, int]] = defaultdict(
        lambda: {"candidate_count": 0, "affected_bet_count": 0}
    )
    for diagnostic in filter_diagnostics:
        reason = str(diagnostic["diagnostic_reason"])
        diagnostics_by_reason[reason]["candidate_count"] += int(diagnostic["candidate_count"])
        diagnostics_by_reason[reason]["affected_bet_count"] += int(diagnostic["affected_bet_count"])
    for reason, values in diagnostics_by_reason.items():
        rows.append(
            {
                "reason": reason,
                "label": DIAGNOSTIC_REASON_LABELS[reason],
                "candidate_count": values["candidate_count"],
                "share_of_candidates": _share(values["candidate_count"], candidate_count),
                "affected_bet_count": values["affected_bet_count"],
                "strict_gate_failure": False,
            }
        )

    rows.sort(
        key=lambda row: (
            int(row.get("candidate_count") or 0),
            int(row.get("affected_bet_count") or 0),
            str(row.get("reason")),
        ),
        reverse=True,
    )
    return rows


def _shortfall_stats(reason: str, rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    key_by_reason = {
        "sample_size": "sample_size_shortfall",
        "roi": "roi_shortfall",
        "bootstrap_roi_low": "bootstrap_roi_low_shortfall",
        "brier_vs_market": "brier_delta_vs_market",
        "avg_clv": "avg_clv_shortfall",
        "clv_win_rate": "clv_win_rate_shortfall",
        "missing_clv": "missing_clv_count",
        "roi_vs_market_baseline": "roi_vs_market_baseline_shortfall",
    }
    metric_key = key_by_reason.get(reason)
    if metric_key is None:
        return {}
    values = [
        float(metrics[metric_key])
        for metrics in (_candidate_metrics(row, {}) for row in rows)
        if metrics.get(metric_key) is not None
    ]
    if not values:
        return {}
    return {
        f"avg_{metric_key}": float(mean(values)),
        f"median_{metric_key}": float(median(values)),
        f"max_{metric_key}": float(max(values)),
    }


def _closest_to_pass_payload(row: Mapping[str, Any], gate: Mapping[str, Any]) -> dict[str, Any]:
    failures = _strict_failures(row)
    return {
        "triage_rank": None,
        "rule_rank": row.get("rank"),
        "rule_id": row.get("rule_id"),
        "league": row.get("league"),
        "market": row.get("market"),
        "model_type": row.get("model_type"),
        "prediction_variant": row.get("prediction_variant"),
        "side": row.get("side"),
        "min_edge": row.get("min_edge"),
        "triage_score": _triage_score(row, gate),
        "remaining_failed_gates": failures,
        "failure_reasons": _failure_reasons(row),
        "metrics": _candidate_metrics(row, gate),
    }


def _closest_sort_key(row: Mapping[str, Any], gate: Mapping[str, Any]) -> tuple[Any, ...]:
    metrics = _candidate_metrics(row, gate)
    return (
        len(_strict_failures(row)),
        _triage_score(row, gate),
        -float(metrics.get("bets") or 0),
        -float(_float(metrics.get("roi"), -999.0)),
        -float(_float(metrics.get("bootstrap_roi_low"), -999.0)),
        float(_float(metrics.get("brier_delta_vs_market"), 999.0)),
        str(row.get("rule_id")),
    )


def _triage_score(row: Mapping[str, Any], gate: Mapping[str, Any]) -> float:
    failures = _strict_failures(row)
    metrics = _candidate_metrics(row, gate)
    shortfall = 0.0
    for reason in _failure_reasons(row):
        shortfall += _normalized_shortfall(reason, metrics)
    return round((len(failures) * 100.0) + shortfall, 6)


def _normalized_shortfall(reason: str, metrics: Mapping[str, Any]) -> float:
    if reason == "sample_size":
        required = float(metrics.get("required_min_bets") or 1.0)
        return float(metrics.get("sample_size_shortfall") or 0.0) / max(required, 1.0)
    if reason == "roi":
        return _shortfall_or_default(metrics.get("roi_shortfall"))
    if reason == "bootstrap_roi_low":
        return _shortfall_or_default(metrics.get("bootstrap_roi_low_shortfall"))
    if reason == "brier_vs_market":
        value = metrics.get("brier_delta_vs_market")
        return 1.0 if value is None else max(float(value), 0.0)
    if reason == "avg_clv":
        return _shortfall_or_default(metrics.get("avg_clv_shortfall"))
    if reason == "clv_win_rate":
        return _shortfall_or_default(metrics.get("clv_win_rate_shortfall"))
    if reason == "missing_clv":
        return 1.0
    if reason == "roi_vs_market_baseline":
        return _shortfall_or_default(metrics.get("roi_vs_market_baseline_shortfall"))
    return 1.0


def _candidate_metrics(row: Mapping[str, Any], gate: Mapping[str, Any]) -> dict[str, Any]:
    bets = _int(row.get("bets", row.get("sample_size")), 0)
    required_min_bets = _required_min_bets(row, gate)
    roi = _float(row.get("roi"))
    bootstrap_roi_low = _float(row.get("bootstrap_roi_low"))
    brier_delta = _float(row.get("brier_delta_vs_market"))
    avg_clv = _float(row.get("avg_closing_line_value"))
    clv_win_rate = _float(row.get("closing_line_value_win_rate"))
    clv_count = _int(row.get("closing_line_value_count"), 0)
    baseline_roi = _baseline_market_roi(row)

    min_roi = _gate_float(gate, "min_roi", 0.05)
    min_bootstrap_roi_low = _gate_float(gate, "min_bootstrap_roi_low", 0.0)
    min_avg_clv = _gate_float(gate, "min_avg_clv", 0.0)
    min_clv_win_rate = _gate_float(gate, "min_clv_win_rate", 0.5)

    return {
        "bets": bets,
        "required_min_bets": required_min_bets,
        "sample_size_shortfall": max(required_min_bets - bets, 0),
        "roi": roi,
        "roi_threshold": min_roi,
        "roi_shortfall": None if roi is None else max(min_roi - roi, 0.0),
        "bootstrap_roi_low": bootstrap_roi_low,
        "bootstrap_roi_low_threshold": min_bootstrap_roi_low,
        "bootstrap_roi_low_shortfall": (
            None
            if bootstrap_roi_low is None
            else max(min_bootstrap_roi_low - bootstrap_roi_low, 0.0)
        ),
        "brier_score": _float(row.get("brier_score")),
        "market_brier_score": _float(row.get("market_brier_score")),
        "brier_delta_vs_market": brier_delta,
        "model_beats_market_brier": bool(row.get("model_beats_market_brier")),
        "avg_closing_line_value": avg_clv,
        "avg_clv_threshold": min_avg_clv,
        "avg_clv_shortfall": None if avg_clv is None else max(min_avg_clv - avg_clv, 0.0),
        "closing_line_value_win_rate": clv_win_rate,
        "clv_win_rate_threshold": min_clv_win_rate,
        "clv_win_rate_shortfall": (
            None if clv_win_rate is None else max(min_clv_win_rate - clv_win_rate, 0.0)
        ),
        "closing_line_value_count": clv_count,
        "missing_clv_count": max(bets - clv_count, 0),
        "clv_coverage": None if bets <= 0 else clv_count / bets,
        "stale_odds_excluded": _stale_odds_excluded(row),
        "roi_market_baseline": baseline_roi,
        "roi_vs_market_baseline_shortfall": (
            None if roi is None or baseline_roi is None else max(baseline_roi - roi, 0.0)
        ),
    }


def _required_min_bets(row: Mapping[str, Any], gate: Mapping[str, Any]) -> int:
    explicit = _int(row.get("required_min_bets"), 0)
    if explicit > 0:
        return explicit
    if str(row.get("league", "")).upper() == "ALL":
        return _gate_int(gate, "min_bets_multi_league", 300)
    return _gate_int(gate, "min_bets_narrow", 150)


def _stale_odds_excluded(row: Mapping[str, Any]) -> int:
    timing = row.get("odds_timing_filter")
    if not isinstance(timing, Mapping):
        return 0
    return _int(timing.get("stale_odds_excluded"), 0)


def _baseline_market_roi(row: Mapping[str, Any]) -> float | None:
    comparison = row.get("market_baseline_comparison")
    if not isinstance(comparison, Mapping):
        return None
    market_only = comparison.get("market_only")
    if not isinstance(market_only, Mapping):
        return None
    return _float(market_only.get("roi"))


def _gate_float(gate: Mapping[str, Any], key: str, default: float) -> float:
    number = _float(gate.get(key))
    return default if number is None else number


def _gate_int(gate: Mapping[str, Any], key: str, default: int) -> int:
    return _int(gate.get(key), default)


def _text(value: Any) -> str:
    if value is None:
        return "unknown"
    return str(value)


def _float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number != number:
        return default
    return number


def _int(value: Any, default: int = 0) -> int:
    number = _float(value)
    return default if number is None else int(number)


def _share(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return float(count / total)


def _shortfall_or_default(value: Any) -> float:
    number = _float(value)
    return 1.0 if number is None else max(float(number), 0.0)
