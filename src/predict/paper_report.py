"""Summarize the internal prospective paper-betting ledger."""

from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np

from src.predict.paper_coverage import capture_coverage

GRADING_GRACE = timedelta(days=7)
BOOTSTRAP_SAMPLES = 3000
BOOTSTRAP_SEED = 42
MIN_CLV_WIN_RATE = 0.5


def _as_utc(value: str | datetime) -> datetime:
    timestamp = datetime.fromisoformat(value) if isinstance(value, str) else value
    if timestamp.utcoffset() is None:
        raise ValueError("Paper report timestamps must include a time zone")
    return timestamp.astimezone(timezone.utc)


def _bootstrap_roi_by_date(
    daily_profits: dict[date, list[float]],
) -> dict[str, float] | None:
    if len(daily_profits) < 2:
        return None
    days = [daily_profits[day] for day in sorted(daily_profits)]
    daily_profit = np.array([math.fsum(profits) for profits in days])
    daily_stake = np.array([len(profits) for profits in days])
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    sampled_days = rng.integers(len(days), size=(BOOTSTRAP_SAMPLES, len(days)))
    rois = daily_profit[sampled_days].sum(axis=1) / daily_stake[sampled_days].sum(axis=1)
    low, median, high = np.percentile(rois, [2.5, 50.0, 97.5])
    return {"low": float(low), "median": float(median), "high": float(high)}


def _is_probability(value: float | None) -> bool:
    return value is not None and 0.0 <= value <= 1.0


def _trial_metrics(
    conn: sqlite3.Connection,
    trial_id: str,
    review_at: datetime,
    evidence_cutoff: datetime,
) -> dict[str, Any]:
    decisions = conn.execute(
        """
        SELECT d.start_time_utc, d.captured_at, d.model_probability, d.market_probability,
               s.settled_at, s.outcome, s.profit_units, s.clv_points
        FROM paper_decisions d
        LEFT JOIN paper_settlements s ON s.trial_id = d.trial_id AND s.game_id = d.game_id
        WHERE d.trial_id = ?
        ORDER BY d.game_id
        """,
        (trial_id,),
    )
    counts: Counter[str] = Counter()
    daily_profits: dict[date, list[float]] = defaultdict(list)
    model_errors = []
    market_errors = []
    clv_values = []
    for row in decisions:
        captured_at = _as_utc(row["captured_at"])
        if captured_at >= review_at or captured_at > evidence_cutoff:
            continue
        if row["settled_at"] is None or _as_utc(row["settled_at"]) > evidence_cutoff:
            counts["pending"] += 1
            continue

        outcome = row["outcome"]
        counts[outcome] += 1
        if outcome == "void":
            continue

        profit = row["profit_units"]
        if profit is not None and math.isfinite(profit):
            day = _as_utc(row["start_time_utc"]).date()
            daily_profits[day].append(profit)

        clv = row["clv_points"]
        if clv is not None and math.isfinite(clv):
            clv_values.append(clv)

        model = row["model_probability"]
        market = row["market_probability"]
        if outcome != "push" and _is_probability(model) and _is_probability(market):
            won = float(outcome == "win")
            model_errors.append((model - won) ** 2)
            market_errors.append((market - won) ** 2)

    graded = counts["win"] + counts["loss"] + counts["push"]
    profit_count = sum(len(profits) for profits in daily_profits.values())
    profit_complete = graded > 0 and profit_count == graded
    profit_units = math.fsum(math.fsum(profits) for profits in daily_profits.values())
    return {
        "decisions": sum(counts.values()),
        "pending": counts["pending"],
        "voids": counts["void"],
        "graded_decisions": graded,
        "wins": counts["win"],
        "losses": counts["loss"],
        "pushes": counts["push"],
        "profit_count": profit_count,
        "profit_units": profit_units if profit_complete or not graded else None,
        "roi": profit_units / graded if profit_complete else None,
        "bootstrap_utc_dates": len(daily_profits),
        "bootstrap_roi_interval": (
            _bootstrap_roi_by_date(daily_profits) if profit_complete else None
        ),
        "brier_count": len(model_errors),
        "model_brier": math.fsum(model_errors) / len(model_errors) if model_errors else None,
        "market_brier": math.fsum(market_errors) / len(market_errors) if market_errors else None,
        "clv_count": len(clv_values),
        "clv_coverage": len(clv_values) / graded if graded else None,
        "avg_clv_points": math.fsum(clv_values) / len(clv_values) if clv_values else None,
        "clv_win_rate": (
            sum(clv > 0 for clv in clv_values) / len(clv_values) if clv_values else None
        ),
    }


def _review_failures(metrics: dict[str, Any], spec: dict[str, Any]) -> list[str]:
    failures = []
    if metrics["pending"]:
        failures.append("unresolved_decisions")
    if metrics["graded_decisions"] < spec["min_decisions"]:
        failures.append("insufficient_graded_decisions")
    if metrics["profit_count"] != metrics["graded_decisions"]:
        failures.append("incomplete_profit")
    if metrics["roi"] is None or metrics["roi"] < spec["min_roi"]:
        failures.append("roi_below_minimum")
    interval = metrics["bootstrap_roi_interval"]
    if interval is None or interval["low"] <= 0.0:
        failures.append("bootstrap_roi_lower_bound_not_positive")
    model_brier = metrics["model_brier"]
    if (
        metrics["brier_count"] != metrics["wins"] + metrics["losses"]
        or model_brier is None
        or model_brier >= metrics["market_brier"]
    ):
        failures.append("brier_does_not_beat_market")
    coverage = metrics["clv_coverage"]
    if coverage is None or coverage < spec["min_clv_coverage"]:
        failures.append("clv_coverage_below_minimum")
    average_clv = metrics["avg_clv_points"]
    if average_clv is None or average_clv <= 0.0:
        failures.append("average_clv_not_positive")
    clv_win_rate = metrics["clv_win_rate"]
    if clv_win_rate is None or clv_win_rate <= MIN_CLV_WIN_RATE:
        failures.append("clv_win_rate_not_above_half")
    return failures


def build_paper_report(conn: sqlite3.Connection, observed_at: datetime) -> dict[str, Any]:
    """Build a report using the caller's transaction and an explicit observation time."""
    observed_at = _as_utc(observed_at)
    trials = []
    for row in conn.execute(
        "SELECT trial_id, registered_at, specification FROM paper_trials ORDER BY trial_id"
    ):
        registered_at = _as_utc(row["registered_at"])
        if registered_at > observed_at:
            continue
        spec = json.loads(row["specification"])
        review_at = _as_utc(spec["review_at"])
        final_review_at = review_at + GRADING_GRACE
        evidence_cutoff = min(observed_at, final_review_at)
        metrics = _trial_metrics(conn, row["trial_id"], review_at, evidence_cutoff)
        verdict = None
        failures = None
        if observed_at < review_at:
            status = "collecting"
        elif observed_at < final_review_at:
            status = "grading"
        else:
            status = "complete"
            failures = _review_failures(metrics, spec)
            verdict = "fail" if failures else "pass"
        trials.append(
            {
                "trial_id": row["trial_id"],
                "registered_at": registered_at.isoformat(),
                "specification": spec,
                "review_at": review_at.isoformat(),
                "final_review_at": final_review_at.isoformat(),
                "evidence_cutoff": evidence_cutoff.isoformat(),
                "review_status": status,
                "review_verdict": verdict,
                "review_failures": failures,
                "capture_coverage": capture_coverage(
                    conn,
                    row["trial_id"],
                    registered_at,
                    review_at,
                    observed_at,
                    min_lead_minutes=spec.get("min_minutes_before_start", 15),
                    max_lead_hours=spec.get("max_hours_before_start", 12),
                ),
                "metrics": metrics,
            }
        )
    return {
        "schema_version": 1,
        "observed_at": observed_at.isoformat(),
        "bootstrap": {
            "method": "utc_game_date_cluster",
            "samples": BOOTSTRAP_SAMPLES,
            "seed": BOOTSTRAP_SEED,
            "confidence_level": 0.95,
        },
        "trials": trials,
    }
