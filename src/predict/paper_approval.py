"""Bind subscriber approval to the completed, frozen paper-trial review."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import yaml

from src.models.prediction_quality import QualityGate, load_rules
from src.predict.paper_ledger import PaperTrial
from src.predict.paper_report import build_paper_report

APPROVAL_METADATA_FIELDS = frozenset({"status", "approved_at", "evidence_cutoff"})


def paper_reviews(db_path: Path, observed_at: datetime) -> dict[str, dict[str, Any]]:
    with closing(sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_trials'"
        ).fetchone():
            return {}
        report = build_paper_report(conn, observed_at)
        return {trial["trial_id"]: trial for trial in report["trials"]}


def rule_for_trial(review: dict[str, Any]) -> dict[str, Any]:
    trial = PaperTrial(**review["specification"])
    return {
        "id": trial.trial_id, "paper_trial_id": trial.trial_id,
        "validation": "prospective_paper", "market": trial.market, "league": trial.league,
        "model_type": trial.model_type, "model_sha256": trial.model_sha256,
        "side": "both", "min_edge": trial.min_edge,
        "book": trial.book, "odds_source": trial.odds_source,
        "max_quote_age_minutes": trial.max_quote_age_minutes,
        "min_minutes_before_start": trial.min_minutes_before_start,
        "max_hours_before_start": trial.max_hours_before_start,
        "same_snapshot_moneyline_inputs": True,
    }


def paper_rule_result(
    rule: dict[str, Any], review: dict[str, Any] | None, gate: QualityGate
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "rule_id": rule["id"], "passes_launch_gate": False,
        "evidence_source": "prospective_paper", "launch_gate_failures": [],
    }
    if review is None:
        result["launch_gate_failures"] = ["paper_trial_evidence_missing"]
        return result
    expected = rule_for_trial(review)
    unknown_fields = sorted(rule.keys() - expected.keys() - APPROVAL_METADATA_FIELDS)
    if unknown_fields:
        result["launch_gate_failures"] = ["paper_trial_unrecognized_rule_fields"]
        result["unrecognized_rule_fields"] = unknown_fields
        return result
    if any(rule.get(field) != value for field, value in expected.items()):
        result["launch_gate_failures"] = ["paper_trial_contract_mismatch"]
        return result
    metrics = review["metrics"]
    interval = metrics["bootstrap_roi_interval"] or {}
    result.update(
        bets=metrics["graded_decisions"],
        required_min_bets=max(gate.min_bets_narrow, review["specification"]["min_decisions"]),
        roi=metrics["roi"], profit_units=metrics["profit_units"],
        win_rate=(metrics["wins"] / (metrics["wins"] + metrics["losses"])
                  if metrics["wins"] + metrics["losses"] else None),
        bootstrap_roi_low=interval.get("low"), bootstrap_roi_median=interval.get("median"),
        bootstrap_roi_high=interval.get("high"),
        brier_score=metrics["model_brier"], market_brier_score=metrics["market_brier"],
        closing_line_value_count=metrics["clv_count"],
        closing_line_value_coverage=metrics["clv_coverage"],
        avg_closing_line_value=metrics["avg_clv_points"],
        closing_line_value_win_rate=metrics["clv_win_rate"],
        evidence_cutoff=review["evidence_cutoff"],
        model_beats_market_brier=(
            metrics["model_brier"] is not None and metrics["market_brier"] is not None
            and metrics["model_brier"] < metrics["market_brier"]
        ),
    )
    failures = result["launch_gate_failures"]
    if review["review_status"] != "complete" or review["review_verdict"] != "pass":
        failures.append("paper_trial_review_did_not_pass")
    if result["bets"] < result["required_min_bets"]:
        failures.append("insufficient_graded_decisions")
    checks = (
        ("roi", gate.min_roi, False),
        ("bootstrap_roi_low", gate.min_bootstrap_roi_low, True),
        ("closing_line_value_coverage", gate.min_clv_coverage, False),
        ("avg_closing_line_value", gate.min_avg_clv, True),
        ("closing_line_value_win_rate", gate.min_clv_win_rate, True),
    )
    for field, minimum, strict in checks:
        value = result[field]
        if value is None or (value <= minimum if strict else value < minimum):
            failures.append(f"{field}_below_launch_threshold")
    if not result["model_beats_market_brier"]:
        failures.append("brier_does_not_beat_market")
    result["passes_launch_gate"] = not failures
    return result


def promote_paper_trial(
    trial_id: str, db_path: Path, rules_path: Path, gate: QualityGate,
    *, observed_at: datetime | None = None,
) -> dict[str, Any]:
    now = observed_at or datetime.now(timezone.utc)
    review = paper_reviews(db_path, now).get(trial_id)
    if review is None:
        raise ValueError(f"Paper trial not found: {trial_id}")
    rule = rule_for_trial(review)
    result = paper_rule_result(rule, review, gate)
    if not result["passes_launch_gate"]:
        raise RuntimeError(f"Paper trial is not eligible: {result['launch_gate_failures']}")
    config = load_rules(rules_path)
    approved = config.get("approved_rules", [])
    existing = next((entry for entry in approved if entry.get("id") == trial_id), None)
    if existing is not None and any(existing.get(key) != value for key, value in rule.items()):
        raise ValueError("The rule ID already belongs to a different approval contract")
    config["approved_rules"] = [
        *[entry for entry in approved if entry.get("id") != trial_id],
        {**rule, "status": "approved", "approved_at": now.isoformat(),
         "evidence_cutoff": review["evidence_cutoff"]},
    ]
    for section in ("candidate_rules", "locked_candidate_rules"):
        config[section] = [entry for entry in config.get(section, []) if entry.get("id") != trial_id]
    rules_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with NamedTemporaryFile(mode="w", encoding="utf-8", dir=rules_path.parent, delete=False) as temp:
            temp_path = Path(temp.name)
            yaml.safe_dump(config, temp, sort_keys=False)
        temp_path.replace(rules_path)
    finally:
        if temp_path:
            temp_path.unlink(missing_ok=True)
    return {"rule_id": trial_id, "promoted": True, "quality_summary": result}
