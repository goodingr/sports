"""Publish subscriber-facing bets after a completed prospective trial passes."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterable, Optional

import pandas as pd
import yaml

from src.db.core import DB_PATH, connect, resolve_db_path
from src.models.prediction_quality import (
    DEFAULT_RELEASE_LEAGUES,
    _json_default,
    _quality_gate_from_config,
    configured_rules,
    filter_bets_for_rule,
    load_rules,
    no_vig_pair,
    rule_is_disabled,
)
from src.predict.paper_approval import paper_reviews, paper_rule_result, promote_paper_trial
from src.predict.published_ledger import publish_release

LOGGER = logging.getLogger(__name__)

DEFAULT_OUTPUT = Path("reports/publishable_bets/latest_publishable_bets.json")
DEFAULT_QUALITY_OUTPUT = Path("reports/publishable_bets/latest_quality_report.json")
BENCHMARK_METADATA_FIELDS = (
    "model_sha256",
    "prediction_variant",
    "feature_contract",
    "feature_variant",
    "price_mode",
    "timing_bucket",
)
LIVE_RULE_ID_FIELDS = ("rule_id", "source_rule_id", "published_rule_id")
QUOTE_METADATA_FIELDS = (
    "selected_totals_snapshot_id", "selected_totals_book_id",
    "selected_h2h_snapshot_id", "selected_h2h_book_id",
    "totals_model_sha256", "moneyline_model_sha256",
)

QUALITY_SUMMARY_FIELDS = (
    "evidence_source",
    "evidence_cutoff",
    "bets",
    "required_min_bets",
    "roi",
    "profit",
    "win_rate",
    "avg_edge",
    "bootstrap_roi_low",
    "bootstrap_roi_median",
    "bootstrap_roi_high",
    "avg_closing_line_value",
    "closing_line_value_win_rate",
    "closing_line_value_count",
    "model_beats_market_brier",
    "brier_score",
    "market_brier_score",
    "passes_launch_gate",
    "max_drawdown",
    "max_losing_streak",
)


def _read_sql(db_path: Path, query: str, params: Iterable[Any] = ()) -> pd.DataFrame:
    with sqlite3.connect(str(db_path)) as conn:
        return pd.read_sql_query(query, conn, params=list(params))


def _table_columns(db_path: Path, table_name: str) -> set[str]:
    with sqlite3.connect(str(db_path)) as conn:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def _normalize_now(now: Optional[str | pd.Timestamp]) -> pd.Timestamp:
    if now is None:
        return pd.Timestamp.utcnow()
    value = pd.Timestamp(now)
    if value.tzinfo is None:
        return value.tz_localize("UTC")
    return value.tz_convert("UTC")


def _latest_pre_game(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    for column in ("predicted_at", "start_time_utc"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce", format="mixed")
    frame = frame[frame["predicted_at"].notna() & frame["start_time_utc"].notna()].copy()
    frame = frame[frame["predicted_at"] < frame["start_time_utc"]].copy()
    identity = ["game_id", "model_type"]
    identity.extend(
        field for field in (*BENCHMARK_METADATA_FIELDS, *LIVE_RULE_ID_FIELDS) if field in frame
    )
    return frame.sort_values("predicted_at").drop_duplicates(identity, keep="last")


def load_current_prediction_output(
    db_path: Path = DB_PATH,
    *,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    now: Optional[str | pd.Timestamp] = None,
) -> pd.DataFrame:
    """Load current pre-game predictions that could become subscriber-facing bets."""
    prediction_columns = _table_columns(db_path, "predictions")
    optional_columns = [
        column
        for column in (
            "prediction_variant",
            "feature_variant",
            "price_mode",
            "timing_bucket",
            "feature_contract",
            *QUOTE_METADATA_FIELDS,
            *LIVE_RULE_ID_FIELDS,
        )
        if column in prediction_columns
    ]
    optional_select = "".join(f",\n            p.{column}" for column in optional_columns)
    query = """
        SELECT
            p.prediction_id,
            p.game_id,
            p.model_type,
            p.predicted_at,
            p.home_prob,
            p.away_prob,
            p.home_moneyline,
            p.away_moneyline,
            p.home_edge,
            p.away_edge,
            p.home_implied_prob,
            p.away_implied_prob,
            p.total_line,
            p.over_prob,
            p.under_prob,
            p.over_moneyline,
            p.under_moneyline,
            p.over_edge,
            p.under_edge,
            p.over_implied_prob,
            p.under_implied_prob,
            p.predicted_total_points
            {optional_select},
            g.start_time_utc,
            COALESCE(g.status, 'scheduled') AS game_status,
            s.league,
            ht.name AS home_team,
            at.name AS away_team
        FROM predictions p
        JOIN games g ON p.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        LEFT JOIN teams ht ON g.home_team_id = ht.team_id
        LEFT JOIN teams at ON g.away_team_id = at.team_id
    """.format(
        optional_select=optional_select
    )
    df = _read_sql(db_path, query)
    if df.empty:
        return df

    now_utc = _normalize_now(now)
    predicted = pd.to_datetime(df["predicted_at"], utc=True, errors="coerce", format="mixed")
    df = _latest_pre_game(df[predicted <= now_utc])
    if df.empty:
        return df

    df = df[df["start_time_utc"] > now_utc].copy()
    df = df[
        df["game_status"].astype(str).str.lower().isin({"scheduled", "pre"})
    ].copy()

    if leagues:
        allowed = {league.upper() for league in leagues}
        df = df[df["league"].astype(str).str.upper().isin(allowed)].copy()

    numeric_cols = [
        "home_prob",
        "away_prob",
        "home_moneyline",
        "away_moneyline",
        "home_edge",
        "away_edge",
        "home_implied_prob",
        "away_implied_prob",
        "total_line",
        "over_prob",
        "under_prob",
        "over_moneyline",
        "under_moneyline",
        "over_edge",
        "under_edge",
        "over_implied_prob",
        "under_implied_prob",
        "predicted_total_points",
    ]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _expand_current_totals(predictions: pd.DataFrame) -> pd.DataFrame:
    required = {"total_line", "over_prob", "under_prob", "over_moneyline", "under_moneyline"}
    if predictions.empty or not required <= set(predictions.columns):
        return pd.DataFrame()

    frame = predictions.dropna(subset=list(required)).copy()
    if frame.empty:
        return frame

    no_vig = frame.apply(
        lambda row: no_vig_pair(row["over_moneyline"], row["under_moneyline"]),
        axis=1,
        result_type="expand",
    )
    frame["over_no_vig_market_prob"] = no_vig[0]
    frame["under_no_vig_market_prob"] = no_vig[1]
    frame["quote_snapshot_id"] = frame.get("selected_totals_snapshot_id")
    frame["quote_book_id"] = frame.get("selected_totals_book_id")
    frame["model_sha256"] = frame.get("totals_model_sha256")

    rows: list[pd.DataFrame] = []
    for side in ("over", "under"):
        side_df = frame.copy()
        side_df["market"] = "totals"
        side_df["side"] = side
        side_df["predicted_prob"] = side_df[f"{side}_prob"]
        side_df["moneyline"] = side_df[f"{side}_moneyline"]
        side_df["no_vig_market_prob"] = side_df[f"{side}_no_vig_market_prob"]
        implied_column = f"{side}_implied_prob"
        side_df["implied_prob"] = side_df["no_vig_market_prob"].fillna(side_df[implied_column])
        side_df["edge"] = side_df["predicted_prob"] - side_df["no_vig_market_prob"]
        rows.append(side_df)
    return pd.concat(rows, ignore_index=True)


def _expand_current_moneyline(predictions: pd.DataFrame) -> pd.DataFrame:
    required = {"home_prob", "away_prob", "home_moneyline", "away_moneyline"}
    if predictions.empty or not required <= set(predictions.columns):
        return pd.DataFrame()

    frame = predictions.dropna(subset=list(required)).copy()
    if frame.empty:
        return frame

    no_vig = frame.apply(
        lambda row: no_vig_pair(row["home_moneyline"], row["away_moneyline"]),
        axis=1,
        result_type="expand",
    )
    frame["home_no_vig_market_prob"] = no_vig[0]
    frame["away_no_vig_market_prob"] = no_vig[1]
    frame["quote_snapshot_id"] = frame.get("selected_h2h_snapshot_id")
    frame["quote_book_id"] = frame.get("selected_h2h_book_id")
    frame["model_sha256"] = frame.get("moneyline_model_sha256")

    rows: list[pd.DataFrame] = []
    for side in ("home", "away"):
        side_df = frame.copy()
        side_df["market"] = "moneyline"
        side_df["side"] = side
        side_df["predicted_prob"] = side_df[f"{side}_prob"]
        side_df["moneyline"] = side_df[f"{side}_moneyline"]
        side_df["no_vig_market_prob"] = side_df[f"{side}_no_vig_market_prob"]
        implied_column = f"{side}_implied_prob"
        side_df["implied_prob"] = side_df["no_vig_market_prob"].fillna(side_df[implied_column])
        side_df["edge"] = side_df["predicted_prob"] - side_df["no_vig_market_prob"]
        rows.append(side_df)
    return pd.concat(rows, ignore_index=True)


def expand_current_predictions(predictions: pd.DataFrame) -> pd.DataFrame:
    """Expand current prediction rows into bet candidates using the quality schema."""
    frames = [_expand_current_totals(predictions), _expand_current_moneyline(predictions)]
    non_empty = [frame for frame in frames if not frame.empty]
    return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()


def _quality_summary(result: dict[str, Any]) -> dict[str, Any]:
    return {field: result.get(field) for field in QUALITY_SUMMARY_FIELDS}


def _safe_float(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _serialize_bet(
    row: pd.Series, rule: dict[str, Any], quality_result: dict[str, Any]
) -> dict[str, Any]:
    market_probability = row.get("no_vig_market_prob")
    if pd.isna(market_probability):
        market_probability = row.get("implied_prob")
    payload = {
        "rule_id": rule.get("id"),
        "market": row.get("market"),
        "league": row.get("league"),
        "game_id": row.get("game_id"),
        "start_time_utc": row.get("start_time_utc"),
        "predicted_at": row.get("predicted_at"),
        "quote_snapshot_id": row.get("quote_snapshot_id"),
        "quote_book_id": _safe_float(row.get("quote_book_id")),
        "input_h2h_snapshot_id": row.get("selected_h2h_snapshot_id"),
        "input_h2h_book_id": _safe_float(row.get("selected_h2h_book_id")),
        "input_home_moneyline": _safe_float(row.get("home_moneyline")),
        "input_away_moneyline": _safe_float(row.get("away_moneyline")),
        "home_team": row.get("home_team"),
        "away_team": row.get("away_team"),
        "side": row.get("side"),
        "odds": _safe_float(row.get("moneyline")),
        "moneyline": _safe_float(row.get("moneyline")),
        "total_line": _safe_float(row.get("total_line", row.get("line"))) if row.get("market") == "totals" else None,
        "predicted_total_points": _safe_float(row.get("predicted_total_points")) if row.get("market") == "totals" else None,
        "book": row.get("book"),
        "edge": _safe_float(row.get("edge")),
        "model_probability": _safe_float(row.get("predicted_prob")),
        "market_probability": _safe_float(market_probability),
        "quality_summary": _quality_summary(quality_result),
        "execution_contract": {
            field: rule[field] for field in (
                "book", "odds_source", "max_quote_age_minutes", "min_minutes_before_start",
                "max_hours_before_start", "min_edge",
                "same_snapshot_moneyline_inputs",
            ) if field in rule
        },
    }
    for field in BENCHMARK_METADATA_FIELDS:
        value = row.get(field, rule.get(field))
        if value is not None and not pd.isna(value):
            payload[field] = value
    return payload


def _required_live_metadata(rule: dict[str, Any]) -> list[str]:
    return [field for field in BENCHMARK_METADATA_FIELDS if rule.get(field) is not None]


def _live_metadata_mismatches(current_bets: pd.DataFrame, rule: dict[str, Any]) -> list[str]:
    mismatches = [field for field in _required_live_metadata(rule) if field not in current_bets]
    for field in _required_live_metadata(rule):
        if field not in current_bets:
            continue
        values = {str(value) for value in current_bets[field].dropna().unique()}
        if str(rule[field]) not in values:
            mismatches.append(field)
    return sorted(set(mismatches))


def _matching_live_rows(
    current_bets: pd.DataFrame, rule: dict[str, Any]
) -> tuple[pd.DataFrame, list[str], bool]:
    if not re.fullmatch(r"[0-9a-f]{64}", str(rule.get("model_sha256", ""))):
        return current_bets.iloc[0:0], ["model_sha256"], False
    rows = current_bets
    present_rule_fields = [field for field in LIVE_RULE_ID_FIELDS if field in rows]
    missing_matching_id = False
    if present_rule_fields:
        tagged = rows[present_rule_fields].astype("string")
        id_matches = tagged.eq(str(rule.get("id")))
        conflicting_id = (tagged.notna() & ~id_matches.fillna(False)).any(axis=1)
        rows = rows[id_matches.any(axis=1) & ~conflicting_id]
        missing_matching_id = rows.empty
    elif any(field != "model_sha256" for field in _required_live_metadata(rule)):
        missing_matching_id = True

    mismatches = _live_metadata_mismatches(rows, rule)
    if missing_matching_id or mismatches:
        return rows.iloc[0:0], mismatches, missing_matching_id
    for field in _required_live_metadata(rule):
        rows = rows[rows[field].astype(str) == str(rule[field])]
    return rows, [], False


def _remove_paid_output(path: Path) -> None:
    path.unlink(missing_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as temp:
            temp_path = Path(temp.name)
            json.dump(payload, temp, indent=2, default=_json_default)
        temp_path.replace(path)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def _persist_release(
    status: dict[str, Any],
    db_path: Path,
    output_path: Path,
    *,
    commit_at: datetime | None = None,
    quality: tuple[Path, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = json.loads(json.dumps(status, default=_json_default, allow_nan=False))
    released = publish_release(payload, db_path, commit_at=commit_at)
    if released.get("superseded"):
        return released
    with connect(db_path) as conn:
        # A newer release may have committed before this job reached the export.
        conn.execute("BEGIN IMMEDIATE")
        latest = conn.execute(
            "SELECT release_id, payload FROM published_releases ORDER BY release_id DESC LIMIT 1"
        ).fetchone()
        if latest["release_id"] != released["release_id"]:
            return {**json.loads(latest["payload"]), "superseded": True}
        if quality is not None:
            _write_json(*quality)
        if released["publishable_profitable_list_exists"]:
            _write_json(output_path, released)
        else:
            _remove_paid_output(output_path)
    return released


def revoke_release(
    db_path: Path, output_path: Path = DEFAULT_OUTPUT, *, reason: str = "release_revoked"
) -> dict[str, Any]:
    return _persist_release({
        "generated_at": _normalize_now(None).isoformat(),
        "bets": [], "passing_approved_rule_ids": [],
        "publishable_profitable_list_exists": False, "reason": reason,
    }, db_path, output_path)


def publishable_bets(
    *,
    db_path: Path = DB_PATH,
    rules_path: Path = Path("config/published_rules.yml"),
    output_path: Path = DEFAULT_OUTPUT,
    quality_output_path: Path = DEFAULT_QUALITY_OUTPUT,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    now: Optional[str | pd.Timestamp] = None,
    benchmark_prediction_paths: Optional[Iterable[Path]] = None,
) -> dict[str, Any]:
    """Write the latest paid bet list only when approved rules pass the gate."""
    observed = _normalize_now(now)
    commit_at = observed.to_pydatetime() if now is not None else None
    rules_config = load_rules(rules_path)
    rules = configured_rules(rules_config)
    approved_rules = [
        rule for rule in rules if rule.get("status") == "approved" and not rule_is_disabled(rule)
    ]

    reviews = paper_reviews(db_path, observed.to_pydatetime()) if approved_rules else {}
    gate = _quality_gate_from_config(rules_config)
    results = [
        paper_rule_result(rule, reviews.get(str(rule.get("paper_trial_id"))), gate)
        for rule in approved_rules
    ]
    result_by_id = {str(result["rule_id"]): result for result in results}
    passing_rules = [
        rule
        for rule in approved_rules
        if result_by_id.get(str(rule.get("id")), {}).get("passes_launch_gate") is True
    ]
    report = {
        "generated_at": observed.isoformat(), "db_path": str(db_path),
        "rules_path": str(rules_path), "launch_gate": gate.__dict__, "rule_results": results,
        "passing_approved_rule_ids": [str(rule["id"]) for rule in passing_rules],
        "publishable_profitable_list_exists": bool(passing_rules),
        "evidence_source": "prospective_paper",
    }
    def persist(status: dict[str, Any]) -> dict[str, Any]:
        return _persist_release(
            status, db_path, output_path, commit_at=commit_at, quality=(quality_output_path, report)
        )

    status: dict[str, Any] = {
        "generated_at": observed.isoformat(),
        "rules_path": str(rules_path),
        "quality_report_path": str(quality_output_path),
        "output_path": str(output_path),
        "approved_rule_ids": [str(rule.get("id")) for rule in approved_rules],
        "passing_approved_rule_ids": [str(rule.get("id")) for rule in passing_rules],
        "skipped_passing_rule_ids": [],
        "live_metadata_mismatches": {},
        "publishable_profitable_list_exists": False,
        "bets": [],
    }

    if not passing_rules:
        status["reason"] = "no_passing_approved_rule"
        return persist(status)

    current_predictions = load_current_prediction_output(
        db_path=db_path, leagues=leagues, now=observed
    )
    current_bets = expand_current_predictions(current_predictions)
    if current_bets.empty:
        status["reason"] = "no_current_prediction_bets"
        return persist(status)

    publish_rows: list[dict[str, Any]] = []
    for rule in passing_rules:
        matching_rows, metadata_mismatches, missing_matching_id = _matching_live_rows(
            current_bets, rule
        )
        if metadata_mismatches or missing_matching_id:
            rule_id = str(rule.get("id"))
            status["skipped_passing_rule_ids"].append(rule_id)
            status["live_metadata_mismatches"][rule_id] = {
                "missing_or_mismatched_fields": metadata_mismatches,
                "missing_matching_rule_id": missing_matching_id,
            }
            continue
        quality_result = result_by_id[str(rule.get("id"))]
        timing_filter = quality_result.get("odds_timing_filter") or {}
        rule_bets = filter_bets_for_rule(
            matching_rows,
            rule,
            max_hours_before_start=timing_filter.get("max_hours_before_start"),
        )
        if rule_bets.empty:
            continue
        for _, row in rule_bets.sort_values(["start_time_utc", "game_id", "side"]).iterrows():
            publish_rows.append(_serialize_bet(row, rule, quality_result))

    if not publish_rows:
        if status["skipped_passing_rule_ids"]:
            status["reason"] = "passing_rules_require_benchmarked_live_metadata"
        else:
            status["reason"] = "no_current_bets_matching_passing_rules"
        return persist(status)

    status["publishable_profitable_list_exists"] = True
    status["bets"] = publish_rows
    status["bet_count"] = len(publish_rows)
    released = persist(status)
    if not released["bets"] and not released.get("superseded"):
        released["reason"] = "no_current_bets_matching_original_quotes"
    return released


def promote_candidate_rule(
    *,
    rule_id: str,
    rules_path: Path = Path("config/published_rules.yml"),
    quality_report_path: Optional[Path] = None,
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    benchmark_prediction_paths: Optional[Iterable[Path]] = None,
) -> dict[str, Any]:
    """Retain the command while directing promotion to frozen trial evidence."""
    config = load_rules(rules_path)
    candidate = next((rule for rule in configured_rules(config) if str(rule.get("id")) == rule_id), None)
    if candidate is None:
        raise ValueError(f"Candidate rule not found: {rule_id}")
    if candidate.get("status") != "locked_candidate" or not candidate.get("locked_at"):
        raise RuntimeError("Lock the candidate before collecting forward evidence for promotion.")
    raise RuntimeError(
        "Promotion requires a completed prospective paper trial. "
        "Use promote-paper --trial-id with its registered trial ID."
    )


def _candidate_from_lock_report(report: dict[str, Any], rule_id: str) -> Optional[dict[str, Any]]:
    rows = report.get("locked_candidate_recommendations", []) or []
    match = next((row for row in rows if str(row.get("rule_id")) == rule_id), None)
    if match is None:
        return None
    required = ["market", "league", "model_type", "side", "min_edge"]
    if any(match.get(field) is None for field in required):
        return None
    return {
        "id": rule_id,
        **({"model_sha256": match["model_sha256"]} if match.get("model_sha256") else {}),
        "status": "locked_candidate",
        "market": match.get("market"),
        "league": match.get("league"),
        "model_type": match.get("model_type"),
        "prediction_variant": match.get("prediction_variant"),
        "side": match.get("side"),
        "min_edge": match.get("min_edge"),
        "feature_variant": match.get("feature_variant"),
        "price_mode": match.get("price_mode"),
        "timing_bucket": match.get("timing_bucket"),
        "validation": "rolling_origin_decision_time",
    }


def lock_candidate_rule(
    *,
    rule_id: str,
    rules_path: Path = Path("config/published_rules.yml"),
    benchmark_report_path: Path,
    locked_at: Optional[str | pd.Timestamp] = None,
) -> dict[str, Any]:
    """Move a benchmark-recommended candidate into locked paper tracking."""
    config = load_rules(rules_path)
    report = json.loads(benchmark_report_path.read_text(encoding="utf-8"))
    candidate_rules = config.get("candidate_rules", []) or []
    locked_candidate_rules = config.get("locked_candidate_rules", []) or []
    existing_locked = next(
        (rule for rule in locked_candidate_rules if str(rule.get("id")) == rule_id),
        None,
    )
    if existing_locked is not None:
        return {
            "rule_id": rule_id,
            "locked": True,
            "already_locked": True,
            "rules_path": str(rules_path),
            "benchmark_report_path": str(benchmark_report_path),
        }

    candidate = next((rule for rule in candidate_rules if str(rule.get("id")) == rule_id), None)
    report_candidate = _candidate_from_lock_report(report, rule_id)
    if report_candidate is None:
        raise RuntimeError(f"Rule {rule_id} was not recommended for locked paper tracking")
    lock_time = _normalize_now(locked_at).isoformat()
    locked = {
        **(candidate or {}),
        **report_candidate,
        "status": "locked_candidate",
        "locked_at": lock_time,
        "lock_source": str(benchmark_report_path),
        "approval_requires": "strict_locked_out_of_sample_results",
    }
    locked.pop("disabled", None)
    config["candidate_rules"] = [rule for rule in candidate_rules if str(rule.get("id")) != rule_id]
    config["locked_candidate_rules"] = [
        rule for rule in locked_candidate_rules if str(rule.get("id")) != rule_id
    ] + [locked]
    rules_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return {
        "rule_id": rule_id,
        "locked": True,
        "already_locked": False,
        "locked_at": lock_time,
        "rules_path": str(rules_path),
        "benchmark_report_path": str(benchmark_report_path),
    }


def _parse_leagues(raw: str) -> list[str]:
    return [league.strip().upper() for league in raw.split(",") if league.strip()]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish quality-gated subscriber-facing bets.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    publish = subparsers.add_parser("publish", help="Write latest publishable bet list.")
    publish.add_argument("--db", type=Path)
    publish.add_argument("--rules", type=Path, default=Path("config/published_rules.yml"))
    publish.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    publish.add_argument("--quality-output", type=Path, default=DEFAULT_QUALITY_OUTPUT)
    publish.add_argument("--leagues", default=",".join(DEFAULT_RELEASE_LEAGUES))
    publish.add_argument("--now", help="UTC timestamp override for reproducible tests.")
    publish.add_argument("--benchmark-predictions", type=Path, action="append", default=[])
    publish.add_argument(
        "--allow-empty",
        action="store_true",
        help=(
            "Exit 0 when the quality gate produces no paid list. This is intended for "
            "automation where fail-closed publishing is a normal, non-outage state."
        ),
    )
    publish.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )

    promote = subparsers.add_parser("promote", help="Promote a passing candidate rule.")
    promote.add_argument("--rule-id", required=True)
    promote.add_argument("--rules", type=Path, default=Path("config/published_rules.yml"))
    promote.add_argument("--quality-report", type=Path)
    promote.add_argument("--db", type=Path, default=DB_PATH)
    promote.add_argument("--leagues", default=",".join(DEFAULT_RELEASE_LEAGUES))
    promote.add_argument("--benchmark-predictions", type=Path, action="append", default=[])
    promote.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    paper = subparsers.add_parser("promote-paper", help="Approve a completed prospective trial.")
    paper.add_argument("--trial-id", required=True)
    paper.add_argument("--db", type=Path)
    paper.add_argument("--rules", type=Path, default=Path("config/published_rules.yml"))
    paper.add_argument("--log-level", default="INFO")

    lock = subparsers.add_parser("lock", help="Lock a benchmark-recommended candidate.")
    lock.add_argument("--rule-id", required=True)
    lock.add_argument("--rules", type=Path, default=Path("config/published_rules.yml"))
    lock.add_argument("--benchmark-report", type=Path, required=True)
    lock.add_argument("--locked-at", help="UTC timestamp override for reproducible tests.")
    lock.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    revoke = subparsers.add_parser("revoke", help="Revoke active picks while preserving the record.")
    revoke.add_argument("--db", type=Path)
    revoke.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    revoke.add_argument("--reason", default="release_revoked")
    revoke.add_argument("--log-level", default="INFO")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if not raw_args or raw_args[0] not in {"publish", "promote", "promote-paper", "lock", "revoke"}:
        raw_args = ["publish", *raw_args]

    parser = _build_parser()
    args = parser.parse_args(raw_args)
    logging.basicConfig(level=getattr(logging, args.log_level))
    leagues = _parse_leagues(args.leagues) if hasattr(args, "leagues") else []

    if args.command == "revoke":
        result = revoke_release(resolve_db_path(args.db), args.output, reason=args.reason)
        print(json.dumps(result, indent=2, default=_json_default))
        return 0
    if args.command == "promote-paper":
        try:
            result = promote_paper_trial(
                args.trial_id, resolve_db_path(args.db), args.rules,
                _quality_gate_from_config(load_rules(args.rules)),
            )
        except (RuntimeError, ValueError, OSError, sqlite3.Error) as exc:
            LOGGER.error("%s", exc)
            return 1
        print(json.dumps(result, indent=2, default=_json_default))
        return 0

    if args.command == "promote":
        try:
            result = promote_candidate_rule(
                rule_id=args.rule_id,
                rules_path=args.rules,
                quality_report_path=args.quality_report,
                db_path=args.db,
                leagues=leagues,
                benchmark_prediction_paths=args.benchmark_predictions,
            )
        except (RuntimeError, ValueError) as exc:
            LOGGER.error("%s", exc)
            return 1
        print(json.dumps(result, indent=2, default=_json_default))
        return 0

    if args.command == "lock":
        try:
            result = lock_candidate_rule(
                rule_id=args.rule_id,
                rules_path=args.rules,
                benchmark_report_path=args.benchmark_report,
                locked_at=args.locked_at,
            )
        except (RuntimeError, ValueError) as exc:
            LOGGER.error("%s", exc)
            return 1
        print(json.dumps(result, indent=2, default=_json_default))
        return 0

    db_path = resolve_db_path(args.db)
    try:
        result = publishable_bets(
            db_path=db_path,
            rules_path=args.rules,
            output_path=args.output,
            quality_output_path=args.quality_output,
            leagues=leagues,
            now=args.now,
            benchmark_prediction_paths=args.benchmark_predictions,
        )
    except (OSError, ValueError, RuntimeError, sqlite3.Error):
        LOGGER.exception("Paid-pick publication failed")
        revoke_release(db_path, args.output, reason="publication_failed")
        return 1
    if result.get("publishable_profitable_list_exists"):
        LOGGER.info("Wrote %d publishable bets to %s", result.get("bet_count", 0), args.output)
        return 0
    LOGGER.warning("No publishable paid bet list: %s", result.get("reason"))
    if getattr(args, "allow_empty", False):
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
