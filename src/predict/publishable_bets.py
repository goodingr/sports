"""Publish subscriber-facing bets behind the historical quality gate."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import yaml

from src.db.core import DB_PATH
from src.models.prediction_quality import (
    DEFAULT_RELEASE_LEAGUES,
    _json_default,
    build_quality_report,
    configured_rules,
    filter_bets_for_rule,
    load_rules,
    no_vig_pair,
    rule_is_disabled,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_OUTPUT = Path("reports/publishable_bets/latest_publishable_bets.json")
DEFAULT_QUALITY_OUTPUT = Path("reports/publishable_bets/latest_quality_report.json")

QUALITY_SUMMARY_FIELDS = (
    "bets",
    "required_min_bets",
    "roi",
    "profit",
    "win_rate",
    "avg_edge",
    "bootstrap_roi_low",
    "bootstrap_roi_median",
    "bootstrap_roi_high",
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


def _normalize_now(now: Optional[str | pd.Timestamp]) -> pd.Timestamp:
    if now is None:
        return pd.Timestamp.utcnow()
    value = pd.Timestamp(now)
    if value.tzinfo is None:
        return value.tz_localize("UTC")
    return value.tz_convert("UTC")


def _latest_pre_game(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame["predicted_at"] = pd.to_datetime(frame["predicted_at"], utc=True, errors="coerce")
    frame["start_time_utc"] = pd.to_datetime(frame["start_time_utc"], utc=True, errors="coerce")
    frame = frame[frame["predicted_at"].notna() & frame["start_time_utc"].notna()].copy()
    frame = frame[frame["predicted_at"] <= frame["start_time_utc"]].copy()
    return frame.sort_values("predicted_at").drop_duplicates(["game_id", "model_type"], keep="last")


def load_current_prediction_output(
    db_path: Path = DB_PATH,
    *,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    now: Optional[str | pd.Timestamp] = None,
) -> pd.DataFrame:
    """Load current pre-game predictions that could become subscriber-facing bets."""
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
            p.predicted_total_points,
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
    """
    df = _read_sql(db_path, query)
    if df.empty:
        return df

    df = _latest_pre_game(df)
    if df.empty:
        return df

    now_utc = _normalize_now(now)
    df = df[df["start_time_utc"] >= now_utc].copy()
    df = df[
        ~df["game_status"].astype(str).str.lower().isin({"final", "completed", "closed", "post"})
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
        side_df["edge"] = side_df[f"{side}_edge"]
        missing_edge = side_df["edge"].isna()
        side_df.loc[missing_edge, "edge"] = (
            side_df.loc[missing_edge, "predicted_prob"] - side_df.loc[missing_edge, "implied_prob"]
        )
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
        side_df["edge"] = side_df[f"{side}_edge"]
        missing_edge = side_df["edge"].isna()
        side_df.loc[missing_edge, "edge"] = (
            side_df.loc[missing_edge, "predicted_prob"] - side_df.loc[missing_edge, "implied_prob"]
        )
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


def _serialize_bet(row: pd.Series, rule: dict[str, Any], quality_result: dict[str, Any]) -> dict[str, Any]:
    market_probability = row.get("no_vig_market_prob")
    if pd.isna(market_probability):
        market_probability = row.get("implied_prob")
    return {
        "rule_id": rule.get("id"),
        "market": row.get("market"),
        "league": row.get("league"),
        "game_id": row.get("game_id"),
        "start_time_utc": row.get("start_time_utc"),
        "home_team": row.get("home_team"),
        "away_team": row.get("away_team"),
        "side": row.get("side"),
        "odds": _safe_float(row.get("moneyline")),
        "edge": _safe_float(row.get("edge")),
        "model_probability": _safe_float(row.get("predicted_prob")),
        "market_probability": _safe_float(market_probability),
        "quality_summary": _quality_summary(quality_result),
    }


def _remove_paid_output(path: Path) -> None:
    if path.exists() and path.is_file():
        path.unlink()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")


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
    rules_config = load_rules(rules_path)
    rules = configured_rules(rules_config)
    approved_rules = [
        rule
        for rule in rules
        if rule.get("status") == "approved" and not rule_is_disabled(rule)
    ]

    report = build_quality_report(
        db_path=db_path,
        rules_path=rules_path,
        leagues=leagues,
        benchmark_prediction_paths=benchmark_prediction_paths,
    )
    _write_json(quality_output_path, report)

    result_by_id = {
        str(result.get("rule_id")): result
        for result in report.get("rule_results", [])
        if result.get("rule_id") is not None
    }
    passing_rules = [
        rule
        for rule in approved_rules
        if result_by_id.get(str(rule.get("id")), {}).get("passes_launch_gate") is True
    ]

    status: dict[str, Any] = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "rules_path": str(rules_path),
        "quality_report_path": str(quality_output_path),
        "output_path": str(output_path),
        "approved_rule_ids": [str(rule.get("id")) for rule in approved_rules],
        "passing_approved_rule_ids": [str(rule.get("id")) for rule in passing_rules],
        "publishable_profitable_list_exists": False,
        "bets": [],
    }

    if not passing_rules:
        _remove_paid_output(output_path)
        status["reason"] = "no_passing_approved_rule"
        return status

    current_predictions = load_current_prediction_output(db_path=db_path, leagues=leagues, now=now)
    current_bets = expand_current_predictions(current_predictions)
    if current_bets.empty:
        _remove_paid_output(output_path)
        status["reason"] = "no_current_prediction_bets"
        return status

    publish_rows: list[dict[str, Any]] = []
    for rule in passing_rules:
        quality_result = result_by_id[str(rule.get("id"))]
        rule_bets = filter_bets_for_rule(current_bets, rule)
        if rule_bets.empty:
            continue
        for _, row in rule_bets.sort_values(["start_time_utc", "game_id", "side"]).iterrows():
            publish_rows.append(_serialize_bet(row, rule, quality_result))

    if not publish_rows:
        _remove_paid_output(output_path)
        status["reason"] = "no_current_bets_matching_passing_rules"
        return status

    status["publishable_profitable_list_exists"] = True
    status["bets"] = publish_rows
    status["bet_count"] = len(publish_rows)
    _write_json(output_path, status)
    return status


def _load_or_build_report(
    *,
    quality_report_path: Optional[Path],
    db_path: Path,
    rules_path: Path,
    leagues: Optional[Iterable[str]],
    benchmark_prediction_paths: Optional[Iterable[Path]],
) -> dict[str, Any]:
    if quality_report_path and quality_report_path.exists():
        return json.loads(quality_report_path.read_text(encoding="utf-8"))
    return build_quality_report(
        db_path=db_path,
        rules_path=rules_path,
        leagues=leagues,
        benchmark_prediction_paths=benchmark_prediction_paths,
    )


def promote_candidate_rule(
    *,
    rule_id: str,
    rules_path: Path = Path("config/published_rules.yml"),
    quality_report_path: Optional[Path] = None,
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    benchmark_prediction_paths: Optional[Iterable[Path]] = None,
) -> dict[str, Any]:
    """Promote a candidate rule into approved_rules only if it passes a strict source."""
    config = load_rules(rules_path)
    candidate_rules = config.get("candidate_rules", []) or []
    approved_rules = config.get("approved_rules", []) or []
    candidate = next((rule for rule in candidate_rules if str(rule.get("id")) == rule_id), None)
    if candidate is None:
        raise ValueError(f"Candidate rule not found: {rule_id}")

    report = _load_or_build_report(
        quality_report_path=quality_report_path,
        db_path=db_path,
        rules_path=rules_path,
        leagues=leagues,
        benchmark_prediction_paths=benchmark_prediction_paths,
    )
    passing_sources: list[str] = []
    for source in report.get("evaluation_sources", []):
        for result in source.get("rule_results", []):
            if str(result.get("rule_id")) == rule_id and result.get("passes_launch_gate") is True:
                passing_sources.append(str(source.get("source_id")))
    if not passing_sources:
        raise RuntimeError(f"Candidate rule {rule_id} does not pass the launch gate")

    promoted = {**candidate, "status": "approved"}
    promoted.pop("disabled", None)
    approved_without_duplicate = [
        rule for rule in approved_rules if str(rule.get("id")) != rule_id
    ]
    candidate_without_promoted = [
        rule for rule in candidate_rules if str(rule.get("id")) != rule_id
    ]
    config["approved_rules"] = [*approved_without_duplicate, promoted]
    config["candidate_rules"] = candidate_without_promoted
    rules_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return {
        "rule_id": rule_id,
        "promoted": True,
        "passing_sources": passing_sources,
        "rules_path": str(rules_path),
    }


def _parse_leagues(raw: str) -> list[str]:
    return [league.strip().upper() for league in raw.split(",") if league.strip()]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish quality-gated subscriber-facing bets.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    publish = subparsers.add_parser("publish", help="Write latest publishable bet list.")
    publish.add_argument("--db", type=Path, default=DB_PATH)
    publish.add_argument("--rules", type=Path, default=Path("config/published_rules.yml"))
    publish.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    publish.add_argument("--quality-output", type=Path, default=DEFAULT_QUALITY_OUTPUT)
    publish.add_argument("--leagues", default=",".join(DEFAULT_RELEASE_LEAGUES))
    publish.add_argument("--now", help="UTC timestamp override for reproducible tests.")
    publish.add_argument("--benchmark-predictions", type=Path, action="append", default=[])
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
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if not raw_args or raw_args[0] not in {"publish", "promote"}:
        raw_args = ["publish", *raw_args]

    parser = _build_parser()
    args = parser.parse_args(raw_args)
    logging.basicConfig(level=getattr(logging, args.log_level))
    leagues = _parse_leagues(args.leagues)

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

    result = publishable_bets(
        db_path=args.db,
        rules_path=args.rules,
        output_path=args.output,
        quality_output_path=args.quality_output,
        leagues=leagues,
        now=args.now,
        benchmark_prediction_paths=args.benchmark_predictions,
    )
    if result.get("publishable_profitable_list_exists"):
        LOGGER.info("Wrote %d publishable bets to %s", result.get("bet_count", 0), args.output)
        return 0
    LOGGER.warning("No publishable paid bet list: %s", result.get("reason"))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
