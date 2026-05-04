"""Research-only NBA totals CLV sweep.

This module deliberately stays outside paid promotion. It ranks fixed NBA totals
research slices using the same strict gate machinery as the betting benchmark,
but it does not edit approved rules or publish picks.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

from src.db.core import DB_PATH
from src.models.betting_benchmark import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_OUTPUT_DIR,
    BenchmarkConfig,
    PredictionVariant,
    StrictGate,
    _american_profit,
    _model_is_available,
    _summarize_rule_bets,
    _totals_closing_line_value,
    load_benchmark_config,
    rank_predeclared_rules,
    rolling_origin_predictions_for_variants,
)
from src.models.prediction_quality import expected_value, settle_total_side
from src.models.train_betting import FeatureContract, load_training_frame

LOGGER = logging.getLogger(__name__)

FEATURE_VARIANTS = ("current_features", "no_availability_features")
PRICE_MODES = ("best_book", "selected_book")
TIMING_BUCKETS = ("all_0_72h", "<1h", "1-6h", "6-24h", "24-72h")
AVAILABILITY_PREFIXES = ("home_injuries_", "away_injuries_")
BOOK_PRIORITY = (
    "draftkings",
    "fanduel",
    "betmgm",
    "caesars",
    "betrivers",
    "pointsbet",
    "pinnacle",
    "bovada",
)
RECOMMEND_DO_NOT_PROMOTE = "do_not_promote"
RECOMMEND_MONITOR = "add_to_strict_benchmark_for_monitoring"
RECOMMEND_MANUAL_REVIEW = "candidate_ready_for_manual_review"


@dataclass(frozen=True)
class NbaTotalsClvSweepArtifacts:
    report_path: Path


def availability_feature_columns(columns: Iterable[str]) -> list[str]:
    """Return the injury availability feature columns used by NBA totals."""
    return [
        column
        for column in columns
        if any(column.startswith(prefix) for prefix in AVAILABILITY_PREFIXES)
    ]


def feature_columns_for_variant(columns: Iterable[str], feature_variant: str) -> list[str]:
    """Apply a named research-only feature variant."""
    columns = list(columns)
    if feature_variant == "current_features":
        return columns
    if feature_variant == "no_availability_features":
        return [
            column
            for column in columns
            if not any(column.startswith(prefix) for prefix in AVAILABILITY_PREFIXES)
        ]
    raise ValueError(f"Unknown feature variant: {feature_variant}")


def contract_for_feature_variant(
    contract: FeatureContract,
    feature_variant: str,
) -> FeatureContract:
    return replace(
        contract,
        feature_columns=feature_columns_for_variant(contract.feature_columns, feature_variant),
    )


def filter_timing_bucket(bets: pd.DataFrame, timing_bucket: str) -> pd.DataFrame:
    """Filter bets to a fixed hours-before-start research bucket."""
    if bets.empty:
        return bets.copy()
    if timing_bucket not in TIMING_BUCKETS:
        raise ValueError(f"Unknown timing bucket: {timing_bucket}")
    if "hours_before_start" not in bets.columns:
        return bets.iloc[0:0].copy()

    hours = pd.to_numeric(bets["hours_before_start"], errors="coerce")
    if timing_bucket == "all_0_72h":
        mask = hours.notna() & (hours >= 0) & (hours <= 72)
    elif timing_bucket == "<1h":
        mask = hours.notna() & (hours >= 0) & (hours < 1)
    elif timing_bucket == "1-6h":
        mask = hours.notna() & (hours >= 1) & (hours < 6)
    elif timing_bucket == "6-24h":
        mask = hours.notna() & (hours >= 6) & (hours < 24)
    else:
        mask = hours.notna() & (hours >= 24) & (hours <= 72)
    return bets[mask].copy()


def _book_rank(book_name: Any) -> int:
    normalized = str(book_name or "").strip().lower()
    try:
        return BOOK_PRIORITY.index(normalized)
    except ValueError:
        return len(BOOK_PRIORITY)


def select_timing_training_frame(df: pd.DataFrame, timing_bucket: str) -> pd.DataFrame:
    """Select one as-of odds row per game for a timing bucket."""
    if df.empty:
        return df.copy()
    scoped = filter_timing_bucket(df, timing_bucket)
    if scoped.empty:
        return scoped
    scoped = scoped.copy()
    scoped["_book_rank"] = scoped["book"].map(_book_rank) if "book" in scoped.columns else 999
    selected = (
        scoped.sort_values(
            ["game_id", "snapshot_time_utc", "_book_rank", "book"],
            ascending=[True, False, True, True],
        )
        .drop_duplicates("game_id", keep="first")
        .drop(columns=["_book_rank"], errors="ignore")
        .sort_values(["start_time_utc", "game_id"])
        .reset_index(drop=True)
    )
    return selected


def _price_metadata(row: pd.Series, side: str, price_mode: str) -> dict[str, Any]:
    selected_column = "over_moneyline" if side == "over" else "under_moneyline"
    best_column = "best_over_moneyline" if side == "over" else "best_under_moneyline"
    best_book_column = "best_over_book" if side == "over" else "best_under_book"
    selected_moneyline = row.get(selected_column)
    best_moneyline = row.get(best_column)
    selected_book = row.get("book")
    best_book = row.get(best_book_column)

    if price_mode == "best_book" and pd.notna(best_moneyline):
        return {
            "moneyline": float(best_moneyline),
            "selected_moneyline": selected_moneyline,
            "best_moneyline": best_moneyline,
            "book": best_book if pd.notna(best_book) else selected_book,
            "selected_book": selected_book,
            "best_book": best_book,
            "price_mode": price_mode,
            "price_source": "best_book",
        }
    return {
        "moneyline": float(selected_moneyline) if pd.notna(selected_moneyline) else np.nan,
        "selected_moneyline": selected_moneyline,
        "best_moneyline": best_moneyline,
        "book": selected_book,
        "selected_book": selected_book,
        "best_book": best_book,
        "price_mode": price_mode,
        "price_source": "selected_book",
    }


def expand_totals_bets_for_price_mode(
    predictions: pd.DataFrame,
    *,
    price_mode: str,
    feature_variant: str,
) -> pd.DataFrame:
    """Expand NBA totals predictions to side-level bets for one price mode."""
    if price_mode not in PRICE_MODES:
        raise ValueError(f"Unknown price mode: {price_mode}")
    if predictions.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for _, row in predictions.iterrows():
        if pd.isna(row.get("actual_total")) or pd.isna(row.get("line")):
            continue
        total = float(row["actual_total"])
        line = float(row["line"])
        for side in ("over", "under"):
            predicted_prob = (
                float(row["predicted_prob"])
                if side == "over"
                else 1.0 - float(row["predicted_prob"])
            )
            market_prob = float(
                row["over_no_vig_prob"] if side == "over" else row["under_no_vig_prob"]
            )
            won = settle_total_side(total, line, side)
            if won is None:
                continue
            price = _price_metadata(row, side, price_mode)
            moneyline = price["moneyline"]
            closing_line_value, closing_total_line, clv_source = _totals_closing_line_value(
                row,
                side,
                price["price_source"],
            )
            profit = _american_profit(moneyline, bool(won))
            rows.append(
                {
                    "game_id": row.get("game_id"),
                    "market": "totals",
                    "league": row.get("league"),
                    "model_type": row["model_type"],
                    "prediction_variant": row["prediction_variant"],
                    "validation_fold": row["validation_fold"],
                    "feature_variant": feature_variant,
                    "price_mode": price_mode,
                    "side": side,
                    "predicted_prob": predicted_prob,
                    "raw_model_prob": row.get("raw_model_prob"),
                    "uncalibrated_prob": row.get("uncalibrated_prob"),
                    "calibration_method": row.get("calibration_method"),
                    "implied_prob": market_prob,
                    "no_vig_market_prob": market_prob,
                    "edge": predicted_prob - market_prob,
                    "moneyline": moneyline,
                    "selected_moneyline": price["selected_moneyline"],
                    "best_moneyline": price["best_moneyline"],
                    "book": price["book"],
                    "selected_book": price["selected_book"],
                    "best_book": price["best_book"],
                    "price_source": price["price_source"],
                    "won": bool(won),
                    "profit": profit,
                    "expected_value": expected_value(predicted_prob, moneyline),
                    "actual_value": profit,
                    "closing_line_value": closing_line_value,
                    "closing_total_line": closing_total_line,
                    "closing_line_value_source": clv_source,
                    "predicted_at": row.get("snapshot_time_utc"),
                    "start_time_utc": row.get("start_time_utc"),
                    "hours_before_start": row.get("hours_before_start"),
                }
            )
    return pd.DataFrame(rows)


def _threshold_tag(value: float) -> str:
    return f"{value:.3f}".replace(".", "")


def _slug(value: str) -> str:
    return (
        str(value)
        .lower()
        .replace("<", "lt")
        .replace(">", "gt")
        .replace("-", "_")
        .replace(" ", "_")
    )


def _rule_id_parts(
    *,
    model_type: str,
    variant: PredictionVariant,
    side: str,
    min_edge: float,
    feature_variant: str,
    price_mode: str,
    timing_bucket: str,
) -> list[str]:
    return [
        "nba",
        "totals",
        model_type.lower(),
        variant.id.lower(),
        side.lower(),
        f"edge_{_threshold_tag(float(min_edge))}",
        _slug(feature_variant),
        _slug(price_mode),
        _slug(timing_bucket),
    ]


def _build_rules_for_slice(
    config: BenchmarkConfig,
    *,
    available_candidates: Iterable[str],
    feature_variant: str,
    price_mode: str,
    timing_bucket: str,
) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    model_specs: list[tuple[str, PredictionVariant, str]] = []
    for model_type in config.baselines:
        for variant in config.baseline_variants:
            model_specs.append((model_type, variant, "baseline"))
    for model_type in available_candidates:
        for variant in config.candidate_variants:
            model_specs.append((model_type, variant, "candidate"))

    for model_type, variant, kind in model_specs:
        for min_edge in config.min_edge_thresholds:
            for side in config.sides.get("totals", ("both",)):
                rule_id = "_".join(
                    _rule_id_parts(
                        model_type=model_type,
                        variant=variant,
                        side=side,
                        min_edge=float(min_edge),
                        feature_variant=feature_variant,
                        price_mode=price_mode,
                        timing_bucket=timing_bucket,
                    )
                )
                rules.append(
                    {
                        "id": rule_id,
                        "status": "candidate",
                        "kind": kind,
                        "market": "totals",
                        "league": "NBA",
                        "model_type": model_type,
                        "prediction_variant": variant.id,
                        "side": str(side).lower(),
                        "min_edge": float(min_edge),
                        "validation": "rolling_origin",
                        "residual": bool(variant.residual),
                        "shrinkage": float(variant.shrinkage),
                        "calibration": variant.calibration,
                        "feature_variant": feature_variant,
                        "price_mode": price_mode,
                        "timing_bucket": timing_bucket,
                    }
                )
    return rules


def _number(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(number):
        return default
    return number


def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    avg_clv = _number(row.get("avg_closing_line_value"), -999.0)
    return (
        bool(row.get("passes_strict_gate")),
        avg_clv > 0,
        _number(row.get("closing_line_value_win_rate"), -999.0),
        _number(row.get("bootstrap_roi_low"), -999.0),
        bool(row.get("model_beats_market_brier")),
        _number(row.get("roi"), -999.0),
        int(row.get("bets") or 0),
    )


def _compact_candidate(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank": row.get("rank"),
        "rule_id": row.get("rule_id"),
        "model": row.get("model_type"),
        "model_type": row.get("model_type"),
        "variant": row.get("prediction_variant"),
        "prediction_variant": row.get("prediction_variant"),
        "side": row.get("side"),
        "edge": row.get("min_edge"),
        "min_edge": row.get("min_edge"),
        "feature_variant": row.get("feature_variant"),
        "price_mode": row.get("price_mode"),
        "timing_bucket": row.get("timing_bucket"),
        "bets": row.get("bets"),
        "roi": row.get("roi"),
        "bootstrap_roi_low": row.get("bootstrap_roi_low"),
        "brier_delta_vs_market": row.get("brier_delta_vs_market"),
        "model_beats_market_brier": row.get("model_beats_market_brier"),
        "avg_clv": row.get("avg_closing_line_value"),
        "avg_closing_line_value": row.get("avg_closing_line_value"),
        "clv_win_rate": row.get("closing_line_value_win_rate"),
        "closing_line_value_win_rate": row.get("closing_line_value_win_rate"),
        "clv_count": row.get("closing_line_value_count"),
        "closing_line_value_count": row.get("closing_line_value_count"),
        "passes_strict_gate": row.get("passes_strict_gate"),
        "failed_gates": row.get("strict_gate_failures", []),
        "strict_gate_failures": row.get("strict_gate_failures", []),
    }


def _best_by_dimension(candidate_rows: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    best: dict[str, dict[str, dict[str, Any]]] = {}
    for dimension in ("feature_variant", "price_mode", "timing_bucket", "model_type", "side"):
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in candidate_rows:
            grouped[str(row.get(dimension))].append(row)
        best[dimension] = {
            value: _compact_candidate(max(rows, key=_ranking_key))
            for value, rows in sorted(grouped.items())
        }
    return best


def _recommendation(candidate_rows: list[dict[str, Any]]) -> str:
    if any(row.get("passes_strict_gate") for row in candidate_rows):
        return RECOMMEND_MANUAL_REVIEW
    has_positive_clv_slice = any(
        _number(row.get("avg_closing_line_value"), -999.0) > 0
        and _number(row.get("closing_line_value_win_rate"), -999.0) > 0.5
        for row in candidate_rows
    )
    if has_positive_clv_slice:
        return RECOMMEND_MONITOR
    return RECOMMEND_DO_NOT_PROMOTE


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _empty_report(
    *,
    generated_at: datetime,
    db_path: Path,
    config_path: Path,
    gate: StrictGate,
    dataset_rows: int,
    feature_columns: list[str],
    errors: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "generated_at": generated_at.isoformat(),
        "db_path": str(db_path),
        "config_path": str(config_path),
        "strict_gate": asdict(gate),
        "dataset_rows": dataset_rows,
        "feature_variant_column_counts": {
            "current_features": len(feature_columns),
            "no_availability_features": len(
                feature_columns_for_variant(feature_columns, "no_availability_features")
            ),
        },
        "timing_bucket_row_counts": {bucket: 0 for bucket in TIMING_BUCKETS},
        "availability_feature_columns": availability_feature_columns(feature_columns),
        "sweep": {
            "feature_variants": list(FEATURE_VARIANTS),
            "price_modes": list(PRICE_MODES),
            "timing_buckets": list(TIMING_BUCKETS),
            "models": [],
        },
        "candidate_rankings": [],
        "best_by_dimension": {
            "feature_variant": {},
            "price_mode": {},
            "timing_bucket": {},
            "model_type": {},
            "side": {},
        },
        "errors": errors,
        "recommendation": RECOMMEND_DO_NOT_PROMOTE,
    }


def run_nba_totals_clv_sweep(
    *,
    db_path: Path = DB_PATH,
    benchmark_config: Path = DEFAULT_CONFIG_PATH,
    output: Optional[Path] = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> NbaTotalsClvSweepArtifacts:
    """Run the fixed NBA totals CLV research sweep and write a JSON report."""
    config = load_benchmark_config(benchmark_config)
    generated_at = datetime.now(timezone.utc)
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    report_path = output or output_dir / f"nba_totals_clv_sweep_{timestamp}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    errors: list[dict[str, Any]] = []
    try:
        loaded_df, base_contract = load_training_frame(
            "totals",
            db_path=db_path,
            leagues=["NBA"],
            latest_only=False,
        )
    except Exception as exc:  # noqa: BLE001 - research report should fail closed.
        LOGGER.warning("Could not load NBA totals training frame: %s", exc)
        report = _empty_report(
            generated_at=generated_at,
            db_path=db_path,
            config_path=benchmark_config,
            gate=config.strict_gate,
            dataset_rows=0,
            feature_columns=[],
            errors=[{"stage": "load_training_frame", "error": str(exc)}],
        )
        report_path.write_text(json.dumps(report, indent=2, default=_json_default), encoding="utf-8")
        return NbaTotalsClvSweepArtifacts(report_path)

    nba_df = loaded_df[loaded_df["league"].astype(str).str.upper() == "NBA"].copy()
    feature_columns = list(base_contract.feature_columns)
    available_candidates = [
        model
        for model in config.candidates
        if model in {"logistic", "gradient_boosting", "random_forest", "xgboost"}
        and _model_is_available(model)
    ]
    skipped_models = [
        model
        for model in config.candidates
        if model in {"logistic", "gradient_boosting", "random_forest", "xgboost"}
        and model not in available_candidates
    ]

    if len(nba_df) < 2:
        report = _empty_report(
            generated_at=generated_at,
            db_path=db_path,
            config_path=benchmark_config,
            gate=config.strict_gate,
            dataset_rows=int(len(nba_df)),
            feature_columns=feature_columns,
            errors=errors,
        )
        report["sweep"]["models"] = available_candidates
        report["sweep"]["skipped_models"] = skipped_models
        report_path.write_text(json.dumps(report, indent=2, default=_json_default), encoding="utf-8")
        return NbaTotalsClvSweepArtifacts(report_path)

    all_bets: list[pd.DataFrame] = []
    feature_counts: dict[str, int] = {}
    timing_bucket_row_counts: dict[str, int] = {bucket: 0 for bucket in TIMING_BUCKETS}
    prediction_counts: dict[str, int] = {}
    model_plan: list[tuple[str, tuple[PredictionVariant, ...]]] = [
        (model_type, config.baseline_variants) for model_type in config.baselines
    ]
    model_plan.extend((model_type, config.candidate_variants) for model_type in available_candidates)

    for feature_variant in FEATURE_VARIANTS:
        contract = contract_for_feature_variant(base_contract, feature_variant)
        feature_counts[feature_variant] = len(contract.feature_columns)
        try:
            contract.validate(nba_df)
        except Exception as exc:  # noqa: BLE001
            errors.append(
                {
                    "stage": "feature_contract",
                    "feature_variant": feature_variant,
                    "error": str(exc),
                }
            )
            continue
        for timing_bucket in TIMING_BUCKETS:
            timing_df = select_timing_training_frame(nba_df, timing_bucket)
            timing_bucket_row_counts[timing_bucket] = max(
                timing_bucket_row_counts[timing_bucket],
                int(len(timing_df)),
            )
            if len(timing_df) < 2:
                continue
            for model_type, variants in model_plan:
                try:
                    predictions_by_variant = rolling_origin_predictions_for_variants(
                        timing_df,
                        contract,
                        model_type,
                        variants,
                        rolling=config.rolling,
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append(
                        {
                            "stage": "rolling_origin_predictions",
                            "feature_variant": feature_variant,
                            "timing_bucket": timing_bucket,
                            "model_type": model_type,
                            "error": str(exc),
                        }
                    )
                    continue

                for variant_id, predictions in predictions_by_variant.items():
                    prediction_counts[
                        f"{feature_variant}:{timing_bucket}:NBA:totals:{model_type}:{variant_id}"
                    ] = int(len(predictions))
                    for price_mode in PRICE_MODES:
                        bets = expand_totals_bets_for_price_mode(
                            predictions,
                            price_mode=price_mode,
                            feature_variant=feature_variant,
                        )
                        if not bets.empty:
                            bets["timing_bucket"] = timing_bucket
                            all_bets.append(bets)

    bets = pd.concat(all_bets, ignore_index=True) if all_bets else pd.DataFrame()
    candidate_rows: list[dict[str, Any]] = []
    for feature_variant in FEATURE_VARIANTS:
        for price_mode in PRICE_MODES:
            for timing_bucket in TIMING_BUCKETS:
                scoped = (
                    bets[
                        (bets["feature_variant"] == feature_variant)
                        & (bets["price_mode"] == price_mode)
                        & (bets["timing_bucket"] == timing_bucket)
                    ].copy()
                    if not bets.empty
                    else pd.DataFrame()
                )
                rules = _build_rules_for_slice(
                    config,
                    available_candidates=available_candidates,
                    feature_variant=feature_variant,
                    price_mode=price_mode,
                    timing_bucket=timing_bucket,
                )
                ranked = rank_predeclared_rules(scoped, rules, config.strict_gate)
                for row in ranked:
                    row["feature_variant"] = feature_variant
                    row["price_mode"] = price_mode
                    row["timing_bucket"] = timing_bucket
                    if row.get("kind") == "candidate":
                        candidate_rows.append(row)

    candidate_rows.sort(key=_ranking_key, reverse=True)
    compact_rankings = []
    for rank, row in enumerate(candidate_rows, start=1):
        row["rank"] = rank
        compact_rankings.append(_compact_candidate(row))

    report = {
        "generated_at": generated_at.isoformat(),
        "db_path": str(db_path),
        "config_path": str(benchmark_config),
        "strict_gate": asdict(config.strict_gate),
        "dataset_rows": int(len(nba_df)),
        "feature_variant_column_counts": feature_counts,
        "timing_bucket_row_counts": timing_bucket_row_counts,
        "availability_feature_columns": availability_feature_columns(feature_columns),
        "sweep": {
            "feature_variants": list(FEATURE_VARIANTS),
            "price_modes": list(PRICE_MODES),
            "timing_buckets": list(TIMING_BUCKETS),
            "models": available_candidates,
            "skipped_models": skipped_models,
            "prediction_variants": [asdict(variant) for variant in config.candidate_variants],
            "min_edge_thresholds": list(config.min_edge_thresholds),
            "sides": list(config.sides.get("totals", ())),
        },
        "prediction_counts": prediction_counts,
        "candidate_rankings": compact_rankings,
        "best_by_dimension": _best_by_dimension(candidate_rows),
        "diagnostics": {
            "candidate_count": len(compact_rankings),
            "passing_candidate_count": sum(
                1 for row in compact_rankings if row.get("passes_strict_gate")
            ),
            "positive_avg_clv_candidate_count": sum(
                1 for row in compact_rankings if _number(row.get("avg_clv"), -999.0) > 0
            ),
            "positive_avg_clv_and_win_rate_candidate_count": sum(
                1
                for row in compact_rankings
                if _number(row.get("avg_clv"), -999.0) > 0
                and _number(row.get("clv_win_rate"), -999.0) > 0.5
            ),
        },
        "errors": errors,
        "recommendation": _recommendation(candidate_rows),
    }
    report_path.write_text(json.dumps(report, indent=2, default=_json_default), encoding="utf-8")
    LOGGER.info("Wrote NBA totals CLV sweep report to %s", report_path)
    return NbaTotalsClvSweepArtifacts(report_path)


def summarize_price_mode(frame: pd.DataFrame, gate: StrictGate) -> dict[str, Any]:
    """Small helper for tests and research comparisons."""
    return _summarize_rule_bets(frame, gate)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run research-only NBA totals CLV sweep.")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument(
        "--benchmark-config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Predeclared benchmark grid config to read variants, edges, sides, and strict gate.",
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))
    artifacts = run_nba_totals_clv_sweep(
        db_path=args.db,
        benchmark_config=args.benchmark_config,
        output=args.output,
        output_dir=args.output_dir,
    )
    report = json.loads(artifacts.report_path.read_text(encoding="utf-8"))
    print("NBA totals CLV sweep report")
    print(f"output={artifacts.report_path}")
    print(f"dataset_rows={report['dataset_rows']}")
    print(f"candidate_count={len(report['candidate_rankings'])}")
    print(f"recommendation={report['recommendation']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
