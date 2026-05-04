"""Decision-time benchmark for profit-first paid-pick research.

This command evaluates the same rolling-origin model families at fixed pregame
decision windows and explicit price modes. It ranks candidates but never
approves or publishes a rule.
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
from src.features.betting_model_input import DEFAULT_RELEASE_LEAGUES
from src.models.betting_benchmark import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_OUTPUT_DIR,
    BenchmarkConfig,
    PredictionVariant,
    _american_profit,
    _model_is_available,
    _moneyline_clv,
    load_benchmark_config,
    rank_predeclared_rules,
    rolling_origin_predictions_for_variants,
)
from src.models.nba_totals_clv_sweep import (
    PRICE_MODES,
    TIMING_BUCKETS,
    _book_rank,
    _number,
    _slug,
    _threshold_tag,
    expand_totals_bets_for_price_mode,
    filter_timing_bucket,
)
from src.models.prediction_quality import expected_value
from src.models.train_betting import FeatureContract, load_training_frame

LOGGER = logging.getLogger(__name__)

FEATURE_VARIANTS = ("current_features", "no_availability_features")
DEFAULT_MARKETS = ("totals", "moneyline")
LOCK_STATUS_RECOMMENDED = "lock_recommended"
LOCK_STATUS_MONITOR = "monitor"
LOCK_STATUS_DIAGNOSTIC = "diagnostic_only"
LOCK_STATUS_REJECTED = "do_not_lock"
RECOMMEND_DO_NOT_PROMOTE = "do_not_promote"
RECOMMEND_MONITOR = "monitor_profitable_slices"
RECOMMEND_LOCK_REVIEW = "manual_locked_candidate_review"


@dataclass(frozen=True)
class DecisionTimeBenchmarkArtifacts:
    report_path: Path


def availability_feature_columns(columns: Iterable[str]) -> list[str]:
    """Return availability columns so feature variants compare identical folds."""
    return [
        column
        for column in columns
        if "injuries_" in column or column.startswith(("availability_", "player_availability_"))
    ]


def feature_columns_for_variant(columns: Iterable[str], feature_variant: str) -> list[str]:
    columns = list(columns)
    if feature_variant == "current_features":
        return columns
    if feature_variant == "no_availability_features":
        availability = set(availability_feature_columns(columns))
        return [column for column in columns if column not in availability]
    raise ValueError(f"Unknown feature variant: {feature_variant}")


def contract_for_feature_variant(
    contract: FeatureContract,
    feature_variant: str,
) -> FeatureContract:
    return replace(
        contract,
        feature_columns=feature_columns_for_variant(contract.feature_columns, feature_variant),
    )


def select_decision_time_training_frame(
    df: pd.DataFrame,
    timing_bucket: str,
    *,
    market: str,
) -> pd.DataFrame:
    """Select exactly one valid pregame snapshot per game, or game/side."""
    if df.empty:
        return df.copy()
    scoped = filter_timing_bucket(df, timing_bucket)
    if scoped.empty:
        return scoped
    scoped = scoped.copy()
    scoped["_book_rank"] = scoped["book"].map(_book_rank) if "book" in scoped.columns else 999
    key_columns = ["game_id"]
    if market == "moneyline" and "side" in scoped.columns:
        key_columns.append("side")
    sort_columns = [
        *key_columns,
        "snapshot_time_utc",
        "_book_rank",
        "book",
    ]
    ascending = [True for _ in key_columns] + [False, True, True]
    selected = (
        scoped.sort_values(sort_columns, ascending=ascending)
        .drop_duplicates(key_columns, keep="first")
        .drop(columns=["_book_rank"], errors="ignore")
        .sort_values(["start_time_utc", "game_id", *(["side"] if "side" in scoped.columns else [])])
        .reset_index(drop=True)
    )
    return selected


def _moneyline_price_metadata(row: pd.Series, price_mode: str) -> dict[str, Any]:
    selected_moneyline = row.get("moneyline")
    best_moneyline = row.get("best_moneyline")
    selected_book = row.get("book")
    best_book = row.get("best_book")
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


def expand_moneyline_bets_for_price_mode(
    predictions: pd.DataFrame,
    *,
    price_mode: str,
    feature_variant: str,
) -> pd.DataFrame:
    """Expand side-level moneyline predictions for one explicit price mode."""
    if price_mode not in PRICE_MODES:
        raise ValueError(f"Unknown price mode: {price_mode}")
    if predictions.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for _, row in predictions.iterrows():
        if pd.isna(row.get("predicted_prob")) or pd.isna(row.get("no_vig_prob")):
            continue
        won_value = row.get("target_win", row.get("won"))
        if pd.isna(won_value):
            continue
        price = _moneyline_price_metadata(row, price_mode)
        moneyline = price["moneyline"]
        predicted_prob = float(row["predicted_prob"])
        market_prob = float(row["no_vig_prob"])
        won = bool(won_value)
        close_moneyline = row.get("close_moneyline")
        rows.append(
            {
                "game_id": row.get("game_id"),
                "market": "moneyline",
                "league": row.get("league"),
                "model_type": row["model_type"],
                "prediction_variant": row["prediction_variant"],
                "validation_fold": row["validation_fold"],
                "feature_variant": feature_variant,
                "price_mode": price_mode,
                "side": row.get("side"),
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
                "won": won,
                "profit": _american_profit(moneyline, won),
                "expected_value": expected_value(predicted_prob, moneyline),
                "actual_value": _american_profit(moneyline, won),
                "closing_line_value": _moneyline_clv(moneyline, close_moneyline),
                "close_moneyline": close_moneyline,
                "closing_line_value_source": (
                    "game_result_close_moneyline"
                    if pd.notna(close_moneyline)
                    else "missing_close_moneyline"
                ),
                "predicted_at": row.get("snapshot_time_utc"),
                "start_time_utc": row.get("start_time_utc"),
                "hours_before_start": row.get("hours_before_start"),
            }
        )
    return pd.DataFrame(rows)


def expand_bets_for_price_mode(
    predictions: pd.DataFrame,
    *,
    market: str,
    price_mode: str,
    feature_variant: str,
) -> pd.DataFrame:
    if market == "totals":
        return expand_totals_bets_for_price_mode(
            predictions,
            price_mode=price_mode,
            feature_variant=feature_variant,
        )
    if market == "moneyline":
        return expand_moneyline_bets_for_price_mode(
            predictions,
            price_mode=price_mode,
            feature_variant=feature_variant,
        )
    raise ValueError(f"Unsupported decision-time market: {market}")


def _rule_id_parts(
    *,
    league: str,
    market: str,
    model_type: str,
    variant: PredictionVariant,
    side: str,
    min_edge: float,
    feature_variant: str,
    price_mode: str,
    timing_bucket: str,
) -> list[str]:
    return [
        league.lower(),
        market.lower(),
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
    market: str,
    league: str,
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

    diagnostic_only = market == "moneyline"
    for model_type, variant, kind in model_specs:
        for min_edge in config.min_edge_thresholds:
            for side in config.sides.get(market, ("both",)):
                rule_id = "_".join(
                    _rule_id_parts(
                        league=league,
                        market=market,
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
                        "market": market,
                        "league": league,
                        "model_type": model_type,
                        "prediction_variant": variant.id,
                        "side": str(side).lower(),
                        "min_edge": float(min_edge),
                        "validation": "rolling_origin_decision_time",
                        "residual": bool(variant.residual),
                        "shrinkage": float(variant.shrinkage),
                        "calibration": variant.calibration,
                        "feature_variant": feature_variant,
                        "price_mode": price_mode,
                        "timing_bucket": timing_bucket,
                        "diagnostic_only": diagnostic_only,
                    }
                )
    return rules


def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        bool(row.get("passes_strict_gate")),
        _number(row.get("avg_closing_line_value"), -999.0) > 0,
        _number(row.get("closing_line_value_win_rate"), -999.0),
        _number(row.get("bootstrap_roi_low"), -999.0),
        bool(row.get("model_beats_market_brier")),
        _number(row.get("roi"), -999.0),
        int(row.get("bets") or 0),
    )


def _lock_status(row: dict[str, Any]) -> str:
    if row.get("diagnostic_only"):
        return LOCK_STATUS_DIAGNOSTIC
    if row.get("passes_strict_gate"):
        return LOCK_STATUS_RECOMMENDED
    if row.get("model_beats_market_brier") and _number(row.get("roi"), -999.0) > 0:
        return LOCK_STATUS_MONITOR
    return LOCK_STATUS_REJECTED


def _compact_candidate(row: dict[str, Any]) -> dict[str, Any]:
    compact = {
        "rank": row.get("rank"),
        "rule_id": row.get("rule_id"),
        "market": row.get("market"),
        "league": row.get("league"),
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
        "diagnostic_only": bool(row.get("diagnostic_only")),
        "lock_status": _lock_status(row),
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
        "clv_slices": row.get("clv_slices", []),
        "passes_strict_gate": row.get("passes_strict_gate"),
        "failed_gates": row.get("strict_gate_failures", []),
        "strict_gate_failures": row.get("strict_gate_failures", []),
    }
    return compact


def _best_by_dimension(
    candidate_rows: list[dict[str, Any]]
) -> dict[str, dict[str, dict[str, Any]]]:
    best: dict[str, dict[str, dict[str, Any]]] = {}
    dimensions = (
        "market",
        "league",
        "feature_variant",
        "price_mode",
        "timing_bucket",
        "model_type",
        "side",
    )
    for dimension in dimensions:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in candidate_rows:
            grouped[str(row.get(dimension))].append(row)
        best[dimension] = {
            value: _compact_candidate(max(rows, key=_ranking_key))
            for value, rows in sorted(grouped.items())
        }
    return best


def _recommendation(compact_rankings: list[dict[str, Any]]) -> str:
    if any(row.get("lock_status") == LOCK_STATUS_RECOMMENDED for row in compact_rankings):
        return RECOMMEND_LOCK_REVIEW
    if any(row.get("lock_status") == LOCK_STATUS_MONITOR for row in compact_rankings):
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


def _parse_csv(raw: str | Iterable[str], *, lower: bool = False) -> list[str]:
    if isinstance(raw, str):
        values = [item.strip() for item in raw.split(",") if item.strip()]
    else:
        values = [str(item).strip() for item in raw if str(item).strip()]
    if lower:
        return [value.lower() for value in values]
    return [value.upper() for value in values]


def _eligible_models(config: BenchmarkConfig) -> tuple[list[str], list[str]]:
    candidates = [
        model
        for model in config.candidates
        if model in {"logistic", "gradient_boosting", "random_forest", "xgboost"}
        and _model_is_available(model)
    ]
    skipped = [
        model
        for model in config.candidates
        if model in {"logistic", "gradient_boosting", "random_forest", "xgboost"}
        and model not in candidates
    ]
    return candidates, skipped


def _empty_loaded_frame() -> tuple[pd.DataFrame, FeatureContract]:
    return pd.DataFrame(), FeatureContract("", "", [], "")


def run_decision_time_benchmark(
    *,
    db_path: Path = DB_PATH,
    config_path: Path = DEFAULT_CONFIG_PATH,
    output: Optional[Path] = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    markets: Iterable[str] = DEFAULT_MARKETS,
    leagues: Optional[Iterable[str]] = None,
) -> DecisionTimeBenchmarkArtifacts:
    """Run the canonical decision-time benchmark and write a JSON report."""
    config = load_benchmark_config(config_path)
    requested_markets = tuple(market.lower() for market in markets)
    requested_leagues = tuple(league.upper() for league in (leagues or config.leagues))
    generated_at = datetime.now(timezone.utc)
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    report_path = output or output_dir / f"decision_time_benchmark_{timestamp}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    available_candidates, skipped_models = _eligible_models(config)
    all_bets: list[pd.DataFrame] = []
    errors: list[dict[str, Any]] = []
    dataset_counts: dict[str, int] = {}
    feature_variant_column_counts: dict[str, dict[str, int]] = {}
    timing_bucket_row_counts: dict[str, dict[str, int]] = {}
    prediction_counts: dict[str, int] = {}
    availability_columns: dict[str, list[str]] = {}
    model_plan: list[tuple[str, tuple[PredictionVariant, ...]]] = [
        (model_type, config.baseline_variants) for model_type in config.baselines
    ]
    model_plan.extend(
        (model_type, config.candidate_variants) for model_type in available_candidates
    )

    for market in requested_markets:
        if market not in {"totals", "moneyline"}:
            errors.append({"stage": "config", "market": market, "error": "unsupported_market"})
            continue
        try:
            loaded_df, base_contract = load_training_frame(
                market,
                db_path=db_path,
                leagues=requested_leagues,
                latest_only=False,
            )
        except Exception as exc:  # noqa: BLE001 - benchmark report should fail closed.
            LOGGER.warning("Could not load %s training frame: %s", market, exc)
            loaded_df, base_contract = _empty_loaded_frame()
            errors.append({"stage": "load_training_frame", "market": market, "error": str(exc)})

        dataset_counts[f"{market}_rows"] = int(len(loaded_df))
        feature_columns = list(base_contract.feature_columns)
        availability_columns[market] = availability_feature_columns(feature_columns)
        feature_variant_column_counts[market] = {}
        timing_bucket_row_counts[market] = {bucket: 0 for bucket in TIMING_BUCKETS}

        if loaded_df.empty:
            continue
        loaded_df = loaded_df[
            loaded_df["league"].astype(str).str.upper().isin(requested_leagues)
        ].copy()

        for feature_variant in FEATURE_VARIANTS:
            contract = contract_for_feature_variant(base_contract, feature_variant)
            feature_variant_column_counts[market][feature_variant] = len(contract.feature_columns)
            try:
                contract.validate(loaded_df)
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    {
                        "stage": "feature_contract",
                        "market": market,
                        "feature_variant": feature_variant,
                        "error": str(exc),
                    }
                )
                continue

            for timing_bucket in TIMING_BUCKETS:
                timing_df = select_decision_time_training_frame(
                    loaded_df,
                    timing_bucket,
                    market=market,
                )
                timing_bucket_row_counts[market][timing_bucket] = max(
                    timing_bucket_row_counts[market][timing_bucket],
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
                                "market": market,
                                "feature_variant": feature_variant,
                                "timing_bucket": timing_bucket,
                                "model_type": model_type,
                                "error": str(exc),
                            }
                        )
                        continue

                    for variant_id, predictions in predictions_by_variant.items():
                        prediction_key = (
                            f"{feature_variant}:{timing_bucket}:{market}:{model_type}:{variant_id}"
                        )
                        prediction_counts[prediction_key] = int(len(predictions))
                        for price_mode in PRICE_MODES:
                            bets = expand_bets_for_price_mode(
                                predictions,
                                market=market,
                                price_mode=price_mode,
                                feature_variant=feature_variant,
                            )
                            if not bets.empty:
                                bets["timing_bucket"] = timing_bucket
                                all_bets.append(bets)

    bets = pd.concat(all_bets, ignore_index=True) if all_bets else pd.DataFrame()
    candidate_rows: list[dict[str, Any]] = []

    for market in requested_markets:
        for league in requested_leagues:
            for feature_variant in FEATURE_VARIANTS:
                for price_mode in PRICE_MODES:
                    for timing_bucket in TIMING_BUCKETS:
                        scoped = (
                            bets[
                                (bets["market"] == market)
                                & (bets["league"].astype(str).str.upper() == league)
                                & (bets["feature_variant"] == feature_variant)
                                & (bets["price_mode"] == price_mode)
                                & (bets["timing_bucket"] == timing_bucket)
                            ].copy()
                            if not bets.empty
                            else pd.DataFrame()
                        )
                        rules = _build_rules_for_slice(
                            config,
                            market=market,
                            league=league,
                            available_candidates=available_candidates,
                            feature_variant=feature_variant,
                            price_mode=price_mode,
                            timing_bucket=timing_bucket,
                        )
                        ranked = rank_predeclared_rules(scoped, rules, config.strict_gate)
                        for row in ranked:
                            row["diagnostic_only"] = row.get("rule", {}).get(
                                "diagnostic_only",
                                False,
                            )
                            if row.get("kind") == "candidate":
                                candidate_rows.append(row)

    candidate_rows.sort(key=_ranking_key, reverse=True)
    compact_rankings = []
    for rank, row in enumerate(candidate_rows, start=1):
        row["rank"] = rank
        compact_rankings.append(_compact_candidate(row))

    locked_candidates = [
        row for row in compact_rankings if row.get("lock_status") == LOCK_STATUS_RECOMMENDED
    ]
    report = {
        "generated_at": generated_at.isoformat(),
        "db_path": str(db_path),
        "config_path": str(config_path),
        "strict_gate": asdict(config.strict_gate),
        "dataset_counts": dataset_counts,
        "feature_variant_column_counts": feature_variant_column_counts,
        "timing_bucket_row_counts": timing_bucket_row_counts,
        "availability_feature_columns": availability_columns,
        "sweep": {
            "markets": list(requested_markets),
            "leagues": list(requested_leagues),
            "feature_variants": list(FEATURE_VARIANTS),
            "price_modes": list(PRICE_MODES),
            "timing_buckets": list(TIMING_BUCKETS),
            "models": available_candidates,
            "skipped_models": skipped_models,
            "prediction_variants": [asdict(variant) for variant in config.candidate_variants],
            "min_edge_thresholds": list(config.min_edge_thresholds),
            "sides": {market: list(config.sides.get(market, ())) for market in requested_markets},
            "diagnostic_markets": ["moneyline"],
        },
        "prediction_counts": prediction_counts,
        "candidate_rankings": compact_rankings,
        "locked_candidate_recommendations": locked_candidates,
        "approved_candidate_count": 0,
        "auto_promotions": [],
        "best_by_dimension": _best_by_dimension(candidate_rows),
        "diagnostics": {
            "candidate_count": len(compact_rankings),
            "strict_passing_candidate_count": sum(
                1 for row in compact_rankings if row.get("passes_strict_gate")
            ),
            "locked_candidate_recommendation_count": len(locked_candidates),
            "diagnostic_candidate_count": sum(
                1 for row in compact_rankings if row.get("diagnostic_only")
            ),
            "positive_avg_clv_candidate_count": sum(
                1 for row in compact_rankings if _number(row.get("avg_clv"), -999.0) > 0
            ),
        },
        "errors": errors,
        "recommendation": _recommendation(compact_rankings),
    }
    report_path.write_text(json.dumps(report, indent=2, default=_json_default), encoding="utf-8")
    LOGGER.info("Wrote decision-time benchmark report to %s", report_path)
    return DecisionTimeBenchmarkArtifacts(report_path)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fixed decision-time betting benchmark.")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--markets", default=",".join(DEFAULT_MARKETS))
    parser.add_argument("--leagues", default=",".join(DEFAULT_RELEASE_LEAGUES))
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level))
    markets = _parse_csv(args.markets, lower=True)
    leagues = _parse_csv(args.leagues)
    artifacts = run_decision_time_benchmark(
        db_path=args.db,
        config_path=args.config,
        output=args.output,
        output_dir=args.output_dir,
        markets=markets,
        leagues=leagues,
    )
    report = json.loads(artifacts.report_path.read_text(encoding="utf-8"))
    print("Decision-time benchmark report")
    print(f"output={artifacts.report_path}")
    print(f"candidate_count={len(report['candidate_rankings'])}")
    print(f"locked_candidate_recommendations={len(report['locked_candidate_recommendations'])}")
    print(f"recommendation={report['recommendation']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
