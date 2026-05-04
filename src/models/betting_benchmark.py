"""Rolling-origin benchmark search for predeclared betting rules.

The benchmark is intentionally conservative:

* every prediction is produced by expanding-window validation;
* model candidates are evaluated as residual signals around the no-vig market;
* probabilities are calibrated inside each origin before edge filtering;
* rule search is limited to the market/league/model/side/threshold grid in config.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd
import yaml
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss

from src.data.availability_quality import warn_if_low_availability_coverage
from src.db.core import DB_PATH
from src.features.betting_model_input import DEFAULT_RELEASE_LEAGUES
from src.models.benchmark_triage import build_benchmark_triage_report
from src.models.prediction_quality import (
    DEFAULT_STAKE,
    american_to_decimal,
    bootstrap_roi_interval,
    calibration_bins,
    closing_line_value_summary,
    expected_value,
    max_drawdown,
    max_losing_streak,
    settle_total_side,
)
from src.models.train_betting import (
    MARKET_BASELINE_MODELS,
    FeatureContract,
    _build_estimator,
    _line_movement_probability,
    _rolling_origin_splits,
    load_training_frame,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/betting_benchmark.yml")
DEFAULT_OUTPUT_DIR = Path("reports/betting_benchmarks")
EPSILON = 1e-6


@dataclass(frozen=True)
class RollingConfig:
    folds: int = 5
    min_train_size: int = 100
    calibration_folds: int = 3
    min_calibration_size: int = 50


@dataclass(frozen=True)
class PredictionVariant:
    id: str
    residual: bool = True
    shrinkage: float = 0.5
    calibration: str = "sigmoid"


@dataclass(frozen=True)
class StrictGate:
    min_bets_narrow: int = 150
    min_bets_multi_league: int = 300
    min_roi: float = 0.05
    min_bootstrap_roi_low: float = 0.0
    min_avg_clv: float = 0.0
    min_clv_win_rate: float = 0.5
    max_hours_before_start: Optional[float] = 72.0
    require_positive_clv: bool = True
    bootstrap_samples: int = 3000
    random_seed: int = 42
    require_brier_beats_market: bool = True
    require_roi_beats_market_baseline: bool = True


@dataclass(frozen=True)
class BenchmarkConfig:
    markets: tuple[str, ...]
    leagues: tuple[str, ...]
    baselines: tuple[str, ...]
    candidates: tuple[str, ...]
    min_edge_thresholds: tuple[float, ...]
    sides: dict[str, tuple[str, ...]]
    baseline_variants: tuple[PredictionVariant, ...]
    candidate_variants: tuple[PredictionVariant, ...]
    rolling: RollingConfig
    strict_gate: StrictGate


@dataclass(frozen=True)
class BenchmarkArtifacts:
    report_path: Path
    best_rule_json_path: Path
    best_rule_yaml_path: Path
    triage_report_path: Path


@dataclass(frozen=True)
class ProbabilityCalibrator:
    method: str
    model: Any = None

    def transform(self, probabilities: np.ndarray) -> np.ndarray:
        values = np.asarray(probabilities, dtype=float)
        if self.method == "sigmoid" and self.model is not None:
            values = self.model.predict_proba(values.reshape(-1, 1))[:, 1]
        elif self.method == "isotonic" and self.model is not None:
            values = self.model.predict(values)
        return np.clip(values, EPSILON, 1.0 - EPSILON)


def _as_tuple(value: Any, default: Iterable[Any]) -> tuple[Any, ...]:
    if value is None:
        return tuple(default)
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    return tuple(value)


def _prediction_variant(raw: dict[str, Any], default_id: str) -> PredictionVariant:
    return PredictionVariant(
        id=str(raw.get("id") or default_id),
        residual=bool(raw.get("residual", True)),
        shrinkage=float(raw.get("shrinkage", 0.5)),
        calibration=str(raw.get("calibration", "sigmoid")).lower(),
    )


def _strict_gate_from_raw(raw: dict[str, Any]) -> StrictGate:
    return StrictGate(
        min_bets_narrow=int(raw.get("min_bets_narrow", StrictGate.min_bets_narrow)),
        min_bets_multi_league=int(
            raw.get("min_bets_multi_league", StrictGate.min_bets_multi_league)
        ),
        min_roi=float(raw.get("min_roi", StrictGate.min_roi)),
        min_bootstrap_roi_low=float(
            raw.get("min_bootstrap_roi_low", StrictGate.min_bootstrap_roi_low)
        ),
        min_avg_clv=float(raw.get("min_avg_clv", StrictGate.min_avg_clv)),
        min_clv_win_rate=float(raw.get("min_clv_win_rate", StrictGate.min_clv_win_rate)),
        max_hours_before_start=(
            None
            if raw.get("max_hours_before_start") is None
            else float(raw.get("max_hours_before_start", StrictGate.max_hours_before_start))
        ),
        require_positive_clv=bool(raw.get("require_positive_clv", StrictGate.require_positive_clv)),
        bootstrap_samples=int(raw.get("bootstrap_samples", StrictGate.bootstrap_samples)),
        random_seed=int(raw.get("random_seed", StrictGate.random_seed)),
        require_brier_beats_market=bool(
            raw.get("require_brier_beats_market", StrictGate.require_brier_beats_market)
        ),
        require_roi_beats_market_baseline=bool(
            raw.get(
                "require_roi_beats_market_baseline",
                StrictGate.require_roi_beats_market_baseline,
            )
        ),
    )


def load_benchmark_config(path: Path = DEFAULT_CONFIG_PATH) -> BenchmarkConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    raw = raw or {}
    sweep = raw.get("sweep", raw)
    variants = sweep.get("variants", {})
    baseline_variants = tuple(
        _prediction_variant(item, "baseline")
        for item in variants.get(
            "baselines",
            [{"id": "baseline", "residual": False, "shrinkage": 1.0, "calibration": "none"}],
        )
    )
    candidate_variants = tuple(
        _prediction_variant(item, f"market_residual_shrink_{index}")
        for index, item in enumerate(
            variants.get(
                "candidates",
                [
                    {
                        "id": "market_residual_shrink_025_sigmoid",
                        "residual": True,
                        "shrinkage": 0.25,
                        "calibration": "sigmoid",
                    },
                    {
                        "id": "market_residual_shrink_050_sigmoid",
                        "residual": True,
                        "shrinkage": 0.50,
                        "calibration": "sigmoid",
                    },
                    {
                        "id": "market_residual_shrink_075_sigmoid",
                        "residual": True,
                        "shrinkage": 0.75,
                        "calibration": "sigmoid",
                    },
                ],
            )
        )
    )
    rolling_raw = raw.get("rolling_origin") or raw.get("rolling") or {}
    gate_raw = raw.get("strict_gate") or raw.get("launch_gate") or {}
    sides_raw = sweep.get("sides", {})
    return BenchmarkConfig(
        markets=tuple(
            str(item).lower() for item in _as_tuple(sweep.get("markets"), ["totals", "moneyline"])
        ),
        leagues=tuple(
            str(item).upper() for item in _as_tuple(sweep.get("leagues"), DEFAULT_RELEASE_LEAGUES)
        ),
        baselines=tuple(
            str(item).lower()
            for item in _as_tuple(sweep.get("baselines"), ["market_only", "line_movement"])
        ),
        candidates=tuple(
            str(item).lower()
            for item in _as_tuple(
                sweep.get("candidates"),
                ["logistic", "gradient_boosting", "random_forest", "xgboost"],
            )
        ),
        min_edge_thresholds=tuple(
            float(item)
            for item in _as_tuple(
                sweep.get("min_edge_thresholds"),
                [0.0, 0.02, 0.04, 0.06, 0.08],
            )
        ),
        sides={
            "totals": tuple(
                str(item).lower()
                for item in _as_tuple(sides_raw.get("totals"), ["over", "under", "both"])
            ),
            "moneyline": tuple(
                str(item).lower()
                for item in _as_tuple(sides_raw.get("moneyline"), ["home", "away", "both"])
            ),
        },
        baseline_variants=baseline_variants,
        candidate_variants=candidate_variants,
        rolling=RollingConfig(
            folds=int(rolling_raw.get("folds", RollingConfig.folds)),
            min_train_size=int(rolling_raw.get("min_train_size", RollingConfig.min_train_size)),
            calibration_folds=int(
                rolling_raw.get("calibration_folds", RollingConfig.calibration_folds)
            ),
            min_calibration_size=int(
                rolling_raw.get("min_calibration_size", RollingConfig.min_calibration_size)
            ),
        ),
        strict_gate=_strict_gate_from_raw(gate_raw),
    )


def _feature_matrix(df: pd.DataFrame, contract: FeatureContract) -> pd.DataFrame:
    return df[contract.feature_columns].apply(pd.to_numeric, errors="coerce")


def _fallback_probability(df: pd.DataFrame, contract: FeatureContract) -> np.ndarray:
    return (
        pd.to_numeric(df[contract.market_probability_column], errors="coerce")
        .fillna(0.5)
        .to_numpy()
    )


def _fit_predict_model(
    model_type: str,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    contract: FeatureContract,
) -> np.ndarray:
    fallback = _fallback_probability(test_df, contract)
    y_train = train_df[contract.target_column].astype(int)
    if y_train.nunique() < 2:
        return np.clip(fallback, EPSILON, 1.0 - EPSILON)
    try:
        estimator = _build_estimator(model_type)
        estimator.fit(_feature_matrix(train_df, contract), y_train)
        probabilities = estimator.predict_proba(_feature_matrix(test_df, contract))[:, 1]
        return np.clip(probabilities, EPSILON, 1.0 - EPSILON)
    except Exception as exc:  # noqa: BLE001 - benchmark should skip failed origins, not crash.
        LOGGER.warning("Falling back to market probability for %s fold: %s", model_type, exc)
        return np.clip(fallback, EPSILON, 1.0 - EPSILON)


def _baseline_probability(
    model_type: str,
    df: pd.DataFrame,
    contract: FeatureContract,
) -> np.ndarray:
    if model_type == "market_only":
        return np.clip(_fallback_probability(df, contract), EPSILON, 1.0 - EPSILON)
    if model_type == "line_movement":
        return np.clip(
            _line_movement_probability(df, contract.market, contract.market_probability_column),
            EPSILON,
            1.0 - EPSILON,
        )
    raise ValueError(f"Unsupported baseline model: {model_type}")


def _out_of_fold_raw_probabilities(
    df: pd.DataFrame,
    contract: FeatureContract,
    model_type: str,
    rolling: RollingConfig,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if len(df) < 3:
        return np.array([]), np.array([]), np.array([])
    min_train = min(rolling.min_calibration_size, max(1, len(df) // 2))
    splits = _rolling_origin_splits(
        df,
        folds=rolling.calibration_folds,
        min_train_size=min_train,
    )
    raw_predictions: list[np.ndarray] = []
    market_predictions: list[np.ndarray] = []
    targets: list[np.ndarray] = []
    for train_idx, validation_idx in splits:
        train_df = df.iloc[train_idx]
        validation_df = df.iloc[validation_idx]
        raw_predictions.append(_fit_predict_model(model_type, train_df, validation_df, contract))
        market_predictions.append(_fallback_probability(validation_df, contract))
        targets.append(validation_df[contract.target_column].astype(int).to_numpy())
    if not raw_predictions:
        return np.array([]), np.array([]), np.array([])
    return (
        np.concatenate(raw_predictions),
        np.concatenate(market_predictions),
        np.concatenate(targets),
    )


def _variant_probabilities(
    raw_probabilities: np.ndarray,
    market_probabilities: np.ndarray,
    variant: PredictionVariant,
) -> np.ndarray:
    raw_values = np.clip(np.asarray(raw_probabilities, dtype=float), EPSILON, 1.0 - EPSILON)
    market_values = np.clip(
        np.asarray(market_probabilities, dtype=float),
        EPSILON,
        1.0 - EPSILON,
    )
    if variant.residual:
        residual_signal = raw_values - market_values
        return np.clip(
            market_values + variant.shrinkage * residual_signal,
            EPSILON,
            1.0 - EPSILON,
        )
    return np.clip(
        market_values + variant.shrinkage * (raw_values - market_values),
        EPSILON,
        1.0 - EPSILON,
    )


def _fit_calibrator(
    probabilities: np.ndarray,
    targets: np.ndarray,
    method: str,
) -> ProbabilityCalibrator:
    method = method.lower()
    if method in {"none", "identity"}:
        return ProbabilityCalibrator("none")
    if len(probabilities) < 8 or len(np.unique(targets)) < 2:
        return ProbabilityCalibrator("identity_insufficient_data")
    try:
        if method == "sigmoid":
            calibrator = LogisticRegression(max_iter=1000)
            calibrator.fit(np.asarray(probabilities).reshape(-1, 1), targets.astype(int))
            return ProbabilityCalibrator("sigmoid", calibrator)
        if method == "isotonic":
            calibrator = IsotonicRegression(out_of_bounds="clip")
            calibrator.fit(probabilities, targets.astype(int))
            return ProbabilityCalibrator("isotonic", calibrator)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Probability calibration fell back to identity: %s", exc)
        return ProbabilityCalibrator("identity_failed")
    raise ValueError("calibration must be one of: none, sigmoid, isotonic")


def rolling_origin_predictions_for_variants(
    df: pd.DataFrame,
    contract: FeatureContract,
    model_type: str,
    variants: Iterable[PredictionVariant],
    rolling: RollingConfig = RollingConfig(),
) -> dict[str, pd.DataFrame]:
    """Return validation predictions for one model across prediction variants."""
    contract.validate(df)
    variants = tuple(variants)
    df = df.sort_values("start_time_utc").reset_index(drop=True)
    splits = _rolling_origin_splits(df, folds=rolling.folds, min_train_size=rolling.min_train_size)
    prediction_frames: dict[str, list[pd.DataFrame]] = {variant.id: [] for variant in variants}

    for fold_index, (train_idx, test_idx) in enumerate(splits, start=1):
        train_df = df.iloc[train_idx].copy()
        test_df = df.iloc[test_idx].copy()
        market_test = _fallback_probability(test_df, contract)
        if model_type in MARKET_BASELINE_MODELS:
            raw_test = _baseline_probability(model_type, test_df, contract)
            raw_oof = np.array([])
            market_oof = np.array([])
            target_oof = np.array([])
        else:
            raw_oof, market_oof, target_oof = _out_of_fold_raw_probabilities(
                train_df,
                contract,
                model_type,
                rolling,
            )
            raw_test = _fit_predict_model(model_type, train_df, test_df, contract)

        for variant in variants:
            uncalibrated = _variant_probabilities(raw_test, market_test, variant)
            residual_signal = np.clip(raw_test, EPSILON, 1.0 - EPSILON) - np.clip(
                market_test,
                EPSILON,
                1.0 - EPSILON,
            )
            if model_type in MARKET_BASELINE_MODELS:
                calibrator = ProbabilityCalibrator("none")
                probabilities = uncalibrated
            else:
                train_variant_probabilities = _variant_probabilities(raw_oof, market_oof, variant)
                calibrator = _fit_calibrator(
                    train_variant_probabilities,
                    target_oof,
                    variant.calibration,
                )
                probabilities = calibrator.transform(uncalibrated)

            fold_predictions = test_df.copy()
            fold_predictions["model_type"] = model_type
            fold_predictions["prediction_variant"] = variant.id
            fold_predictions["validation_fold"] = fold_index
            fold_predictions["market_prob"] = market_test
            fold_predictions["raw_model_prob"] = raw_test
            fold_predictions["residual_signal"] = residual_signal
            fold_predictions["uncalibrated_prob"] = uncalibrated
            fold_predictions["predicted_prob"] = probabilities
            fold_predictions["calibration_method"] = calibrator.method
            prediction_frames[variant.id].append(fold_predictions)

    return {
        variant_id: pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        for variant_id, frames in prediction_frames.items()
    }


def _best_or_selected(row: pd.Series, best_column: str, selected_column: str) -> float:
    best = row.get(best_column)
    if pd.notna(best):
        return float(best)
    return float(row[selected_column])


def _selected_price_metadata(
    row: pd.Series,
    *,
    best_moneyline_column: str,
    selected_moneyline_column: str,
    best_book_column: str,
) -> dict[str, Any]:
    best_moneyline = row.get(best_moneyline_column)
    selected_moneyline = row.get(selected_moneyline_column)
    selected_book = row.get("book")
    best_book = row.get(best_book_column)
    use_best = pd.notna(best_moneyline)
    return {
        "moneyline": float(best_moneyline if use_best else selected_moneyline),
        "selected_moneyline": selected_moneyline,
        "best_moneyline": best_moneyline,
        "book": best_book if use_best and pd.notna(best_book) else selected_book,
        "selected_book": selected_book,
        "best_book": best_book,
        "price_source": "best_book" if use_best else "selected_book",
    }


def _american_profit(
    moneyline: float | int | None,
    won: bool,
    stake: float = DEFAULT_STAKE,
) -> float:
    if moneyline is None or pd.isna(moneyline):
        return np.nan
    moneyline = float(moneyline)
    if moneyline == 0:
        return np.nan
    if won:
        if moneyline > 0:
            return stake * (moneyline / 100.0)
        return stake * (100.0 / abs(moneyline))
    return -stake


def _moneyline_clv(bet_moneyline: float | int | None, close_moneyline: float | int | None) -> float:
    bet_decimal = american_to_decimal(bet_moneyline)
    close_decimal = american_to_decimal(close_moneyline)
    if bet_decimal is None or close_decimal is None:
        return np.nan
    return float(bet_decimal - close_decimal)


def _totals_closing_line_value(
    row: pd.Series,
    side: str,
    price_source: str,
) -> tuple[float, float, str]:
    """Return same-book totals CLV, close line, and source label."""
    same_book_columns = {
        "selected_book_total_close",
        "best_over_book_total_close",
        "best_under_book_total_close",
    }
    same_book_available = any(column in row.index for column in same_book_columns)
    if price_source == "best_book":
        close_line = row.get(f"best_{side}_book_total_close")
        source = f"same_book_{side}_best_book"
    else:
        close_line = row.get("selected_book_total_close")
        source = "same_book_selected_book"

    if pd.isna(close_line) and not same_book_available:
        close_line = row.get("total_close")
        source = "legacy_game_results_total_close"
    if pd.isna(close_line):
        return np.nan, np.nan, source

    line = float(row["line"])
    closing_line_value = float(close_line) - line if side == "over" else line - float(close_line)
    return closing_line_value, float(close_line), source


def expand_benchmark_bets(predictions: pd.DataFrame, contract: FeatureContract) -> pd.DataFrame:
    """Expand validation rows into bettable side rows before threshold filtering."""
    if predictions.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    if contract.market == "totals":
        for _, row in predictions.iterrows():
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
                price = _selected_price_metadata(
                    row,
                    best_moneyline_column=(
                        "best_over_moneyline" if side == "over" else "best_under_moneyline"
                    ),
                    selected_moneyline_column=(
                        "over_moneyline" if side == "over" else "under_moneyline"
                    ),
                    best_book_column="best_over_book" if side == "over" else "best_under_book",
                )
                moneyline = price["moneyline"]
                won = settle_total_side(total, line, side)
                if won is None:
                    continue
                closing_line_value, closing_total_line, clv_source = _totals_closing_line_value(
                    row,
                    side,
                    price["price_source"],
                )
                rows.append(
                    {
                        "game_id": row.get("game_id"),
                        "market": "totals",
                        "league": row["league"],
                        "model_type": row["model_type"],
                        "prediction_variant": row["prediction_variant"],
                        "validation_fold": row["validation_fold"],
                        "side": side,
                        "predicted_prob": predicted_prob,
                        "raw_model_prob": row["raw_model_prob"],
                        "uncalibrated_prob": row["uncalibrated_prob"],
                        "calibration_method": row["calibration_method"],
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
                        "profit": _american_profit(moneyline, bool(won)),
                        "expected_value": expected_value(predicted_prob, moneyline),
                        "actual_value": _american_profit(moneyline, bool(won)),
                        "closing_line_value": closing_line_value,
                        "closing_total_line": closing_total_line,
                        "closing_line_value_source": clv_source,
                        "predicted_at": row["snapshot_time_utc"],
                        "start_time_utc": row["start_time_utc"],
                        "hours_before_start": row.get("hours_before_start"),
                    }
                )
    elif contract.market == "moneyline":
        for _, row in predictions.iterrows():
            predicted_prob = float(row["predicted_prob"])
            market_prob = float(row["no_vig_prob"])
            price = _selected_price_metadata(
                row,
                best_moneyline_column="best_moneyline",
                selected_moneyline_column="moneyline",
                best_book_column="best_book",
            )
            moneyline = price["moneyline"]
            close_moneyline = row.get("close_moneyline")
            won = bool(row["target_win"])
            rows.append(
                {
                    "game_id": row.get("game_id"),
                    "market": "moneyline",
                    "league": row["league"],
                    "model_type": row["model_type"],
                    "prediction_variant": row["prediction_variant"],
                    "validation_fold": row["validation_fold"],
                    "side": row["side"],
                    "predicted_prob": predicted_prob,
                    "raw_model_prob": row["raw_model_prob"],
                    "uncalibrated_prob": row["uncalibrated_prob"],
                    "calibration_method": row["calibration_method"],
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
                    "predicted_at": row["snapshot_time_utc"],
                    "start_time_utc": row["start_time_utc"],
                    "hours_before_start": row.get("hours_before_start"),
                }
            )
    else:
        raise ValueError("market must be one of: totals, moneyline")
    return pd.DataFrame(rows)


def _brier_or_none(frame: pd.DataFrame, probability_column: str) -> Optional[float]:
    if frame.empty:
        return None
    clean = frame.dropna(subset=["won", probability_column])
    if clean.empty:
        return None
    return float(
        brier_score_loss(clean["won"].astype(int), clean[probability_column].astype(float))
    )


def _summarize_rule_bets(frame: pd.DataFrame, gate: StrictGate) -> dict[str, Any]:
    if frame.empty:
        return {
            "sample_size": 0,
            "bets": 0,
            "profit": 0.0,
            "roi": None,
            "bootstrap_roi_low": None,
            "bootstrap_roi_median": None,
            "bootstrap_roi_high": None,
            "bootstrap_roi_interval": None,
            "win_rate": None,
            "brier_score": None,
            "market_brier_score": None,
            "brier_delta_vs_market": None,
            "model_beats_market_brier": False,
            "closing_line_value_count": 0,
            "avg_closing_line_value": None,
            "closing_line_value_win_rate": None,
            "calibration_bins": [],
            "max_drawdown": 0.0,
            "losing_streak": 0,
        }
    clean = frame.dropna(subset=["profit", "predicted_prob", "won"]).copy()
    if clean.empty:
        return _summarize_rule_bets(pd.DataFrame(), gate)
    total_staked = DEFAULT_STAKE * len(clean)
    roi = float(clean["profit"].sum() / total_staked) if total_staked else None
    ci_low, ci_median, ci_high = bootstrap_roi_interval(
        clean["profit"],
        samples=gate.bootstrap_samples,
        seed=gate.random_seed,
    )
    brier = _brier_or_none(clean, "predicted_prob")
    market_brier = _brier_or_none(clean, "no_vig_market_prob")
    clv = closing_line_value_summary(clean)
    return {
        "sample_size": int(len(clean)),
        "bets": int(len(clean)),
        "profit": float(clean["profit"].sum()),
        "roi": roi,
        "bootstrap_roi_low": ci_low,
        "bootstrap_roi_median": ci_median,
        "bootstrap_roi_high": ci_high,
        "bootstrap_roi_interval": {
            "low": ci_low,
            "median": ci_median,
            "high": ci_high,
        },
        "win_rate": float(clean["won"].mean()),
        "avg_edge": float(clean["edge"].mean()) if "edge" in clean.columns else None,
        "avg_expected_value": float(clean["expected_value"].mean()),
        "actual_value_per_bet": float(clean["actual_value"].mean()),
        "brier_score": brier,
        "market_brier_score": market_brier,
        "brier_delta_vs_market": (
            None if brier is None or market_brier is None else float(brier - market_brier)
        ),
        "model_beats_market_brier": bool(
            brier is not None and market_brier is not None and brier < market_brier
        ),
        **clv,
        "calibration_bins": calibration_bins(clean),
        "max_drawdown": max_drawdown(clean.sort_values("start_time_utc")["profit"]),
        "losing_streak": max_losing_streak(clean.sort_values("start_time_utc")["won"]),
    }


def _hours_bucket(hours: Any) -> str:
    if pd.isna(hours):
        return "missing"
    value = float(hours)
    if value < 1:
        return "<1h"
    if value < 6:
        return "1-6h"
    if value < 24:
        return "6-24h"
    if value < 72:
        return "24-72h"
    return ">=72h"


def _filter_rule_bets(
    bets: pd.DataFrame,
    rule: dict[str, Any],
    *,
    max_hours_before_start: Optional[float] = None,
) -> pd.DataFrame:
    frame = bets.copy()
    frame = frame[frame["market"] == str(rule["market"]).lower()]
    frame = frame[frame["league"].astype(str).str.upper() == str(rule["league"]).upper()]
    frame = frame[frame["model_type"] == str(rule["model_type"])]
    frame = frame[frame["prediction_variant"] == str(rule["prediction_variant"])]
    for field in ("feature_variant", "price_mode", "timing_bucket"):
        if rule.get(field) is not None:
            if field not in frame.columns:
                return frame.iloc[0:0].copy()
            frame = frame[frame[field].astype(str) == str(rule[field])]
    if str(rule["side"]).lower() != "both":
        frame = frame[frame["side"] == str(rule["side"]).lower()]
    frame = frame[frame["edge"] >= float(rule["min_edge"])]
    if max_hours_before_start is not None and "hours_before_start" in frame.columns:
        hours = pd.to_numeric(frame["hours_before_start"], errors="coerce")
        if hours.notna().any():
            frame = frame[hours.notna() & (hours <= max_hours_before_start)].copy()
    return frame


def _rule_timing_exclusion_count(
    bets: pd.DataFrame,
    rule: dict[str, Any],
    max_hours_before_start: Optional[float],
) -> int:
    if max_hours_before_start is None or bets.empty or "hours_before_start" not in bets.columns:
        return 0
    raw = _filter_rule_bets(bets, rule, max_hours_before_start=None)
    if raw.empty:
        return 0
    filtered = _filter_rule_bets(
        bets,
        rule,
        max_hours_before_start=max_hours_before_start,
    )
    return int(len(raw) - len(filtered))


def _rule_clv_slices(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty or "closing_line_value" not in frame.columns:
        return []
    clean = frame.copy()
    clean["closing_line_value"] = pd.to_numeric(clean["closing_line_value"], errors="coerce")
    clean = clean[clean["closing_line_value"].notna()].copy()
    if clean.empty:
        return []
    if "book" not in clean.columns:
        clean["book"] = "unknown"
    if "price_source" not in clean.columns:
        clean["price_source"] = "unknown"
    if "closing_line_value_source" not in clean.columns:
        clean["closing_line_value_source"] = "unknown"
    if "hours_before_start" not in clean.columns:
        clean["hours_before_start"] = np.nan
    clean["hours_bucket"] = clean["hours_before_start"].apply(_hours_bucket)

    rows: list[dict[str, Any]] = []
    group_columns = ["book", "price_source", "closing_line_value_source", "hours_bucket"]
    for keys, group in clean.groupby(group_columns, dropna=False):
        book, price_source, clv_source, hours_bucket = keys
        total_staked = DEFAULT_STAKE * len(group)
        roi = float(group["profit"].sum() / total_staked) if total_staked else None
        clv = closing_line_value_summary(group)
        rows.append(
            {
                "book": str(book) if pd.notna(book) else "unknown",
                "price_source": str(price_source),
                "closing_line_value_source": str(clv_source),
                "hours_bucket": str(hours_bucket),
                "bets": int(len(group)),
                "roi": roi,
                "avg_edge": float(group["edge"].mean()) if "edge" in group.columns else None,
                **clv,
            }
        )
    rows.sort(
        key=lambda row: (
            row["bets"],
            (
                row.get("avg_closing_line_value")
                if row.get("avg_closing_line_value") is not None
                else -999.0
            ),
        ),
        reverse=True,
    )
    return rows


def _threshold_tag(value: float) -> str:
    return f"{value:.3f}".replace(".", "")


def _sort_number(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(number):
        return default
    return number


def build_predeclared_rules(config: BenchmarkConfig) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    model_specs: list[tuple[str, PredictionVariant, str]] = []
    for model_type in config.baselines:
        for variant in config.baseline_variants:
            model_specs.append((model_type, variant, "baseline"))
    for model_type in config.candidates:
        for variant in config.candidate_variants:
            model_specs.append((model_type, variant, "candidate"))

    for market in config.markets:
        for league in config.leagues:
            for model_type, variant, kind in model_specs:
                for min_edge in config.min_edge_thresholds:
                    for actual_side in config.sides.get(market, ("both",)):
                        rule_id = "_".join(
                            [
                                str(league).lower(),
                                str(market).lower(),
                                str(model_type).lower(),
                                variant.id.lower(),
                                str(actual_side).lower(),
                                f"edge_{_threshold_tag(float(min_edge))}",
                            ]
                        )
                        rules.append(
                            {
                                "id": rule_id,
                                "status": "candidate",
                                "kind": kind,
                                "market": str(market).lower(),
                                "league": str(league).upper(),
                                "model_type": str(model_type).lower(),
                                "prediction_variant": variant.id,
                                "side": str(actual_side).lower(),
                                "min_edge": float(min_edge),
                                "validation": "rolling_origin",
                                "residual": bool(variant.residual),
                                "shrinkage": float(variant.shrinkage),
                                "calibration": variant.calibration,
                            }
                        )
    return rules


def _candidate_rule_yaml(rule: dict[str, Any], publishable: bool) -> str:
    block = {
        "candidate_rule": {
            "id": rule["id"],
            "status": "candidate",
            "publishable": bool(publishable),
            "market": rule["market"],
            "league": rule["league"],
            "model_type": rule["model_type"],
            "prediction_variant": rule["prediction_variant"],
            "side": rule["side"],
            "min_edge": rule["min_edge"],
            "validation": "rolling_origin",
            "residual": rule["residual"],
            "shrinkage": rule["shrinkage"],
            "calibration": rule["calibration"],
        }
    }
    for field in ("feature_variant", "price_mode", "timing_bucket"):
        if rule.get(field) is not None:
            block["candidate_rule"][field] = rule[field]
    return yaml.safe_dump(block, sort_keys=False)


def _compact_metrics(row: Optional[dict[str, Any]]) -> dict[str, Any]:
    if row is None:
        return {
            "bets": 0,
            "roi": None,
            "bootstrap_roi_low": None,
            "win_rate": None,
            "brier_score": None,
            "market_brier_score": None,
            "rule_id": None,
        }
    return {
        "bets": row.get("bets"),
        "roi": row.get("roi"),
        "bootstrap_roi_low": row.get("bootstrap_roi_low"),
        "win_rate": row.get("win_rate"),
        "brier_score": row.get("brier_score"),
        "market_brier_score": row.get("market_brier_score"),
        "rule_id": row.get("rule_id"),
        "min_edge": row.get("min_edge"),
    }


def _baseline_lookup_key(
    result: dict[str, Any],
) -> tuple[str, str, str, float, str, str, Any, Any, Any]:
    return (
        result["market"],
        result["league"],
        result["side"],
        float(result["min_edge"]),
        result["model_type"],
        result["prediction_variant"],
        result.get("feature_variant"),
        result.get("price_mode"),
        result.get("timing_bucket"),
    )


def _find_baseline_result(
    lookup: dict[tuple[str, str, str, float, str, str, Any, Any, Any], dict[str, Any]],
    result: dict[str, Any],
    baseline_model: str,
) -> Optional[dict[str, Any]]:
    same_threshold_key = (
        result["market"],
        result["league"],
        result["side"],
        float(result["min_edge"]),
        baseline_model,
        "baseline",
        result.get("feature_variant"),
        result.get("price_mode"),
        result.get("timing_bucket"),
    )
    baseline = lookup.get(same_threshold_key)
    if baseline is not None and baseline.get("bets", 0) > 0:
        return baseline
    zero_threshold_key = (
        result["market"],
        result["league"],
        result["side"],
        0.0,
        baseline_model,
        "baseline",
        result.get("feature_variant"),
        result.get("price_mode"),
        result.get("timing_bucket"),
    )
    return lookup.get(zero_threshold_key) or baseline


def _gate_failures(
    result: dict[str, Any],
    gate: StrictGate,
    market_baseline: Optional[dict[str, Any]],
) -> list[str]:
    failures: list[str] = []
    required_min_bets = (
        gate.min_bets_multi_league
        if str(result["league"]).upper() == "ALL"
        else gate.min_bets_narrow
    )
    if result.get("kind") != "candidate":
        failures.append("baseline_rules_are_not_publishable")
    if int(result.get("bets") or 0) < required_min_bets:
        failures.append(f"sample_size_below_{required_min_bets}")
    if result.get("roi") is None or float(result["roi"]) < gate.min_roi:
        failures.append(f"roi_below_{gate.min_roi}")
    if (
        result.get("bootstrap_roi_low") is None
        or float(result["bootstrap_roi_low"]) <= gate.min_bootstrap_roi_low
    ):
        failures.append(f"bootstrap_roi_low_not_above_{gate.min_bootstrap_roi_low}")
    if gate.require_brier_beats_market and not result.get("model_beats_market_brier"):
        failures.append("brier_does_not_beat_market")
    if gate.require_positive_clv:
        if int(result.get("closing_line_value_count") or 0) <= 0:
            failures.append("missing_clv")
        if (
            result.get("avg_closing_line_value") is None
            or float(result["avg_closing_line_value"]) <= gate.min_avg_clv
        ):
            failures.append(f"avg_clv_not_above_{gate.min_avg_clv}")
        if (
            result.get("closing_line_value_win_rate") is None
            or float(result["closing_line_value_win_rate"]) <= gate.min_clv_win_rate
        ):
            failures.append(f"clv_win_rate_not_above_{gate.min_clv_win_rate}")
    baseline_roi = market_baseline.get("roi") if market_baseline else None
    if (
        gate.require_roi_beats_market_baseline
        and baseline_roi is not None
        and result.get("roi") is not None
        and float(result["roi"]) <= float(baseline_roi)
    ):
        failures.append("roi_does_not_beat_market_baseline")
    return failures


def rank_predeclared_rules(
    bets: pd.DataFrame,
    rules: Iterable[dict[str, Any]],
    gate: StrictGate,
) -> list[dict[str, Any]]:
    """Evaluate predeclared rules and attach market baseline comparisons."""
    results: list[dict[str, Any]] = []
    for rule in rules:
        filtered = (
            _filter_rule_bets(
                bets,
                rule,
                max_hours_before_start=gate.max_hours_before_start,
            )
            if not bets.empty
            else pd.DataFrame()
        )
        result = _summarize_rule_bets(filtered, gate)
        result.update(
            {
                "rule_id": rule["id"],
                "status": rule.get("status", "candidate"),
                "kind": rule.get("kind", "candidate"),
                "market": rule["market"],
                "league": rule["league"],
                "model_type": rule["model_type"],
                "prediction_variant": rule["prediction_variant"],
                "feature_variant": rule.get("feature_variant"),
                "price_mode": rule.get("price_mode"),
                "timing_bucket": rule.get("timing_bucket"),
                "side": rule["side"],
                "min_edge": float(rule["min_edge"]),
                "rule": rule,
                "odds_timing_filter": {
                    "max_hours_before_start": gate.max_hours_before_start,
                    "stale_odds_excluded": (
                        _rule_timing_exclusion_count(
                            bets,
                            rule,
                            gate.max_hours_before_start,
                        )
                        if not bets.empty
                        else 0
                    ),
                },
                "clv_slices": _rule_clv_slices(filtered),
            }
        )
        results.append(result)

    lookup = {_baseline_lookup_key(result): result for result in results}
    for result in results:
        market_only = _find_baseline_result(lookup, result, "market_only")
        line_movement = _find_baseline_result(lookup, result, "line_movement")
        baseline_comparison = {
            "market_only": _compact_metrics(market_only),
            "line_movement": _compact_metrics(line_movement),
            "roi_delta_vs_market_only": None,
            "brier_delta_vs_market_probability": result.get("brier_delta_vs_market"),
        }
        market_roi = baseline_comparison["market_only"].get("roi")
        if result.get("roi") is not None and market_roi is not None:
            baseline_comparison["roi_delta_vs_market_only"] = float(result["roi"]) - float(
                market_roi
            )
        failures = _gate_failures(result, gate, market_only)
        publishable = not failures
        result["required_min_bets"] = (
            gate.min_bets_multi_league
            if str(result["league"]).upper() == "ALL"
            else gate.min_bets_narrow
        )
        result["market_baseline_comparison"] = baseline_comparison
        result["strict_gate_failures"] = failures
        result["publishable"] = publishable
        result["passes_strict_gate"] = publishable
        result["passes_launch_gate"] = publishable
        result["candidate_rule_yaml"] = _candidate_rule_yaml(result["rule"], publishable)

    results.sort(
        key=lambda row: (
            bool(row.get("publishable")),
            bool(row.get("model_beats_market_brier")),
            -_sort_number(row.get("brier_delta_vs_market"), 999.0),
            _sort_number(row.get("bootstrap_roi_low"), -999.0),
            _sort_number(row.get("avg_closing_line_value"), -999.0),
            _sort_number(row.get("closing_line_value_win_rate"), -999.0),
            _sort_number(row.get("roi"), -999.0),
            -_sort_number(row.get("max_drawdown"), 999999.0),
            row.get("bets") or 0,
        ),
        reverse=True,
    )
    for rank, result in enumerate(results, start=1):
        result["rank"] = rank
    return results


def _model_is_available(model_type: str) -> bool:
    if model_type in MARKET_BASELINE_MODELS:
        return True
    try:
        _build_estimator(model_type)
        return True
    except ImportError:
        return False


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


def _best_candidate_payload(ranked_rules: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [row for row in ranked_rules if row.get("kind") == "candidate"]
    if not candidates:
        return {"best_candidate_rule": None}
    best = candidates[0]
    return {
        "best_candidate_rule": best["rule"],
        "publishable": bool(best.get("publishable")),
        "rank": best.get("rank"),
        "metrics": {
            "roi": best.get("roi"),
            "bootstrap_roi_low": best.get("bootstrap_roi_low"),
            "bootstrap_roi_median": best.get("bootstrap_roi_median"),
            "bootstrap_roi_high": best.get("bootstrap_roi_high"),
            "bootstrap_roi_interval": best.get("bootstrap_roi_interval"),
            "brier_score": best.get("brier_score"),
            "market_brier_score": best.get("market_brier_score"),
            "win_rate": best.get("win_rate"),
            "sample_size": best.get("sample_size"),
            "avg_closing_line_value": best.get("avg_closing_line_value"),
            "closing_line_value_win_rate": best.get("closing_line_value_win_rate"),
            "closing_line_value_count": best.get("closing_line_value_count"),
            "max_drawdown": best.get("max_drawdown"),
            "losing_streak": best.get("losing_streak"),
            "odds_timing_filter": best.get("odds_timing_filter"),
            "clv_slices": best.get("clv_slices", []),
        },
        "strict_gate_failures": best.get("strict_gate_failures", []),
    }


def _candidate_rule_rankings(ranked_rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in ranked_rules:
        if row.get("kind") != "candidate":
            continue
        rows.append(
            {
                "rank": row.get("rank"),
                "rule_id": row.get("rule_id"),
                "market": row.get("market"),
                "league": row.get("league"),
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
                "max_drawdown": row.get("max_drawdown"),
                "losing_streak": row.get("losing_streak"),
                "required_min_bets": row.get("required_min_bets"),
                "odds_timing_filter": row.get("odds_timing_filter"),
                "clv_slices": row.get("clv_slices", []),
                "passes_strict_gate": row.get("passes_strict_gate"),
                "strict_gate_failures": row.get("strict_gate_failures", []),
            }
        )
    return rows


def run_betting_benchmark(
    db_path: Path = DB_PATH,
    config_path: Path = DEFAULT_CONFIG_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> BenchmarkArtifacts:
    config = load_benchmark_config(config_path)
    warn_if_low_availability_coverage(db_path=db_path, leagues=config.leagues)
    output_dir.mkdir(parents=True, exist_ok=True)
    available_candidates = [model for model in config.candidates if _model_is_available(model)]
    skipped_models = [model for model in config.candidates if model not in available_candidates]
    if skipped_models:
        LOGGER.info("Skipping unavailable benchmark candidates: %s", ", ".join(skipped_models))

    all_bets: list[pd.DataFrame] = []
    dataset_counts: dict[str, Any] = {}
    prediction_counts: dict[str, int] = {}

    if not db_path.exists():
        LOGGER.warning("Benchmark database does not exist: %s", db_path)
        dataset_counts["_database"] = {
            "loaded_rows": 0,
            "leagues": {},
            "error": f"database not found: {db_path}",
        }
    else:
        for market in config.markets:
            try:
                loaded_df, contract = load_training_frame(
                    market,
                    db_path=db_path,
                    leagues=config.leagues,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Skipping %s benchmark data load: %s", market, exc)
                dataset_counts[market] = {
                    "loaded_rows": 0,
                    "leagues": {},
                    "error": str(exc),
                }
                continue
            dataset_counts[market] = {"loaded_rows": int(len(loaded_df)), "leagues": {}}
            if loaded_df.empty:
                continue
            for league in config.leagues:
                league_df = loaded_df[loaded_df["league"].astype(str).str.upper() == league].copy()
                dataset_counts[market]["leagues"][league] = int(len(league_df))
                if len(league_df) < 2:
                    continue
                model_plan: list[tuple[str, tuple[PredictionVariant, ...]]] = [
                    (model_type, config.baseline_variants) for model_type in config.baselines
                ]
                model_plan.extend(
                    (model_type, config.candidate_variants) for model_type in available_candidates
                )
                for model_type, variants in model_plan:
                    predictions_by_variant = rolling_origin_predictions_for_variants(
                        league_df,
                        contract,
                        model_type,
                        variants,
                        rolling=config.rolling,
                    )
                    for variant_id, predictions in predictions_by_variant.items():
                        prediction_counts[f"{market}:{league}:{model_type}:{variant_id}"] = int(
                            len(predictions)
                        )
                        bets = expand_benchmark_bets(predictions, contract)
                        if not bets.empty:
                            all_bets.append(bets)

    bets = pd.concat(all_bets, ignore_index=True) if all_bets else pd.DataFrame()
    rules = [
        rule
        for rule in build_predeclared_rules(config)
        if rule["model_type"] in {*config.baselines, *available_candidates}
    ]
    ranked_rules = rank_predeclared_rules(bets, rules, config.strict_gate)
    generated_at = datetime.now(timezone.utc)
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    report_path = output_dir / f"betting_benchmark_{timestamp}.json"
    triage_report_path = output_dir / f"triage_{timestamp}.json"
    report = {
        "generated_at": generated_at.isoformat(),
        "db_path": str(db_path),
        "config_path": str(config_path),
        "triage_report_path": str(triage_report_path),
        "rolling_origin": asdict(config.rolling),
        "strict_gate": asdict(config.strict_gate),
        "sweep": {
            "markets": list(config.markets),
            "leagues": list(config.leagues),
            "baselines": list(config.baselines),
            "candidates": list(config.candidates),
            "available_candidates": available_candidates,
            "skipped_models": skipped_models,
            "min_edge_thresholds": list(config.min_edge_thresholds),
            "sides": {key: list(value) for key, value in config.sides.items()},
            "candidate_variants": [asdict(variant) for variant in config.candidate_variants],
        },
        "dataset_counts": dataset_counts,
        "prediction_counts": prediction_counts,
        "ranked_rules": ranked_rules,
        "candidate_rule_rankings": _candidate_rule_rankings(ranked_rules),
        "best_candidate": _best_candidate_payload(ranked_rules),
    }
    triage_report = build_benchmark_triage_report(
        ranked_rules,
        strict_gate=asdict(config.strict_gate),
        generated_at=report["generated_at"],
        source_report_path=report_path,
    )

    best_rule_json_path = output_dir / "best_candidate_rule.json"
    best_rule_yaml_path = output_dir / "best_candidate_rule.yml"
    report_path.write_text(json.dumps(report, indent=2, default=_json_default), encoding="utf-8")
    triage_report_path.write_text(
        json.dumps(triage_report, indent=2, default=_json_default),
        encoding="utf-8",
    )
    best_payload = report["best_candidate"]
    best_rule_json_path.write_text(
        json.dumps(best_payload, indent=2, default=_json_default),
        encoding="utf-8",
    )
    best_rule_yaml_path.write_text(yaml.safe_dump(best_payload, sort_keys=False), encoding="utf-8")
    LOGGER.info("Wrote ranked betting benchmark to %s", report_path)
    LOGGER.info("Wrote betting benchmark triage report to %s", triage_report_path)
    LOGGER.info(
        "Wrote best candidate rule exports to %s and %s",
        best_rule_json_path,
        best_rule_yaml_path,
    )
    return BenchmarkArtifacts(
        report_path,
        best_rule_json_path,
        best_rule_yaml_path,
        triage_report_path,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run rolling-origin betting rule benchmark search."
    )
    parser.add_argument("--db", type=Path, default=DB_PATH, help="SQLite database path.")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Predeclared benchmark grid config.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for benchmark artifacts.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    artifacts = run_betting_benchmark(
        db_path=args.db,
        config_path=args.config,
        output_dir=args.output_dir,
    )
    print(f"Ranked benchmark written to {artifacts.report_path}")
    print(f"Triage report written to {artifacts.triage_report_path}")
    print(f"Best candidate JSON written to {artifacts.best_rule_json_path}")
    print(f"Best candidate YAML written to {artifacts.best_rule_yaml_path}")


if __name__ == "__main__":
    main()
