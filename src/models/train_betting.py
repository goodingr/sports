"""Quality-first betting outcome training and rolling validation.

This module trains and validates models against betting outcomes instead of
only score error. It intentionally starts with explicit market baselines so a
candidate model must prove it beats the sportsbook-derived probability.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:  # pragma: no cover - optional runtime dependency
    from xgboost import XGBClassifier
except ImportError:  # pragma: no cover
    XGBClassifier = None  # type: ignore[assignment]

from src.db.core import DB_PATH
from src.features.betting_model_input import (
    DEFAULT_RELEASE_LEAGUES,
    build_moneyline_side_model_input,
    build_totals_model_input,
)
from src.models.prediction_quality import (
    american_profit,
    expected_value,
    settle_total_side,
    summarize_bets,
)

LOGGER = logging.getLogger(__name__)

MARKET_BASELINE_MODELS = {"market_only", "line_movement"}

FORBIDDEN_FEATURE_COLUMNS = {
    "actual_total",
    "home_score",
    "away_score",
    "target_over",
    "target_home_win",
    "is_push",
    "total_close",
}

TOTALS_FEATURE_COLUMNS = [
    "line",
    "over_moneyline",
    "under_moneyline",
    "best_over_moneyline",
    "best_under_moneyline",
    "over_no_vig_prob",
    "under_no_vig_prob",
    "market_hold",
    "hours_before_start",
    "opening_line",
    "opening_over_moneyline",
    "opening_under_moneyline",
    "line_movement",
    "over_moneyline_movement",
    "under_moneyline_movement",
    "book_line_count",
    "line_std",
    "home_score_for_l5",
    "home_score_against_l5",
    "home_game_total_l5",
    "home_games_l5",
    "home_rest_days",
    "home_back_to_back",
    "away_score_for_l5",
    "away_score_against_l5",
    "away_game_total_l5",
    "away_games_l5",
    "away_rest_days",
    "away_back_to_back",
    "rest_diff",
    "score_for_l5_diff",
    "score_against_l5_diff",
]

MONEYLINE_FEATURE_COLUMNS = [
    "is_home",
    "moneyline",
    "opponent_moneyline",
    "best_moneyline",
    "no_vig_prob",
    "opponent_no_vig_prob",
    "market_hold",
    "hours_before_start",
    "opening_moneyline",
    "moneyline_movement",
    "team_score_for_l5",
    "team_score_against_l5",
    "team_game_total_l5",
    "team_games_l5",
    "team_rest_days",
    "team_back_to_back",
    "opponent_score_for_l5",
    "opponent_score_against_l5",
    "opponent_game_total_l5",
    "opponent_games_l5",
    "opponent_rest_days",
    "opponent_back_to_back",
    "rest_diff",
    "score_for_l5_diff",
    "score_against_l5_diff",
]

NON_FEATURE_COLUMNS = {
    "game_id",
    "league",
    "start_time_utc",
    "snapshot_id",
    "snapshot_time_utc",
    "book",
    "book_id",
    "home_team",
    "away_team",
    "team",
    "opponent",
    "side",
    "best_book",
    "best_over_book",
    "best_under_book",
    "opening_snapshot_time_utc",
}

OPTIONAL_AGENT_FEATURE_PREFIXES = {
    "totals": (
        "home_fd_",
        "away_fd_",
        "home_injuries_",
        "away_injuries_",
        "home_player_",
        "away_player_",
        "home_rolling_",
        "away_rolling_",
        "home_season_",
        "away_season_",
        "home_ust_",
        "away_ust_",
    ),
    "moneyline": (
        "fd_",
        "injuries_",
        "opponent_fd_",
        "opponent_injuries_",
        "opponent_player_",
        "opponent_rolling_",
        "opponent_season_",
        "opponent_ust_",
        "player_",
        "rolling_",
        "season_",
        "team_player_",
        "team_rolling_",
        "team_season_",
        "team_ust_",
        "ust_",
    ),
}


@dataclass(frozen=True)
class FeatureContract:
    market: str
    target_column: str
    feature_columns: list[str]
    market_probability_column: str

    def validate(self, df: pd.DataFrame) -> None:
        forbidden = sorted(set(self.feature_columns).intersection(FORBIDDEN_FEATURE_COLUMNS))
        if forbidden:
            raise ValueError(f"Feature contract contains leakage columns: {forbidden}")
        missing = [column for column in self.feature_columns if column not in df.columns]
        if missing:
            raise ValueError(f"Training frame is missing feature columns: {missing}")
        if self.target_column not in df.columns:
            raise ValueError(f"Training frame is missing target column: {self.target_column}")
        if self.market_probability_column not in df.columns:
            raise ValueError(
                "Training frame is missing market baseline column: "
                f"{self.market_probability_column}"
            )


@dataclass(frozen=True)
class BettingTrainingArtifacts:
    model_path: Path
    metrics_path: Path
    predictions_path: Path


def _contract_for_market(market: str, df: pd.DataFrame) -> FeatureContract:
    market_key = market.lower()
    if market_key == "totals":
        columns = _available_feature_columns(df, TOTALS_FEATURE_COLUMNS, market_key)
        return FeatureContract("totals", "target_over", columns, "over_no_vig_prob")
    if market_key == "moneyline":
        columns = _available_feature_columns(df, MONEYLINE_FEATURE_COLUMNS, market_key)
        return FeatureContract("moneyline", "target_win", columns, "no_vig_prob")
    raise ValueError("market must be one of: totals, moneyline")


def _available_feature_columns(
    df: pd.DataFrame,
    declared_columns: Iterable[str],
    market: str,
) -> list[str]:
    columns = [column for column in declared_columns if column in df.columns]
    optional_prefixes = OPTIONAL_AGENT_FEATURE_PREFIXES.get(market, ())
    for column in df.columns:
        if (
            column in columns
            or column in NON_FEATURE_COLUMNS
            or column in FORBIDDEN_FEATURE_COLUMNS
        ):
            continue
        if not any(column.startswith(prefix) for prefix in optional_prefixes):
            continue
        numeric = pd.to_numeric(df[column], errors="coerce")
        if numeric.notna().any():
            columns.append(column)
    return columns


def load_training_frame(
    market: str,
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
) -> tuple[pd.DataFrame, FeatureContract]:
    """Load a leakage-safe training frame and matching feature contract."""
    if market == "totals":
        df = build_totals_model_input(db_path=db_path, leagues=leagues, latest_only=True)
        if df.empty:
            return df, _contract_for_market(market, df)
        df = df[df["target_over"].notna()].copy()
    elif market == "moneyline":
        df = build_moneyline_side_model_input(db_path=db_path, leagues=leagues, latest_only=True)
        if df.empty:
            return df, _contract_for_market(market, df)
        df = df[df["target_win"].notna()].copy()
    else:
        raise ValueError("market must be one of: totals, moneyline")

    df["start_time_utc"] = pd.to_datetime(df["start_time_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["start_time_utc"]).sort_values("start_time_utc").reset_index(drop=True)
    contract = _contract_for_market(market, df)
    contract.validate(df)
    return df, contract


def _build_estimator(model_type: str) -> Pipeline:
    if model_type == "logistic":
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("clf", LogisticRegression(max_iter=1000)),
            ]
        )
    if model_type == "gradient_boosting":
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "clf",
                    HistGradientBoostingClassifier(
                        learning_rate=0.05,
                        max_iter=400,
                        max_leaf_nodes=31,
                        l2_regularization=0.1,
                        early_stopping=True,
                        random_state=42,
                    ),
                ),
            ]
        )
    if model_type == "random_forest":
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "clf",
                    RandomForestClassifier(
                        n_estimators=300,
                        min_samples_leaf=5,
                        max_features="sqrt",
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        )
    if model_type == "xgboost":
        if XGBClassifier is None:
            raise ImportError("xgboost is required for model_type='xgboost'")
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "clf",
                    XGBClassifier(
                        objective="binary:logistic",
                        eval_metric="logloss",
                        learning_rate=0.04,
                        max_depth=4,
                        n_estimators=500,
                        subsample=0.85,
                        colsample_bytree=0.85,
                        reg_lambda=1.0,
                        random_state=42,
                        n_jobs=1,
                    ),
                ),
            ]
        )
    raise ValueError(
        "model_type must be market_only, line_movement, logistic, "
        "gradient_boosting, random_forest, or xgboost"
    )


def _rolling_origin_splits(
    df: pd.DataFrame,
    folds: int = 5,
    min_train_size: int = 100,
) -> list[tuple[np.ndarray, np.ndarray]]:
    if len(df) < 2:
        raise ValueError("Need at least two settled games for validation")
    ordered_idx = np.arange(len(df))
    min_train_size = min(min_train_size, max(1, len(df) // 2))
    remaining = len(df) - min_train_size
    if remaining <= 0:
        return [(ordered_idx[:-1], ordered_idx[-1:])]
    fold_count = max(1, min(folds, remaining))
    test_size = max(1, remaining // fold_count)
    splits: list[tuple[np.ndarray, np.ndarray]] = []
    for fold in range(fold_count):
        train_end = min_train_size + fold * test_size
        test_end = len(df) if fold == fold_count - 1 else min(len(df), train_end + test_size)
        if train_end >= test_end:
            continue
        splits.append((ordered_idx[:train_end], ordered_idx[train_end:test_end]))
    return splits or [(ordered_idx[:-1], ordered_idx[-1:])]


def _line_movement_probability(
    df: pd.DataFrame,
    market: str,
    market_probability_column: str,
) -> np.ndarray:
    base = pd.to_numeric(df[market_probability_column], errors="coerce").fillna(0.5).to_numpy()
    if market == "totals" and "line_movement" in df.columns:
        adjustment = (
            pd.to_numeric(df["line_movement"], errors="coerce").fillna(0.0).to_numpy()
            * 0.015
        )
    elif market == "moneyline" and "moneyline_movement" in df.columns:
        adjustment = (
            -pd.to_numeric(df["moneyline_movement"], errors="coerce").fillna(0.0).to_numpy()
            * 0.0005
        )
    else:
        adjustment = 0.0
    return np.clip(base + adjustment, 0.01, 0.99)


def _predict_probabilities(
    model_type: str,
    estimator: Any,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    contract: FeatureContract,
) -> np.ndarray:
    if model_type == "market_only":
        return (
            pd.to_numeric(test_df[contract.market_probability_column], errors="coerce")
            .fillna(0.5)
            .to_numpy()
        )
    if model_type == "line_movement":
        return _line_movement_probability(
            test_df,
            contract.market,
            contract.market_probability_column,
        )

    X_train = train_df[contract.feature_columns].apply(pd.to_numeric, errors="coerce")
    y_train = train_df[contract.target_column].astype(int)
    X_test = test_df[contract.feature_columns].apply(pd.to_numeric, errors="coerce")
    estimator.fit(X_train, y_train)
    return np.clip(estimator.predict_proba(X_test)[:, 1], 0.001, 0.999)


def _probability_metrics(
    y_true: np.ndarray,
    probs: np.ndarray,
    market_probs: np.ndarray,
) -> dict[str, Any]:
    labels = np.array([0, 1])
    try:
        model_log_loss = float(log_loss(y_true, np.column_stack([1 - probs, probs]), labels=labels))
    except ValueError:
        model_log_loss = None
    try:
        market_log_loss = float(
            log_loss(y_true, np.column_stack([1 - market_probs, market_probs]), labels=labels)
        )
    except ValueError:
        market_log_loss = None
    model_brier = float(brier_score_loss(y_true, probs))
    market_brier = float(brier_score_loss(y_true, market_probs))
    return {
        "brier_score": model_brier,
        "market_brier_score": market_brier,
        "beats_market_brier": model_brier < market_brier,
        "log_loss": model_log_loss,
        "market_log_loss": market_log_loss,
        "accuracy": float(np.mean((probs >= 0.5).astype(int) == y_true)),
    }


def _best_or_selected(row: pd.Series, best_column: str, selected_column: str) -> float:
    best = row.get(best_column)
    if pd.notna(best):
        return float(best)
    return float(row[selected_column])


def _totals_validation_bets(df: pd.DataFrame, edge_threshold: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        total = float(row["actual_total"])
        line = float(row["line"])
        for side in ("over", "under"):
            prob = float(row["predicted_prob"] if side == "over" else 1.0 - row["predicted_prob"])
            market_prob = float(
                row["over_no_vig_prob"] if side == "over" else row["under_no_vig_prob"]
            )
            best_col = "best_over_moneyline" if side == "over" else "best_under_moneyline"
            selected_col = "over_moneyline" if side == "over" else "under_moneyline"
            moneyline = _best_or_selected(row, best_col, selected_col)
            edge = prob - market_prob
            if edge < edge_threshold:
                continue
            won = settle_total_side(total, line, side)
            if won is None:
                continue
            rows.append(
                {
                    "league": row["league"],
                    "model_type": row["model_type"],
                    "side": side,
                    "predicted_prob": prob,
                    "implied_prob": market_prob,
                    "no_vig_market_prob": market_prob,
                    "edge": edge,
                    "moneyline": moneyline,
                    "won": won,
                    "profit": american_profit(moneyline, won),
                    "expected_value": expected_value(prob, moneyline),
                    "actual_value": american_profit(moneyline, won),
                    "predicted_at": row["snapshot_time_utc"],
                    "start_time_utc": row["start_time_utc"],
                }
            )
    return pd.DataFrame(rows)


def _moneyline_validation_bets(df: pd.DataFrame, edge_threshold: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        prob = float(row["predicted_prob"])
        market_prob = float(row["no_vig_prob"])
        edge = prob - market_prob
        if edge < edge_threshold:
            continue
        moneyline = _best_or_selected(row, "best_moneyline", "moneyline")
        won = bool(row["target_win"])
        rows.append(
            {
                "league": row["league"],
                "model_type": row["model_type"],
                "side": row["side"],
                "predicted_prob": prob,
                "implied_prob": market_prob,
                "no_vig_market_prob": market_prob,
                "edge": edge,
                "moneyline": moneyline,
                "won": won,
                "profit": american_profit(moneyline, won),
                "expected_value": expected_value(prob, moneyline),
                "actual_value": american_profit(moneyline, won),
                "predicted_at": row["snapshot_time_utc"],
                "start_time_utc": row["start_time_utc"],
            }
        )
    return pd.DataFrame(rows)


def rolling_origin_validate(
    df: pd.DataFrame,
    contract: FeatureContract,
    model_type: str,
    edge_threshold: float = 0.04,
    folds: int = 5,
    min_train_size: int = 100,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Run expanding-window validation and return row-level predictions plus summary."""
    contract.validate(df)
    df = df.sort_values("start_time_utc").reset_index(drop=True)
    splits = _rolling_origin_splits(df, folds=folds, min_train_size=min_train_size)
    prediction_frames: list[pd.DataFrame] = []
    fold_metrics: list[dict[str, Any]] = []

    for fold_index, (train_idx, test_idx) in enumerate(splits, start=1):
        train_df = df.iloc[train_idx].copy()
        test_df = df.iloc[test_idx].copy()
        estimator = None if model_type in MARKET_BASELINE_MODELS else _build_estimator(model_type)
        probs = _predict_probabilities(model_type, estimator, train_df, test_df, contract)
        market_probs = (
            pd.to_numeric(test_df[contract.market_probability_column], errors="coerce")
            .fillna(0.5)
            .to_numpy()
        )
        y_true = test_df[contract.target_column].astype(int).to_numpy()

        fold = _probability_metrics(y_true, probs, market_probs)
        fold.update(
            {
                "fold": fold_index,
                "train_rows": int(len(train_df)),
                "test_rows": int(len(test_df)),
                "start": test_df["start_time_utc"].min().isoformat(),
                "end": test_df["start_time_utc"].max().isoformat(),
            }
        )
        fold_metrics.append(fold)

        fold_predictions = test_df.copy()
        fold_predictions["predicted_prob"] = probs
        fold_predictions["market_prob"] = market_probs
        fold_predictions["model_type"] = model_type
        fold_predictions["validation_fold"] = fold_index
        prediction_frames.append(fold_predictions)

    predictions = (
        pd.concat(prediction_frames, ignore_index=True) if prediction_frames else pd.DataFrame()
    )
    y_all = predictions[contract.target_column].astype(int).to_numpy()
    probs_all = predictions["predicted_prob"].to_numpy()
    market_all = predictions["market_prob"].to_numpy()
    summary = _probability_metrics(y_all, probs_all, market_all) if len(predictions) else {}

    bets = (
        _totals_validation_bets(predictions, edge_threshold)
        if contract.market == "totals"
        else _moneyline_validation_bets(predictions, edge_threshold)
    )
    summary.update(
        {
            "market": contract.market,
            "model_type": model_type,
            "rows": int(len(df)),
            "validation_rows": int(len(predictions)),
            "edge_threshold": edge_threshold,
            "folds": fold_metrics,
            "betting_rule": summarize_bets(bets) if not bets.empty else {"bets": 0, "roi": None},
        }
    )
    return predictions, summary


def train_betting_model(
    market: str,
    model_type: str,
    db_path: Path = DB_PATH,
    leagues: Optional[Iterable[str]] = DEFAULT_RELEASE_LEAGUES,
    edge_threshold: float = 0.04,
    output_dir: Path = Path("reports/backtests"),
    models_dir: Path = Path("models"),
) -> BettingTrainingArtifacts:
    df, contract = load_training_frame(market, db_path=db_path, leagues=leagues)
    if df.empty:
        raise RuntimeError(f"No settled {market} training rows were found")
    predictions, metrics = rolling_origin_validate(
        df,
        contract,
        model_type=model_type,
        edge_threshold=edge_threshold,
    )

    final_estimator = None
    if model_type not in MARKET_BASELINE_MODELS:
        final_estimator = _build_estimator(model_type)
        final_estimator.fit(
            df[contract.feature_columns].apply(pd.to_numeric, errors="coerce"),
            df[contract.target_column].astype(int),
        )

    league_tag = "all" if not leagues else "_".join(league.lower() for league in leagues)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    model_name = f"{market}_{model_type}_{league_tag}_{timestamp}"
    models_dir.mkdir(parents=True, exist_ok=True)
    model_path = models_dir / f"{model_name}.pkl"
    joblib.dump(
        {
            "estimator": final_estimator,
            "model_type": model_type,
            "feature_contract": asdict(contract),
            "leagues": list(leagues or []),
            "trained_at": datetime.now(timezone.utc).isoformat(),
        },
        model_path,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    predictions_path = output_dir / f"{model_name}_validation_predictions.parquet"
    predictions.to_parquet(predictions_path, index=False)
    metrics_path = output_dir / f"{model_name}_quality.json"
    metrics["feature_contract"] = asdict(contract)
    metrics["leagues"] = list(leagues or [])
    metrics["model_path"] = str(model_path)
    metrics["predictions_path"] = str(predictions_path)
    metrics_path.write_text(json.dumps(metrics, indent=2, default=str), encoding="utf-8")
    LOGGER.info("Saved %s betting model bundle to %s", market, model_path)
    LOGGER.info("Saved validation metrics to %s", metrics_path)
    return BettingTrainingArtifacts(model_path, metrics_path, predictions_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train quality-first betting outcome models.")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--market", choices=["totals", "moneyline"], default="totals")
    parser.add_argument(
        "--model-type",
        choices=[
            "market_only",
            "line_movement",
            "logistic",
            "gradient_boosting",
            "random_forest",
            "xgboost",
        ],
        default="logistic",
    )
    parser.add_argument("--leagues", default=",".join(DEFAULT_RELEASE_LEAGUES))
    parser.add_argument("--edge-threshold", type=float, default=0.04)
    parser.add_argument("--output-dir", type=Path, default=Path("reports/backtests"))
    parser.add_argument("--models-dir", type=Path, default=Path("models"))
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run the predeclared rolling-origin benchmark grid instead of training one model.",
    )
    parser.add_argument(
        "--benchmark-config",
        type=Path,
        default=Path("config/betting_benchmark.yml"),
        help="Benchmark grid config used with --benchmark.",
    )
    parser.add_argument(
        "--benchmark-output-dir",
        type=Path,
        default=Path("reports/betting_benchmarks"),
        help="Benchmark artifact directory used with --benchmark.",
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
    if args.benchmark:
        from src.models.betting_benchmark import run_betting_benchmark

        artifacts = run_betting_benchmark(
            db_path=args.db,
            config_path=args.benchmark_config,
            output_dir=args.benchmark_output_dir,
        )
        print(f"Ranked benchmark written to {artifacts.report_path}")
        print(f"Best candidate JSON written to {artifacts.best_rule_json_path}")
        print(f"Best candidate YAML written to {artifacts.best_rule_yaml_path}")
        return
    leagues = [league.strip().upper() for league in args.leagues.split(",") if league.strip()]
    artifacts = train_betting_model(
        args.market,
        args.model_type,
        db_path=args.db,
        leagues=leagues,
        edge_threshold=args.edge_threshold,
        output_dir=args.output_dir,
        models_dir=args.models_dir,
    )
    print(f"Model bundle saved to {artifacts.model_path}")
    print(f"Validation metrics written to {artifacts.metrics_path}")
    print(f"Validation predictions written to {artifacts.predictions_path}")


if __name__ == "__main__":
    main()
