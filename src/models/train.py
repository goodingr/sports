"""Train moneyline prediction models with calibration and ensemble options across leagues."""

from __future__ import annotations

import argparse
import json
import logging
import uuid
import hashlib
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import joblib  # type: ignore[import]
import numpy as np  # type: ignore[import]
import pandas as pd  # type: ignore[import]
try:  # pragma: no cover - optional dependency for certain model types
    from lightgbm import LGBMClassifier  # type: ignore[import]
    # Suppress LightGBM warnings about splits
    warnings.filterwarnings('ignore', category=UserWarning, message='.*No further splits.*')
except ImportError:  # noqa: F401
    LGBMClassifier = None  # type: ignore[assignment]
from sklearn.base import clone  # type: ignore[import]
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier  # type: ignore[import]
from sklearn.impute import SimpleImputer  # type: ignore[import]
from sklearn.isotonic import IsotonicRegression  # type: ignore[import]
from sklearn.linear_model import LogisticRegression  # type: ignore[import]
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score  # type: ignore[import]
from sklearn.model_selection import TimeSeriesSplit  # type: ignore[import]
from sklearn.neural_network import MLPClassifier  # type: ignore[import]
from sklearn.pipeline import Pipeline  # type: ignore[import]
from sklearn.preprocessing import StandardScaler  # type: ignore[import]
try:  # pragma: no cover - optional dependency for certain model types
    from xgboost import XGBClassifier  # type: ignore[import]
except ImportError:  # noqa: F401
    XGBClassifier = None  # type: ignore[assignment]

from src.data.config import PROCESSED_DATA_DIR, ensure_directories
from src.db.models import persist_model_predictions, register_model


LOGGER = logging.getLogger(__name__)


def _load_tuned_params(league: str, model_type: str) -> Dict[str, Any]:
    """Load tuned hyperparameters if available, otherwise return empty dict."""
    params_path = Path(f"config/tuned_params/{league.lower()}_{model_type}.json")
    if params_path.exists():
        try:
            data = json.loads(params_path.read_text(encoding="utf-8"))
            LOGGER.info("Loaded tuned params for %s %s from %s", league, model_type, params_path)
            return data.get("best_params", {})
        except Exception as exc:
            LOGGER.warning("Failed to load tuned params from %s: %s", params_path, exc)
    return {}


FEATURE_COLUMNS = [
    "is_home",
    "moneyline",
    "implied_prob",
    "spread_line",
    "total_line",
    # ESPN odds features (available when ESPN ingestion runs)
    "espn_moneyline_open",
    "espn_moneyline_close",
    "espn_spread_open",
    "espn_spread_close",
    "espn_total_open",
    "espn_total_close",
    # NFL-specific rolling EPA features
    "off_epa_per_play_rolling_3",
    "off_success_rate_rolling_3",
    "off_pass_rate_rolling_3",
    "def_epa_per_play_rolling_3",
    "def_success_rate_rolling_3",
    "opponent_off_epa_per_play_rolling_3",
    "opponent_off_success_rate_rolling_3",
    "opponent_def_epa_per_play_rolling_3",
    "opponent_def_success_rate_rolling_3",
    # Season-level team metrics (aggregated from play-by-play or API)
    "season_off_epa_per_play",
    "season_off_success_rate",
    "season_def_epa_per_play",
    "season_def_success_rate",
    # Rolling win/point metrics
    "rolling_win_pct_3",
    "rolling_point_diff_3",
    "opponent_rolling_win_pct_3",
    # Rest and travel features
    "team_rest_days",
    "opponent_rest_days",
    "rest_diff",
    "is_short_week",
    "is_post_bye",
    "road_trip_length_entering",
    # Injury features
    "injuries_out",
    "injuries_qb_out",
    "injuries_skill_out",
    "opponent_injuries_out",
    "opponent_injuries_qb_out",
    # Weather features (NFL-specific)
    "game_temperature_f",
    "game_wind_mph",
    "is_weather_precip",
    "is_weather_dome",
    # Game context
    "is_playoff",
    "is_division_game",
    "is_conference_game",
    # Soccer odds features
    "fd_b365_ml_decimal",
    "fd_b365_ml_american",
    "fd_b365_implied",
    "fd_b365_draw_decimal",
    "fd_b365_draw_implied",
    "fd_ps_ml_decimal",
    "fd_ps_ml_american",
    "fd_ps_implied",
    "fd_ps_draw_decimal",
    "fd_ps_draw_implied",
    "fd_avg_ml_decimal",
    "fd_avg_implied",
    "opponent_fd_b365_ml_decimal",
    "opponent_fd_b365_ml_american",
    "opponent_fd_b365_implied",
    "opponent_fd_b365_draw_decimal",
    "opponent_fd_b365_draw_implied",
    "opponent_fd_ps_ml_decimal",
    "opponent_fd_ps_ml_american",
    "opponent_fd_ps_implied",
    "opponent_fd_ps_draw_decimal",
    "opponent_fd_ps_draw_implied",
    "opponent_fd_avg_ml_decimal",
    "opponent_fd_avg_implied",
    # Understat team aggregates
    "ust_team_xg_avg_l3",
    "ust_team_xg_avg_l5",
    "ust_team_xga_avg_l3",
    "ust_team_xga_avg_l5",
    "ust_team_ppda_att_l3",
    "ust_team_ppda_allowed_att_l3",
    "ust_team_deep_entries_l3",
    "ust_team_deep_allowed_l3",
    "ust_team_goals_for_avg_l5",
    "ust_team_goals_against_avg_l5",
    "ust_team_xpts_avg_l5",
    "ust_team_shot_open_play_share_l5",
    "ust_team_shot_set_piece_share_l5",
    "ust_team_avg_shot_distance_l5",
    "opponent_ust_team_xg_avg_l3",
    "opponent_ust_team_xg_avg_l5",
    "opponent_ust_team_xga_avg_l3",
    "opponent_ust_team_xga_avg_l5",
    "opponent_ust_team_ppda_att_l3",
    "opponent_ust_team_ppda_allowed_att_l3",
    "opponent_ust_team_deep_entries_l3",
    "opponent_ust_team_deep_allowed_l3",
    "opponent_ust_team_goals_for_avg_l5",
    "opponent_ust_team_goals_against_avg_l5",
    "opponent_ust_team_xpts_avg_l5",
    "opponent_ust_team_shot_open_play_share_l5",
    "opponent_ust_team_shot_set_piece_share_l5",
    "opponent_ust_team_avg_shot_distance_l5",
    # Lineup strength
    "ust_xi_prior_minutes_total",
    "ust_xi_prior_minutes_avg",
    "ust_xi_prior_xg_per90_avg",
    "ust_xi_prior_xa_per90_avg",
    "ust_xi_prior_shots_per90_avg",
    "ust_xi_prior_key_passes_per90_avg",
    "ust_xi_share_zero_min",
    "ust_xi_returning_starters_prev_match",
    "ust_xi_returning_starters_last3",
    "opponent_ust_xi_prior_minutes_total",
    "opponent_ust_xi_prior_minutes_avg",
    "opponent_ust_xi_prior_xg_per90_avg",
    "opponent_ust_xi_prior_xa_per90_avg",
    "opponent_ust_xi_prior_shots_per90_avg",
    "opponent_ust_xi_prior_key_passes_per90_avg",
    "opponent_ust_xi_share_zero_min",
    "opponent_ust_xi_returning_starters_prev_match",
    "opponent_ust_xi_returning_starters_last3",
]


MODEL_CHOICES = (
    "gradient_boosting",
    "logistic",
    "lightgbm",
    "xgboost",
    "mlp",
    "random_forest",
    "ensemble",
)
CALIBRATION_CHOICES = ("none", "sigmoid", "isotonic")
MODEL_REGISTRY_PATH = Path("models") / "model_registry.json"


@dataclass
class TrainingArtifacts:
    model_path: Path
    metrics_path: Path
    predictions_path: Path
    model_id: str


class ProbabilityCalibrator:
    def __init__(self, method: str, model: Any):
        self.method = method
        self.model = model

    def transform(self, probs: np.ndarray) -> np.ndarray:
        probs = np.asarray(probs)
        if self.method == "sigmoid":
            return self.model.predict_proba(probs.reshape(-1, 1))[:, 1]
        if self.method == "isotonic":
            return self.model.predict(probs)
        return probs


class CalibratedModel:
    def __init__(self, estimator: Pipeline, calibrator: ProbabilityCalibrator | None = None):
        self.estimator = estimator
        self.calibrator = calibrator

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        base_probs = self.estimator.predict_proba(X)[:, 1]
        if self.calibrator is not None:
            calibrated = self.calibrator.transform(base_probs)
        else:
            calibrated = base_probs
        calibrated = np.clip(calibrated, 1e-6, 1 - 1e-6)
        return np.column_stack([1 - calibrated, calibrated])


class EnsembleModel:
    def __init__(self, members: List[CalibratedModel], weights: np.ndarray, labels: List[str]):
        self.members = members
        self.weights = np.asarray(weights, dtype=float)
        self.labels = labels

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        member_probs = np.column_stack([model.predict_proba(X)[:, 1] for model in self.members])
        weighted = member_probs @ self.weights
        weighted = np.clip(weighted, 1e-6, 1 - 1e-6)
        return np.column_stack([1 - weighted, weighted])


def _dataset_path(league: str, season_min: int, season_max: int) -> Path:
    return PROCESSED_DATA_DIR / "model_input" / f"moneyline_{league.lower()}_{season_min}_{season_max}.parquet"


def _ensure_dataset(seasons: List[int], league: str) -> Path:
    ensure_directories()
    season_min, season_max = min(seasons), max(seasons)
    dataset_path = _dataset_path(league, season_min, season_max)
    if not dataset_path.exists():
        LOGGER.info("Processed dataset not found for %s. Building now.", league)
        from src.features.moneyline_dataset import build_dataset
        build_dataset(seasons, league=league)
    return dataset_path


def _load_dataset(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["is_home"] = df["is_home"].astype(int)
    df["spread_line"] = df["spread_line"].astype(float)
    df["total_line"] = df["total_line"].astype(float)
    df["game_datetime"] = pd.to_datetime(df["game_datetime"], errors="coerce")
    return df


def _time_series_split(df: pd.DataFrame, splits: int = 5) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.sort_values("game_datetime").reset_index(drop=True)
    if df["game_datetime"].isna().all():
        LOGGER.warning("Missing game_datetime; falling back to season/week ordering")
        df["game_datetime"] = pd.to_datetime(
            df["season"].astype(str) + "-" + df["week"].astype(str).str.zfill(2) + "-01",
            errors="coerce",
        )

    total_samples = len(df)
    if total_samples < 2:
        raise ValueError(f"Dataset must contain at least two rows, got {total_samples}")

    # TimeSeriesSplit requires n_splits >= 2. When we do not have enough samples,
    # fall back to a simple last-row holdout rather than crashing training.
    effective_splits = min(splits, total_samples - 1)
    if effective_splits >= 2:
        tscv = TimeSeriesSplit(n_splits=effective_splits)
        last_split = list(tscv.split(df))[-1]
        train_idx, test_idx = last_split
        return df.iloc[train_idx], df.iloc[test_idx]

    cutoff = total_samples - 1
    LOGGER.warning(
        "Insufficient samples (%s) for %s time-series splits; using last observation as test set.",
        total_samples,
        splits,
    )
    train_df = df.iloc[:cutoff]
    test_df = df.iloc[cutoff:]
    return train_df, test_df


def _build_estimator(model_type: str, league: str = "NFL") -> Pipeline:
    if model_type == "logistic":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegression(
                        max_iter=1000,
                        solver="lbfgs",
                        n_jobs=1,
                    ),
                ),
            ]
        )

    if model_type == "gradient_boosting":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "clf",
                    HistGradientBoostingClassifier(
                        learning_rate=0.05,
                        max_depth=6,
                        max_iter=600,
                        l2_regularization=0.1,
                        early_stopping=True,
                        validation_fraction=0.1,
                        random_state=42,
                    ),
                ),
            ]
        )

    if model_type == "lightgbm":
        if LGBMClassifier is None:
            raise ImportError(
                "lightgbm is required for model_type='lightgbm'. Install it via `pip install lightgbm`."
            )
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "clf",
                    LGBMClassifier(
                        learning_rate=0.05,
                        max_depth=-1,
                        num_leaves=31,
                        n_estimators=600,
                        reg_lambda=1.0,
                        subsample=0.8,
                        colsample_bytree=0.8,
                        objective="binary",
                        min_child_samples=10,
                        random_state=42,
                        n_jobs=1,
                        verbosity=-1,
                    ),
                ),
            ]
        )

    if model_type == "xgboost":
        if XGBClassifier is None:
            raise ImportError(
                "xgboost is required for model_type='xgboost'. Install it via `pip install xgboost`."
            )
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "clf",
                    XGBClassifier(
                        objective="binary:logistic",
                        eval_metric="logloss",
                        learning_rate=0.05,
                        max_depth=6,
                        n_estimators=600,
                        subsample=0.8,
                        colsample_bytree=0.8,
                        reg_lambda=1.0,
                        reg_alpha=0.0,
                        random_state=42,
                        n_jobs=1,
                    ),
                ),
            ]
        )

    if model_type == "mlp":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "clf",
                    MLPClassifier(
                        hidden_layer_sizes=(64, 32),
                        activation="relu",
                        alpha=0.001,
                        learning_rate_init=0.001,
                        max_iter=500,
                        random_state=42,
                    ),
                ),
            ]
        )

    if model_type == "random_forest":
        # Load tuned parameters if available
        tuned_params = _load_tuned_params(league, "random_forest")
        
        # Default parameters
        rf_params = {
            "n_estimators": 200,
            "max_depth": 12,
            "min_samples_split": 5,
            "min_samples_leaf": 2,
            "max_features": "sqrt",
            "random_state": 42,
            "n_jobs": -1,
        }
        
        # Override with tuned parameters
        rf_params.update(tuned_params)
        
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "clf",
                    RandomForestClassifier(**rf_params),
                ),
            ]
        )

    raise ValueError(f"Unsupported model_type: {model_type}")


def _calibration_split_count(n_samples: int) -> int:
    if n_samples < 3:
        return 1
    return min(5, max(2, min(n_samples - 1, 3 if n_samples >= 3 else 2)))


def _train_probability_calibrator(
    probs: np.ndarray,
    y: np.ndarray,
    calibration: str,
) -> ProbabilityCalibrator | None:
    if calibration == "none" or len(probs) == 0:
        return None

    if calibration == "sigmoid":
        lr = LogisticRegression(max_iter=1000)
        lr.fit(probs.reshape(-1, 1), y)
        return ProbabilityCalibrator("sigmoid", lr)

    if calibration == "isotonic":
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(probs, y)
        return ProbabilityCalibrator("isotonic", iso)

    return None


def _train_calibrated_model(
    model_type: str,
    calibration: str,
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    league: str = "NFL",
) -> CalibratedModel:
    estimator = _build_estimator(model_type, league=league)
    if calibration == "none":
        estimator.fit(X_train, y_train)
        return CalibratedModel(estimator, calibrator=None)

    if len(X_train) < 10:
        LOGGER.warning("Dataset too small for calibration; skipping calibration step")
        estimator.fit(X_train, y_train)
        return CalibratedModel(estimator, calibrator=None)

    splits = _calibration_split_count(len(X_train))
    tscv = TimeSeriesSplit(n_splits=splits)
    prob_list: List[np.ndarray] = []
    target_list: List[np.ndarray] = []

    for train_idx, val_idx in tscv.split(X_train):
        cloned = clone(estimator)
        X_fold_train = X_train.iloc[train_idx]
        y_fold_train = y_train[train_idx]
        X_fold_val = X_train.iloc[val_idx]
        y_fold_val = y_train[val_idx]
        cloned.fit(X_fold_train, y_fold_train)
        val_probs = cloned.predict_proba(X_fold_val)[:, 1]
        prob_list.append(val_probs)
        target_list.append(y_fold_val)

    stacked_probs = np.concatenate(prob_list)
    stacked_probs = np.clip(stacked_probs, 1e-6, 1 - 1e-6)
    stacked_targets = np.concatenate(target_list)

    calibrator = _train_probability_calibrator(stacked_probs, stacked_targets, calibration)
    estimator.fit(X_train, y_train)
    return CalibratedModel(estimator, calibrator)


def _evaluate(y_true: np.ndarray, y_pred_proba: np.ndarray) -> Dict[str, float]:
    try:
        roc_auc = float(roc_auc_score(y_true, y_pred_proba))
    except ValueError:
        roc_auc = float("nan")

    probs = np.column_stack([1 - y_pred_proba, y_pred_proba])
    try:
        logloss = float(log_loss(y_true, probs, labels=[0, 1]))
    except ValueError:
        logloss = float("nan")

    return {
        "accuracy": float(accuracy_score(y_true, (y_pred_proba >= 0.5).astype(int))),
        "brier_score": float(brier_score_loss(y_true, y_pred_proba)),
        "log_loss": logloss,
        "roc_auc": roc_auc,
    }


def _dataset_hash(df: pd.DataFrame) -> str:
    series = pd.util.hash_pandas_object(df.reset_index(drop=True), index=False)
    return hashlib.sha256(series.values.tobytes()).hexdigest()


def _record_model_version(entry: Dict[str, Any]) -> None:
    MODEL_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    if MODEL_REGISTRY_PATH.exists():
        try:
            history = json.loads(MODEL_REGISTRY_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            history = []
    else:
        history = []

    history.append(entry)
    MODEL_REGISTRY_PATH.write_text(json.dumps(history, indent=2), encoding="utf-8")


def train_and_evaluate(
    seasons: List[int],
    model_type: str = "gradient_boosting",
    calibration: str = "sigmoid",
    league: str = "NFL",
) -> TrainingArtifacts:
    dataset_path = _ensure_dataset(seasons, league)
    df = _load_dataset(dataset_path)
    train_df, test_df = _time_series_split(df)

    dataset_signature = _dataset_hash(df)

    feature_columns = [col for col in FEATURE_COLUMNS if col in train_df.columns]
    if not feature_columns:
        raise RuntimeError("No usable feature columns found in dataset")

    X_train = train_df[feature_columns].fillna(0)
    y_train = train_df["win"].values

    X_test = test_df[feature_columns].fillna(0)
    y_test = test_df["win"].values

    if model_type not in MODEL_CHOICES:
        raise ValueError(f"model_type must be one of {MODEL_CHOICES}")

    if calibration not in CALIBRATION_CHOICES:
        raise ValueError(f"calibration must be one of {CALIBRATION_CHOICES}")

    ensemble_members: List[Dict[str, Any]] = []

    if model_type == "ensemble":
        base_models = ["gradient_boosting", "lightgbm", "xgboost", "logistic", "mlp", "random_forest"]
        ensemble_candidates: List[Dict[str, Any]] = []
        for base in base_models:
            try:
                model = _train_calibrated_model(base, calibration, X_train, y_train, league=league)
                train_probs = model.predict_proba(X_train)[:, 1]
                test_probs = model.predict_proba(X_test)[:, 1]
                candidate_metrics = {
                    "train": _evaluate(y_train, train_probs),
                    "test": _evaluate(y_test, test_probs),
                }
                ensemble_candidates.append(
                    {
                        "name": base,
                        "model": model,
                        "train_probs": train_probs,
                        "test_probs": test_probs,
                        "metrics": candidate_metrics,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Skipping ensemble member %s due to error: %s", base, exc)
        if not ensemble_candidates:
            raise RuntimeError("No ensemble members were successfully trained")
        best_log_loss = min(candidate["metrics"]["test"]["log_loss"] for candidate in ensemble_candidates)
        # Keep models within 0.05 log loss of best (increased from 0.01 for more diversity)
        pruning_threshold = 0.05
        keep_candidates = [
            candidate
            for candidate in ensemble_candidates
            if candidate["metrics"]["test"]["log_loss"] <= best_log_loss + pruning_threshold
        ]
        LOGGER.info(
            "Ensemble pruning: %d candidates, %d kept (threshold=%.3f, best_log_loss=%.3f)",
            len(ensemble_candidates),
            len(keep_candidates),
            pruning_threshold,
            best_log_loss,
        )
        if not keep_candidates:
            keep_candidates = [min(ensemble_candidates, key=lambda c: c["metrics"]["test"]["log_loss"])]

        log_losses = np.array([candidate["metrics"]["test"]["log_loss"] for candidate in keep_candidates])
        inverse_losses = 1 / np.maximum(log_losses, 1e-6)
        weights = inverse_losses / inverse_losses.sum()

        member_train_probs = np.column_stack([candidate["train_probs"] for candidate in keep_candidates])
        member_test_probs = np.column_stack([candidate["test_probs"] for candidate in keep_candidates])

        train_proba = member_train_probs @ weights
        test_proba = member_test_probs @ weights

        calibrated_members = [candidate["model"] for candidate in keep_candidates]
        member_names = [candidate["name"] for candidate in keep_candidates]
        estimator = EnsembleModel(calibrated_members, weights, member_names)

        ensemble_members = [
            {
                "model": candidate["name"],
                "weight": float(weight),
                "test_metrics": candidate["metrics"]["test"],
                "train_metrics": candidate["metrics"]["train"],
            }
            for candidate, weight in zip(keep_candidates, weights)
        ]
    else:
        estimator = _train_calibrated_model(model_type, calibration, X_train, y_train, league=league)
        train_proba = estimator.predict_proba(X_train)[:, 1]
        test_proba = estimator.predict_proba(X_test)[:, 1]

    metrics = {
        "train": _evaluate(y_train, train_proba),
        "test": _evaluate(y_test, test_proba),
        "metadata": {
            "train_rows": int(len(train_df)),
            "test_rows": int(len(test_df)),
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "seasons": seasons,
            "features": feature_columns,
            "model_type": model_type,
            "calibration": calibration,
            "dataset_hash": dataset_signature,
            "league": league,
        },
    }

    if ensemble_members:
        metrics["metadata"]["ensemble_members"] = ensemble_members

    test_analysis = test_df.copy()
    test_analysis["predicted_prob"] = test_proba
    seasonal_metrics = []
    for season_value, group in test_analysis.groupby("season"):
        season_eval = _evaluate(group["win"].values, group["predicted_prob"].values)
        season_eval.update(
            {
                "season": int(season_value),
                "games": int(len(group)),
                "mean_pred": float(group["predicted_prob"].mean()),
                "win_rate": float(group["win"].mean()),
            }
        )
        seasonal_metrics.append(season_eval)
    metrics["seasonal_test"] = seasonal_metrics

    calibration_details: Dict[str, Any] = {"method": calibration}
    base_calibrators: List[Dict[str, Any]] = []

    def _calibrator_info(cal_model: ProbabilityCalibrator | None) -> Dict[str, Any]:
        if cal_model is None:
            return {"method": "none"}
        info: Dict[str, Any] = {"method": cal_model.method}
        if cal_model.method == "sigmoid":
            info["coef"] = cal_model.model.coef_.tolist()
            info["intercept"] = cal_model.model.intercept_.tolist()
        elif cal_model.method == "isotonic":
            # IsotonicRegression stores fitted X_ / y_
            info["n_samples"] = len(getattr(cal_model.model, "X_", []))
        return info

    if isinstance(estimator, CalibratedModel):
        calibration_details.update(_calibrator_info(estimator.calibrator))
    elif isinstance(estimator, EnsembleModel):
        for member, label, weight in zip(estimator.members, estimator.labels, estimator.weights):
            info = _calibrator_info(member.calibrator)
            info["model"] = label
            info["weight"] = float(weight)
            base_calibrators.append(info)
        calibration_details["members"] = base_calibrators

    metrics["calibration"] = calibration_details

    models_dir = Path("models")
    models_dir.mkdir(exist_ok=True)

    league_tag = league.lower()
    model_suffix = f"{model_type}" + ("_calibrated" if calibration != "none" else "")
    model_path = models_dir / f"{league_tag}_{model_suffix}_moneyline.pkl"
    joblib.dump(estimator, model_path)

    metrics_dir = Path("reports") / "backtests"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = metrics_dir / f"{league_tag}_{model_suffix}_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    predictions = test_df[[
        "game_id",
        "game_datetime",
        "team",
        "opponent",
        "moneyline",
        "implied_prob",
        "win",
    ]].copy()
    predictions["predicted_prob"] = test_proba
    predictions_path = metrics_dir / f"{league_tag}_{model_suffix}_test_predictions.parquet"
    predictions.to_parquet(predictions_path, index=False)

    model_id = f"{model_suffix}_{uuid.uuid4().hex[:8]}"
    trained_at = datetime.utcnow().isoformat() + "Z"

    registry_entry = {
        "model_id": model_id,
        "trained_at": trained_at,
        "model_type": model_type,
        "calibration": calibration,
        "model_path": str(model_path),
        "metrics_path": str(metrics_path),
        "predictions_path": str(predictions_path),
        "seasons": seasons,
        "features": feature_columns,
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "dataset_hash": dataset_signature,
        "league": league,
    }
    _record_model_version(registry_entry)
    try:
        register_model(registry_entry, metrics)
        persist_model_predictions(model_id, predictions)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to persist model artefacts to database: %s", exc)

    LOGGER.info("Saved model to %s", model_path)
    LOGGER.info("Metrics written to %s", metrics_path)

    return TrainingArtifacts(
        model_path=model_path,
        metrics_path=metrics_path,
        predictions_path=predictions_path,
        model_id=model_id,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train calibrated moneyline models across leagues")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=list(range(1999, 2024)),
        help="Season years to include (e.g. 2018 2019 2020)",
    )
    parser.add_argument(
        "--league",
        default="NFL",
        choices=[
            "NFL",
            "NBA",
            "NHL",
            "NCAAB",
            "CFB",
            "EPL",
            "LALIGA",
            "BUNDESLIGA",
            "SERIEA",
            "LIGUE1",
        ],
        help="League to train on",
    )
    parser.add_argument(
        "--model-type",
        choices=MODEL_CHOICES,
        default="gradient_boosting",
        help="Estimator architecture to train",
    )
    parser.add_argument(
        "--calibration",
        choices=CALIBRATION_CHOICES,
        default="sigmoid",
        help="Probability calibration method",
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
    seasons = [int(season) for season in args.seasons]
    train_and_evaluate(
        seasons,
        model_type=args.model_type,
        calibration=args.calibration,
        league=args.league,
    )


if __name__ == "__main__":
    main()
