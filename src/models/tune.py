"""Hyperparameter tuning using Optuna for sports betting models."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import optuna
import pandas as pd
from optuna.pruners import MedianPruner
from sklearn.model_selection import TimeSeriesSplit

from src.data.config import PROCESSED_DATA_DIR
from src.models.train import (
    _build_estimator,
    _calibration_split_count,
    _evaluate,
    _train_calibrated_model,
    FEATURE_COLUMNS,
    MODEL_CHOICES,
)

LOGGER = logging.getLogger(__name__)


def _load_dataset(league: str, seasons: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load training dataset for specified league and seasons."""
    season_str = f"{min(seasons)}_{max(seasons)}"
    dataset_path = PROCESSED_DATA_DIR / "model_input" / f"moneyline_{league.lower()}_{season_str}.parquet"
    
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")
    
    df = pd.read_parquet(dataset_path)
    
    # Split train/test (last season for test)
    test_season = max(seasons)
    train_df = df[df["season"] < test_season].copy()
    test_df = df[df["season"] == test_season].copy()
    
    return train_df, test_df


def objective_random_forest(
    trial: optuna.Trial,
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    calibration: str = "sigmoid",
) -> float:
    """Optuna objective function for Random Forest hyperparameter tuning."""
    # Suggest hyperparameters
    n_estimators = trial.suggest_int("n_estimators", 100, 500, step=50)
    max_depth = trial.suggest_int("max_depth", 5, 25)
    min_samples_split = trial.suggest_int("min_samples_split", 2, 20)
    min_samples_leaf = trial.suggest_int("min_samples_leaf", 1, 10)
    max_features = trial.suggest_categorical("max_features", ["sqrt", "log2", 0.5, 0.7])
    
    # Time series cross-validation
    n_splits = _calibration_split_count(len(X_train))
    tscv = TimeSeriesSplit(n_splits=n_splits)
    
    log_losses = []
    
    for fold_idx, (train_idx, val_idx) in enumerate(tscv.split(X_train)):
        X_tr = X_train.iloc[train_idx]
        y_tr = y_train[train_idx]
        X_val = X_train.iloc[val_idx]
        y_val = y_train[val_idx]
        
        # Build model with suggested params
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.impute import SimpleImputer
        from sklearn.pipeline import Pipeline
        
        rf = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            random_state=42,
            n_jobs=-1,
        )
        
        pipeline = Pipeline([
            ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
            ("classifier", rf),
        ])
        
        # Train and calibrate
        from src.models.train import CalibratedModel, ProbabilityCalibrator
        
        pipeline.fit(X_tr, y_tr)
        train_proba = pipeline.predict_proba(X_tr)[:, 1]
        
        if calibration != "none":
            # Train calibrator
            from sklearn.linear_model import LogisticRegression as LR
            from sklearn.isotonic import IsotonicRegression as IR
            
            if calibration == "sigmoid":
                cal_model = LR(max_iter=1000)
                cal_model.fit(train_proba.reshape(-1, 1), y_tr)
                calibrator = ProbabilityCalibrator(method="sigmoid", model=cal_model)
            else:  # isotonic
                cal_model = IR(out_of_bounds="clip")
                cal_model.fit(train_proba, y_tr)
                calibrator = ProbabilityCalibrator(method="isotonic", model=cal_model)
            
            model = CalibratedModel(pipeline, calibrator)
        else:
            model = CalibratedModel(pipeline, None)
        
        # Evaluate on validation set
        val_proba = model.predict_proba(X_val)[:, 1]
        metrics = _evaluate(y_val, val_proba)
        log_losses.append(metrics["log_loss"])
        
        # Report intermediate value for pruning
        trial.report(np.mean(log_losses), fold_idx)
        
        if trial.should_prune():
            raise optuna.TrialPruned()
    
    return np.mean(log_losses)


def objective_ensemble(
    trial: optuna.Trial,
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    calibration: str = "sigmoid",
) -> float:
    """Optuna objective function for Ensemble pruning threshold tuning."""
    # Only tune the pruning threshold for ensemble
    # Base models use their default or tuned parameters
    pruning_threshold = trial.suggest_float("pruning_threshold", 0.01, 0.15)
    
    # Time series cross-validation
    n_splits = _calibration_split_count(len(X_train))
    tscv = TimeSeriesSplit(n_splits=n_splits)
    
    log_losses = []
    
    for fold_idx, (train_idx, val_idx) in enumerate(tscv.split(X_train)):
        X_tr = X_train.iloc[train_idx]
        y_tr = y_train[train_idx]
        X_val = X_train.iloc[val_idx]
        y_val = y_train[val_idx]
        
        # Train ensemble with suggested pruning threshold
        # This is simplified - in practice, we'd modify train.py to accept this param
        from src.models.train import EnsembleModel
        
        base_models = ["gradient_boosting", "lightgbm", "xgboost", "logistic", "mlp", "random_forest"]
        ensemble_candidates = []
        
        for base in base_models:
            try:
                model = _train_calibrated_model(base, calibration, X_tr, y_tr)
                val_probs = model.predict_proba(X_val)[:, 1]
                candidate_metrics = _evaluate(y_val, val_probs)
                ensemble_candidates.append({
                    "name": base,
                    "model": model,
                    "val_probs": val_probs,
                    "log_loss": candidate_metrics["log_loss"],
                })
            except Exception as exc:
                LOGGER.warning("Skipping ensemble member %s: %s", base, exc)
        
        if not ensemble_candidates:
            raise optuna.TrialPruned()
        
        # Apply pruning threshold
        best_log_loss = min(c["log_loss"] for c in ensemble_candidates)
        keep_candidates = [
            c for c in ensemble_candidates
            if c["log_loss"] <= best_log_loss + pruning_threshold
        ]
        
        if not keep_candidates:
            keep_candidates = [min(ensemble_candidates, key=lambda c: c["log_loss"])]
        
        # Calculate ensemble prediction
        log_losses_kept = np.array([c["log_loss"] for c in keep_candidates])
        inverse_losses = 1 / np.maximum(log_losses_kept, 1e-6)
        weights = inverse_losses / inverse_losses.sum()
        
        member_val_probs = np.column_stack([c["val_probs"] for c in keep_candidates])
        ensemble_val_proba = member_val_probs @ weights
        
        metrics = _evaluate(y_val, ensemble_val_proba)
        log_losses.append(metrics["log_loss"])
        
        trial.report(np.mean(log_losses), fold_idx)
        
        if trial.should_prune():
            raise optuna.TrialPruned()
    
    return np.mean(log_losses)


def tune_model(
    league: str,
    model_type: str,
    seasons: list[int],
    n_trials: int = 100,
    calibration: str = "sigmoid",
) -> Dict[str, Any]:
    """Run Optuna hyperparameter tuning study."""
    LOGGER.info("Starting hyperparameter tuning for %s %s", league, model_type)
    
    # Load data
    train_df, test_df = _load_dataset(league, seasons)
    
    feature_columns = [col for col in FEATURE_COLUMNS if col in train_df.columns]
    X_train = train_df[feature_columns].fillna(0)
    y_train = train_df["win"].values
    X_test = test_df[feature_columns].fillna(0)
    y_test = test_df["win"].values
    
    # Create study
    study_name = f"{league}_{model_type}_tuning_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    study = optuna.create_study(
        direction="minimize",
        pruner=MedianPruner(n_startup_trials=5, n_warmup_steps=2),
        study_name=study_name,
    )
    
    # Select objective function
    if model_type == "random_forest":
        objective = lambda trial: objective_random_forest(trial, X_train, y_train, calibration)
    elif model_type == "ensemble":
        objective = lambda trial: objective_ensemble(trial, X_train, y_train, calibration)
    else:
        raise ValueError(f"Tuning not supported for model type: {model_type}")
    
    # Optimize
    LOGGER.info("Running %d trials...", n_trials)
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
    
    # Log results
    LOGGER.info("Best trial: %d", study.best_trial.number)
    LOGGER.info("Best log loss: %.4f", study.best_value)
    LOGGER.info("Best params: %s", study.best_params)
    
    # Save best params
    params_dir = Path("config/tuned_params")
    params_dir.mkdir(parents=True, exist_ok=True)
    params_path = params_dir / f"{league.lower()}_{model_type}.json"
    
    params_data = {
        "league": league,
        "model_type": model_type,
        "best_params": study.best_params,
        "best_log_loss": study.best_value,
        "n_trials": n_trials,
        "tuned_at": datetime.utcnow().isoformat() + "Z",
        "seasons": seasons,
    }
    
    params_path.write_text(json.dumps(params_data, indent=2), encoding="utf-8")
    LOGGER.info("Saved tuned params to %s", params_path)
    
    # Generate visualizations
    viz_dir = Path("reports/tuning") / league.lower() / model_type
    viz_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        import optuna.visualization as vis
        
        # Optimization history
        fig = vis.plot_optimization_history(study)
        fig.write_html(str(viz_dir / "optimization_history.html"))
        
        # Parameter importances
        if len(study.best_params) > 1:
            fig = vis.plot_param_importances(study)
            fig.write_html(str(viz_dir / "param_importances.html"))
        
        # Parallel coordinate plot
        fig = vis.plot_parallel_coordinate(study)
        fig.write_html(str(viz_dir / "parallel_coordinate.html"))
        
        LOGGER.info("Saved visualizations to %s", viz_dir)
    except Exception as exc:
        LOGGER.warning("Failed to generate visualizations: %s", exc)
    
    return params_data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hyperparameter tuning with Optuna")
    parser.add_argument(
        "--league",
        required=True,
        choices=["NFL", "NBA", "NHL", "NCAAB", "CFB", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"],
        help="League to tune models for",
    )
    parser.add_argument(
        "--model-type",
        required=True,
        choices=["random_forest", "ensemble"],
        help="Model type to tune",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        default=list(range(2021, 2026)),
        help="Seasons to include in training",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=50,
        help="Number of Optuna trials to run",
    )
    parser.add_argument(
        "--calibration",
        choices=["none", "sigmoid", "isotonic"],
        default="sigmoid",
        help="Calibration method",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    tune_model(
        league=args.league,
        model_type=args.model_type,
        seasons=args.seasons,
        n_trials=args.n_trials,
        calibration=args.calibration,
    )


if __name__ == "__main__":
    main()
