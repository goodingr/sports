"""Feature importance analysis to identify which missing features have the largest impact."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance

from src.models.forward_test import load_model, get_model_features
from src.models.train import (
    CalibratedModel,  # noqa: F401 - needed for joblib unpickling
    EnsembleModel,  # noqa: F401 - needed for joblib unpickling
    ProbabilityCalibrator,  # noqa: F401 - needed for joblib unpickling
)

LOGGER = logging.getLogger(__name__)


def analyze_feature_importance(
    model_path: Path,
    test_data: Optional[pd.DataFrame] = None,
    n_repeats: int = 10,
    random_state: int = 42,
) -> pd.DataFrame:
    """Analyze feature importance using permutation importance.
    
    Args:
        model_path: Path to trained model
        test_data: Test dataset (if None, will try to load from model registry)
        n_repeats: Number of times to permute each feature
        random_state: Random seed
        
    Returns:
        DataFrame with feature importance scores
    """
    # Load model
    model = joblib.load(model_path)
    
    # Extract underlying estimator if it's a CalibratedModel
    if hasattr(model, "base_estimator"):
        # CalibratedModel wraps the base estimator
        base_model = model.base_estimator
    elif hasattr(model, "estimator"):
        base_model = model.estimator
    else:
        base_model = model
    
    # Get expected features
    model_features = get_model_features(model_path)
    
    if test_data is None:
        # Try to load test data from model registry
        LOGGER.warning("No test data provided, feature importance may be limited")
        return pd.DataFrame()
    
    # Ensure we have the required features
    missing_features = [f for f in model_features if f not in test_data.columns]
    if missing_features:
        LOGGER.warning("Missing features in test data: %s", missing_features)
        # Fill with NaN
        for feature in missing_features:
            test_data[feature] = np.nan
    
    # Prepare features
    X = test_data[model_features].fillna(0)
    y = test_data.get("win", None)
    
    if y is None:
        LOGGER.warning("No target variable 'win' found, using dummy predictions")
        # Use model predictions as baseline
        y_pred = model.predict_proba(X)[:, 1]
        # Create dummy y based on predictions
        y = (y_pred > 0.5).astype(int)
    
    # Calculate permutation importance
    LOGGER.info("Calculating permutation importance (this may take a while)...")
    
    # Use a custom scorer that works with the full calibrated model
    from sklearn.metrics import make_scorer, log_loss
    
    def custom_scorer(y_true, y_pred_proba):
        """Custom scorer for log loss."""
        eps = 1e-15
        y_pred_proba = np.clip(y_pred_proba, eps, 1 - eps)
        return -log_loss(y_true, y_pred_proba)
    
    # For permutation importance, we need to use the base model but score with calibrated predictions
    # Create a wrapper that uses base model for permutation but scores with full model
    class ModelWrapper:
        def __init__(self, base_model, full_model):
            self.base_model = base_model
            self.full_model = full_model
        
        def predict_proba(self, X):
            # Use full calibrated model for predictions
            return self.full_model.predict_proba(X)
    
    wrapped_model = ModelWrapper(base_model, model)
    
    # Use neg_log_loss scoring
    perm_importance = permutation_importance(
        wrapped_model,
        X,
        y,
        n_repeats=n_repeats,
        random_state=random_state,
        scoring="neg_log_loss",
        n_jobs=-1,
    )
    
    # Create results DataFrame
    results = pd.DataFrame({
        "feature": model_features,
        "importance_mean": perm_importance.importances_mean,
        "importance_std": perm_importance.importances_std,
        "importance_min": perm_importance.importances_min(axis=0),
        "importance_max": perm_importance.importances_max(axis=0),
    })
    
    # Sort by importance
    results = results.sort_values("importance_mean", ascending=False)
    
    return results


def analyze_missing_feature_impact(
    model_path: Path,
    test_data: pd.DataFrame,
    features_to_test: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Analyze impact of missing features by setting them to NaN/0 and measuring prediction change.
    
    Args:
        model_path: Path to trained model
        test_data: Test dataset
        features_to_test: List of features to test (if None, tests all features)
        
    Returns:
        DataFrame with impact scores for each feature
    """
    # Load model
    model = joblib.load(model_path)
    model_features = get_model_features(model_path)
    
    # Limit test data size for performance
    if len(test_data) > 1000:
        test_data = test_data.sample(n=1000, random_state=42)
        LOGGER.info("Sampled 1000 rows for missing feature impact analysis")
    
    # Get baseline predictions
    X_baseline = test_data[model_features].fillna(0)
    baseline_preds = model.predict_proba(X_baseline)[:, 1]
    
    if features_to_test is None:
        features_to_test = model_features
    
    results = []
    
    for feature in features_to_test:
        if feature not in model_features:
            continue
        
        # Set feature to NaN/0
        X_missing = X_baseline.copy()
        X_missing[feature] = 0  # or np.nan, depending on how model handles it
        
        # Get predictions with missing feature
        missing_preds = model.predict_proba(X_missing)[:, 1]
        
        # Calculate impact (mean absolute difference)
        impact = np.mean(np.abs(baseline_preds - missing_preds))
        
        # Calculate percentage of predictions that change significantly (>5%)
        significant_changes = np.sum(np.abs(baseline_preds - missing_preds) > 0.05) / len(baseline_preds)
        
        results.append({
            "feature": feature,
            "mean_impact": impact,
            "max_impact": np.max(np.abs(baseline_preds - missing_preds)),
            "pct_significant_changes": significant_changes * 100,
        })
    
    df = pd.DataFrame(results)
    df = df.sort_values("mean_impact", ascending=False)
    
    return df


def generate_feature_importance_report(
    model_path: Path,
    league: str,
    output_path: Optional[Path] = None,
) -> str:
    """Generate a comprehensive feature importance report.
    
    Args:
        model_path: Path to trained model
        league: League name
        output_path: Optional path to save report
        
    Returns:
        Report text
    """
    # Try to load test data from processed datasets
    processed_dir = Path("data/processed/model_input")
    pattern = f"moneyline_{league.lower()}_*.parquet"
    dataset_files = list(processed_dir.glob(pattern))
    
    if not dataset_files:
        return f"No test data found for {league}. Cannot generate feature importance report."
    
    # Load most recent dataset
    test_data = pd.read_parquet(sorted(dataset_files)[-1])
    
    # Get missing feature impact (more reliable than permutation importance for our use case)
    impact_df = analyze_missing_feature_impact(model_path, test_data)
    
    # Try permutation importance (may fail for some model types)
    try:
        importance_df = analyze_feature_importance(model_path, test_data)
    except Exception as e:
        LOGGER.warning("Could not calculate permutation importance: %s", e)
        importance_df = pd.DataFrame()
    
    # Generate report
    report_lines = [
        f"Feature Importance Report for {league}",
        "=" * 80,
        "",
        "MISSING FEATURE IMPACT ANALYSIS",
        "-" * 80,
        "This analysis shows how much predictions change when each feature is missing.",
        "Higher impact = more important feature when missing.",
        "",
    ]
    
    if not impact_df.empty:
        for idx, row in impact_df.head(20).iterrows():
            report_lines.append(
                f"{row['feature']:40s} | Mean Impact: {row['mean_impact']:8.6f} | "
                f"Max Impact: {row['max_impact']:8.6f} | "
                f"Significant Changes: {row['pct_significant_changes']:5.1f}%"
            )
    else:
        report_lines.append("Could not calculate missing feature impact")
    
    report_lines.extend([
        "",
        "RECOMMENDATIONS",
        "-" * 80,
    ])
    
    if not impact_df.empty:
        top_missing_impact = impact_df.head(10)
        report_lines.append("Top 10 features that have the largest impact when missing:")
        for idx, row in top_missing_impact.iterrows():
            report_lines.append(f"  {idx + 1}. {row['feature']} (impact: {row['mean_impact']:.6f})")
    
    report_text = "\n".join(report_lines)
    
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(report_text)
        LOGGER.info("Feature importance report saved to %s", output_path)
    
    return report_text


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze feature importance for a trained model")
    parser.add_argument("--league", required=True, choices=["NBA", "NFL", "CFB", "MLB", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"])
    parser.add_argument("--model", type=Path, default=None, help="Path to model file")
    parser.add_argument("--output", type=Path, default=None, help="Path to save report")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    if args.model is None:
        args.model = Path(f"models/{args.league.lower()}_gradient_boosting_calibrated_moneyline.pkl")
    
    if args.output is None:
        args.output = Path(f"reports/feature_importance_{args.league.lower()}.txt")
    
    report = generate_feature_importance_report(args.model, args.league, args.output)
    print(report)

