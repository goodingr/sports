"""Check ensemble model feature names"""
import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))

import joblib
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator
import src.models.train

model_path = Path("models/ncaab_ensemble_calibrated_moneyline.pkl")
model = joblib.load(model_path)

print("Model type:", type(model))
if hasattr(model, "feature_names_in_"):
    print(f"\nModel expects {len(model.feature_names_in_)} features:")
    for i, feat in enumerate(model.feature_names_in_[:30]):
        print(f"  {i}: {feat}")
    if len(model.feature_names_in_) > 30:
        print(f"  ... ({len(model.feature_names_in_) - 30} more)")
        print(f"\nLast 10 features:")
        for i, feat in enumerate(model.feature_names_in_[-10:]):
            print(f"  {len(model.feature_names_in_) - 10 + i}: {feat}")
elif hasattr(model, "estimator"):
    print("\nModel has estimator attribute")
    if hasattr(model.estimator, "feature_names_in_"):
        print(f"Estimator expects {len(model.estimator.feature_names_in_)} features:")
        for i, feat in enumerate(model.estimator.feature_names_in_[:30]):
            print(f"  {i}: {feat}")
        if len(model.estimator.feature_names_in_) > 30:
            print(f"  ... ({len(model.estimator.feature_names_in_) - 30} more)")
