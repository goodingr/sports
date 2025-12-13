"""Debug ensemble model feature requirements"""
import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))

import joblib
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator
import src.models.train

model_path = Path("models/ncaab_ensemble_calibrated_moneyline.pkl")
ensemble = joblib.load(model_path)

print(f"Model type: {type(ensemble)}")
print(f"Model attributes: {dir(ensemble)}")

# Check if it has estimator
if hasattr(ensemble, "estimator"):
    estimator = ensemble.estimator
    print(f"\nEstimator type: {type(estimator)}")
    print(f"Estimator attributes: {[a for a in dir(estimator) if 'feature' in a.lower()]}")
    
    if hasattr(estimator, "feature_names_in_"):
        features = estimator.feature_names_in_
        print(f"\nEstimator expects {len(features)} features:")
        
        # Save to file for easy viewing
        with open("ensemble_features.txt", "w") as f:
            for i, feat in enumerate(features):
                line = f"{i}: {feat}"
                print(line)
                f.write(line + "\n")
        
        print(f"\nSaved all features to ensemble_features.txt")
    else:
        print("\nEstimator does not have feature_names_in_")
        
        # Try to get from sub-models
        if hasattr(estimator, "models_"):
            print(f"Estimator has {len(estimator.models_)} models")
            for i, model in enumerate(estimator.models_[:3]):
                print(f"  Model {i}: {type(model)}")
                if hasattr(model, "feature_names_in_"):
                    print(f"    Features: {len(model.feature_names_in_)}")
else:
    print("\nNo estimator attribute")
