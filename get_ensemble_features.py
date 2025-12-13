"""Get features from ensemble members"""
import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))

import joblib
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator
import src.models.train

model_path = Path("models/ncaab_ensemble_calibrated_moneyline.pkl")
ensemble = joblib.load(model_path)

print(f"Ensemble has {len(ensemble.members)} members")
print(f"Member types: {[type(m).__name__ for m in ensemble.members]}")

# Check first member
first_member = ensemble.members[0]
print(f"\nFirst member type: {type(first_member)}")

if hasattr(first_member, "estimator"):
    estimator = first_member.estimator
    print(f"Member estimator type: {type(estimator)}")
    
    if hasattr(estimator, "feature_names_in_"):
        features = estimator.feature_names_in_
        print(f"\nMember expects {len(features)} features")
        
        # Save to file
        with open("ensemble_features.txt", "w") as f:
            for i, feat in enumerate(features):
                f.write(f"{i}: {feat}\n")
        
        print("Saved features to ensemble_features.txt")
        print("\nFirst 20 features:")
        for i, feat in enumerate(features[:20]):
            print(f"  {i}: {feat}")
        print(f"  ... ({len(features) - 20} more)")
    else:
        print("Member estimator has no feature_names_in_")
        print(f"Estimator attributes: {[a for a in dir(estimator) if not a.startswith('_')][:20]}")
