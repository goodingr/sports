"""Quick test of ensemble predictions for a single league"""
import sys
from pathlib import Path
sys.path.append(str(Path.cwd()))

import logging
logging.basicConfig(level=logging.INFO)

# Import model classes for pickle
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator
import src.models.train  # Needed for joblib unpickling

from src.predict.engine import PredictionEngine

# Test NCAAB ensemble (we know this should work)
print("Testing NCAAB ensemble...")
engine = PredictionEngine("ensemble")
result = engine.predict("NCAAB", days_ahead=7)
if result is not None:
    print(f"SUCCESS: NCAAB ensemble: {len(result)} predictions")
else:
    print("FAILED: NCAAB ensemble failed")

# Test NHL ensemble (this was failing)
print("\nTesting NHL ensemble...")
engine2 = PredictionEngine("ensemble")
result2 = engine2.predict("NHL", days_ahead=7)
if result2 is not None:
    print(f"SUCCESS: NHL ensemble: {len(result2)} predictions")
else:
    print("FAILED: NHL ensemble failed")

# Test LIGUE1 ensemble (this was failing)
print("\nTesting LIGUE1 ensemble...")
engine3 = PredictionEngine("ensemble")
result3 = engine3.predict("LIGUE1", days_ahead=7)
if result3 is not None:
    print(f"SUCCESS: LIGUE1 ensemble: {len(result3)} predictions")
else:
    print("FAILED: LIGUE1 ensemble failed")
