"""CLI runner for the prediction system."""

import argparse
import logging
import sys
from typing import List

from src.predict.config import SUPPORTED_LEAGUES
from src.predict.engine import PredictionEngine
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator # Fix for joblib loading
from src.db.core import DB_PATH

LOGGER = logging.getLogger(__name__)

import sys
def run_predictions(leagues: List[str], model_type: str = "ensemble"):
    """Run predictions for specified leagues."""
    engine = PredictionEngine(model_type=model_type)
    
    for league in leagues:
        LOGGER.info(f"Running predictions for {league}...")
        engine.predict(league)

def main():
    parser = argparse.ArgumentParser(description="Sports Prediction Runner")
    
    parser.add_argument("--leagues", nargs="+", help="Leagues to predict (default: all)")
    parser.add_argument("--model-type", default="ensemble", help="Model type to use")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
        
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    leagues = args.leagues or SUPPORTED_LEAGUES
    # Handle comma-separated
    if len(leagues) == 1 and "," in leagues[0]:
        leagues = [l.strip() for l in leagues[0].split(",")]
        
    run_predictions(leagues, model_type=args.model_type)

if __name__ == "__main__":
    main()
