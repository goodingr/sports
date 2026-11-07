"""CLI runner for the prediction system."""

import argparse
import logging
from pathlib import Path
from typing import List

from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator  # noqa: F401
from src.predict.config import SUPPORTED_LEAGUES
from src.predict.engine import PredictionEngine
from src.predict.paper_ledger import (
    DEFAULT_RULES,
    capture_predictions,
    load_trial,
    record_capture_failure,
    register_trial,
)

LOGGER = logging.getLogger(__name__)


def run_predictions(
    leagues: List[str],
    model_type: str = "ensemble",
    *,
    paper_rules: Path | None = None,
    paper_only: bool = False,
):
    """Run predictions for specified leagues."""
    engine = PredictionEngine(model_type=model_type)
    trial = load_trial(paper_rules) if paper_rules is not None else None
    if paper_only and (
        trial is None or any((league.upper(), model_type) != (trial.league, trial.model_type) for league in leagues)
    ):
        raise ValueError("Paper-only predictions require the registered league and model type")

    for league in leagues:
        LOGGER.info(f"Running predictions for {league}...")
        if trial is None or (league.upper(), model_type) != (trial.league, trial.model_type):
            engine.predict(league)
            continue

        register_trial(trial)
        try:
            predictions = engine.predict_pinned_totals(league, trial.model_sha256)
            counts = capture_predictions(trial, predictions, engine.totals_model_sha256)
            LOGGER.info("Paper trial %s capture: %s", trial.trial_id, counts)
        except Exception:
            try:
                record_capture_failure(trial)
            except Exception:
                LOGGER.exception("Could not record the failed paper capture")
            raise
        if (predictions is None or predictions.empty) and counts.get("prediction_missing"):
            raise RuntimeError(
                f"Paper trial {trial.trial_id} produced no predictions during an active capture window"
            )
        if not paper_only:
            engine.predict(league, trace_totals=True)


def main():
    parser = argparse.ArgumentParser(description="Sports Prediction Runner")

    parser.add_argument("--leagues", nargs="+", help="Leagues to predict (default: all)")
    parser.add_argument("--model-type", default="ensemble", help="Model type to use")
    paper = parser.add_mutually_exclusive_group()
    paper.add_argument(
        "--paper", action="store_true", help="Capture matching NBA totals decisions"
    )
    paper.add_argument(
        "--paper-only", action="store_true", help="Capture NBA totals without saving ordinary predictions"
    )
    parser.add_argument("--paper-rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))

    leagues = args.leagues or SUPPORTED_LEAGUES
    # Handle comma-separated
    if len(leagues) == 1 and "," in leagues[0]:
        leagues = [league.strip() for league in leagues[0].split(",")]

    run_predictions(
        leagues,
        model_type=args.model_type,
        paper_rules=args.paper_rules if args.paper or args.paper_only else None,
        paper_only=args.paper_only,
    )


if __name__ == "__main__":
    main()
