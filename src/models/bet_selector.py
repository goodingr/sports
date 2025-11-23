"""Compute betting edges and bankroll recommendations from model predictions."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from src.db.models import find_model_id_by_predictions_path, persist_recommendations


LOGGER = logging.getLogger(__name__)


def american_to_decimal(moneyline: pd.Series) -> pd.Series:
    ml = moneyline.astype(float)
    decimals = np.where(ml > 0, (ml / 100.0) + 1.0, (100.0 / (-ml)) + 1.0)
    return pd.Series(decimals, index=moneyline.index)


def expected_value(prob: pd.Series, moneyline: pd.Series) -> pd.Series:
    ml = moneyline.astype(float)
    payout = np.where(ml > 0, ml, 100.0)
    risk = np.where(ml > 0, 100.0, -ml)
    return prob * payout - (1 - prob) * risk


def kelly_fraction(prob: pd.Series, decimal_odds: pd.Series) -> pd.Series:
    edge = prob * (decimal_odds - 1) - (1 - prob)
    denom = decimal_odds - 1
    fraction = edge / denom
    fraction = fraction.clip(lower=0.0)
    return fraction


def enrich_predictions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["decimal_odds"] = american_to_decimal(df["moneyline"])
    df["market_prob"] = 1 / df["decimal_odds"]
    df["edge"] = df["predicted_prob"] - df["market_prob"]
    df["expected_value"] = expected_value(df["predicted_prob"], df["moneyline"])
    df["kelly_fraction"] = kelly_fraction(df["predicted_prob"], df["decimal_odds"])
    df["kelly_fraction"] = df["kelly_fraction"].clip(upper=0.02)
    return df


def simulate_bankroll(df: pd.DataFrame, starting_bankroll: float = 10_000.0) -> Dict[str, float]:
    bankroll = starting_bankroll
    peak = bankroll
    trough = bankroll

    history = []
    for _, row in df.sort_values("game_datetime").iterrows():
        stake = bankroll * row["kelly_fraction"]
        outcome = row["win"]
        profit = 0.0
        if stake > 0:
            if outcome == 1:
                if row["moneyline"] > 0:
                    profit = stake * (row["moneyline"] / 100.0)
                else:
                    profit = stake * (100.0 / (-row["moneyline"]))
            else:
                profit = -stake
        bankroll += profit
        peak = max(peak, bankroll)
        trough = min(trough, bankroll)
        history.append(bankroll)

    roi = (bankroll - starting_bankroll) / starting_bankroll
    max_drawdown = (trough - peak) / peak if peak > 0 else 0.0
    return {
        "starting_bankroll": starting_bankroll,
        "ending_bankroll": bankroll,
        "roi": roi,
        "max_drawdown": max_drawdown,
    }


def select_bets(
    predictions_path: Path,
    output_dir: Path,
    edge_threshold: float = 0.02,
    league: str = "NFL",
) -> Dict[str, Path]:
    df = pd.read_parquet(predictions_path)
    enriched = enrich_predictions(df)
    bankroll_all = simulate_bankroll(enriched) if not enriched.empty else {
        "starting_bankroll": 10000.0,
        "ending_bankroll": 10000.0,
        "roi": 0.0,
        "max_drawdown": 0.0,
    }

    recommendations = enriched[enriched["edge"] >= edge_threshold].sort_values("edge", ascending=False)
    recommendations = recommendations.copy()
    if "kelly_fraction" in recommendations.columns and "stake" not in recommendations.columns:
        recommendations["stake"] = recommendations["kelly_fraction"]

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    rec_path = output_dir / f"recommendations_{timestamp}.csv"
    recommendations.to_csv(rec_path, index=False)

    bankroll_recommended = (
        simulate_bankroll(recommendations)
        if not recommendations.empty
        else {
            "starting_bankroll": 10000.0,
            "ending_bankroll": 10000.0,
            "roi": 0.0,
            "max_drawdown": 0.0,
        }
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "edge_threshold": edge_threshold,
        "league": league,
        "total_predictions": int(len(enriched)),
        "recommended_bets": int(len(recommendations)),
        "bankroll_all": bankroll_all,
        "bankroll_recommended": bankroll_recommended,
    }

    summary_path = output_dir / f"summary_{timestamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    model_id = find_model_id_by_predictions_path(str(predictions_path))
    if model_id:
        try:
            persist_recommendations(
                model_id=model_id,
                recommendations=recommendations,
                recommended_at=datetime.now(timezone.utc),
                snapshot_id=None,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to persist recommendations to database: %s", exc)
    else:
        LOGGER.warning("Unable to resolve model for predictions file %s; skipping DB persistence", predictions_path)

    LOGGER.info("Saved %d recommendations to %s", len(recommendations), rec_path)
    return {"summary": summary_path, "recommendations": rec_path}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate bet recommendations from model predictions")
    parser.add_argument(
        "--predictions",
        type=Path,
        default=None,
        help="Path to parquet predictions file (defaults based on league)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/recommendations"),
        help="Directory for bet recommendation exports",
    )
    parser.add_argument(
        "--edge-threshold",
        type=float,
        default=0.06,
        help="Minimum probability edge to include a bet",
    )
    parser.add_argument(
        "--league",
        default="NFL",
        choices=["NFL", "NBA"],
        help="League associated with the predictions file",
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
    predictions_path = args.predictions
    if predictions_path is None:
        # Try ensemble first, then gradient_boosting, then any model
        default_paths = [
            Path(f"reports/backtests/{args.league.lower()}_ensemble_calibrated_test_predictions.parquet"),
            Path(f"reports/backtests/{args.league.lower()}_gradient_boosting_calibrated_test_predictions.parquet"),
        ]
        predictions_path = None
        for path in default_paths:
            if path.exists():
                predictions_path = path
                break
        if predictions_path is None:
            raise FileNotFoundError(
                f"No predictions file found. Tried: {[str(p) for p in default_paths]}. "
                "Please specify --predictions explicitly."
            )
    select_bets(predictions_path, args.output_dir, args.edge_threshold, league=args.league)


if __name__ == "__main__":
    main()

