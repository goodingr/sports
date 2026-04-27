"""Legacy forward-test compatibility wrappers.

Current prediction generation is owned by :mod:`src.predict.runner` and stores
current predictions in SQLite. This module keeps older tests and utility imports
working while delegating live prediction runs to the canonical path.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
import requests

from src.data.team_mappings import normalize_team_code
from src.models.feature_loader import FeatureLoader
from src.models.train import FEATURE_COLUMNS
from src.predict.config import MODEL_REGISTRY_PATH, SUPPORTED_LEAGUES
from src.predict.engine import PredictionEngine
from src.predict.runner import run_predictions
from src.db.core import connect

LOGGER = logging.getLogger(__name__)

FORWARD_TEST_DIR = Path("data/forward_test")


def _moneyline_to_implied(price: Optional[float]) -> float:
    if price is None or price == 0:
        return float("nan")
    if price > 0:
        return 100.0 / (price + 100.0)
    return -price / (-price + 100.0)


def _extract_moneylines(game: Dict[str, object], home_team: str, away_team: str) -> Tuple[Optional[float], Optional[float]]:
    home_price = None
    away_price = None
    for bookmaker in game.get("bookmakers", []) or []:
        for market in bookmaker.get("markets", []) or []:
            if market.get("key") != "h2h":
                continue
            for outcome in market.get("outcomes", []) or []:
                name = str(outcome.get("name", "")).strip()
                try:
                    price = float(outcome.get("price"))
                except (TypeError, ValueError):
                    continue
                if name.lower() in {"home", home_team.lower()}:
                    home_price = price
                elif name.lower() in {"away", away_team.lower()}:
                    away_price = price
            if home_price is not None and away_price is not None:
                return home_price, away_price
    return home_price, away_price


def _apply_rest(row: Dict[str, object], team_rest: float, opponent_rest: float, *, is_home: bool) -> None:
    row["team_rest_days"] = team_rest
    row["opponent_rest_days"] = opponent_rest
    row["rest_diff"] = team_rest - opponent_rest
    row["is_short_week"] = 1.0 if team_rest < 5.0 else 0.0
    row["is_post_bye"] = 1.0 if team_rest > 10.0 else 0.0
    row["road_trip_length_entering"] = 0.0 if is_home else 1.0


def prepare_features(
    game: Dict[str, object],
    *,
    league: str = "NFL",
    model_features: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Prepare the older two-row home/away feature shape used by legacy tests."""
    league_upper = league.upper()
    loader = FeatureLoader(league_upper)
    home_raw = str(game.get("home_team", ""))
    away_raw = str(game.get("away_team", ""))
    home_team = normalize_team_code(league_upper, home_raw) or home_raw.upper()
    away_team = normalize_team_code(league_upper, away_raw) or away_raw.upper()
    game_date = pd.to_datetime(game.get("commence_time"), utc=True, errors="coerce")
    home_ml, away_ml = _extract_moneylines(game, home_raw, away_raw)

    rows = []
    for is_home, team, opponent, moneyline, opponent_moneyline, rest, opp_rest in [
        (True, home_team, away_team, home_ml, away_ml, 7.0, 6.0),
        (False, away_team, home_team, away_ml, home_ml, 6.0, 7.0),
    ]:
        row: Dict[str, object] = {
            "game_id": game.get("id") or game.get("game_id"),
            "commence_time": game_date,
            "team": team,
            "opponent": opponent,
            "is_home": 1 if is_home else 0,
            "moneyline": moneyline,
            "implied_prob": _moneyline_to_implied(moneyline),
            "opponent_moneyline": opponent_moneyline,
        }
        _apply_rest(row, rest, opp_rest, is_home=is_home)

        if model_features is None or "injuries_out" in model_features:
            row["injuries_out"] = loader.get_injury_count(team, game_date, status="Out")

        if league_upper == "NFL":
            row.update(loader.get_weather_features(game_id=str(row["game_id"]), game_date=game_date))

        for feature in model_features or []:
            row.setdefault(feature, np.nan)
        rows.append(row)

    return pd.DataFrame(rows)


def save_predictions(df: pd.DataFrame, timestamp: Optional[str] = None, model_type: str = "ensemble") -> Path:
    """Legacy parquet writer that keeps one row per game in predictions_master."""
    output_dir = FORWARD_TEST_DIR / model_type
    output_dir.mkdir(parents=True, exist_ok=True)
    master_path = output_dir / "predictions_master.parquet"

    new_df = df.copy()
    if "predicted_at" in new_df.columns:
        new_df["predicted_at"] = pd.to_datetime(new_df["predicted_at"], utc=True, errors="coerce")

    if master_path.exists():
        existing = pd.read_parquet(master_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    if "game_id" in combined.columns:
        sort_col = "predicted_at" if "predicted_at" in combined.columns else None
        if sort_col:
            combined = combined.sort_values(sort_col)
        combined = combined.drop_duplicates(subset=["game_id"], keep="last")

    combined.to_parquet(master_path, index=False)
    if timestamp:
        snapshot_path = output_dir / f"predictions_{timestamp}.parquet"
        new_df.to_parquet(snapshot_path, index=False)
    return master_path


def _fetch_espn_cfb_scores(dates: Iterable[date]) -> Dict[Tuple[str, str, date], Tuple[int, int]]:
    scores: Dict[Tuple[str, str, date], Tuple[int, int]] = {}
    for target_date in dates:
        response = requests.get(
            "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
            params={"dates": target_date.strftime("%Y%m%d"), "limit": 1000},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        for event in payload.get("events", []) or []:
            competition = (event.get("competitions") or [{}])[0]
            state = competition.get("status", {}).get("type", {}).get("state")
            if state != "post":
                continue
            competitors = competition.get("competitors") or []
            home = next((item for item in competitors if item.get("homeAway") == "home"), None)
            away = next((item for item in competitors if item.get("homeAway") == "away"), None)
            if not home or not away:
                continue
            home_code = normalize_team_code("CFB", home.get("team", {}).get("displayName"))
            away_code = normalize_team_code("CFB", away.get("team", {}).get("displayName"))
            if not home_code or not away_code:
                continue
            try:
                home_score = int(home.get("score"))
                away_score = int(away.get("score"))
            except (TypeError, ValueError):
                continue
            scores[(home_code, away_code, target_date)] = (home_score, away_score)
    return scores


def _fetch_recent_scores(*_: object, **__: object) -> Dict[Tuple[str, str, date], Tuple[int, int]]:
    return {}


def _prediction_files(model_type: Optional[str]) -> List[Path]:
    if model_type:
        return [FORWARD_TEST_DIR / model_type / "predictions_master.parquet"]
    return sorted(FORWARD_TEST_DIR.glob("*/predictions_master.parquet"))


def update_results(league: Optional[str] = None, model_type: str = "ensemble") -> int:
    """Update legacy parquet predictions with final scores where available."""
    files = _prediction_files(model_type)
    updated_rows = 0
    for path in files:
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        if df.empty:
            continue
        if league and "league" in df.columns:
            target_df = df[df["league"].astype(str).str.upper() == league.upper()]
        else:
            target_df = df
        if target_df.empty or "commence_time" not in target_df.columns:
            continue

        dates = {
            pd.Timestamp(value).date()
            for value in pd.to_datetime(target_df["commence_time"], utc=True, errors="coerce").dropna()
        }
        score_map = _fetch_recent_scores(league=league)
        if league and league.upper() == "CFB":
            score_map.update(_fetch_espn_cfb_scores(dates))

        for idx, row in target_df.iterrows():
            if row.get("result") not in (None, "", np.nan) and pd.notna(row.get("result")):
                continue
            commence = pd.to_datetime(row.get("commence_time"), utc=True, errors="coerce")
            if pd.isna(commence):
                continue
            key = (
                normalize_team_code(league or row.get("league", ""), row.get("home_team")) or str(row.get("home_team")),
                normalize_team_code(league or row.get("league", ""), row.get("away_team")) or str(row.get("away_team")),
                commence.date(),
            )
            if key not in score_map:
                continue
            home_score, away_score = score_map[key]
            df.at[idx, "home_score"] = home_score
            df.at[idx, "away_score"] = away_score
            df.at[idx, "result"] = "home" if home_score > away_score else "away" if away_score > home_score else "tie"
            df.at[idx, "result_updated_at"] = datetime.now(timezone.utc).isoformat()
            updated_rows += 1

        df.to_parquet(path, index=False)
    return updated_rows


def load_model(league: str, model_type: str = "ensemble"):
    engine = PredictionEngine(model_type=model_type)
    if not engine.load_model(league):
        return None
    return engine.model


def get_model_features(model, league: str = "NFL", model_type: str = "ensemble") -> List[str]:
    if hasattr(model, "feature_names_in_"):
        return list(model.feature_names_in_)
    if hasattr(model, "estimator") and hasattr(model.estimator, "feature_names_in_"):
        return list(model.estimator.feature_names_in_)
    return FEATURE_COLUMNS


def make_predictions(games: List[Dict[str, object]], model, league: str = "NFL") -> pd.DataFrame:
    features = [prepare_features(game, league=league) for game in games]
    if not features:
        return pd.DataFrame()
    return pd.concat(features, ignore_index=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Legacy forward-test wrapper; current predictions use SQLite.")
    subparsers = parser.add_subparsers(dest="command")

    predict_parser = subparsers.add_parser("predict", help="Run current SQLite-backed predictions")
    predict_parser.add_argument("--league", dest="leagues", action="append")
    predict_parser.add_argument("--model-type", default="ensemble")

    update_parser = subparsers.add_parser("update", help="Update legacy parquet results")
    update_parser.add_argument("--league")
    update_parser.add_argument("--model-type", default="ensemble")

    subparsers.add_parser("report", help="Print a small legacy parquet summary")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    if args.command == "predict":
        leagues = args.leagues or SUPPORTED_LEAGUES
        run_predictions(leagues, model_type=args.model_type)
    elif args.command == "update":
        updated = update_results(league=args.league, model_type=args.model_type)
        print(f"Updated results: {updated}")
    elif args.command == "report":
        files = _prediction_files(None)
        total = sum(len(pd.read_parquet(path)) for path in files if path.exists())
        print(f"Legacy prediction rows: {total}")
    else:
        _parse_args()


if __name__ == "__main__":
    main()
