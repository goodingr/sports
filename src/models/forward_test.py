"""Forward testing system for live NBA, NFL, CFB, and European soccer games."""
import argparse
import json
import logging
import math
import sqlite3
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
import requests
from zoneinfo import ZoneInfo

from src.data.config import OddsAPISettings
from src.data.ingest_odds import fetch_odds
from src.data.ingest_results_soccer import fetch_from_espn
from src.data.team_mappings import normalize_team_code
from src.db.core import connect
from src.models.feature_loader import FeatureLoader
from src.models.train import (
    FEATURE_COLUMNS,  # noqa: F401 - retained for backward compatibility
    CalibratedModel,  # noqa: F401 - needed for joblib unpickling
    EnsembleModel,  # noqa: F401 - needed for joblib unpickling
    ProbabilityCalibrator,  # noqa: F401 - needed for joblib unpickling
)

LOGGER = logging.getLogger(__name__)


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

SUPPORTED_LEAGUES: List[str] = ["NBA", "NFL", "NHL", "CFB", "EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"]

# Soccer leagues use three-way markets (home/draw/away)
SOCCER_LEAGUES = {"EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"}


LEAGUE_SPORT_KEYS: Dict[str, str] = {
    "NBA": "basketball_nba",
    "NFL": "americanfootball_nfl",
    "NHL": "icehockey_nhl",
    "CFB": "americanfootball_ncaaf",
    "EPL": "soccer_epl",
    "LALIGA": "soccer_spain_la_liga",
    "BUNDESLIGA": "soccer_germany_bundesliga",
    "SERIEA": "soccer_italy_serie_a",
    "LIGUE1": "soccer_france_ligue_one",
}


FORWARD_TEST_DIR = Path("data/forward_test")
FORWARD_TEST_DIR.mkdir(parents=True, exist_ok=True)

MODEL_REGISTRY_PATH = Path("models") / "model_registry.json"
DEFAULT_REST_DAYS = 7.0
SHORT_WEEK_THRESHOLD = 5.0
BYE_THRESHOLD = 10.0
ESPN_CFB_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
EASTERN_TZ = ZoneInfo("America/New_York")
SOCCER_SCORE_LOOKBACK_DAYS = 10


def _get_sport_key(league: str) -> str:
    try:
        return LEAGUE_SPORT_KEYS[league.upper()]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise ValueError(
            f"Unknown league: {league}. Must be one of {', '.join(sorted(LEAGUE_SPORT_KEYS))}."
        ) from exc


def _estimate_rest_days(commence_time: Optional[str], *, travel_penalty: float = 0.0) -> float:
    try:
        if commence_time:
            datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
    except Exception:
        pass
    return max(1.0, DEFAULT_REST_DAYS - travel_penalty)


def _apply_rest_metrics(row: Dict[str, float], team_rest: float, opponent_rest: float, *, is_home: bool) -> None:
    row["team_rest_days"] = team_rest
    row["opponent_rest_days"] = opponent_rest
    row["rest_diff"] = team_rest - opponent_rest
    row["is_short_week"] = 1.0 if team_rest < SHORT_WEEK_THRESHOLD else 0.0
    row["is_post_bye"] = 1.0 if team_rest > BYE_THRESHOLD else 0.0
    row["road_trip_length_entering"] = 0.0 if is_home else 1.0


def _moneyline_to_prob(value: float | int | None) -> float:
    if value is None or value == 0:
        return 0.5
    try:
        ml = float(value)
    except (TypeError, ValueError):
        return 0.5
    if ml > 0:
        prob = 100.0 / (ml + 100.0)
    else:
        prob = -ml / (-ml + 100.0)
    return float(np.clip(prob, 1e-6, 1 - 1e-6))


def _moneyline_to_decimal(value: float | int | None) -> Optional[float]:
    if value is None:
        return None
    try:
        ml = float(value)
    except (TypeError, ValueError):
        return None
    if ml == 0:
        return None
    if ml > 0:
        return 1.0 + ml / 100.0
    return 1.0 + 100.0 / abs(ml)


def _extract_moneyline_prices(game: Dict, league_upper: str, home_team: str, away_team: str) -> List[Tuple[float, float, Optional[float]]]:
    """Extract moneyline prices. For soccer, includes draw price as third element."""
    prices: List[Tuple[float, float, Optional[float]]] = []
    is_soccer = league_upper in SOCCER_LEAGUES
    
    for bookmaker in game.get("bookmakers", []):
        home_price = None
        away_price = None
        draw_price = None
        
        for market in bookmaker.get("markets", []):
            market_key = (market.get("key") or "").lower()
            if market_key != "h2h":
                continue
            
            for outcome in market.get("outcomes", []):
                name_raw = outcome.get("name", "").strip()
                if not name_raw:
                    continue
                
                price = outcome.get("price")
                if price is None:
                    continue
                price_val = float(price)
                
                # For soccer, check for draw/tie
                if is_soccer:
                    name_lower = name_raw.lower()
                    if name_lower in ("draw", "tie", "x"):
                        draw_price = price_val
                        continue
                
                # Match team names
                outcome_team = normalize_team_code(league_upper, name_raw)
                if outcome_team == home_team:
                    home_price = price_val
                elif outcome_team == away_team:
                    away_price = price_val
            
            break  # only need the h2h market per bookmaker
        
        # For soccer, require all three prices; for others, just home/away
        if is_soccer:
            if home_price is not None and away_price is not None and draw_price is not None:
                prices.append((home_price, away_price, draw_price))
        else:
            if home_price is not None and away_price is not None:
                prices.append((home_price, away_price, None))
    
    return prices


def get_model_features(model_path: Path) -> List[str]:
    """Get the list of features expected by the model from registry."""
    if MODEL_REGISTRY_PATH.exists():
        try:
            with open(MODEL_REGISTRY_PATH) as f:
                registry = json.load(f)

            model_filename = model_path.name
            # Handle both Windows and Unix paths in registry. Prefer the most recent entry.
            for entry in reversed(registry):
                registry_path = entry.get("model_path", "")
                if registry_path.endswith(model_filename) or registry_path.replace("\\", "/").endswith(model_filename):
                    features = entry.get("features", [])
                    if features:
                        return features
        except Exception as e:
            LOGGER.warning("Failed to read model registry: %s", e)
    
    # Fallback: return basic features
    return [
        "is_home", "moneyline", "implied_prob", "spread_line", "total_line",
        "espn_moneyline_open", "espn_moneyline_close", "espn_spread_open",
        "espn_spread_close", "espn_total_open", "espn_total_close"
    ]


def load_model(model_path: Optional[Path] = None, league: str = "NBA") -> object:
    """Load the trained model for the specified league."""
    league_upper = league.upper()
    if model_path is None:
        if league_upper == "NBA":
            model_path = Path("models/nba_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "NFL":
            model_path = Path("models/nfl_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "CFB":
            model_path = Path("models/cfb_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "EPL":
            model_path = Path("models/epl_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "LALIGA":
            model_path = Path("models/laliga_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "BUNDESLIGA":
            model_path = Path("models/bundesliga_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "SERIEA":
            model_path = Path("models/seriea_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "LIGUE1":
            model_path = Path("models/ligue1_gradient_boosting_calibrated_moneyline.pkl")
        else:
            raise ValueError(
                f"Unknown league: {league}. Must be NBA, NFL, CFB, EPL, LALIGA, BUNDESLIGA, SERIEA, or LIGUE1."
            )
    
    if not model_path.exists():
        raise FileNotFoundError(
            f"Model not found for {league_upper}: expected at {model_path}. "
            "Train the league-specific model before running forward_test."
        )
 
    LOGGER.info("Loading model from %s", model_path)
    return joblib.load(model_path)


def load_totals_model(league: str) -> Optional[dict]:
    path = Path("models") / f"{league.lower()}_totals_gradient_boosting.pkl"
    if not path.exists():
        return None
    try:
        return joblib.load(path)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to load totals model for %s: %s", league, exc)
        return None


def fetch_live_games(league: str = "NBA", dotenv_path: Optional[Path] = None) -> List[Dict]:
    """Fetch current/upcoming games with odds for the specified league."""
    league_upper = league.upper()
    sport_key = _get_sport_key(league_upper)
    
    LOGGER.info("Fetching live %s games with odds...", league_upper)
    try:
        settings = OddsAPISettings.from_env(dotenv_path)
    except RuntimeError as e:
        LOGGER.error("Failed to load Odds API settings: %s", e)
        LOGGER.error("Please ensure ODDS_API_KEY is set in .env file or environment")
        return []
    
    settings.sport = sport_key
    settings.market = "h2h,spreads,totals"
    settings.region = "us"
    payload = fetch_odds(settings)
    
    if not payload or "results" not in payload:
        LOGGER.warning("No games found")
        return []
    
    results = payload.get("results", [])
    LOGGER.info("Found %d games", len(results))
    
    # Filter to games that haven't started yet
    live_games = []
    now = datetime.now(timezone.utc)
    
    for game in results:
        commence_time = game.get("commence_time")
        if commence_time:
            try:
                game_time = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                # Only include games in the future or very recent (within last hour)
                if game_time > now or (now - game_time).total_seconds() < 3600:
                    live_games.append(game)
            except Exception:
                continue
    
    LOGGER.info("%d games are upcoming or recent", len(live_games))
    return live_games


def prepare_features(
    game: Dict,
    league: str = "NBA",
    model_features: Optional[List[str]] = None,
    loader: Optional[FeatureLoader] = None,
) -> pd.DataFrame:
    """Prepare feature vector for a game."""
    league_upper = league.upper()
    home_team = normalize_team_code(league_upper, game.get("home_team", ""))
    away_team = normalize_team_code(league_upper, game.get("away_team", ""))
    
    # Initialize feature loader once per league if not provided
    loader = loader or FeatureLoader(league_upper)
    
    # Parse game date if available
    game_date = None
    commence_time = game.get("commence_time", "")
    if commence_time:
        try:
            game_date = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
        except Exception:
            pass
    
    # Get current season (approximate)
    current_season = datetime.now().year
    if game_date:
        # For sports that start in one year and end in the next, adjust season
        if league_upper in {"NBA", "NFL"}:
            # NBA/NFL seasons span two years
            if game_date.month >= 10:  # October onwards
                current_season = game_date.year
            else:
                current_season = game_date.year - 1
        else:
            current_season = game_date.year
    
    # Extract moneylines across all bookmakers and choose the best price
    is_soccer = league_upper in SOCCER_LEAGUES
    moneyline_prices = _extract_moneyline_prices(game, league_upper, home_team, away_team)
    home_ml = None
    away_ml = None
    draw_ml = None
    best_home_decimal = -np.inf
    best_away_decimal = -np.inf
    best_draw_decimal = -np.inf
    
    for price_tuple in moneyline_prices:
        home_price = price_tuple[0]
        away_price = price_tuple[1]
        draw_price = price_tuple[2] if len(price_tuple) > 2 else None
        
        home_decimal = _moneyline_to_decimal(home_price)
        away_decimal = _moneyline_to_decimal(away_price)
        draw_decimal = _moneyline_to_decimal(draw_price) if draw_price is not None else None
        
        if home_decimal is not None and home_decimal > best_home_decimal:
            best_home_decimal = home_decimal
            home_ml = home_price
        if away_decimal is not None and away_decimal > best_away_decimal:
            best_away_decimal = away_decimal
            away_ml = away_price
        if is_soccer and draw_decimal is not None and draw_decimal > best_draw_decimal:
            best_draw_decimal = draw_decimal
            draw_ml = draw_price
    
    if home_ml is None and moneyline_prices:
        home_ml = moneyline_prices[0][0]
    if away_ml is None and moneyline_prices:
        away_ml = moneyline_prices[0][1]
    if is_soccer and draw_ml is None and moneyline_prices and len(moneyline_prices[0]) > 2 and moneyline_prices[0][2] is not None:
        draw_ml = moneyline_prices[0][2]

    # Extract spreads/totals (use first available)
    home_spread = None
    away_spread = None
    posted_total = None
    over_price = None
    under_price = None
    for bookmaker in game.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            market_key = (market.get("key") or "").lower()
            if market_key == "spreads" and (home_spread is None or away_spread is None):
                for outcome in market.get("outcomes", []):
                    name_raw = outcome.get("name", "").strip()
                    if not name_raw:
                        continue
                    outcome_team = normalize_team_code(league_upper, name_raw)
                    point = outcome.get("point")
                    if point is None:
                        continue

                    if outcome_team == home_team:
                        home_spread = float(point)
                    elif outcome_team == away_team:
                        away_spread = float(point)
            elif market_key == "totals":
                for outcome in market.get("outcomes", []):
                    point = outcome.get("point")
                    name_lower = (outcome.get("name") or "").strip().lower()
                    if point is not None and posted_total is None:
                        posted_total = float(point)
                    price_raw = outcome.get("price")
                    price_val = _safe_float(price_raw)
                    if name_lower.startswith("over") and over_price is None:
                        over_price = price_val
                    elif name_lower.startswith("under") and under_price is None:
                        under_price = price_val
        if (
            home_spread is not None
            and away_spread is not None
            and posted_total is not None
            and over_price is not None
            and under_price is not None
        ):
            break
    
    # Create features for home team
    def implied_prob(ml):
        if ml is None or ml == 0:
            return None
        if ml > 0:
            return 100 / (ml + 100)
        return -ml / (-ml + 100)
    
    # For soccer, calculate true implied probabilities accounting for draw
    if is_soccer and draw_ml is not None:
        # Calculate raw implied probabilities (including vig)
        home_implied_raw = implied_prob(home_ml)
        away_implied_raw = implied_prob(away_ml)
        draw_implied_raw = implied_prob(draw_ml)
        
        # Total probability (typically 105-110% with vig)
        total_implied = (home_implied_raw or 0) + (away_implied_raw or 0) + (draw_implied_raw or 0)
        
        if total_implied > 0:
            # Remove vig by normalizing
            home_implied = (home_implied_raw or 0) / total_implied
            away_implied = (away_implied_raw or 0) / total_implied
        else:
            home_implied = home_implied_raw
            away_implied = away_implied_raw
    else:
        # Two-way market: normalize home + away to 1.0
        home_implied_raw = implied_prob(home_ml)
        away_implied_raw = implied_prob(away_ml)
        prob_sum = (home_implied_raw or 0) + (away_implied_raw or 0)
        if prob_sum > 0:
            home_implied = (home_implied_raw or 0) / prob_sum
            away_implied = (away_implied_raw or 0) / prob_sum
        else:
            home_implied = home_implied_raw
            away_implied = away_implied_raw
    
    spread_home = home_spread if home_spread is not None else (-away_spread if away_spread is not None else None)
    spread_away = away_spread if away_spread is not None else (-home_spread if home_spread is not None else None)
    
    # Base features available in live games
    base_features: Dict[str, List[float]] = {
        "is_home": [1, 0],
        "moneyline": [home_ml if home_ml is not None else np.nan, away_ml if away_ml is not None else np.nan],
        "implied_prob": [home_implied if home_implied is not None else np.nan, away_implied if away_implied is not None else np.nan],
        "spread_line": [spread_home if spread_home is not None else np.nan, spread_away if spread_away is not None else np.nan],
        "total_line": [posted_total if posted_total is not None else np.nan, posted_total if posted_total is not None else np.nan],
        "espn_moneyline_open": [home_ml if home_ml is not None else np.nan, away_ml if away_ml is not None else np.nan],
        "espn_moneyline_close": [home_ml if home_ml is not None else np.nan, away_ml if away_ml is not None else np.nan],
        "espn_spread_open": [spread_home if spread_home is not None else np.nan, spread_away if spread_away is not None else np.nan],
        "espn_spread_close": [spread_home if spread_home is not None else np.nan, spread_away if spread_away is not None else np.nan],
        "espn_total_open": [posted_total if posted_total is not None else np.nan, posted_total if posted_total is not None else np.nan],
        "espn_total_close": [posted_total if posted_total is not None else np.nan, posted_total if posted_total is not None else np.nan],
        "over_moneyline": [over_price if over_price is not None else np.nan, over_price if over_price is not None else np.nan],
        "under_moneyline": [under_price if under_price is not None else np.nan, under_price if under_price is not None else np.nan],
        "over_implied_prob": [implied_prob(over_price) if over_price is not None else np.nan] * 2,
        "under_implied_prob": [implied_prob(under_price) if under_price is not None else np.nan] * 2,
    }
    
    # Create feature rows for both teams
    home_row: Dict[str, Any] = {}
    away_row: Dict[str, Any] = {}
    
    # Add base features
    for feature, values in base_features.items():
        home_row[feature] = values[0]
        away_row[feature] = values[1]

    home_rest = _estimate_rest_days(commence_time)
    away_rest = _estimate_rest_days(commence_time, travel_penalty=1.0)
    _apply_rest_metrics(home_row, home_rest, away_rest, is_home=True)
    _apply_rest_metrics(away_row, away_rest, home_rest, is_home=False)
    
    # Load advanced features based on league
    if model_features:
        # NFL/CFB features
        if league_upper in {"NFL", "CFB"}:
            # Season-level team metrics
            team_metrics = loader.load_team_metrics(season=current_season)
            if not team_metrics.empty and "team" in team_metrics.columns:
                home_metrics = team_metrics[team_metrics["team"].astype(str).str.upper() == home_team.upper()]
                away_metrics = team_metrics[team_metrics["team"].astype(str).str.upper() == away_team.upper()]
                
                for metric in ["season_off_epa_per_play", "season_off_success_rate", 
                               "season_def_epa_per_play", "season_def_success_rate"]:
                    if metric in model_features:
                        home_row[metric] = float(home_metrics[metric].iloc[0]) if not home_metrics.empty and metric in home_metrics.columns else np.nan
                        away_row[metric] = float(away_metrics[metric].iloc[0]) if not away_metrics.empty and metric in away_metrics.columns else np.nan
                        # Opponent metrics (swap home/away)
                        opp_metric = metric.replace("season_", "opponent_")
                        if opp_metric in model_features:
                            home_row[opp_metric] = away_row[metric]
                            away_row[opp_metric] = home_row[metric]
            
            # Rolling metrics
            for metric in ["off_epa_per_play_rolling_3", "off_success_rate_rolling_3", "off_pass_rate_rolling_3",
                          "def_epa_per_play_rolling_3", "def_success_rate_rolling_3",
                          "rolling_win_pct_3", "rolling_point_diff_3"]:
                if metric in model_features:
                    home_row[metric] = loader.get_rolling_metric(home_team, metric.replace("_rolling_3", ""), 
                                                                 game_date=game_date, window=3, default=np.nan)
                    away_row[metric] = loader.get_rolling_metric(away_team, metric.replace("_rolling_3", ""), 
                                                                 game_date=game_date, window=3, default=np.nan)
                    # Opponent metrics
                    opp_metric = metric.replace("off_", "opponent_off_").replace("def_", "opponent_def_").replace("rolling_", "opponent_rolling_")
                    if opp_metric in model_features:
                        home_row[opp_metric] = away_row[metric]
                        away_row[opp_metric] = home_row[metric]
            
            # Injuries
            for injury_type in ["injuries_out", "injuries_qb_out", "injuries_skill_out"]:
                if injury_type in model_features:
                    status_map = {
                        "injuries_out": "Out",
                        "injuries_qb_out": "Out",
                        "injuries_skill_out": "Out",
                    }
                    position_map = {
                        "injuries_qb_out": "QB",
                        "injuries_skill_out": None,  # Will filter by skill positions
                    }
                    status = status_map.get(injury_type, "Out")
                    position = position_map.get(injury_type)
                    
                    if injury_type == "injuries_skill_out":
                        # Count skill position players (RB, WR, TE)
                        injuries_df = loader.load_injuries(game_date=game_date)
                        if not injuries_df.empty:
                            home_team_inj = injuries_df[
                                (injuries_df["team"].astype(str).str.upper() == home_team.upper()) &
                                (injuries_df["status"] == status) &
                                (injuries_df["position"].isin(["RB", "WR", "TE"]))
                            ]
                            away_team_inj = injuries_df[
                                (injuries_df["team"].astype(str).str.upper() == away_team.upper()) &
                                (injuries_df["status"] == status) &
                                (injuries_df["position"].isin(["RB", "WR", "TE"]))
                            ]
                            home_row[injury_type] = len(home_team_inj)
                            away_row[injury_type] = len(away_team_inj)
                        else:
                            home_row[injury_type] = 0
                            away_row[injury_type] = 0
                    else:
                        home_row[injury_type] = loader.get_injury_count(home_team, game_date=game_date, 
                                                                       status=status, position=position)
                        away_row[injury_type] = loader.get_injury_count(away_team, game_date=game_date, 
                                                                         status=status, position=position)
                    
                    # Opponent injuries
                    opp_injury = injury_type.replace("injuries_", "opponent_injuries_")
                    if opp_injury in model_features:
                        home_row[opp_injury] = away_row[injury_type]
                        away_row[opp_injury] = home_row[injury_type]
            
            # Weather (NFL-specific)
            if league_upper == "NFL":
                weather = loader.get_weather_features(game_id=game.get("id"), game_date=game_date)
                for weather_feature in ["game_temperature_f", "game_wind_mph", "is_weather_precip", "is_weather_dome"]:
                    if weather_feature in model_features:
                        home_row[weather_feature] = weather.get(weather_feature, np.nan)
                        away_row[weather_feature] = weather.get(weather_feature, np.nan)
            
            # Game context (placeholder - would need schedule data)
            for context_feature in ["is_playoff", "is_division_game", "is_conference_game"]:
                if context_feature in model_features:
                    home_row[context_feature] = 0.0  # Default to regular season
                    away_row[context_feature] = 0.0
        
        # NBA features
        elif league_upper == "NBA":
            # Season-level team metrics
            team_metrics = loader.load_team_metrics(season=current_season)
            if not team_metrics.empty:
                for metric in ["E_OFF_RATING", "E_DEF_RATING", "E_NET_RATING", "E_PACE"]:
                    if metric in model_features:
                        home_row[metric] = loader.get_team_metric(home_team, metric, season=current_season, default=np.nan)
                        away_row[metric] = loader.get_team_metric(away_team, metric, season=current_season, default=np.nan)
            
            # Rolling metrics
            for metric in ["rolling_win_pct_3", "rolling_point_diff_3"]:
                if metric in model_features:
                    home_row[metric] = loader.get_rolling_metric(home_team, metric.replace("rolling_", "").replace("_3", ""), 
                                                                 game_date=game_date, window=3, default=np.nan)
                    away_row[metric] = loader.get_rolling_metric(away_team, metric.replace("rolling_", "").replace("_3", ""), 
                                                                 game_date=game_date, window=3, default=np.nan)
            
            # Injuries
            if "injuries_out" in model_features:
                home_row["injuries_out"] = loader.get_injury_count(home_team, game_date=game_date, status="Out")
                away_row["injuries_out"] = loader.get_injury_count(away_team, game_date=game_date, status="Out")
        
        # Soccer features
        elif league_upper in SOCCER_LEAGUES:
            # Advanced stats (xG, xGA, etc.)
            soccer_metrics = [
                "xG",
                "xGA",
                "xGD",
                "avg_xG",
                "avg_xGA",
                "shots",
                "shots_on_target",
                "avg_shots",
                "avg_shots_on_target",
                "expected_points",
                "points",
            ]
            for metric in soccer_metrics:
                if metric in (model_features or []):
                    home_row[metric] = loader.get_advanced_metric(
                        home_team,
                        metric,
                        season=current_season,
                        default=np.nan,
                    )
                    away_row[metric] = loader.get_advanced_metric(
                        away_team,
                        metric,
                        season=current_season,
                        default=np.nan,
                    )
            
            # Form (last 5 games)
            if "form_last_5" in model_features:
                home_row["form_last_5"] = loader.get_rolling_metric(
                    home_team,
                    "form",
                    game_date=game_date,
                    window=5,
                    default=np.nan,
                )
                away_row["form_last_5"] = loader.get_rolling_metric(
                    away_team,
                    "form",
                    game_date=game_date,
                    window=5,
                    default=np.nan,
                )
        
        # Fill any missing features with NaN (not 0) to avoid false edges
        for feature in model_features:
            if feature not in home_row:
                home_row[feature] = np.nan
            if feature not in away_row:
                away_row[feature] = np.nan
    
    features = [home_row, away_row]
    df = pd.DataFrame(features)
    
    return df


def make_predictions(games: List[Dict], model: Any, league: str = "NBA", model_path: Optional[Path] = None) -> pd.DataFrame:
    """Make predictions on live games."""
    predictions = []
    
    # Get the features the model expects
    if model_path is None:
        league_upper = league.upper()
        if league_upper == "NBA":
            model_path = Path("models/nba_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "NFL":
            model_path = Path("models/nfl_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "CFB":
            model_path = Path("models/cfb_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "EPL":
            model_path = Path("models/epl_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "LALIGA":
            model_path = Path("models/laliga_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "BUNDESLIGA":
            model_path = Path("models/bundesliga_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "SERIEA":
            model_path = Path("models/seriea_gradient_boosting_calibrated_moneyline.pkl")
        elif league_upper == "LIGUE1":
            model_path = Path("models/ligue1_gradient_boosting_calibrated_moneyline.pkl")
        else:
            raise ValueError(f"Unknown league: {league}")
    model_features = get_model_features(model_path)
    
    league_upper = league.upper()
    feature_loader = FeatureLoader(league_upper)
    totals_model_bundle = load_totals_model(league_upper)
    
    for game in games:
        try:
            game_id = game.get("id", f"{league_upper}_{game.get('commence_time', 'unknown')}")
            home_team = normalize_team_code(league_upper, game.get("home_team", ""))
            away_team = normalize_team_code(league_upper, game.get("away_team", ""))
            commence_time = game.get("commence_time", "")
            season_year = datetime.now().year
            commence_dt = None
            if commence_time:
                try:
                    commence_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                except Exception:
                    commence_dt = None
            if commence_dt:
                if league_upper in {"NBA", "NFL"}:
                    if commence_dt.month >= 8:
                        season_year = commence_dt.year
                    else:
                        season_year = commence_dt.year - 1
                else:
                    season_year = commence_dt.year
            is_soccer = league_upper in SOCCER_LEAGUES
            moneyline_prices = _extract_moneyline_prices(game, league_upper, home_team, away_team)
            
            # Prepare features
            features_df = prepare_features(
                game,
                league=league_upper,
                model_features=model_features,
                loader=feature_loader,
            )
            
            # Get features for prediction (only use features the model expects)
            missing_features = [f for f in model_features if f not in features_df.columns]
            
            if missing_features:
                LOGGER.warning("Missing features for game %s: %s", game_id, missing_features)
                # Fill missing features with NaN (not 0) to avoid false edges
                for feature in missing_features:
                    features_df[feature] = np.nan
            
            X = features_df[model_features]
            
            proba = model.predict_proba(X)

            def _normalize_prob(value: float | int | None) -> Optional[float]:
                if value is None:
                    return None
                try:
                    as_float = float(value)
                except (TypeError, ValueError):
                    return None
                if np.isnan(as_float):
                    return None
                return max(0.0, min(1.0, as_float))

            home_prob = _normalize_prob(proba[0, 1])
            away_prob_raw = _normalize_prob(proba[1, 1])

            if not is_soccer and home_prob is not None:
                away_prob = _normalize_prob(1.0 - home_prob)
            else:
                away_prob = away_prob_raw
            draw_prob = None  # Will be calculated for soccer below
            
            # Normalize probabilities to sum to 1.0 for two-way markets
            if not is_soccer:
                total_prob = home_prob + away_prob
                if total_prob > 0:
                    home_prob = home_prob / total_prob
                    away_prob = away_prob / total_prob
                else:
                    # Fallback if both are 0 or invalid
                    home_prob = 0.5
                    away_prob = 0.5
            
            home_ml = features_df.iloc[0]["moneyline"]
            away_ml = features_df.iloc[1]["moneyline"]
            home_implied = features_df.iloc[0]["implied_prob"]
            away_implied = features_df.iloc[1]["implied_prob"]
            spread_home = features_df.iloc[0].get("spread_line")
            total_line_value = features_df.iloc[0].get("total_line")
            over_price = features_df.iloc[0].get("over_moneyline")
            under_price = features_df.iloc[0].get("under_moneyline")
            over_implied = features_df.iloc[0].get("over_implied_prob")
            under_implied = features_df.iloc[0].get("under_implied_prob")
            total_line_numeric = float(total_line_value) if total_line_value is not None and not pd.isna(total_line_value) else None
            over_price_numeric = float(over_price) if over_price is not None and not pd.isna(over_price) else None
            under_price_numeric = float(under_price) if under_price is not None and not pd.isna(under_price) else None
            predicted_total = None
            if totals_model_bundle and total_line_numeric is not None and over_price_numeric is not None and under_price_numeric is not None:
                feature_values = {
                    "total_close": total_line_numeric,
                    "spread_close": float(spread_home) if spread_home is not None and not pd.isna(spread_home) else 0.0,
                    "home_moneyline_close": float(home_ml) if home_ml is not None and not pd.isna(home_ml) else 0.0,
                    "away_moneyline_close": float(away_ml) if away_ml is not None and not pd.isna(away_ml) else 0.0,
                }
                totals_features = pd.DataFrame([feature_values], columns=totals_model_bundle["feature_names"])
                try:
                    predicted_total = float(totals_model_bundle["regressor"].predict(totals_features)[0])
                    residual_std = float(totals_model_bundle.get("residual_std") or 12.0)
                    diff = predicted_total - total_line_numeric
                    over_prob_pred = 0.5 * (1.0 + math.erf(diff / (residual_std * math.sqrt(2.0))))
                    over_prob_pred = min(max(over_prob_pred, 0.0), 1.0)
                    under_prob_pred = 1.0 - over_prob_pred
                    over_edge_pred = (
                        over_prob_pred - _moneyline_to_prob(over_price_numeric) if over_price_numeric is not None else None
                    )
                    under_edge_pred = (
                        under_prob_pred - _moneyline_to_prob(under_price_numeric) if under_price_numeric is not None else None
                    )
                except Exception as exc:  # noqa: BLE001
                    LOGGER.debug("Totals model failed for %s: %s", game_id, exc)
                    predicted_total = None
                    over_prob_pred = None
                    under_prob_pred = None
                    over_edge_pred = None
                    under_edge_pred = None
            else:
                over_prob_pred = None
                under_prob_pred = None
                over_edge_pred = None
                under_edge_pred = None
            
            home_edge = None
            away_edge = None
            
            # For soccer, also calculate draw predictions
            draw_ml = None
            draw_implied = None
            draw_edge = None
            
            if is_soccer:
                # Extract draw moneyline from moneyline_prices
                for price_tuple in moneyline_prices:
                    if len(price_tuple) > 2 and price_tuple[2] is not None:
                        draw_ml = price_tuple[2]
                        break

                if draw_ml is not None:
                    # Recalculate ALL implied probabilities together to ensure they sum to 1.0
                    # (The ones in features_df were normalized without draw, so we need to renormalize)
                    def implied_prob(ml):
                        if ml is None or ml == 0:
                            return None
                        if ml > 0:
                            return 100 / (ml + 100)
                        return -ml / (-ml + 100)

                    home_implied_raw = implied_prob(home_ml) if (home_ml is not None and pd.notna(home_ml)) else 0
                    away_implied_raw = implied_prob(away_ml) if (away_ml is not None and pd.notna(away_ml)) else 0
                    draw_implied_raw = implied_prob(draw_ml)

                    # Normalize all three together to remove vig
                    total_implied = (home_implied_raw or 0) + (away_implied_raw or 0) + (draw_implied_raw or 0)
                    if total_implied > 0:
                        # Recalculate home and away implied to match draw normalization
                        home_implied = (home_implied_raw or 0) / total_implied
                        away_implied = (away_implied_raw or 0) / total_implied
                        draw_implied = draw_implied_raw / total_implied
                    else:
                        draw_implied = None

                # For soccer, model predicts home/away, so derive draw_prob as residual
                # Ensure probabilities sum to 1.0
                valid_probs: Dict[str, float] = {}
                if home_prob is not None and not pd.isna(home_prob):
                    valid_probs["home"] = max(float(home_prob), 0.0)
                if away_prob is not None and not pd.isna(away_prob):
                    valid_probs["away"] = max(float(away_prob), 0.0)
                if draw_prob is not None and not pd.isna(draw_prob):
                    valid_probs["draw"] = max(float(draw_prob), 0.0)

                if "draw" not in valid_probs and "home" in valid_probs and "away" in valid_probs:
                    residual = max(0.0, 1.0 - (valid_probs["home"] + valid_probs["away"]))
                    if residual > 0:
                        valid_probs["draw"] = residual
                        draw_prob = residual

                if valid_probs:
                    total_pred = sum(valid_probs.values())
                    if total_pred > 0:
                        home_val = valid_probs.get("home")
                        away_val = valid_probs.get("away")
                        draw_val = valid_probs.get("draw")

                        home_prob = (home_val / total_pred) if home_val is not None else None
                        away_prob = (away_val / total_pred) if away_val is not None else None
                        draw_prob = (draw_val / total_pred) if draw_val is not None else None
                    else:
                        # If no valid predictions, skip this game
                        LOGGER.warning("No valid predictions for game %s, skipping", game_id)
                        continue

                if home_prob is not None and pd.notna(home_implied):
                    home_edge = home_prob - home_implied
                if away_prob is not None and pd.notna(away_implied):
                    away_edge = away_prob - away_implied
                if draw_prob is not None and draw_implied is not None and pd.notna(draw_implied):
                    draw_edge = draw_prob - draw_implied
            else:
                if pd.notna(home_implied):
                    home_edge = home_prob - home_implied
                if pd.notna(away_implied):
                    away_edge = away_prob - away_implied
            
            predictions.append({
                "league": league_upper,
                "game_id": game_id,
                "commence_time": commence_time,
                "home_team": home_team,
                "away_team": away_team,
                "home_moneyline": home_ml,
                "away_moneyline": away_ml,
                "draw_moneyline": draw_ml if is_soccer else None,
                "total_line": total_line_numeric,
                "over_moneyline": over_price_numeric,
                "under_moneyline": under_price_numeric,
                "predicted_total_points": predicted_total,
                "home_predicted_prob": home_prob,
                "away_predicted_prob": away_prob,
                "draw_predicted_prob": draw_prob if is_soccer else None,
                "home_implied_prob": home_implied,
                "away_implied_prob": away_implied,
                "draw_implied_prob": draw_implied if is_soccer else None,
                "over_implied_prob": float(over_implied) if over_implied is not None and not pd.isna(over_implied) else None,
                "under_implied_prob": float(under_implied) if under_implied is not None and not pd.isna(under_implied) else None,
                "home_edge": home_edge,
                "away_edge": away_edge,
                "draw_edge": draw_edge if is_soccer else None,
                "over_predicted_prob": over_prob_pred,
                "under_predicted_prob": under_prob_pred,
                "over_edge": over_edge_pred,
                "under_edge": under_edge_pred,
                "predicted_at": datetime.now(timezone.utc).isoformat(),
                "result": None,  # Will be filled when game completes
                "home_score": None,
                "away_score": None,
            })
            
        except Exception as exc:
            LOGGER.warning("Failed to predict game %s: %s", game.get("id"), exc)
            continue
    
    if not predictions:
        return pd.DataFrame(
            columns=[
                "league",
                "game_id",
                "commence_time",
                "home_team",
                "away_team",
                "home_moneyline",
                "away_moneyline",
                "draw_moneyline",
                "home_predicted_prob",
                "away_predicted_prob",
                "draw_predicted_prob",
                "home_implied_prob",
                "away_implied_prob",
                "draw_implied_prob",
                "home_edge",
                "away_edge",
                "draw_edge",
                "predicted_at",
                "result",
                "home_score",
                "away_score",
            ]
        )
    
    return pd.DataFrame(predictions)


def save_predictions(predictions: pd.DataFrame, timestamp: Optional[str] = None) -> Path:
    """Save predictions to forward test directory."""
    if predictions.empty:
        LOGGER.warning("No predictions to save")
        return FORWARD_TEST_DIR / "predictions_empty.parquet"
    
    # Normalize datetime columns for consistency (avoids dtype issues when merging)
    datetime_cols = ["commence_time", "predicted_at", "result_updated_at"]
    for column in datetime_cols:
        if column in predictions.columns:
            predictions[column] = pd.to_datetime(predictions[column], errors="coerce", utc=True)

    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    predictions_path = FORWARD_TEST_DIR / f"predictions_{timestamp}.parquet"
    predictions.to_parquet(predictions_path, index=False)
    LOGGER.info("Saved %d predictions to %s", len(predictions), predictions_path)
    
    # Also save to master predictions file
    master_path = FORWARD_TEST_DIR / "predictions_master.parquet"
    if master_path.exists():
        try:
            existing = pd.read_parquet(master_path)
            for column in datetime_cols:
                if column in existing.columns:
                    existing[column] = pd.to_datetime(existing[column], errors="coerce", utc=True)
            # Check if game_id column exists in both
            if "game_id" in existing.columns and "game_id" in predictions.columns:
                # Merge with existing (update if game_id exists, add if new)
                existing = existing[~existing["game_id"].isin(predictions["game_id"])]
                combined = pd.concat([existing, predictions], ignore_index=True)
                # Keep the latest prediction per league/game combination
                if "predicted_at" in combined.columns:
                    combined = combined.sort_values("predicted_at").drop_duplicates(
                        subset=[col for col in ["league", "game_id"] if col in combined.columns],
                        keep="last",
                    )
                else:
                    combined = combined.drop_duplicates(
                        subset=[col for col in ["league", "game_id"] if col in combined.columns],
                        keep="last",
                    )
                combined.to_parquet(master_path, index=False)
                LOGGER.info("Updated master predictions file with %d total predictions", len(combined))
            else:
                # Append if structure is different
                combined = pd.concat([existing, predictions], ignore_index=True)
                if "predicted_at" in combined.columns:
                    combined = combined.sort_values("predicted_at").drop_duplicates(
                        subset=[col for col in ["league", "game_id"] if col in combined.columns],
                        keep="last",
                    )
                else:
                    combined = combined.drop_duplicates(
                        subset=[col for col in ["league", "game_id"] if col in combined.columns],
                        keep="last",
                    )
                combined.to_parquet(master_path, index=False)
        except Exception as exc:
            LOGGER.warning("Failed to merge with existing predictions: %s. Creating new file.", exc)
            predictions.to_parquet(master_path, index=False)
    else:
        predictions.to_parquet(master_path, index=False)
        LOGGER.info("Created master predictions file")
    
    return predictions_path


def _fetch_recent_scores(
    league: str,
    *,
    days_from: int = 5,
    dotenv_path: Optional[Path] = None,
) -> Dict[str, Tuple[int, int]]:
    """Fetch recently completed scores for the given league."""
    try:
        settings = OddsAPISettings.from_env(dotenv_path)
    except RuntimeError as exc:
        LOGGER.warning("Unable to load Odds API settings for %s scores: %s", league, exc)
        return {}

    sport_key = _get_sport_key(league)
    url = f"{settings.base_url}/sports/{sport_key}/scores/"
    params = {"apiKey": settings.api_key, "daysFrom": min(max(1, days_from), 3)}

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - defensive network guard
        LOGGER.warning("Failed to fetch %s scores from The Odds API: %s", league, exc)
        return {}

    results: Dict[str, Tuple[int, int]] = {}
    for event in response.json() or []:
        if not event.get("completed"):
            continue
        event_id = event.get("id")
        if not event_id:
            continue

        scores = event.get("scores") or []
        home_score = None
        away_score = None
        for entry in scores:
            name = entry.get("name")
            score_value = entry.get("score")
            if name is None or score_value is None:
                continue
            try:
                parsed_score = int(score_value)
            except (TypeError, ValueError):
                continue
            if name == event.get("home_team"):
                home_score = parsed_score
            elif name == event.get("away_team"):
                away_score = parsed_score

        if home_score is None or away_score is None:
            if len(scores) == 2:
                try:
                    home_score = int(scores[0].get("score"))
                    away_score = int(scores[1].get("score"))
                except (TypeError, ValueError):
                    continue
            else:
                continue

        results[str(event_id)] = (home_score, away_score)

    return results


def _fetch_espn_cfb_scores(dates: Iterable[date]) -> Dict[Tuple[str, str, date], Tuple[int, int]]:
    """Fetch final CFB scores from ESPN scoreboard for specific dates."""
    unique_dates = sorted({d for d in dates if d is not None})
    if not unique_dates:
        return {}

    results: Dict[Tuple[str, str, date], Tuple[int, int]] = {}
    for target_date in unique_dates:
        params = {"dates": target_date.strftime("%Y%m%d")}
        try:
            response = requests.get(ESPN_CFB_SCOREBOARD_URL, params=params, timeout=20)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network guard
            LOGGER.warning("Failed to fetch ESPN CFB scores for %s: %s", target_date, exc)
            continue

        payload = response.json()
        for event in payload.get("events", []):
            competitions = event.get("competitions") or []
            if not competitions:
                continue
            competition = competitions[0]
            status_state = competition.get("status", {}).get("type", {}).get("state")
            if status_state != "post":
                continue

            competitors = competition.get("competitors") or []
            if len(competitors) != 2:
                continue

            home_comp = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away_comp = next((c for c in competitors if c.get("homeAway") == "away"), None)
            if not home_comp or not away_comp:
                continue

            try:
                home_score = int(home_comp.get("score"))
                away_score = int(away_comp.get("score"))
            except (TypeError, ValueError):
                continue

            home_name = home_comp.get("team", {}).get("displayName", "")
            away_name = away_comp.get("team", {}).get("displayName", "")
            home_code = normalize_team_code("CFB", home_name)
            away_code = normalize_team_code("CFB", away_name)
            if not home_code or not away_code:
                continue

            key = (home_code.upper(), away_code.upper(), target_date)
            results[key] = (home_score, away_score)

    return results


def _fetch_soccer_scores_from_espn(
    league_dates: Dict[str, List[pd.Timestamp]],
) -> Dict[Tuple[str, str, str, date], Tuple[int, int]]:
    """Pull final soccer scores from ESPN for the provided leagues/date ranges."""
    if not league_dates:
        return {}

    lookup: Dict[Tuple[str, str, str, date], Tuple[int, int]] = {}
    for league_code, timestamps in league_dates.items():
        valid_dates = sorted(
            {ts.date() for ts in timestamps if ts is not None and not pd.isna(ts)}
        )
        if not valid_dates:
            continue
        start_date = min(valid_dates)
        end_date = max(valid_dates)
        try:
            espn_df = fetch_from_espn(
                leagues=[league_code],
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:  # pragma: no cover - network guard
            LOGGER.warning("Failed to fetch ESPN soccer scores for %s: %s", league_code, exc)
            continue
        if espn_df.empty:
            continue

        for _, rec in espn_df.iterrows():
            if not rec.get("is_final"):
                continue
            home_name = rec.get("home_team_name")
            away_name = rec.get("away_team_name")
            if not home_name or not away_name:
                continue
            home_code = normalize_team_code(league_code, home_name)
            away_code = normalize_team_code(league_code, away_name)
            if not home_code or not away_code:
                continue
            home_score = rec.get("home_score")
            away_score = rec.get("away_score")
            if home_score is None or away_score is None:
                continue
            try:
                home_score_int = int(home_score)
                away_score_int = int(away_score)
            except (TypeError, ValueError):
                continue
            gameday = rec.get("gameday")
            if not gameday:
                continue
            try:
                game_date = datetime.strptime(str(gameday), "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue
            key = (league_code.upper(), home_code.upper(), away_code.upper(), game_date)
            lookup[key] = (home_score_int, away_score_int)

    return lookup


def update_results(
    *,
    league: Optional[str] = None,
    dotenv_path: Optional[Path] = None,
) -> None:
    """Update forward test predictions with actual game results."""
    master_path = FORWARD_TEST_DIR / "predictions_master.parquet"
    if not master_path.exists():
        LOGGER.warning("No predictions file found")
        return
    
    predictions = pd.read_parquet(master_path)

    now = datetime.now(timezone.utc)
    max_time_delta = pd.Timedelta(hours=12)

    # Ensure commence_time is timezone-aware UTC for comparisons
    if "commence_time" in predictions.columns:
        predictions["commence_time"] = pd.to_datetime(predictions["commence_time"], errors="coerce", utc=True)
    else:
        predictions["commence_time"] = pd.NaT

    league_upper = league.upper() if league else None
    if league_upper and "league" in predictions.columns:
        target_mask = predictions["league"].astype(str).str.upper() == league_upper
    elif league_upper:
        LOGGER.warning("Predictions file missing league column; updating all leagues instead of %s", league_upper)
        target_mask = pd.Series(True, index=predictions.index)
    else:
        target_mask = pd.Series(True, index=predictions.index)

    target_predictions = predictions.loc[target_mask].copy()
    if target_predictions.empty:
        LOGGER.info("No predictions found for league %s", league_upper or "ALL")
        return

    save_required = False

    def _apply_result(index: int, home_score: int, away_score: int) -> None:
        nonlocal save_required
        if home_score > away_score:
            result_str = "home"
        elif away_score > home_score:
            result_str = "away"
        else:
            result_str = "tie"
        result_timestamp = datetime.now(timezone.utc).isoformat()
        for frame in (predictions, target_predictions):
            if index in frame.index:
                frame.at[index, "home_score"] = home_score
                frame.at[index, "away_score"] = away_score
                frame.at[index, "result"] = result_str
                frame.at[index, "result_updated_at"] = result_timestamp
        save_required = True

    def _clear_result(index: int) -> None:
        nonlocal save_required
        columns_to_clear = [
            col for col in ["result", "home_score", "away_score", "result_updated_at"] if col in predictions.columns
        ]
        if columns_to_clear:
            predictions.loc[index, columns_to_clear] = None
            if index in target_predictions.index:
                target_predictions.loc[index, columns_to_clear] = None
            save_required = True

    soccer_scores: Dict[Tuple[str, str, str, date], Tuple[int, int]] = {}
    soccer_subset = pd.DataFrame()
    if "league" in target_predictions.columns and "commence_time" in target_predictions.columns:
        soccer_subset = target_predictions[
            target_predictions["league"].astype(str).str.upper().isin(SOCCER_LEAGUES)
            & target_predictions["commence_time"].notna()
            & (target_predictions["commence_time"] <= now)
            & (
                target_predictions["result"].isna()
                | (target_predictions["commence_time"] >= now - pd.Timedelta(days=SOCCER_SCORE_LOOKBACK_DAYS))
            )
        ]
        if not soccer_subset.empty:
            league_dates: Dict[str, List[pd.Timestamp]] = {}
            grouped = soccer_subset.groupby(soccer_subset["league"].astype(str).str.upper())
            for league_code, group in grouped:
                league_dates[league_code] = [ts for ts in group["commence_time"] if pd.notna(ts)]
            soccer_scores = _fetch_soccer_scores_from_espn(league_dates)

    def _lookup_soccer_score(row: pd.Series) -> Optional[Tuple[int, int]]:
        if not soccer_scores:
            return None
        league_code = str(row.get("league") or league_upper or "").upper()
        if league_code not in SOCCER_LEAGUES:
            return None
        home_team = str(row.get("home_team") or "").upper()
        away_team = str(row.get("away_team") or "").upper()
        if not home_team or not away_team:
            return None
        ts = row.get("commence_time")
        if ts is None or pd.isna(ts):
            return None
        if isinstance(ts, str):
            try:
                ts = pd.to_datetime(ts, utc=True)
            except Exception:
                return None
        try:
            base_date = ts.date()
        except Exception:
            return None
        date_candidates = list(
            dict.fromkeys(
                [
                    base_date,
                    base_date - timedelta(days=1),
                    base_date + timedelta(days=1),
                ]
            )
        )
        for cand in date_candidates:
            key = (league_code, home_team, away_team, cand)
            match = soccer_scores.get(key)
            if match:
                return match
        return None

    if soccer_scores and not soccer_subset.empty:
        for idx, row in soccer_subset.iterrows():
            match = _lookup_soccer_score(row)
            if not match:
                continue
            stored_home = row.get("home_score")
            stored_away = row.get("away_score")
            try:
                stored_home_int = None if pd.isna(stored_home) else int(stored_home)
            except (TypeError, ValueError):
                stored_home_int = None
            try:
                stored_away_int = None if pd.isna(stored_away) else int(stored_away)
            except (TypeError, ValueError):
                stored_away_int = None
            if (
                row.get("result") is None
                or stored_home_int is None
                or stored_away_int is None
                or stored_home_int != match[0]
                or stored_away_int != match[1]
            ):
                _apply_result(idx, match[0], match[1])

    # Clear any results that were accidentally recorded for future games
    if "result" in target_predictions.columns:
        future_results_mask = (
            target_predictions["commence_time"].notna()
            & (target_predictions["commence_time"] > now)
            & target_predictions["result"].notna()
        )
        if future_results_mask.any():
            LOGGER.info("Clearing results for %d future games", future_results_mask.sum())
            for idx in target_predictions[future_results_mask].index:
                _clear_result(idx)

    def _find_final_score(row: pd.Series, conn) -> Optional[Tuple[int, int]]:
        league = row.get("league")
        game_id = row.get("game_id")
        commence_time = row.get("commence_time")
        home_team = row.get("home_team")
        away_team = row.get("away_team")

        if league is None or pd.isna(league):
            if isinstance(game_id, str):
                if game_id.startswith("NFL_"):
                    league = "NFL"
                elif game_id.startswith("CFB_"):
                    league = "CFB"
                elif game_id.startswith("EPL_"):
                    league = "EPL"
                elif game_id.startswith("LALIGA_"):
                    league = "LALIGA"
                elif game_id.startswith("BUNDESLIGA_"):
                    league = "BUNDESLIGA"
                elif game_id.startswith("SERIEA_"):
                    league = "SERIEA"
                elif game_id.startswith("LIGUE1_"):
                    league = "LIGUE1"
                else:
                    league = "NBA"
            else:
                league = "NBA"
        league = str(league).upper()

        def _valid_match(db_row: sqlite3.Row) -> Optional[Tuple[int, int]]:
            start_time_value = db_row["start_time_utc"]
            if start_time_value is None:
                return None
            try:
                start_time = pd.to_datetime(start_time_value, utc=True)
            except Exception:
                return None

            if commence_time is not None and not pd.isna(commence_time):
                time_diff = abs(start_time - commence_time)
                if time_diff > max_time_delta:
                    is_midnight = (
                        start_time.hour == 0
                        and start_time.minute == 0
                        and start_time.second == 0
                        and start_time.microsecond == 0
                    )
                    date_diff_days = abs((start_time.date() - commence_time.date()).days)
                    if not (is_midnight and date_diff_days <= 1):
                        return None

            home_score = db_row["home_score"]
            away_score = db_row["away_score"]
            if home_score is None or away_score is None:
                return None
            return int(home_score), int(away_score)

        # First try exact match by game_id / odds_api_id
        result_rows = conn.execute(
            """
            SELECT r.home_score, r.away_score, g.start_time_utc
            FROM game_results r
            JOIN games g ON g.game_id = r.game_id
            WHERE g.sport_id = (SELECT sport_id FROM sports WHERE league = ?)
            AND (g.game_id = ? OR g.odds_api_id = ?)
            AND g.status = 'final'
            AND g.start_time_utc IS NOT NULL
            """,
            (league, game_id, game_id),
        ).fetchall()
        for db_row in result_rows or []:
            match = _valid_match(db_row)
            if match:
                return match

        # Fallback: match by teams and date if commence_time available
        if commence_time is not None and not pd.isna(commence_time) and home_team and away_team:
            try:
                game_date = commence_time.date()
                date_candidates = [game_date]
                date_candidates.append((commence_time - pd.Timedelta(days=1)).date())
                date_candidates.append((commence_time + pd.Timedelta(days=1)).date())
                # Ensure uniqueness
                date_candidates = list(dict.fromkeys(date_candidates))
                while len(date_candidates) < 3:
                    date_candidates.append(date_candidates[-1])

                result_rows = conn.execute(
                    """
                    SELECT r.home_score, r.away_score, g.start_time_utc
                    FROM game_results r
                    JOIN games g ON g.game_id = r.game_id
                    JOIN teams ht ON ht.team_id = g.home_team_id
                    JOIN teams at ON at.team_id = g.away_team_id
                    WHERE g.sport_id = (SELECT sport_id FROM sports WHERE league = ?)
                    AND ht.code = ?
                    AND at.code = ?
                    AND (DATE(g.start_time_utc) = ?
                         OR DATE(g.start_time_utc) = ?
                         OR DATE(g.start_time_utc) = ?)
                    AND g.status = 'final'
                    AND g.start_time_utc IS NOT NULL
                    """,
                    (
                        league,
                        home_team,
                        away_team,
                        date_candidates[0].isoformat(),
                        date_candidates[1].isoformat(),
                        date_candidates[2].isoformat(),
                    ),
                ).fetchall()
            except Exception as exc:  # pragma: no cover - safety
                LOGGER.debug("Could not match by teams/date for %s: %s", game_id, exc)
                result_rows = []
            for db_row in result_rows or []:
                match = _valid_match(db_row)
                if match:
                    return match

        return None

    # Build quick lookup of recently completed scores per league via The Odds API
    if league_upper:
        leagues_to_check = [league_upper]
    elif "league" in target_predictions.columns:
        leagues_to_check = sorted(
            {str(value).upper() for value in target_predictions["league"].dropna().unique() if str(value)}
        )
    else:
        leagues_to_check = sorted(SUPPORTED_LEAGUES)

    score_lookup: Dict[str, Tuple[int, int]] = {}
    for league_code in leagues_to_check:
        fetched_scores = _fetch_recent_scores(league_code, days_from=7, dotenv_path=dotenv_path)
        if fetched_scores:
            score_lookup.update(fetched_scores)

    with connect() as conn:
        # Re-validate recent games that already have results to ensure they are confirmed
        if "result" in target_predictions.columns:
            recent_mask = (
                target_predictions["result"].notna()
                & target_predictions["commence_time"].notna()
                & (target_predictions["commence_time"] > now - pd.Timedelta(hours=6))
            )
            if recent_mask.any():
                for idx, row in target_predictions.loc[recent_mask].iterrows():
                    if not _find_final_score(row, conn):
                        LOGGER.info("Clearing result for %s (not confirmed in DB)", row.get("game_id"))
                        _clear_result(idx)

        # Find predictions without results (only games that have already started)
        incomplete = target_predictions[target_predictions["result"].isna()].copy()
        if "commence_time" in incomplete.columns:
            incomplete = incomplete[incomplete["commence_time"].notna() & (incomplete["commence_time"] <= now)].copy()

        espn_cfb_scores: Dict[Tuple[str, str, date], Tuple[int, int]] = {}
        if league_upper == "CFB" and not incomplete.empty and "commence_time" in incomplete.columns:
            cfb_dates: List[date] = []
            for ts in incomplete["commence_time"].dropna():
                try:
                    local_date = ts.tz_convert(EASTERN_TZ).date()
                except Exception:
                    local_date = ts.date()
                cfb_dates.append(local_date)
            espn_cfb_scores = _fetch_espn_cfb_scores(cfb_dates)

        if len(incomplete) == 0:
            if save_required:
                predictions.to_parquet(master_path, index=False)
                LOGGER.info("Saved predictions after clearing invalid results")
            LOGGER.info("All predictions already have results")
            return

        LOGGER.info(
            "Checking %d predictions for results%s",
            len(incomplete),
            f" ({league_upper})" if league_upper else "",
        )

        for idx, row in incomplete.iterrows():
            game_id = row.get("game_id")
            if game_id and score_lookup:
                score_pair = score_lookup.get(str(game_id))
            else:
                score_pair = None

            if score_pair:
                home_score, away_score = score_pair
                _apply_result(idx, home_score, away_score)
                continue

            match = _find_final_score(row, conn)

            if not match:
                match = _lookup_soccer_score(row)

            if not match and league_upper == "CFB" and espn_cfb_scores:
                ts = row.get("commence_time")
                if ts is not None and not pd.isna(ts):
                    try:
                        local_date = ts.tz_convert(EASTERN_TZ).date()
                    except Exception:
                        local_date = ts.date()
                    home_code = str(row.get("home_team") or "").upper()
                    away_code = str(row.get("away_team") or "").upper()
                    match = espn_cfb_scores.get((home_code, away_code, local_date))

            if not match:
                continue
            home_score, away_score = match
            _apply_result(idx, home_score, away_score)

    if not save_required:
        LOGGER.info("No completed games found to update")
        return

    predictions.to_parquet(master_path, index=False)
    LOGGER.info("Updated predictions saved")


def generate_report(league: Optional[str] = None) -> Dict:
    """Generate forward testing performance report."""
    master_path = FORWARD_TEST_DIR / "predictions_master.parquet"
    if not master_path.exists():
        return {"error": "No predictions file found"}
    
    predictions = pd.read_parquet(master_path)
    
    # Filter by league if specified
    if league:
        predictions = predictions[predictions.get("league", "").str.upper() == league.upper()]
    
    # Filter to completed games
    completed = predictions[predictions["result"].notna()].copy()
    
    if len(completed) == 0:
        return {
            "total_predictions": len(predictions),
            "completed_games": 0,
            "message": "No games completed yet. Run update_results after games finish.",
        }
    
    # Calculate performance
    
    # Create bet rows (home and away)
    bets = []
    for _, row in completed.iterrows():
        # Home team bet
        if row["home_edge"] and row["home_edge"] >= 0.06:
            bets.append({
                "game_id": row["game_id"],
                "team": row["home_team"],
                "opponent": row["away_team"],
                "moneyline": row["home_moneyline"],
                "predicted_prob": row["home_predicted_prob"],
                "implied_prob": row["home_implied_prob"],
                "edge": row["home_edge"],
                "win": 1 if row["result"] == "home" else 0,
            })
        
        # Away team bet
        if row["away_edge"] and row["away_edge"] >= 0.06:
            bets.append({
                "game_id": row["game_id"],
                "team": row["away_team"],
                "opponent": row["home_team"],
                "moneyline": row["away_moneyline"],
                "predicted_prob": row["away_predicted_prob"],
                "implied_prob": row["away_implied_prob"],
                "edge": row["away_edge"],
                "win": 1 if row["result"] == "away" else 0,
            })
    
    if len(bets) == 0:
        return {
            "total_predictions": len(predictions),
            "completed_games": len(completed),
            "recommended_bets": 0,
            "message": "No bets met edge threshold (>= 0.06)",
        }
    
    bets_df = pd.DataFrame(bets)
    
    # Simulate betting
    starting = 10000
    bankroll: float = float(starting)
    for _, bet in bets_df.iterrows():
        stake = 100
        ml = float(bet["moneyline"])
        if bet["win"] == 1:
            if ml > 0:
                profit = stake * (ml / 100.0)
            else:
                profit = stake * (100.0 / (-ml))
        else:
            profit = -stake
        bankroll += profit
    
    wins = bets_df["win"].sum()
    total = len(bets_df)
    
    return {
        "total_predictions": len(predictions),
        "completed_games": len(completed),
        "recommended_bets": len(bets_df),
        "wins": int(wins),
        "losses": int(total - wins),
        "win_rate": float(wins / total) if total > 0 else 0,
        "starting_bankroll": starting,
        "ending_bankroll": float(bankroll),
        "net_profit": float(bankroll - starting),
        "roi": float((bankroll - starting) / starting) if starting > 0 else 0,
        "mean_predicted_prob": float(bets_df["predicted_prob"].mean()) if len(bets_df) > 0 else 0,
        "mean_edge": float(bets_df["edge"].mean()) if len(bets_df) > 0 else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Forward test NBA/NFL/CFB/European soccer betting model on live games")
    parser.add_argument(
        "action",
        choices=["predict", "update", "report"],
        help="Action to perform: predict (make new predictions), update (update with results), report (show performance)",
    )
    parser.add_argument(
        "--league",
        choices=SUPPORTED_LEAGUES + ["ALL"],
        default=None,
        help="League to use (default: ALL for predict/update, NBA for report).",
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=None,
        help="Path to model file (defaults to league-specific model)",
    )
    parser.add_argument(
        "--dotenv",
        type=Path,
        default=None,
        help="Path to .env file containing ODDS_API_KEY",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()
    
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    if args.action == "predict":
        leagues = SUPPORTED_LEAGUES if args.league in (None, "ALL") else [args.league]
        for league in leagues:
            LOGGER.info("=== Running forward test for %s ===", league)
            try:
                model = load_model(args.model, league=league)
            except FileNotFoundError as exc:
                LOGGER.warning("Skipping %s: %s", league, exc)
                continue

            games = fetch_live_games(league=league, dotenv_path=args.dotenv)
            if not games:
                LOGGER.info("No live games found for %s", league)
                continue

            predictions = make_predictions(games, model, league=league, model_path=args.model)
            save_predictions(predictions)

            edge_threshold = 0.06
            recs = predictions[
                (predictions["home_edge"] >= edge_threshold) | (predictions["away_edge"] >= edge_threshold)
            ]

            if len(recs) > 0:
                LOGGER.info("\n=== RECOMMENDATIONS (Edge >= %.2f) ===", edge_threshold)
                for _, row in recs.iterrows():
                    if row["home_edge"] >= edge_threshold:
                        LOGGER.info(
                            "  %s vs %s: Home edge=%.1f%%, Pred=%.1f%%, ML=%s",
                            row["home_team"],
                            row["away_team"],
                            row["home_edge"] * 100,
                            row["home_predicted_prob"] * 100,
                            int(row["home_moneyline"]) if pd.notna(row["home_moneyline"]) else "N/A",
                        )
                    if row["away_edge"] >= edge_threshold:
                        LOGGER.info(
                            "  %s vs %s: Away edge=%.1f%%, Pred=%.1f%%, ML=%s",
                            row["away_team"],
                            row["home_team"],
                            row["away_edge"] * 100,
                            row["away_predicted_prob"] * 100,
                            int(row["away_moneyline"]) if pd.notna(row["away_moneyline"]) else "N/A",
                        )
            else:
                LOGGER.info("No bets meet edge threshold of %.2f for %s", edge_threshold, league)

    elif args.action == "update":
        target_league = None if args.league in (None, "ALL") else args.league
        update_results(league=target_league, dotenv_path=args.dotenv)
    
    elif args.action == "report":
        league = args.league or SUPPORTED_LEAGUES[0]
        report = generate_report(league=league)
        
        if "error" in report:
            print(f"Error: {report['error']}")
        elif "message" in report:
            print(report["message"])
        else:
            print(f"\n=== FORWARD TESTING REPORT ({league}) ===")
            print(f"Total Predictions: {report['total_predictions']}")
            print(f"Completed Games: {report['completed_games']}")
            print(f"Recommended Bets: {report['recommended_bets']}")
            if report["recommended_bets"] > 0:
                print(f"Wins: {report['wins']}")
                print(f"Losses: {report['losses']}")
                print(f"Win Rate: {report['win_rate']:.1%}")
                print(f"Mean Predicted Prob: {report['mean_predicted_prob']:.1%}")
                print(f"Mean Edge: {report['mean_edge']:.1%}")
                print("\nBankroll Simulation:")
                print(f"  Starting: ${report['starting_bankroll']:,.0f}")
                print(f"  Ending: ${report['ending_bankroll']:,.0f}")
                print(f"  Net Profit: ${report['net_profit']:,.0f}")
                print(f"  ROI: {report['roi']:.1%}")


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
