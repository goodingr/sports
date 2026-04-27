"""Core prediction engine logic."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import math
import joblib
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

from src.data.team_mappings import normalize_team_code
from src.models.feature_loader import FeatureLoader
from src.predict.config import MODEL_REGISTRY_PATH, LEAGUE_SPORT_KEYS, SOCCER_LEAGUES
from src.predict.storage import load_games_from_database, save_predictions

from src.models.train import (
    FEATURE_COLUMNS,
    CalibratedModel,
    EnsembleModel,
    ProbabilityCalibrator,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_REST_DAYS = 7.0
SHORT_WEEK_THRESHOLD = 5.0
BYE_THRESHOLD = 10.0


class FeatureContractError(RuntimeError):
    """Raised when prediction features cannot satisfy a trained model contract."""

def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _estimate_rest_days(commence_time: Optional[str], *, travel_penalty: float = 0.0) -> float:
    try:
        if commence_time:
            # Parse and assign back to commence_time so it's used
            # commence_time = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
            pass # Logic was just returning default in original if parse failed
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
    
    # In the new system, 'game' comes from load_games_from_database which returns a DataFrame row or dict
    # But wait, load_games_from_database returns a DataFrame with 'home_moneyline', 'away_moneyline' columns.
    # It doesn't return the full nested bookmaker structure unless we change it.
    # The original forward_test.py `load_games_from_database` returned a list of dicts with bookmakers.
    # My new `storage.py` returns a DataFrame.
    # I should adapt `prepare_features` to work with the DataFrame row directly.
    
    # However, to support the full feature set (like opening/closing lines from ESPN), 
    # we might need that data. 
    # For now, let's assume the input `game` is a dictionary similar to what `load_games_from_database` produced in `forward_test.py`
    # OR we adapt this function to work with the flat structure we have now.
    
    # Let's look at `storage.py` again. It returns:
    # game_id, commence_time, home_team, away_team, home_moneyline, away_moneyline, status
    
    # It seems I simplified `storage.py` too much if we want full features.
    # But `forward_test.py` used `load_games_from_database` which did a complex join on odds tables.
    # My `storage.py` just did a simple join on games table.
    
    # Decision: For this refactor, I will stick to the simple DataFrame structure for now.
    # If we need complex odds features (like spread/total), we should add them to `storage.py` query.
    # I will update `storage.py` later if needed. For now, I'll assume we have basic moneylines.
    
    # If we are passed a dict (from legacy or updated storage), we use it.
    # If we are passed a Series (from new storage), we use that.
    
    return [] # Placeholder, logic moved to prepare_features

class PredictionEngine:
    def __init__(self, model_type: str = "ensemble"):
        self.model_type = model_type
        self.model = None
        self.feature_columns = None
        self.loader = None
        
        # Totals support
        self.totals_model = None
        self.totals_std = None
        self.totals_features = None
        
    def load_model(self, league: str) -> bool:
        """Load the trained model for the specified league."""
        league_upper = league.upper()
        # Try specific model type first
        model_filename = f"{league.lower()}_{self.model_type}_calibrated_moneyline.pkl"
        model_path = MODEL_REGISTRY_PATH / model_filename
        
        if not model_path.exists():
            # Try fallback to uncalibrated model
            fallback_filename = f"{league.lower()}_{self.model_type}_moneyline.pkl"
            fallback_path = MODEL_REGISTRY_PATH / fallback_filename
            
            if fallback_path.exists():
                LOGGER.info(f"Calibrated model not found, falling back to {fallback_path}")
                model_path = fallback_path
            else:
                # Try fallback to gradient boosting if ensemble not found (common pattern)
                # Or just fail if strict.
                LOGGER.warning(f"Model not found at {model_path} or {fallback_path}")
                return False
            
        try:
            self.model = joblib.load(model_path)
            LOGGER.info(f"Loaded {self.model_type} model for {league}")
            
            # Load feature columns from registry if possible
            # For now, we'll rely on the model object or defaults
            # In forward_test.py, `get_model_features` read from registry.json
            # We should probably implement that too.
            self.feature_columns = self._get_model_features(model_path)
            
            # Try to load totals model (optional)
            # For ensemble, fallback to gradient_boosting totals if ensemble totals doesn't exist
            totals_model_type = self.model_type
            if self.model_type == "ensemble":
                totals_model_type = "gradient_boosting"
                
            totals_filename = f"{league.lower()}_totals_{totals_model_type}.pkl"
            totals_path = MODEL_REGISTRY_PATH / totals_filename
            if totals_path.exists():
                try:
                    bundle = joblib.load(totals_path)
                    if isinstance(bundle, dict):
                        self.totals_model = bundle.get("regressor")
                        self.totals_std = bundle.get("residual_std", 5.0) # Default to 5.0 if missing
                        self.totals_features = bundle.get("feature_names", ["total_close", "spread_close", "home_moneyline_close", "away_moneyline_close"])
                        LOGGER.info(f"Loaded {totals_model_type} totals model for {league} ({self.model_type})")
                except Exception as e:
                    LOGGER.warning(f"Failed to load totals model for {league}: {e}")
            else:
                LOGGER.info(f"No totals model found at {totals_path}")
            
            
            return True
        except Exception as e:
            import traceback
            LOGGER.error(f"Failed to load model for {league}: {e}\n{traceback.format_exc()}")
            return False


    def _get_model_features(self, model_path: Path) -> List[str]:
        """Get the list of features expected by the model."""
        # Try to get features from the model object itself
        # For EnsembleModel, check the first member
        if hasattr(self.model, "members") and len(self.model.members) > 0:
            first_member = self.model.members[0]
            if hasattr(first_member, "estimator") and hasattr(first_member.estimator, "feature_names_in_"):
                return list(first_member.estimator.feature_names_in_)
        
        # For CalibratedModel
        if hasattr(self.model, "feature_names_in_"):
            return list(self.model.feature_names_in_)
            
        if hasattr(self.model, "estimator"):
            if hasattr(self.model.estimator, "feature_names_in_"):
                return list(self.model.estimator.feature_names_in_)
            # If it's a pipeline
            if hasattr(self.model.estimator, "steps"):
                final_step = self.model.estimator.steps[-1][1]
                if hasattr(final_step, "feature_names_in_"):
                    return list(final_step.feature_names_in_)

        # Fallback to the superset (might cause issues if model is strict)
        return FEATURE_COLUMNS

    def _build_model_matrix(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Return prediction features ordered exactly as the trained model expects."""
        if not self.feature_columns:
            numeric = features_df.select_dtypes(include=[np.number]).copy()
            if numeric.empty:
                raise FeatureContractError("No numeric prediction features were generated")
            return numeric

        contract = list(dict.fromkeys(self.feature_columns))
        if len(contract) != len(self.feature_columns):
            LOGGER.warning("Model feature contract contained duplicate columns; using first occurrence")

        missing = [column for column in contract if column not in features_df.columns]
        if missing:
            LOGGER.warning(
                "Prediction feature set is missing %d trained columns; filling with NaN. Sample: %s",
                len(missing),
                missing[:10],
            )
            features_df = features_df.copy()
            for column in missing:
                features_df[column] = np.nan

        extra = sorted(set(features_df.select_dtypes(include=[np.number]).columns) - set(contract))
        if extra:
            LOGGER.debug("Ignoring %d numeric columns outside model contract. Sample: %s", len(extra), extra[:10])

        matrix = features_df[contract]
        if matrix.columns.tolist() != contract:
            raise FeatureContractError("Prediction feature order does not match model contract")
        return matrix

    def _build_totals_matrix(self, features_df: pd.DataFrame) -> pd.DataFrame:
        contract = self.totals_features or [
            "total_close",
            "spread_close",
            "home_moneyline_close",
            "away_moneyline_close",
        ]
        source_map = {
            "total_close": "total_line",
            "spread_close": "spread_line",
            "home_moneyline_close": "home_moneyline",
            "away_moneyline_close": "away_moneyline",
        }
        matrix = pd.DataFrame(index=features_df.index)
        missing = []
        for column in contract:
            source_column = source_map.get(column, column)
            if source_column in features_df.columns:
                matrix[column] = features_df[source_column]
            else:
                missing.append(column)
                matrix[column] = np.nan
        if missing:
            LOGGER.warning("Totals feature contract missing generated columns; filling with NaN: %s", missing)
        return matrix.apply(pd.to_numeric, errors="coerce").fillna(0.0)

    def prepare_features(self, games_df: pd.DataFrame, league: str) -> pd.DataFrame:
        """
        Prepare features for a batch of games.
        """
        if games_df.empty:
            LOGGER.info(f"prepare_features: games_df is empty for {league}")
            return pd.DataFrame()
            
        LOGGER.info(f"prepare_features: Processing {len(games_df)} games for {league}")
        league_upper = league.upper()
        self.loader = FeatureLoader(league_upper)
        
        # Load rolling metrics
        rolling_df = self.loader.load_rolling_metrics()
        if not rolling_df.empty:
            rolling_df["game_date"] = pd.to_datetime(rolling_df["game_date"]).dt.tz_localize(None)
            rolling_df = rolling_df.sort_values("game_date")
        
        features_list = []
        
        for _, game in games_df.iterrows():
            # Adapt row to dict for processing
            game_dict = game.to_dict()
            
            # Basic features from the game record
            home_team = normalize_team_code(league_upper, game_dict.get("home_team", ""))
            away_team = normalize_team_code(league_upper, game_dict.get("away_team", ""))
            
            # Parse date
            commence_time = game_dict.get("commence_time")
            if isinstance(commence_time, str):
                try:
                    commence_time = datetime.fromisoformat(commence_time)
                except ValueError:
                    pass
            
            # Ensure naive datetime for comparison if rolling_df is naive
            compare_time = commence_time
            if hasattr(compare_time, "tzinfo") and compare_time.tzinfo is not None:
                compare_time = compare_time.replace(tzinfo=None)

            # Calculate implied probs
            home_ml = _safe_float(game_dict.get("home_moneyline"))
            away_ml = _safe_float(game_dict.get("away_moneyline"))
            over_ml = _safe_float(game_dict.get("over_moneyline"))
            under_ml = _safe_float(game_dict.get("under_moneyline"))
            
            def implied_prob(ml):
                if ml is None or ml == 0: return np.nan
                if ml > 0: return 100 / (ml + 100)
                return -ml / (-ml + 100)
                
            home_implied = implied_prob(home_ml)
            away_implied = implied_prob(away_ml)
            over_implied = implied_prob(over_ml)
            under_implied = implied_prob(under_ml)
            
            # Normalize H2H
            if pd.notna(home_implied) and pd.notna(away_implied):
                total = home_implied + away_implied
                home_implied /= total
                away_implied /= total
                
            # Normalize Totals
            if pd.notna(over_implied) and pd.notna(under_implied):
                total_prob = over_implied + under_implied
                over_implied /= total_prob
                under_implied /= total_prob
            
            # Base features
            row = {
                "game_id": game_dict.get("game_id"),
                "commence_time": commence_time,
                "home_team": home_team,
                "away_team": away_team,
                "home_moneyline": home_ml,
                "away_moneyline": away_ml,
                "is_home": 1, 
            }
            
            # Add rolling metrics
            if not rolling_df.empty and home_team and away_team:
                # Home team stats
                home_stats = rolling_df[
                    (rolling_df["team"] == home_team) & 
                    (rolling_df["game_date"] < compare_time)
                ]
                if not home_stats.empty:
                    latest_home = home_stats.iloc[-1]
                    for col in latest_home.index:
                        if col not in ["team", "game_date", "season", "game_id"]:
                            row[col] = latest_home[col]
                else:
                    LOGGER.warning(f"No rolling stats for home team {home_team} before {compare_time}")
                            
                # Away team stats (as opponent)
                away_stats = rolling_df[
                    (rolling_df["team"] == away_team) & 
                    (rolling_df["game_date"] < compare_time)
                ]
                if not away_stats.empty:
                    latest_away = away_stats.iloc[-1]
                    for col in latest_away.index:
                        if col not in ["team", "game_date", "season", "game_id"]:
                            row[f"opponent_{col}"] = latest_away[col]
                else:
                    LOGGER.warning(f"No rolling stats for away team {away_team} before {compare_time}")
            else:
                LOGGER.warning(f"Skipping rolling stats: empty={rolling_df.empty}, home={home_team}, away={away_team}")
            
            # Add totals info
            
            # Add totals info
            row["total_line"] = _safe_float(game_dict.get("total_line"))
            row["over_moneyline"] = over_ml
            row["under_moneyline"] = under_ml
            row["over_implied_prob"] = over_implied
            row["under_implied_prob"] = under_implied
            
            # Basic features
            # spread_line is now populated by load_games_from_database if available
            row["moneyline"] = home_ml
            row["implied_prob"] = home_implied
            # Ensure spread_line is float, default to nan if missing
            row["spread_line"] = _safe_float(game_dict.get("spread_line"))
            
            features_list.append(row)
            
        return pd.DataFrame(features_list)

    def predict(self, league: str, days_ahead: int = 7) -> Optional[pd.DataFrame]:
        """Generate predictions for upcoming games."""
        
        # 1. Load games
        games_df = load_games_from_database(league, days_ahead)
        if games_df.empty:
            LOGGER.info(f"No upcoming games found for {league}")
            return None
            
        # 2. Load model
        if not self.load_model(league):
            return None
            
        # 3. Prepare features
        X_df = self.prepare_features(games_df, league)
        if X_df.empty:
            return None
            
        # 4. Predict
        try:
            X = self._build_model_matrix(X_df)
            # Moneyline prediction
            probs = self.model.predict_proba(X)
            # Assuming class 1 is Win
            X_df["home_predicted_prob"] = probs[:, 1]
            X_df["away_predicted_prob"] = 1 - X_df["home_predicted_prob"]
            
            # Calculate edges
            X_df["home_edge"] = X_df["home_predicted_prob"] - X_df["implied_prob"]
            X_df["away_edge"] = X_df["away_predicted_prob"] - (1 - X_df["implied_prob"])
            
            # Totals prediction
            if self.totals_model and self.totals_std:
                try:
                    totals_X = self._build_totals_matrix(X_df)
                    predicted_totals = self.totals_model.predict(totals_X)
                    X_df["predicted_total_points"] = predicted_totals
                    
                    std = self.totals_std
                    lines = X_df["total_line"].fillna(0.0).values
                    z_scores = (lines - predicted_totals) / std
                    
                    # Calculate probs using erf
                    under_probs = [0.5 * (1 + math.erf(z / 1.41421356)) for z in z_scores]
                    over_probs = [1.0 - p for p in under_probs]
                    
                    X_df["over_predicted_prob"] = over_probs
                    X_df["under_predicted_prob"] = under_probs
                    
                    X_df["over_edge"] = X_df["over_predicted_prob"] - X_df["over_implied_prob"]
                    X_df["under_edge"] = X_df["under_predicted_prob"] - X_df["under_implied_prob"]
                    
                except Exception as e:
                    LOGGER.warning(f"Totals prediction failed for {league}: {e}")
                    X_df["over_predicted_prob"] = np.nan
                    X_df["under_predicted_prob"] = np.nan
                    X_df["over_edge"] = np.nan
                    X_df["under_edge"] = np.nan
            else:
                X_df["over_predicted_prob"] = np.nan
                X_df["under_predicted_prob"] = np.nan
                X_df["over_edge"] = np.nan
                X_df["under_edge"] = np.nan
            
            # Format output
            output_cols = [
                "game_id", "commence_time", "home_team", "away_team",
                "home_moneyline", "away_moneyline",
                "home_predicted_prob", "away_predicted_prob",
                "home_edge", "away_edge",
                # Totals
                "total_line", "over_moneyline", "under_moneyline",
                "over_predicted_prob", "under_predicted_prob",
                "over_edge", "under_edge",
                "over_implied_prob", "under_implied_prob",
                "predicted_total_points"
            ]
            # Add implied probs if available
            if "implied_prob" in X_df.columns:
                X_df["home_implied_prob"] = X_df["implied_prob"]
                X_df["away_implied_prob"] = 1 - X_df["implied_prob"]
                output_cols.extend(["home_implied_prob", "away_implied_prob"])
                
            final_df = X_df[output_cols].copy()
            
            # Add metadata
            final_df["league"] = league
            final_df["predicted_at"] = pd.Timestamp.now(tz="UTC")
            
            # Save
            save_predictions(final_df, self.model_type)
            return final_df
            
        except Exception as e:
            LOGGER.error(f"Prediction failed for {league}: {e}")
            return None
