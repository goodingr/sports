
import sqlite3
import pandas as pd
import numpy as np
import joblib
import math
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

DB_PATH = "data/betting.db"
MODELS_DIR = Path("models")

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

def load_totals_model(league: str, model_type: str = "gradient_boosting") -> Optional[dict]:
    # Map ensemble to gradient_boosting for totals if no ensemble totals model exists
    if model_type == "ensemble":
        model_type = "gradient_boosting"
        
    path = MODELS_DIR / f"{league.lower()}_totals_{model_type}.pkl"
    if not path.exists():
        # Fallback to gradient boosting if specific model doesn't exist
        fallback_path = MODELS_DIR / f"{league.lower()}_totals_gradient_boosting.pkl"
        if fallback_path.exists():
            return joblib.load(fallback_path)
        return None
    try:
        return joblib.load(path)
    except Exception as exc:
        LOGGER.warning("Failed to load totals model for %s (%s): %s", league, model_type, exc)
        return None

def regenerate_totals():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get all predictions that have a total_line but might need better probabilities
    # We'll fetch everything with a total_line to be safe and overwrite
    LOGGER.info("Fetching predictions...")
    cursor.execute("""
        SELECT p.prediction_id, p.game_id, p.total_line, p.over_moneyline, p.under_moneyline, 
               p.home_moneyline, p.away_moneyline, p.model_type,
               g.sport_id, s.league
        FROM predictions p
        JOIN games g ON p.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE p.total_line IS NOT NULL
    """)
    
    rows = cursor.fetchall()
    LOGGER.info(f"Found {len(rows)} predictions to process.")
    
    # Cache models to avoid reloading
    models_cache = {}
    
    updated_count = 0
    
    for row in rows:
        league = row['league']
        model_type = row['model_type']
        
        cache_key = (league, model_type)
        if cache_key not in models_cache:
            models_cache[cache_key] = load_totals_model(league, model_type)
            
        totals_model_bundle = models_cache[cache_key]
        
        if not totals_model_bundle:
            continue
            
        # Extract features
        total_line = row['total_line']
        over_price = row['over_moneyline']
        under_price = row['under_moneyline']
        
        # Skip if essential data is missing
        if total_line is None or over_price is None or under_price is None:
            continue
            
        # Prepare features for model
        # Note: The model expects specific feature names. 
        # Based on the user's code: total_close, spread_close, home_moneyline_close, away_moneyline_close
        
        feature_values = {
            "total_close": float(total_line),
            "spread_close": 0.0,
            "home_moneyline_close": float(row['home_moneyline']) if row['home_moneyline'] is not None else 0.0,
            "away_moneyline_close": float(row['away_moneyline']) if row['away_moneyline'] is not None else 0.0,
        }
        
        totals_features = pd.DataFrame([feature_values], columns=totals_model_bundle["feature_names"])
        
        try:
            predicted_total = float(totals_model_bundle["regressor"].predict(totals_features)[0])
            residual_std = float(totals_model_bundle.get("residual_std") or 12.0)
            
            diff = predicted_total - float(total_line)
            over_prob_pred = 0.5 * (1.0 + math.erf(diff / (residual_std * math.sqrt(2.0))))
            over_prob_pred = min(max(over_prob_pred, 0.0), 1.0)
            under_prob_pred = 1.0 - over_prob_pred
            
            over_edge_pred = over_prob_pred - _moneyline_to_prob(over_price)
            under_edge_pred = under_prob_pred - _moneyline_to_prob(under_price)
            
            # Update DB
            cursor.execute("""
                UPDATE predictions 
                SET over_prob = ?, 
                    under_prob = ?, 
                    over_edge = ?, 
                    under_edge = ?,
                    predicted_total_points = ?
                WHERE prediction_id = ?
            """, (over_prob_pred, under_prob_pred, over_edge_pred, under_edge_pred, predicted_total, row['prediction_id']))
            
            updated_count += 1
            
        except Exception as exc:
            LOGGER.debug(f"Prediction failed for {row['game_id']}: {exc}")
            continue
            
    conn.commit()
    conn.close()
    LOGGER.info(f"Successfully regenerated totals for {updated_count} predictions.")

if __name__ == "__main__":
    regenerate_totals()
