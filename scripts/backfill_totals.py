
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.predict.engine import PredictionEngine
from src.predict.config import SUPPORTED_LEAGUES
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator
import src.models.train

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)

TARGET_DATES = [
    "2025-12-04",
    "2025-12-05",
    "2025-12-06",
    "2025-12-07"
]

MODELS = ["ensemble", "random_forest", "gradient_boosting"]

def backfill():
    for model_type in MODELS:
        LOGGER.info(f"Backfilling for model: {model_type}")
        engine = PredictionEngine(model_type=model_type)
        
        for league in SUPPORTED_LEAGUES:
            LOGGER.info(f"Processing {league}...")
            
            from src.db.core import connect
            import pandas as pd
            
            with connect() as conn:
                # Get sport_id
                sport_row = conn.execute("SELECT sport_id FROM sports WHERE league = ?", (league,)).fetchone()
                if not sport_row:
                    continue
                sport_id = sport_row[0]
                
                for target_date in TARGET_DATES:
                    # Get snapshots from target_date and 2 days prior
                    # We want to maximize odds coverage.
                    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
                    start_dt = target_dt - timedelta(days=2)
                    start_date_str = start_dt.strftime("%Y-%m-%d")
                    
                    snapshot_rows = conn.execute(
                        """
                        SELECT snapshot_id, fetched_at_utc
                        FROM odds_snapshots 
                        WHERE sport_id = ? 
                          AND date(fetched_at_utc) BETWEEN ? AND ?
                        ORDER BY fetched_at_utc ASC
                        """, 
                        (sport_id, start_date_str, target_date)
                    ).fetchall()
                    
                    if not snapshot_rows:
                        continue
                        
                    snapshot_ids = [row[0] for row in snapshot_rows]
                    
                    # Get games for this date
                    query = f"""
                        SELECT 
                            g.game_id,
                            g.start_time_utc as commence_time,
                            ht.name as home_team,
                            at.name as away_team,
                            g.status
                        FROM games g
                        JOIN teams ht ON g.home_team_id = ht.team_id
                        JOIN teams at ON g.away_team_id = at.team_id
                        WHERE g.sport_id = ?
                          AND date(g.start_time_utc) = ?
                    """
                    games_df = pd.read_sql_query(query, conn, params=(sport_id, target_date))
                    
                    if games_df.empty:
                        continue
                        
                    # Get odds from ALL snapshots
                    game_ids = games_df["game_id"].tolist()
                    placeholders_g = ",".join("?" * len(game_ids))
                    placeholders_s = ",".join("?" * len(snapshot_ids))
                    
                    odds_query = f"""
                        SELECT game_id, market, outcome, price_american, line, snapshot_id
                        FROM odds
                        WHERE snapshot_id IN ({placeholders_s})
                          AND market IN ('h2h', 'totals')
                          AND game_id IN ({placeholders_g})
                    """
                    all_odds_df = pd.read_sql_query(odds_query, conn, params=snapshot_ids + game_ids)
                    
                    # Pivot odds
                    games_df["home_moneyline"] = None
                    games_df["away_moneyline"] = None
                    games_df["total_line"] = None
                    games_df["over_moneyline"] = None
                    games_df["under_moneyline"] = None
                    
                    if not all_odds_df.empty:
                        # We need to prioritize LATEST odds.
                        # But we don't have fetched_at in odds table, only snapshot_id.
                        # We have snapshot_ids ordered by fetched_at ASC in our list.
                        # So we can map snapshot_id to index/rank.
                        snapshot_rank = {sid: i for i, sid in enumerate(snapshot_ids)}
                        all_odds_df["rank"] = all_odds_df["snapshot_id"].map(snapshot_rank)
                        all_odds_df = all_odds_df.sort_values("rank")
                        
                        # Now iterate games and fill
                        for idx, row in games_df.iterrows():
                            gid = row["game_id"]
                            g_odds = all_odds_df[all_odds_df["game_id"] == gid]
                            
                            if g_odds.empty:
                                continue
                                
                            # H2H - take last valid
                            h2h = g_odds[g_odds["market"] == "h2h"]
                            if not h2h.empty:
                                # Get latest snapshot for h2h
                                last_rank = h2h["rank"].max()
                                latest_h2h = h2h[h2h["rank"] == last_rank]
                                for _, o in latest_h2h.iterrows():
                                    # Normalize strings
                                    outcome_norm = o["outcome"].lower().strip() if isinstance(o["outcome"], str) else ""
                                    home_norm = row["home_team"].lower().strip() if isinstance(row["home_team"], str) else ""
                                    away_norm = row["away_team"].lower().strip() if isinstance(row["away_team"], str) else ""
                                    
                                    # Match logic
                                    if outcome_norm == "home" or outcome_norm == home_norm or home_norm in outcome_norm or outcome_norm in home_norm:
                                        games_df.at[idx, "home_moneyline"] = o["price_american"]
                                    elif outcome_norm == "away" or outcome_norm == away_norm or away_norm in outcome_norm or outcome_norm in away_norm:
                                        games_df.at[idx, "away_moneyline"] = o["price_american"]
                                        
                            # Totals - take last valid
                            totals = g_odds[g_odds["market"] == "totals"]
                            if not totals.empty:
                                last_rank = totals["rank"].max()
                                latest_totals = totals[totals["rank"] == last_rank]
                                if not latest_totals.empty:
                                    games_df.at[idx, "total_line"] = latest_totals.iloc[0]["line"]
                                    for _, o in latest_totals.iterrows():
                                        if o["outcome"] == "Over":
                                            games_df.at[idx, "over_moneyline"] = o["price_american"]
                                        elif o["outcome"] == "Under":
                                            games_df.at[idx, "under_moneyline"] = o["price_american"]
                    
                    # Predict logic
                    try:
                        if engine.load_model(league):
                            # Prepare features
                            X_df = engine.prepare_features(games_df, league)
                            if not X_df.empty:
                                # Filter features based on model requirements
                                model = engine.model
                                feature_names = None
                                if hasattr(model, "feature_names_in_"):
                                    feature_names = model.feature_names_in_
                                elif hasattr(model, "estimator"):
                                    if hasattr(model.estimator, "feature_names_in_"):
                                        feature_names = model.estimator.feature_names_in_
                                    elif hasattr(model.estimator, "steps"):
                                        feature_names = model.estimator.steps[-1][1].feature_names_in_
                                
                                if feature_names is not None:
                                    # Ensure we have all features (fill 0) and only those features
                                    for col in feature_names:
                                        if col not in X_df.columns:
                                            X_df[col] = 0.0
                                    X = X_df[feature_names]
                                else:
                                    # Fallback
                                    if engine.feature_columns:
                                        for col in engine.feature_columns:
                                            if col not in X_df.columns:
                                                X_df[col] = 0.0
                                        X = X_df[engine.feature_columns]
                                    else:
                                        X = X_df.select_dtypes(include=[float, int])
                                
                                # Moneyline
                                try:
                                    probs = engine.model.predict_proba(X)
                                    X_df["home_predicted_prob"] = probs[:, 1]
                                    X_df["away_predicted_prob"] = 1 - X_df["home_predicted_prob"]
                                    X_df["home_edge"] = X_df["home_predicted_prob"] - X_df["implied_prob"]
                                    X_df["away_edge"] = X_df["away_predicted_prob"] - (1 - X_df["implied_prob"])
                                except Exception as e:
                                    LOGGER.error(f"Moneyline prediction failed for {league}: {e}")
                                    pass

                                # Totals
                                if engine.totals_model and engine.totals_std:
                                    try:
                                        totals_X = pd.DataFrame()
                                        totals_X["total_close"] = X_df["total_line"]
                                        totals_X["spread_close"] = X_df.get("spread_line", 0.0)
                                        totals_X["home_moneyline_close"] = X_df["home_moneyline"]
                                        totals_X["away_moneyline_close"] = X_df["away_moneyline"]
                                        totals_X = totals_X.fillna(0.0)
                                        
                                        predicted_totals = engine.totals_model.predict(totals_X)
                                        X_df["predicted_total_points"] = predicted_totals
                                        
                                        import math
                                        std = engine.totals_std
                                        lines = X_df["total_line"].fillna(0.0).values
                                        z_scores = (lines - predicted_totals) / std
                                        under_probs = [0.5 * (1 + math.erf(z / 1.41421356)) for z in z_scores]
                                        over_probs = [1.0 - p for p in under_probs]
                                        
                                        X_df["over_predicted_prob"] = over_probs
                                        X_df["under_predicted_prob"] = under_probs
                                        X_df["over_edge"] = X_df["over_predicted_prob"] - X_df["over_implied_prob"]
                                        X_df["under_edge"] = X_df["under_predicted_prob"] - X_df["under_implied_prob"]
                                    except Exception as e:
                                        LOGGER.warning(f"Totals failed: {e}")
                                        X_df["over_predicted_prob"] = None
                                else:
                                    X_df["over_predicted_prob"] = None

                                # Save
                                from src.predict.storage import save_predictions
                                save_predictions(X_df, model_type)
                                LOGGER.info(f"Saved {len(X_df)} predictions for {league} on {target_date}")
                    except Exception as e:
                        LOGGER.error(f"Failed processing {league} on {target_date}: {e}")
                        continue

if __name__ == "__main__":
    backfill()
