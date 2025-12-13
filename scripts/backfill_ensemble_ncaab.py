"""
Backfill NCAAB predictions for ensemble model only.
"""
import sys
import logging
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from src.predict.engine import PredictionEngine
from src.predict.config import SUPPORTED_LEAGUES
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator
import src.models.train
from datetime import datetime, timedelta
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOGGER = logging.getLogger(__name__)

TARGET_DATES = ["2025-12-05", "2025-12-06", "2025-12-07"]

def backfill_ensemble_ncaab():
    engine = PredictionEngine(model_type="ensemble")
    league = "NCAAB"
    
    from src.db.core import connect
    
    LOGGER.info(f"Backfilling ensemble for {league}...")
    
    with connect() as conn:
        sport_row = conn.execute("SELECT sport_id FROM sports WHERE league = ?", (league,)).fetchone()
        if not sport_row:
            LOGGER.error(f"Sport not found: {league}")
            return
        sport_id = sport_row[0]
        
        for target_date in TARGET_DATES:
            LOGGER.info(f"Processing {target_date}...")
            
            # Get snapshots
            target_dt = datetime.strptime(target_date, "%Y-%m-%d")
            start_dt = target_dt - timedelta(days=2)
            start_date_str = start_dt.strftime("%Y-%m-%d")
            
            snapshot_rows = conn.execute("""
                SELECT snapshot_id
                FROM odds_snapshots 
                WHERE sport_id = ? 
                  AND date(fetched_at_utc) BETWEEN ? AND ?
                ORDER BY fetched_at_utc ASC
            """, (sport_id, start_date_str, target_date)).fetchall()
            
            if not snapshot_rows:
                LOGGER.warning(f"No snapshots found for {target_date}")
                continue
                
            snapshot_ids = [row[0] for row in snapshot_rows]
            
            # Get games
            query = """
                SELECT g.game_id, g.start_time_utc as commence_time,
                       ht.name as home_team, at.name as away_team, g.status
                FROM games g
                JOIN teams ht ON g.home_team_id = ht.team_id
                JOIN teams at ON g.away_team_id = at.team_id
                WHERE g.sport_id = ? AND date(g.start_time_utc) = ?
            """
            games_df = pd.read_sql_query(query, conn, params=(sport_id, target_date))
            
            if games_df.empty:
                LOGGER.info(f"No games found for {target_date}")
                continue
                
            LOGGER.info(f"Found {len(games_df)} games")
            
            # Get odds
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
            odds_df = pd.read_sql_query(odds_query, conn, params=snapshot_ids + game_ids)
            
            # Pivot odds
            games_df["home_moneyline"] = None
            games_df["away_moneyline"] = None
            games_df["total_line"] = None
            games_df["over_moneyline"] = None
            games_df["under_moneyline"] = None
            
            if not odds_df.empty:
                snapshot_rank = {sid: i for i, sid in enumerate(snapshot_ids)}
                odds_df["rank"] = odds_df["snapshot_id"].map(snapshot_rank)
                
                for idx, row in games_df.iterrows():
                    gid = row["game_id"]
                    g_odds = odds_df[odds_df["game_id"] == gid]
                    
                    if g_odds.empty:
                        continue
                    
                    # H2H
                    h2h = g_odds[g_odds["market"] == "h2h"]
                    if not h2h.empty:
                        last_h2h = h2h[h2h["rank"] == h2h["rank"].max()]
                        for _, o in last_h2h.iterrows():
                            if o["outcome"] == row["home_team"]:
                                games_df.at[idx, "home_moneyline"] = o["price_american"]
                            elif o["outcome"] == row["away_team"]:
                                games_df.at[idx, "away_moneyline"] = o["price_american"]
                    
                    # Totals
                    totals = g_odds[g_odds["market"] == "totals"]
                    if not totals.empty:
                        last_totals = totals[totals["rank"] == totals["rank"].max()]
                        if not last_totals.empty:
                            games_df.at[idx, "total_line"] = last_totals.iloc[0]["line"]
                            for _, o in last_totals.iterrows():
                                if o["outcome"] == "Over":
                                    games_df.at[idx, "over_moneyline"] = o["price_american"]
                                elif o["outcome"] == "Under":
                                    games_df.at[idx, "under_moneyline"] = o["price_american"]
            
            # Predict
            try:
                if engine.load_model(league):
                    X_df = engine.prepare_features(games_df, league)
                    
                    if not X_df.empty:
                        # Get feature names from model
                        model = engine.model
                        feature_names = None
                        
                        # Try different model structures
                        if hasattr(model, "feature_names_in_"):
                            feature_names = model.feature_names_in_
                        elif hasattr(model, "estimator") and hasattr(model.estimator, "feature_names_in_"):
                            feature_names = model.estimator.feature_names_in_
                        elif hasattr(model, "members") and len(model.members) > 0:
                            # EnsembleModel - get from first member
                            first_member = model.members[0]
                            if hasattr(first_member, "estimator") and hasattr(first_member.estimator, "feature_names_in_"):
                                feature_names = first_member.estimator.feature_names_in_
                        
                        if feature_names is not None:
                            # Ensure all required features exist
                            for col in feature_names:
                                if col not in X_df.columns:
                                    X_df[col] = 0.0
                            # Select features in correct order
                            X = X_df[feature_names]
                        else:
                            X = X_df[engine.feature_columns] if engine.feature_columns else X_df
                        
                        # Moneyline
                        try:
                            probs = engine.model.predict_proba(X)
                            X_df["home_predicted_prob"] = probs[:, 1]
                            X_df["away_predicted_prob"] = 1 - X_df["home_predicted_prob"]
                            X_df["home_edge"] = X_df["home_predicted_prob"] - X_df["implied_prob"]
                            X_df["away_edge"] = X_df["away_predicted_prob"] - (1 - X_df["implied_prob"])
                        except Exception as e:
                            LOGGER.error(f"Moneyline prediction failed: {e}")
                        
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
                        
                        # Save
                        from src.predict.storage import save_predictions
                        save_predictions(X_df, "ensemble")
                        LOGGER.info(f"Saved {len(X_df)} predictions for {target_date}")
            except Exception as e:
                LOGGER.error(f"Failed processing {target_date}: {e}")
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    backfill_ensemble_ncaab()
    print("\nDone! Check results:")
    print("  sqlite3 data/betting.db \"SELECT date(g.start_time_utc), COUNT(*) FROM predictions p JOIN games g ON p.game_id = g.game_id JOIN sports s ON g.sport_id = s.sport_id WHERE s.league = 'NCAAB' AND p.model_type = 'ensemble' GROUP BY date(g.start_time_utc) ORDER BY date(g.start_time_utc);\"")
