
import logging
import pandas as pd
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path.cwd()))

from src.db.core import connect
from src.predict.storage import save_predictions

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def backfill_missing_odds():
    """
    Backfill missing sportsbook odds for existing predictions.
    Uses the improved fetching logic (multi-snapshot + fuzzy match) 
    that was recently added to storage.py.
    """
    with connect() as conn:
        # 1. Identify predictions with missing odds
        # We look for NULL home_moneyline or home_implied_prob
        query_missing = """
            SELECT DISTINCT g.game_id, g.home_team_id, ht.name as home_team, at.name as away_team
            FROM games g
            JOIN predictions p ON g.game_id = p.game_id
            JOIN teams ht ON g.home_team_id = ht.team_id
            JOIN teams at ON g.away_team_id = at.team_id
            WHERE p.home_moneyline IS NULL 
               OR p.home_implied_prob IS NULL
        """
        missing_df = pd.read_sql_query(query_missing, conn)
        
    if missing_df.empty:
        LOGGER.info("No predictions found with missing odds.")
        return

    LOGGER.info(f"Found {len(missing_df)} games with missing odds in predictions table.")
    
    # 2. Fetch valid odds for these games using the robust logic
    # Reuse logical snippets from storage.py, adapted for batch update
    
    game_ids = missing_df["game_id"].tolist()
    
    # Chunking to avoid SQL limits if many games
    chunk_size = 50
    game_id_chunks = [game_ids[i:i + chunk_size] for i in range(0, len(game_ids), chunk_size)]
    
    updated_count = 0
    
    with connect() as conn:
        for chunk in game_id_chunks:
            placeholders = ",".join("?" * len(chunk))
            
            # Fetch from ALL snapshots, join with timestamp
            odds_query = f"""
                SELECT o.game_id, o.market, o.outcome, o.price_american, o.line, s.fetched_at_utc
                FROM odds o
                JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
                WHERE o.game_id IN ({placeholders})
                  AND o.market IN ('h2h', 'totals', 'spreads')
            """
            
            odds_df = pd.read_sql_query(odds_query, conn, params=chunk)
            
            if odds_df.empty:
                continue
            
            try:
                # Dedup: keep latest
                # Ensure string first
                odds_df["fetched_at_utc"] = pd.to_datetime(odds_df["fetched_at_utc"].astype(str), format="mixed", errors='coerce')
                odds_df = odds_df.dropna(subset=["fetched_at_utc"])
                odds_df = odds_df.sort_values("fetched_at_utc", ascending=False)
                odds_df = odds_df.drop_duplicates(subset=["game_id", "market", "outcome"], keep="first")
            except Exception as e:
                LOGGER.error(f"Error sorting/deduplicating odds: {e}")
                continue
            
            # Prepare updates
            updates = []
            
            for game_id in chunk:
                # Find team names for this game to help matching
                game_info = missing_df[missing_df["game_id"] == game_id].iloc[0]
                home_team = game_info["home_team"]
                away_team = game_info["away_team"]
                
                home_norm = home_team.lower().strip()
                away_norm = away_team.lower().strip()
                
                game_odds = odds_df[odds_df["game_id"] == game_id]
                if game_odds.empty:
                    continue
                    
                update_vals = {
                    "game_id": game_id,
                    "home_moneyline": None,
                    "away_moneyline": None,
                    "home_implied_prob": None,
                    "away_implied_prob": None
                }
                
                if home_norm == "miss valley st delta devils": # Debug specific case
                    LOGGER.info(f"Checking odds for {home_norm}...")
                    
                # Moneyline
                h2h_odds = game_odds[game_odds["market"] == "h2h"]
                for _, odd in h2h_odds.iterrows():
                    outcome = str(odd["outcome"]).lower().strip()
                    price = odd["price_american"]
                    
                    if home_norm == "miss valley st delta devils":
                        LOGGER.info(f"  Comparing outcome '{outcome}' vs '{home_norm}' / '{away_norm}'")

                    if outcome == "home" or outcome == home_norm or outcome in home_norm or home_norm in outcome:
                        update_vals["home_moneyline"] = price
                    elif outcome == "away" or outcome == away_norm or outcome in away_norm or away_norm in outcome:
                        update_vals["away_moneyline"] = price
                        
                # Calculate implied probs if we found moneylines
                def calc_implied(ml):
                    if ml is None or pd.isna(ml) or ml == 0: return None
                    if ml > 0: return 100 / (ml + 100)
                    return abs(ml) / (abs(ml) + 100)
                    
                if update_vals["home_moneyline"] is not None:
                    update_vals["home_implied_prob"] = calc_implied(update_vals["home_moneyline"])
                if update_vals["away_moneyline"] is not None:
                    update_vals["away_implied_prob"] = calc_implied(update_vals["away_moneyline"])
                    
                # Normalize if both present
                if update_vals["home_implied_prob"] and update_vals["away_implied_prob"]:
                    total = update_vals["home_implied_prob"] + update_vals["away_implied_prob"]
                    update_vals["home_implied_prob"] /= total
                    update_vals["away_implied_prob"] /= total
                
                if update_vals["home_moneyline"] is not None:
                    updates.append(update_vals)
            
            # Execute batch update
            if updates:
                cursor = conn.cursor()
                cursor.executemany("""
                    UPDATE predictions
                    SET home_moneyline = :home_moneyline,
                        away_moneyline = :away_moneyline,
                        home_implied_prob = :home_implied_prob,
                        away_implied_prob = :away_implied_prob
                    WHERE game_id = :game_id AND home_moneyline IS NULL
                """, updates)
                updated_count += cursor.rowcount
                conn.commit()
                
    LOGGER.info(f"Backfill complete. Updated {updated_count} prediction records.")

if __name__ == "__main__":
    backfill_missing_odds()
