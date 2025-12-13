
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add project root to path
sys.path.append(str(Path.cwd()))

from src.db.core import connect

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def cleanup_duplicates():
    """
    Find and remove duplicate games, prioritizing the one with odds or Odds API ID.
    """
    LOGGER.info("Starting duplicate cleanup...")
    
    with connect() as conn:
        # 1. Get recent and future games
        # We join with teams to facilitate grouping by names/IDs
        query = """
            SELECT 
                g.game_id, 
                g.start_time_utc, 
                g.home_team_id, 
                g.away_team_id,
                g.sport_id,
                (SELECT count(*) FROM odds o WHERE o.game_id = g.game_id) as odds_count
            FROM games g
            WHERE g.start_time_utc > datetime('now', '-7 days')
        """
        df = pd.read_sql_query(query, conn)
        
    if df.empty:
        LOGGER.info("No recent games found.")
        return

    LOGGER.info(f"Fetched {len(df)} games from DB.")
    
    # Debug: Check for specific known duplicates
    debug_ids = ['EPL_740743', 'EPL_7e99696bef694600ec9beff2dc1d553e', 'NHL_f57340991e86dc61f37839555eeeb8e2', 'f57340991e86dc61f37839555eeeb8e2']
    debug_rows = df[df['game_id'].isin(debug_ids)]
    if not debug_rows.empty:
        LOGGER.info("Debug rows found:")
        for _, row in debug_rows.iterrows():
            teams = sorted([row['home_team_id'], row['away_team_id']])
            key = (row['start_time_utc'], teams[0], teams[1])
            LOGGER.info(f"ID: {row['game_id']}, Time: {row['start_time_utc']}, Teams: {teams}, Key: {key}")

    # Create a normalized 'matchup_key' (sorted team IDs) to catch A vs B and B vs A
    # Use dictionary for robust grouping
    groups = {}
    
    for _, row in df.iterrows():
        # Key: (Time string prefix to minute, ID1, ID2) to be safe against second diffs
        # Only take YYYY-MM-DDTHH:MM of the timestamp
        time_key = str(row['start_time_utc'])[:16] 
        teams = sorted([row['home_team_id'], row['away_team_id']])
        key = (time_key, teams[0], teams[1])
        
        if key not in groups:
            groups[key] = []
        groups[key].append(row)
        
    duplicates_found = 0
    games_to_delete = []
    
    for key, rows in groups.items():
        if len(rows) > 1:
            duplicates_found += 1
            # Sort: Odds count DESC, then ID len DESC
            # We want to KEEP the one with odds, or the one with the long ID (Odds API)
            rows.sort(key=lambda x: (x['odds_count'], len(x['game_id'])), reverse=True)
            
            best_game = rows[0]
            for bad_game in rows[1:]:
                games_to_delete.append(bad_game['game_id'])
                LOGGER.info(f"Duplicate Matchup {key}: Keeping {best_game['game_id']} (Odds: {best_game['odds_count']}). Deleting {bad_game['game_id']} (Odds: {bad_game['odds_count']})")
                
    if not games_to_delete:
        LOGGER.info("No games to delete.")
        return

    LOGGER.info(f"Deleting {len(games_to_delete)} duplicate games...")
    
    with connect() as conn:
        cursor = conn.cursor()
        
        # Delete from predictions first (foreign key)
        placeholders = ",".join("?" * len(games_to_delete))
        cursor.execute(f"DELETE FROM predictions WHERE game_id IN ({placeholders})", games_to_delete)
        
        # Delete from odds (just in case bad ones had some odds?)
        cursor.execute(f"DELETE FROM odds WHERE game_id IN ({placeholders})", games_to_delete)
        
        # Delete from games
        cursor.execute(f"DELETE FROM games WHERE game_id IN ({placeholders})", games_to_delete)
        
        conn.commit()
        LOGGER.info("Deletion complete.")

if __name__ == "__main__":
    cleanup_duplicates()
