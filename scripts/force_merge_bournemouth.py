import sqlite3
import pandas as pd
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path.cwd()))

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

DB_PATH = "data/betting.db"

def force_merge():
    """
    Manually merge the specific Chelsea vs Bournemouth duplicates found.
    Target IDs:
    1. EPL_740743 (Home: 329 AFC Bournemouth)
    2. EPL_7e99696bef694600ec9beff2dc1d553e (Home: 431 Bournemouth)
    Both on 2025-12-06.
    """
    LOGGER.info("Starting Force Merge for Bournemouth...")
    
    conn = sqlite3.connect(DB_PATH, timeout=10.0) # 10s timeout
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()
    
    # 1. Define Cluster
    ids_to_merge = ['EPL_740743', 'EPL_7e99696bef694600ec9beff2dc1d553e']
    
    # Verify they exist
    placeholders = ",".join(f"'{gid}'" for gid in ids_to_merge)
    existing = pd.read_sql_query(f"SELECT * FROM games WHERE game_id IN ({placeholders})", conn)
    
    if len(existing) < 2:
        LOGGER.warning(f"Found only {len(existing)} games. Cannot merge. IDs found: {existing['game_id'].tolist()}")
        # Check if we can find by fuzzy search?
        # Assuming the user report + my finding is accurate.
        return

    LOGGER.info(f"Merging: {ids_to_merge}")

    # 2. Generate Canonical ID
    # LEAGUE_YYYYMMDD_HOME_AWAY
    # EPL_20251206_AFC_BOURNEMOUTH_CHELSEA
    new_id = "EPL_20251206_AFC_BOURNEMOUTH_CHELSEA"
    
    # 3. Create Canonical Game (Copy from EPL_7e99... as it has odds)
    # Priority: EPL_7e99... (Hash) likely has better data? Or EPL_740743?
    # EPL_7e99... has odds. EPL_740743 has none.
    # So we use EPL_7e99... as source for timestamps/metadata.
    
    source_id = 'EPL_7e99696bef694600ec9beff2dc1d553e'
    
    cursor.execute(f"SELECT * FROM games WHERE game_id = '{source_id}'")
    source_row = cursor.fetchone() # Tuple
    
    if not source_row:
        LOGGER.error("Source game not found!")
        return

    # Check if new_id already exists
    cursor.execute("SELECT game_id FROM games WHERE game_id = ?", (new_id,))
    if not cursor.fetchone():
        # Insert
        # Columns in 'games': game_id, sport_id, season, game_type, week, start_time_utc, home_team_id, away_team_id, venue, status, odds_api_id, espn_id...
        # We can just update EPL_7e99... to new_id? 
        # But we must handle the OTHER duplicate.
        
        # INSERT OR IGNORE Canonical
        cols = ["game_id", "sport_id", "season", "game_type", "week", "start_time_utc", "home_team_id", "away_team_id", "venue", "status", "odds_api_id", "espn_id"]
        
        # Copy logic via SQL
        sql = f"""
        INSERT OR IGNORE INTO games ({', '.join(cols)})
        SELECT '{new_id}', sport_id, season, game_type, week, start_time_utc, home_team_id, away_team_id, venue, status, odds_api_id, espn_id
        FROM games WHERE game_id = '{source_id}'
        """
        cursor.execute(sql)
    
    # 4. Migrate Children (Odds, Predictions, etc) for ALL old IDs
    tables = ["odds", "predictions", "game_results", "team_features", "model_input", "model_predictions"]
    
    for table in tables:
        for old_id in ids_to_merge:
            try:
                # Update to New ID
                cursor.execute(f"UPDATE OR IGNORE {table} SET game_id = ? WHERE game_id = ?", (new_id, old_id))
                # Delete remnant (if IGNORE triggered)
                cursor.execute(f"DELETE FROM {table} WHERE game_id = ?", (old_id,))
            except Exception as e:
                LOGGER.warning(f"Error migrating {table}: {e}")

    # 5. Delete Old Games
    cursor.execute(f"DELETE FROM games WHERE game_id IN ({placeholders})")
    
    conn.commit()
    LOGGER.info(f"Successfully merged into {new_id}")
    conn.close()

if __name__ == "__main__":
    force_merge()
