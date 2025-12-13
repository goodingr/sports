import sqlite3
import pandas as pd
import logging
import sys
from pathlib import Path
from datetime import timedelta

# Add project root to path
sys.path.append(str(Path.cwd()))

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

DB_PATH = "data/betting.db"

def force_merge_all_duplicates():
    """
    Scans for duplicate games (Same Home/Away + Same Date) and merges them.
    Handles 'Ghost' games (no odds) vs 'Real' games (has odds).
    """
    LOGGER.info("Scanning for ALL duplicate games...")
    
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()
    
    # Fetch all games
    query = """
        SELECT 
            g.game_id, g.start_time_utc, g.home_team_id, g.away_team_id, 
            g.sport_id, g.odds_api_id, g.espn_id,
            ht.name as home_team, at.name as away_team
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        WHERE g.start_time_utc > '2024-10-01'
        ORDER BY g.start_time_utc DESC
    """
    df = pd.read_sql_query(query, conn)
    
    # Normalize date to YYYY-MM-DD for grouping
    df['date_str'] = df['start_time_utc'].apply(lambda x: str(x)[:10])
    
    # Identify Duplicates
    # Group by [Date, Set(Teams)]?
    # Simpler: Iterate and fuzzy match? Or just Strict Match on sorted Team IDs?
    
    # 1. Duplicates by Strict Team ID Match + same date
    # Valid duplicates: (Date, HomeID, AwayID)
    # Also swapped: (Date, AwayID, HomeID)
    
    df['team_set'] = df.apply(lambda x: tuple(sorted([x['home_team_id'], x['away_team_id']])), axis=1)
    
    # Group by Team Set ONLY, then find time clusters
    groups = df.groupby('team_set')
    
    total_merged = 0
    
    for team_set, group in groups:
        if len(group) < 2: continue
        
        # Sort by time
        group = group.sort_values('start_time_utc')
        games = group.to_dict('records')
        
        visited = set()
        for i in range(len(games)):
            if i in visited: continue
            
            cluster = [games[i]]
            visited.add(i)
            
            t1 = pd.to_datetime(games[i]['start_time_utc'])
            
            for j in range(i+1, len(games)):
                if j in visited: continue
                t2 = pd.to_datetime(games[j]['start_time_utc'])
                
                # If within 24 hours, treat as duplicate
                # (Allows for timezone shifts or slight schedule updates)
                if abs((t2 - t1).total_seconds()) < 24 * 3600:
                    cluster.append(games[j])
                    visited.add(j)
            
            if len(cluster) > 1:
                date_str = str(t1)[:10]
                LOGGER.info(f"Found duplicate cluster: {date_str} Teams: {team_set} Count: {len(cluster)}")
                process_cluster(cursor, pd.DataFrame(cluster), date_str)
                total_merged += (len(cluster) - 1)
            
    conn.commit()
    LOGGER.info(f"Total merged clusters: {total_merged}")
    conn.close()

def process_cluster(cursor, group, date):
    # Determine Canonical ID
    ids = group['game_id'].tolist()
    LOGGER.info(f"Merging IDs: {ids}")
    
    # 1. Pick Source (Best Metadata)
    # Prefer game with odds_api_id
    source = group.sort_values(by='odds_api_id', na_position='last').iloc[0]
    
    # 2. Generate New ID
    # LEAGUE_YYYYMMDD_HOME_AWAY
    # Need league slug. Sport ID -> League?
    # Quick hack: Use source's existing ID prefix if it looks standard, or just 'GAME'
    prefix = "GAME"
    if "_" in source['game_id']:
        prefix = source['game_id'].split("_")[0]
        
    home_name = source['home_team'].replace(" ", "_").upper()
    away_name = source['away_team'].replace(" ", "_").upper()
    clean_date = date.replace("-", "")
    new_id = f"{prefix}_{clean_date}_{home_name}_{away_name}"
    
    # 3. Create Canonical (if not exists)
    # Copy source row
    cols = ["game_id", "sport_id", "season", "game_type", "week", "start_time_utc", "home_team_id", "away_team_id", "venue", "status", "odds_api_id", "espn_id"]
    
    # Check if target exists
    cursor.execute("SELECT game_id FROM games WHERE game_id = ?", (new_id,))
    if not cursor.fetchone():
        # Insert
        try:
            # We construct a dynamic SELECT
            source_id = source['game_id']
            sql = f"""
            INSERT OR IGNORE INTO games ({', '.join(cols)})
            SELECT '{new_id}', sport_id, season, game_type, week, start_time_utc, home_team_id, away_team_id, venue, status, odds_api_id, espn_id
            FROM games WHERE game_id = '{source_id}'
            """
            cursor.execute(sql)
        except Exception as e:
            LOGGER.error(f"Failed to create canonical {new_id}: {e}")
            return

    # 4. Migrate Children
    tables = ["odds", "predictions", "game_results", "team_features", "model_input", "model_predictions"]
    
    # IDs to migrate = all in group (including source, we move everything to new_id)
    # But excludes new_id itself if it was already in the group (unlikely)
    ids_to_migrate = [gid for gid in ids if gid != new_id]
    
    for table in tables:
        for old_id in ids_to_migrate:
            try:
                cursor.execute(f"UPDATE OR IGNORE {table} SET game_id = ? WHERE game_id = ?", (new_id, old_id))
                cursor.execute(f"DELETE FROM {table} WHERE game_id = ?", (old_id,))
            except Exception:
                pass

    # 5. Delete Old Games
    ids_sql = ",".join(f"'{gid}'" for gid in ids_to_migrate)
    cursor.execute(f"DELETE FROM games WHERE game_id IN ({ids_sql})")

if __name__ == "__main__":
    force_merge_all_duplicates()
