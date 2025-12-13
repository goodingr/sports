import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import difflib
import re

# Add project root to path
sys.path.append(str(Path.cwd()))

from src.db.core import connect

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def normalize_name(name):
    """Normalize team name for comparison and ID generation."""
    if not name: return ""
    n = name.lower().strip()
    return n

def get_comparison_name(name):
    """Aggressive normalization for similarity check."""
    if not name: return ""
    n = name.lower()
    remove_words = ["fc", "cf", "sc", "united", "city", "state", "university", "st", "univ", "college", "afc"]
    tokens = n.split()
    tokens = [t for t in tokens if t not in remove_words]
    return " ".join(tokens)

def are_teams_similar(name1, name2):
    """Check if two team names are similar enough."""
    n1 = get_comparison_name(name1)
    n2 = get_comparison_name(name2)
    
    if "bournemouth" in n1 or "bournemouth" in n2:
        LOGGER.info(f"Comparing: '{n1}' vs '{n2}'")

    if not n1 or not n2: return False
    if n1 == n2: return True
    
    matcher = difflib.SequenceMatcher(None, n1, n2)
    if matcher.ratio() > 0.80: return True
    
    # Substring check for reliability (e.g. "Bournemouth" in "AFC Bournemouth")
    if n1 in n2 or n2 in n1:
        if len(n1) > 3 and len(n2) > 3: return True
    return False

def generate_internal_id(league, date_str, home_name, away_name):
    """
    Generate canonical ID: LEAGUE_YYYYMMDD_HOME_AWAY
    e.g. EPL_20251206_BOURNEMOUTH_CHELSEA
    """
    def clean_slug(s):
        s = re.sub(r'[^\w\s]', '', s)
        s = re.sub(r'\s+', '_', s.strip())
        return s.upper()

    h_slug = clean_slug(home_name)
    a_slug = clean_slug(away_name)
    d_slug = date_str.replace("-", "")
    
    return f"{league}_{d_slug}_{h_slug}_{a_slug}"

def refactor_ids():
    LOGGER.info("Starting Game ID Refactor...")
    
    with connect() as conn:
        # Fetch all games - using recent cutoff as before
        query = """
            SELECT 
                g.game_id, g.start_time_utc, g.home_team_id, g.away_team_id, 
                g.sport_id, g.odds_api_id, g.espn_id,
                ht.name as home_team, at.name as away_team,
                s.league
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.team_id
            JOIN teams at ON g.away_team_id = at.team_id
            JOIN sports s ON g.sport_id = s.sport_id
            WHERE g.start_time_utc IS NOT NULL AND g.start_time_utc > '1900-01-01'
            ORDER BY g.start_time_utc DESC
        """
        df = pd.read_sql_query(query, conn)
        
    if df.empty:
        LOGGER.info("No games found.")
        return

    df['date_str'] = df['start_time_utc'].apply(lambda x: str(x)[:10])
    
    # Groups to process
    groups = df.groupby(['league', 'date_str'])
    
    total_merged = 0
    total_processed = 0
    
    with connect() as conn:
        cursor = conn.cursor()
        
        for (league, date), group in groups:
            # Cluster games within this league/date bucket
            games = group.to_dict('records')
            visited = set()
            
            for i in range(len(games)):
                if i in visited: continue
                visited.add(i)
                
                # Start a cluster with game i
                cluster = [games[i]]
                
                # Find matching games in the rest of the list
                for j in range(i + 1, len(games)):
                    if j in visited: continue
                    
                    g1 = games[i]
                    g2 = games[j]
                    
                    # Fuzzy match check
                    match = False
                    
                    if are_teams_similar(g1['home_team'], g2['home_team']) and \
                       are_teams_similar(g1['away_team'], g2['away_team']):
                        match = True
                    elif are_teams_similar(g1['home_team'], g2['away_team']) and \
                         are_teams_similar(g1['away_team'], g2['home_team']):
                        match = True
                        
                    if match:
                        cluster.append(g2)
                        visited.add(j)
            
                # Process the cluster (Singletons AND Duplicates)
                if len(cluster) > 1:
                    ids = [g['game_id'] for g in cluster]
                    teams = [f"{g['home_team']} vs {g['away_team']}" for g in cluster]
                    LOGGER.info(f"Merging duplicates: {teams} (IDs: {ids})")

                primary_game = cluster[0]
                new_id = generate_internal_id(league, date, primary_game['home_team'], primary_game['away_team'])
                
                # Metadata consolidation
                merged_odds_api_id = None
                merged_espn_id = None
                
                existing_ids = [g['game_id'] for g in cluster]
                
                for g in cluster:
                    if g['odds_api_id'] and g['odds_api_id'].strip(): merged_odds_api_id = g['odds_api_id']
                    if g['espn_id'] and g['espn_id'].strip(): merged_espn_id = g['espn_id']
                    
                    gid = g['game_id']
                    # Heuristics for extracting legacy IDs from the game_id column itself if needed
                    # (Simplified from previous version)
                    if len(gid) > 30 and "_" not in gid: 
                        if not merged_odds_api_id: merged_odds_api_id = gid
                
                # Update DB
                target_exists = new_id in existing_ids
                
                if not target_exists:
                    # Insert new canonical record
                    cols = ["game_id", "sport_id", "season", "game_type", "week", "start_time_utc", "home_team_id", "away_team_id", "venue", "status", "odds_api_id", "espn_id"]
                    source_id = primary_game['game_id']
                    
                    copy_sql = f"""
                        INSERT OR IGNORE INTO games ({', '.join(cols)})
                        SELECT '{new_id}', sport_id, season, game_type, week, start_time_utc, home_team_id, away_team_id, venue, status, :o_id, :e_id
                        FROM games WHERE game_id = :source_id
                    """
                    cursor.execute(copy_sql, {"o_id": merged_odds_api_id, "e_id": merged_espn_id, "source_id": source_id})
                else:
                    # Update existing
                    cursor.execute("""
                        UPDATE games SET odds_api_id = ?, espn_id = ? WHERE game_id = ?
                    """, (merged_odds_api_id, merged_espn_id, new_id))
                    
                # Migrate children
                ids_to_migrate = [gid for gid in existing_ids if gid != new_id]
                
                if ids_to_migrate:
                    def migrate_table(table_name, id_col="game_id"):
                        for old_id in ids_to_migrate:
                            try:
                                cursor.execute(f"UPDATE OR IGNORE {table_name} SET {id_col} = ? WHERE {id_col} = ?", (new_id, old_id))
                                cursor.execute(f"DELETE FROM {table_name} WHERE {id_col} = ?", (old_id,))
                            except Exception: pass
                    
                    migrate_table("predictions")
                    migrate_table("odds")
                    migrate_table("game_results")
                    migrate_table("team_features")
                    migrate_table("model_input")
                    migrate_table("model_predictions")
                    
                    placeholders = ",".join(f"'{gid}'" for gid in ids_to_migrate)
                    cursor.execute(f"DELETE FROM games WHERE game_id IN ({placeholders})")
                    
                    total_processed += len(ids_to_migrate)
        
        conn.commit()
        LOGGER.info(f"Refactor complete. Updated/Merged {total_processed} records.")

if __name__ == "__main__":
    refactor_ids()
