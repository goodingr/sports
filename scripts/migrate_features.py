"""
Migrate feature data from Parquet files to the SQLite database.
Reads rolling_metrics.parquet, team_metrics.parquet, and injuries.parquet
and populates team_features and injury_reports tables.
"""

import json
import logging
import sqlite3
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add src to path
sys.path.append(str(Path.cwd()))

from src.db.core import connect
from src.data.config import RAW_DATA_DIR
from src.data.team_mappings import normalize_team_code

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
LOGGER = logging.getLogger(__name__)

RAW_SOURCES_DIR = RAW_DATA_DIR / "sources"
SOCCER_LEAGUES = {"EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"}
LEAGUE_STORAGE_OVERRIDES = {league: "soccer" for league in SOCCER_LEAGUES}

def _latest_source_directory(league: str, source_subdir: str) -> Path | None:
    """Find the most recent source directory for a given league and subdirectory."""
    search_roots = [RAW_SOURCES_DIR / league.lower() / source_subdir]

    override_root = LEAGUE_STORAGE_OVERRIDES.get(league.upper())
    if override_root:
        search_roots.append(RAW_SOURCES_DIR / override_root / source_subdir)

    for base_dir in search_roots:
        if not base_dir.exists():
            continue

        subdirs = [d for d in base_dir.iterdir() if d.is_dir()]
        if not subdirs:
            continue

        subdirs.sort(key=lambda x: x.name, reverse=True)
        return subdirs[0]

    return None

def migrate_rolling_metrics(conn, league: str):
    """Migrate rolling metrics to team_features as 'game_stats'."""
    src_dir = _latest_source_directory(league, "rolling_metrics")
    if not src_dir:
        LOGGER.warning(f"No rolling_metrics directory found for {league}")
        return

    parquet_file = src_dir / "rolling_metrics.parquet"
    if not parquet_file.exists():
        LOGGER.warning(f"No rolling_metrics.parquet found at {parquet_file}")
        return

    df = pd.read_parquet(parquet_file)
    if df.empty:
        return

    LOGGER.info(f"Migrating {len(df)} rolling metric records for {league}")
    
    count = 0
    cursor = conn.cursor()
    
    # Normalize columns
    if "team" not in df.columns:
         LOGGER.warning("Rolling metrics missing 'team' column")
         return

    for _, row in df.iterrows():
        team_code = normalize_team_code(league, row["team"])
        if not team_code:
            continue
            
        game_id = row.get("game_id") # Ideally we have game_id
        game_date = row.get("game_date")
        
        # We store the entire row as JSON features
        feature_data = row.to_dict()
        # Ensure timestamp serialization
        for k, v in feature_data.items():
            if isinstance(v, (pd.Timestamp, datetime)):
                feature_data[k] = v.isoformat()
        
        feature_json = json.dumps(feature_data)
        
        # We need a team_id. For now, we rely on team_code matching via teams table if needed,
        # but team_features stores team_id. 
        # Let's try to lookup team_id.
        team_row = cursor.execute("SELECT team_id FROM teams WHERE sport_id = (SELECT sport_id FROM sports WHERE league = ?) AND (name = ? OR code = ?)", (league, team_code, team_code)).fetchone()
        
        team_id = team_row[0] if team_row else None
        
        # If we can't find team_id, we might skip or use a placeholder? 
        # For this refactor, let's skip if we can't link to a team, as features are team-bound.
        if not team_id:
             # Try broader search (some team names might not match perfectly)
             # But without team_id, we can't join effectively.
             # Actually, team_mappings normalize_team_code should align with DB mostly.
             pass

        # If we have no team_id, we can insert with 0 or NULL if schema allows, but schema says team_id INTEGER.
        # Let's insert mainly if we have a match.
        if team_id:
            try:
                cursor.execute("""
                    INSERT INTO team_features (game_id, team_id, feature_set, feature_json)
                    VALUES (?, ?, 'game_stats', ?)
                """, (game_id, team_id, feature_json))
                count += 1
            except sqlite3.Error as e:
                LOGGER.warning(f"Insert error: {e}")
                pass

    conn.commit()
    LOGGER.info(f"Migrated {count} rolling metric records for {league}")

def migrate_team_metrics(conn, league: str):
    """Migrate season metrics to team_features as 'season_stats'."""
    src_dir = _latest_source_directory(league, "team_metrics")
    if not src_dir:
        return

    parquet_file = src_dir / "team_metrics.parquet"
    if not parquet_file.exists():
        return

    df = pd.read_parquet(parquet_file)
    if df.empty:
        return

    LOGGER.info(f"Migrating {len(df)} team metric records for {league}")
    cursor = conn.cursor()
    count = 0

    # Handle column variations
    team_col = "team"
    if "TEAM_ABBREVIATION" in df.columns:
        team_col = "TEAM_ABBREVIATION"
    
    for _, row in df.iterrows():
        raw_team = row.get(team_col)
        if not raw_team:
            continue
            
        team_code = normalize_team_code(league, raw_team)
        if not team_code:
            continue

        team_row = cursor.execute("SELECT team_id FROM teams WHERE sport_id = (SELECT sport_id FROM sports WHERE league = ?) AND (name = ? OR code = ?)", (league, team_code, team_code)).fetchone()
        team_id = team_row[0] if team_row else None
        
        if team_id:
            feature_data = row.to_dict()
            for k, v in feature_data.items():
                if isinstance(v, (pd.Timestamp, datetime)):
                    feature_data[k] = v.isoformat()
            
            feature_json = json.dumps(feature_data)
            
            try:
                cursor.execute("""
                    INSERT INTO team_features (game_id, team_id, feature_set, feature_json)
                    VALUES (NULL, ?, 'season_stats', ?)
                """, (team_id, feature_json))
                count += 1
            except Exception:
                pass
                
    conn.commit()
    LOGGER.info(f"Migrated {count} team metric records for {league}")

def migrate_injuries(conn, league: str):
    """Migrate injuries to injury_reports table."""
    src_dir = _latest_source_directory(league, "injuries_espn" if league == "NBA" else "injuries")
    if not src_dir and league == "NBA": # Try fallback
         src_dir = _latest_source_directory(league, "injuries")
         
    if not src_dir:
        LOGGER.warning(f"No injuries directory for {league}")
        return

    # Try common filenames
    df = pd.DataFrame()
    for fname in ["injuries.parquet", "injury_reports.parquet", "injuries.csv"]:
        fpath = src_dir / fname
        if fpath.exists():
            if fname.endswith(".csv"):
                df = pd.read_csv(fpath)
            else:
                df = pd.read_parquet(fpath)
            break
            
    if df.empty:
        return

    LOGGER.info(f"Migrating {len(df)} injury records for {league}")
    cursor = conn.cursor()
    count = 0
    
    # Normalize columns
    team_col = None
    for c in ["team", "team_code", "club_code", "team_abbreviation"]:
        if c in df.columns:
            team_col = c
            break
            
    for _, row in df.iterrows():
        raw_team = row.get(team_col)
        team_code = normalize_team_code(league, raw_team)
        
        team_row = cursor.execute("SELECT team_id FROM teams WHERE sport_id = (SELECT sport_id FROM sports WHERE league = ?) AND (name = ? OR code = ?)", (league, team_code, team_code)).fetchone()
        team_id = team_row[0] if team_row else None # Can be null for injuries sometimes
        
        # Check source key constraint? Schema doesn't enforce strict unique on source_key for updates, but auto-inc ID.
        
        player_name = row.get("player") or row.get("name") or row.get("player_name")
        if not player_name:
            continue
            
        status = row.get("status") or row.get("injury_status") or row.get("game_status")
        created_at = datetime.utcnow().isoformat()
        
        try:
            cursor.execute("""
                INSERT INTO injury_reports (
                    league, team_id, team_code, 
                    player_name, position, status, 
                    report_date, detail, source_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                league, team_id, team_code,
                player_name, row.get("pos") or row.get("position"), status,
                row.get("date") or row.get("game_date") or created_at,
                row.get("description") or row.get("desc"),
                f"{league}_{player_name}_{status}_{datetime.now().timestamp()}" # unique key approximation
            ))
            count += 1
        except Exception as e:
            # LOGGER.warning(f"Insert error: {e}")
            pass

    conn.commit()
    LOGGER.info(f"Migrated {count} injury records for {league}")

def main():
    leagues = ["NBA", "NFL"] + list(SOCCER_LEAGUES)
    
    with connect() as conn:
        for league in leagues:
            LOGGER.info(f"Processing {league}...")
            migrate_rolling_metrics(conn, league)
            migrate_team_metrics(conn, league)
            migrate_injuries(conn, league)
            
if __name__ == "__main__":
    main()
