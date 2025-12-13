
import sys
from pathlib import Path
import pandas as pd
import sqlite3

sys.path.append(str(Path.cwd()))
from src.data.config import RAW_DATA_DIR
from src.data.team_mappings import normalize_team_code
from src.db.core import connect


def _latest_source_directory(league: str, source_subdir: str) -> Path | None:
    search_roots = [RAW_DATA_DIR / "sources" / league.lower() / source_subdir]
    for base_dir in search_roots:
        if not base_dir.exists(): continue
        subdirs = [d for d in base_dir.iterdir() if d.is_dir()]
        if not subdirs: continue
        subdirs.sort(key=lambda x: x.name, reverse=True)
        return subdirs[0]
    return None

def debug_nba():
    league = "NBA"
    src_dir = _latest_source_directory(league, "rolling_metrics")
    if not src_dir:
        print("Source dir not found")
        return
        
    parquet_path = src_dir / "rolling_metrics.parquet"
    if not parquet_path.exists():
        print("Parquet not found")
        return

    df = pd.read_parquet(parquet_path)
    print("Columns:", df.columns)
    print("First 5 teams raw:", df["team"].head().tolist())
    
    with connect() as conn:
        cursor = conn.cursor()
        for _, row in df.head(5).iterrows():
            raw_team = row["team"]
            team_code = normalize_team_code(league, raw_team)
            print(f"Raw: '{raw_team}' -> Normalized: '{team_code}'")
            
            # Test query
            query = "SELECT team_id, code, name FROM teams WHERE sport_id = (SELECT sport_id FROM sports WHERE league = ?) AND (name = ? OR code = ?)"
            res = cursor.execute(query, (league, team_code, team_code)).fetchone()
            print(f"DB Match: {res}")

if __name__ == "__main__":
    debug_nba()
