import json
import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add project root to path
sys.path.append(str(Path.cwd()))

from src.db.core import connect
from src.db.loaders import load_odds_snapshot

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/odds")

def ingest_snapshots():
    """
    Iterates through raw odds snapshots and re-ingests them using the loader logic.
    Focuses on recent snapshots (Nov/Dec 2025) to recover missing data.
    """
    LOGGER.info(f"Scanning {RAW_DIR} for snapshots...")
    
    # Files to process
    files_to_process = []
    
    for sport_dir in RAW_DIR.iterdir():
        if not sport_dir.is_dir():
            continue
            
        sport_key = sport_dir.name
        LOGGER.info(f"Found sport directory: {sport_key}")
        
        for json_file in sport_dir.glob("*.json"):
            # Filter by date/filename to avoid processing ancient history
            # Filename format: odds_YYYY-MM-DDTHH-MM-SSZ.json
            try:
                # Extract date part
                date_str = json_file.stem.replace("odds_", "").split("T")[0]
                if date_str < "2025-11-20":
                    continue
                files_to_process.append((sport_key, json_file))
            except Exception:
                pass
                
    LOGGER.info(f"Found {len(files_to_process)} snapshots to ingest.")
    
    # Sort by timestamp (filename) to ingest in order
    files_to_process.sort(key=lambda x: x[1].name)
    
    with connect() as conn:
        for sport, filepath in files_to_process:
            LOGGER.info(f"Ingesting {sport} : {filepath.name}")
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # load_odds_snapshot expects (conn, data, sport)
                # Ensure data is a list
                if isinstance(data, dict):
                    data = [data]
                
                # Mock a simpler loader response or just let it run?
                # load_odds_snapshot returns nothing?
                load_odds_snapshot(conn, data, sport)
                conn.commit()
                
            except Exception as e:
                LOGGER.error(f"Failed to ingest {filepath}: {e}")
                conn.rollback()

if __name__ == "__main__":
    ingest_snapshots()
