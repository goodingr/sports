import sys
import json
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.data.ingest_odds import load_odds_snapshot
from src.db.core import connect

# Configure logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def reingest_epl():
    odds_dir = project_root / "data" / "raw" / "odds" / "soccer_epl"
    if not odds_dir.exists():
        print(f"Directory not found: {odds_dir}")
        return

    files = sorted(list(odds_dir.glob("odds_2025-11-*.json")))
    print(f"Found {len(files)} EPL odds files for Nov 2025")
    
    # conn = connect()  <-- Removed
    
    for file_path in files:
        print(f"Processing {file_path.name}...")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                payload = json.load(f)
            
            # Add raw_path to payload if not present, or pass as arg
            load_odds_snapshot(payload, raw_path=str(file_path), sport_key='soccer_epl')
            print("  ✓ Success")
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            
    # conn.close() <-- Removed

if __name__ == "__main__":
    reingest_epl()
