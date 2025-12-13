import sqlite3
import re
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path.cwd()))

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

REPORT_PATH = "duplicate_report.txt"
DB_PATH = "data/betting.db"

def parse_report_and_merge():
    """
    Parses 'duplicate_report.txt' to identify duplicate pairs and merges them.
    Report format:
    1. LEAGUE - DATE (Direct)
    ----------------...
    Game ID | ...
    ----------------...
    ID_1    | ...
    ID_2    | ...
    """
    LOGGER.info(f"Parsing {REPORT_PATH}...")
    
    with open(REPORT_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    pairs = []
    current_pair = []
    
    # Simple state machine
    for line in lines:
        line = line.strip()
        # Skip headers/separators
        if not line or line.startswith("=") or line.startswith("-") or line.startswith("Duplicate") or line.startswith("Generated") or line.startswith("Found"):
            continue
        if line.startswith("Game ID"):
            continue
        
        # Check for Section Header "1. LEAGUE..."
        if re.match(r'^\d+\.', line):
            if current_pair:
                # If we have a pending pair (should be length 1 or 2?), warn if incomplete
                if len(current_pair) >= 2:
                    pairs.append(current_pair)
                current_pair = []
            continue
            
        # Parse Game Row
        # "BUNDESLIGA_746812                   | St. Pauli                 | Union Berlin              | 0     | 0"
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 1:
            game_id = parts[0]
            # Simple validation: looks like an ID?
            if len(game_id) > 5:
                # Add extra data for decision making: odds count, preds count
                odds_count = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
                preds_count = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
                
                current_pair.append({
                    "id": game_id,
                    "odds": odds_count,
                    "preds": preds_count
                })

    # Capture last pair
    if len(current_pair) >= 2:
        pairs.append(current_pair)
        
    LOGGER.info(f"Found {len(pairs)} pairs to merge.")
    
    if not pairs:
        return

    # Execute Merges
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()
    
    total_merged = 0
    
    for pair in pairs:
        # Sort pair to pick Best Target
        # Priority: Has Odds > Has Preds > Length (Hash vs Short?) matches standard?
        # Let's say we prefer the one with Odds.
        pair.sort(key=lambda x: (x['odds'], x['preds']), reverse=True)
        
        target = pair[0]
        duplicates = pair[1:]
        
        target_id = target['id']
        dup_ids = [d['id'] for d in duplicates]
        
        LOGGER.info(f"Merging {dup_ids} -> {target_id}")
        
        # 4. Migrate Children
        tables = ["odds", "predictions", "game_results", "team_features", "model_input", "model_predictions"]
        
        for table in tables:
            for old_id in dup_ids:
                try:
                    cursor.execute(f"UPDATE OR IGNORE {table} SET game_id = ? WHERE game_id = ?", (target_id, old_id))
                    cursor.execute(f"DELETE FROM {table} WHERE game_id = ?", (old_id,))
                except Exception:
                    pass

        # 5. Delete Old Games
        placeholders = ",".join(f"'{gid}'" for gid in dup_ids)
        cursor.execute(f"DELETE FROM games WHERE game_id IN ({placeholders})")
        
        total_merged += 1
        
    conn.commit()
    LOGGER.info(f"Successfully processed {total_merged} pairs.")
    conn.close()

if __name__ == "__main__":
    parse_report_and_merge()
