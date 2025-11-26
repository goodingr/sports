import sys
from pathlib import Path
import pandas as pd
import logging

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.dashboard.data import load_forward_test_data, _expand_totals

# Setup logging
logging.basicConfig(level=logging.INFO)

def debug_duplicates():
    print("Loading data...")
    df = load_forward_test_data(model_type="ensemble")
    
    # Filter for the specific game mentioned
    # Borussia Monchengladbach vs RB Leipzig
    mask = (df['home_team'].str.contains("Monchengladbach", case=False, na=False)) | \
           (df['away_team'].str.contains("Monchengladbach", case=False, na=False))
    
    relevant = df[mask].copy()
    
    if relevant.empty:
        print("No games found for Monchengladbach")
        return

    print(f"\nFound {len(relevant)} rows for Monchengladbach:")
    cols = ['game_id', 'commence_time', 'home_team', 'away_team']
    print(relevant[cols].to_string())
    
    print("\nExpanding totals...")
    totals = _expand_totals(relevant)
    
    if totals.empty:
        print("No totals data found")
        return
        
    print(f"\nExpanded totals ({len(totals)} rows):")
    t_cols = ['game_id', 'side', 'total_line', 'predicted_prob', 'edge', 'commence_time']
    print(totals[t_cols].to_string())
    
    # Check for duplicates based on my logic
    if "game_id" in totals.columns and "side" in totals.columns:
        dupes = totals[totals.duplicated(subset=["game_id", "side"], keep=False)]
        if not dupes.empty:
            print(f"\nDuplicates based on game_id + side:")
            print(dupes[t_cols].to_string())
        else:
            print("\nNo duplicates found based on game_id + side")
            
    # Apply robust deduplication
    dedupe_cols = [col for col in ["home_team", "away_team", "commence_time", "side"] if col in totals.columns]
    if len(dedupe_cols) >= 4:
        deduped = totals.drop_duplicates(subset=dedupe_cols, keep="first")
        print(f"\nAfter robust deduplication: {len(deduped)} rows")
        if len(deduped) < len(totals):
            print("SUCCESS: Duplicates removed!")
        else:
            print("WARNING: No duplicates removed.")

if __name__ == "__main__":
    debug_duplicates()
