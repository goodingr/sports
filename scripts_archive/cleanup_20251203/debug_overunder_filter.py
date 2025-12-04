import sys
from pathlib import Path
import pandas as pd
import logging

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.dashboard.data import get_overunder_recommendations, load_forward_test_data

# Setup logging
logging.basicConfig(level=logging.INFO)

def debug_overunder_filter():
    print("Loading data...")
    df = load_forward_test_data(model_type="ensemble")
    
    print(f"Total rows: {len(df)}")
    
    # Simulate dashboard call
    recommended = get_overunder_recommendations(df, edge_threshold=0.0)
    
    print(f"Recommended rows: {len(recommended)}")
    
    # Check for past games
    now = pd.Timestamp.now(tz="UTC")
    print(f"Current UTC time: {now}")
    
    if "commence_time" in recommended.columns:
        # Check timezone of commence_time
        if not recommended.empty:
            sample_ts = recommended["commence_time"].iloc[0]
            if sample_ts.tzinfo is None:
                 recommended["commence_time"] = recommended["commence_time"].dt.tz_localize("UTC")
            
        past = recommended[recommended["commence_time"] <= now]
        
        if not past.empty:
            print(f"\nWARNING: Found {len(past)} past games in recommended!")
            print(past[['game_id', 'commence_time', 'home_team', 'away_team']].head().to_string())
        else:
            print("\nSUCCESS: No past games found in recommended.")
            
    # Check for duplicates
    if "game_id" in recommended.columns and "side" in recommended.columns:
        dupes = recommended[recommended.duplicated(subset=["game_id", "side"], keep=False)]
        if not dupes.empty:
            print(f"\nWARNING: Found {len(dupes)} duplicates!")
            print(dupes[['game_id', 'side', 'total_line']].head().to_string())
        else:
            print("\nSUCCESS: No duplicates found.")

if __name__ == "__main__":
    debug_overunder_filter()
