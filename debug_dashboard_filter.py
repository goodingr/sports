import sys
from pathlib import Path
import pandas as pd
import logging

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.dashboard.data import get_recommended_bets, load_forward_test_data

# Setup logging
logging.basicConfig(level=logging.INFO)

def debug_dashboard_filter():
    print("Loading data...")
    df = load_forward_test_data(model_type="ensemble")
    
    print(f"Total rows: {len(df)}")
    
    # Simulate dashboard call
    recommended = get_recommended_bets(df, edge_threshold=0.0)
    
    print(f"Recommended rows: {len(recommended)}")
    
    # Check for past games
    now = pd.Timestamp.now(tz="UTC")
    print(f"Current UTC time: {now}")
    
    if "commence_time" in recommended.columns:
        # Check timezone of commence_time
        sample_ts = recommended["commence_time"].iloc[0] if not recommended.empty else None
        print(f"Sample commence_time: {sample_ts} (tz: {getattr(sample_ts, 'tzinfo', 'None')})")
        
        # Check for past games
        # We need to handle potential timezone mismatch in the check itself
        try:
            if recommended["commence_time"].dt.tz is None:
                 recommended["commence_time"] = recommended["commence_time"].dt.tz_localize("UTC")
            
            past = recommended[recommended["commence_time"] <= now]
            
            if not past.empty:
                print(f"\nWARNING: Found {len(past)} past games in recommended!")
                print(past[['game_id', 'commence_time', 'home_team', 'away_team']].head().to_string())
            else:
                print("\nSUCCESS: No past games found in recommended.")
                
        except Exception as e:
            print(f"Error checking dates: {e}")
            # Print raw values
            print(recommended["commence_time"].head())

if __name__ == "__main__":
    debug_dashboard_filter()
