import sys
from pathlib import Path
import pandas as pd
import logging

# Add project root to path
sys.path.append(str(Path.cwd()))

from src.dashboard.data import load_forward_test_data, _expand_predictions

logging.basicConfig(level=logging.INFO)

def debug_data():
    print("Loading data...")
    df = load_forward_test_data(league='NCAAB')
    
    # Target specific game: NCAAB_1f65cf2314a8279af67ac7bedbb04fe7
    target_id = 'NCAAB_1f65cf2314a8279af67ac7bedbb04fe7'
    
    print(f"Looking for {target_id}...")
    
    if target_id not in df['game_id'].values:
        print("Target ID NOT FOUND in loaded dataframe!")
        # Print a few others to see ID format
        print("Sample IDs:", df['game_id'].head().tolist())
        return

    row = df[df['game_id'] == target_id].iloc[0]
    
    print("\n--- RAW DATAFRAME ROW ---")
    print(f"Home ML: {repr(row['home_moneyline'])} (Type: {type(row['home_moneyline'])})")
    print(f"Away ML: {repr(row['away_moneyline'])} (Type: {type(row['away_moneyline'])})")
    
    print("\n--- EXPANSION TEST ---")
    # Call _expand_predictions and see if it survives
    bets = _expand_predictions(df[df['game_id'] == target_id])
    
    if bets.empty:
        print("SUCCESS: Game was filtered out by _expand_predictions.")
    else:
        print("FAILURE: Game SURVIVED expansion!")
        print(bets[['game_id', 'team', 'moneyline']])
        print(f"Bets Moneyline Value: {repr(bets.iloc[0]['moneyline'])}")

if __name__ == "__main__":
    debug_data()
