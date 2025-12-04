import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.dashboard.data import get_overunder_recommendations, get_totals_odds_for_recommended, load_forward_test_data

def debug_columns():
    """
    Check what columns exist in best_odds before merge
    """
    print("Loading data...")
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    recommended = get_overunder_recommendations(df, edge_threshold=0.0)
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    
    with open('debug_output.txt', 'w') as f:
        f.write(f"totals_odds_df columns: {totals_odds_df.columns.tolist()}\n")
        f.write(f"totals_odds_df shape: {totals_odds_df.shape}\n")
        
        if not totals_odds_df.empty:
            best_odds = totals_odds_df.loc[totals_odds_df.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
            f.write(f"\nbest_odds columns: {best_odds.columns.tolist()}\n")
            f.write(f"best_odds shape: {best_odds.shape}\n")
            
            f.write(f"\nbest_odds sample:\n")
            f.write(best_odds.head(3).to_string())
    
    print("Output written to debug_output.txt")

if __name__ == "__main__":
    debug_columns()
