import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.dashboard.data import get_overunder_recommendations, get_totals_odds_for_recommended, load_forward_test_data

def simulate_app_py_logic():
    """
    Simulate exactly what app.py does to see if line is being updated
    """
    print("Loading forward test data...")
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    
    edge_threshold = 0.0
    recommended = get_overunder_recommendations(df, edge_threshold=edge_threshold)
    
    print(f"\nTotal recommendations: {len(recommended)}")
    print(f"\nBEFORE MERGE - Sample lines:")
    print(recommended[['home_team', 'away_team', 'side', 'total_line']].head(5).to_string())
    
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    
    if not totals_odds_df.empty:
        # Exactly as in app.py
        best_odds = totals_odds_df.loc[totals_odds_df.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
        
        print(f"\nBest odds found: {len(best_odds)} records")
        
        recommended = recommended.merge(
            best_odds[['forward_game_id', 'outcome', 'book', 'moneyline', 'line']],
            left_on=['game_id', 'side'],
            right_on=['forward_game_id', 'outcome'],
            how='left',
            suffixes=('', '_sportsbook')
        )
        
        print(f"\nAFTER MERGE - Checking columns:")
        print(f"Has 'line_sportsbook': {'line_sportsbook' in recommended.columns}")
        
        if 'line_sportsbook' in recommended.columns:
            num_with_sportsbook_line = recommended['line_sportsbook'].notna().sum()
            print(f"Rows with sportsbook line data: {num_with_sportsbook_line}")
            
            has_sportsbook_data = recommended['line_sportsbook'].notna()
            recommended.loc[has_sportsbook_data, 'total_line'] = recommended.loc[has_sportsbook_data, 'line_sportsbook']
            
            print(f"\nAFTER LINE UPDATE - Sample with sportsbook data:")
            with_data = recommended[has_sportsbook_data].head(5)
            print(with_data[['home_team', 'away_team', 'side', 'total_line', 'line_sportsbook', 'book_sportsbook', 'moneyline']].to_string())
        else:
            print("ERROR: No line_sportsbook column created!")
    else:
        print("No odds data found!")

if __name__ == "__main__":
    simulate_app_py_logic()
