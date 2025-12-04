import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.dashboard.data import get_overunder_recommendations, get_totals_odds_for_recommended, load_forward_test_data

def test_merge_detailed():
    """
    Test the exact merge logic from app.py with detailed output
    """
    print("Loading data...")
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    recommended = get_overunder_recommendations(df, edge_threshold=0.0)
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    
    print(f"\nRecommendations: {len(recommended)}")
    print(f"Odds records: {len(totals_odds_df)}")
    print(f"Unique games with odds: {totals_odds_df['forward_game_id'].nunique()}")
    
    # Check column names
    print(f"\nrecommended columns: {recommended.columns.tolist()}")
    print(f"totals_odds_df columns: {totals_odds_df.columns.tolist()}")
    
    # Exact merge as in app.py
    best_odds = totals_odds_df.loc[totals_odds_df.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
    
    print(f"\nbest_odds shape: {best_odds.shape}")
    print(f"best_odds columns: {best_odds.columns.tolist()}")
    
    # Select columns for merge - EXACTLY as in app.py line 1112
    merge_cols = ['forward_game_id', 'outcome', 'book', 'moneyline', 'line']
    print(f"\nColumns to merge: {merge_cols}")
    
    # Check if all columns exist
    missing = [c for c in merge_cols if c not in best_odds.columns]
    if missing:
        print(f"ERROR: Missing columns in best_odds: {missing}")
        return
    
    best_odds_subset = best_odds[merge_cols]
    print(f"best_odds_subset shape: {best_odds_subset.shape}")
    
    # Merge
    print("\nPerforming merge...")
    merged = recommended.merge(
        best_odds_subset,
        left_on=['game_id', 'side'],
        right_on=['forward_game_id', 'outcome'],
        how='left',
        suffixes=('', '_sportsbook')
    )
    
    print(f"merged shape: {merged.shape}")
    print(f"merged columns: {merged.columns.tolist()}")
    
    # Check if _sportsbook columns exist
    sportsbook_cols = [c for c in merged.columns if '_sportsbook' in c]
    print(f"\nColumns with '_sportsbook': {sportsbook_cols}")
    
    # Use the FIXED logic from app.py
    line_col = 'line_sportsbook' if 'line_sportsbook' in merged.columns else 'line'
    
    if line_col in merged.columns:
        num_with_data = merged[line_col].notna().sum()
        print(f"\nRows with {line_col} data: {num_with_data}")
        
        print(f"\nSample merged rows with sportsbook data:")
        with_data = merged[merged[line_col].notna()].head(3)
        book_col = 'book_sportsbook' if 'book_sportsbook' in merged.columns else 'book'
        print(with_data[['game_id', 'side', 'total_line', line_col, book_col]].to_string())
        
        # Test the line update logic
        has_sportsbook_data = merged[line_col].notna()
        merged.loc[has_sportsbook_data, 'total_line'] = merged.loc[has_sportsbook_data, line_col]
        
        print(f"\nAfter updating total_line:")
        print(merged[has_sportsbook_data].head(3)[['game_id', 'side', 'total_line', line_col, book_col]].to_string())
        
        print(f"\n✓ SUCCESS: Lines updated for {num_with_data} bets")
    else:
        print("\nERROR: No line column found!")

if __name__ == "__main__":
    test_merge_detailed()
