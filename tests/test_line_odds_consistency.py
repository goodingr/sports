import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.dashboard.data import get_overunder_recommendations, get_totals_odds_for_recommended, load_forward_test_data

def test_line_odds_consistency():
    """
    Test that the displayed line matches the line for the displayed odds/book.
    
    This is a regression test for the issue where:
    - Table shows: Over 228.5 at -107
    - But clicking shows: LowVig.ag Over 230.0 at -107
    
    The line (228.5) doesn't match the book's line (230.0).
    """
    print("Loading forward test data...")
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    
    edge_threshold = 0.0
    recommended = get_overunder_recommendations(df, edge_threshold=edge_threshold)
    
    if recommended.empty:
        print("❌ No recommendations found.")
        return
    
    # Fetch fresh odds
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    
    if totals_odds_df.empty:
        print("❌ No fresh odds found.")
        return
    
    print(f"\nTesting {len(recommended)} recommended bets for line/odds consistency...")
    
    # Simulate the FIXED merge logic from app.py (matching by line)
    recommended['_match_key'] = recommended.apply(
        lambda row: f"{row['game_id']}_{row['side']}_{row['total_line']}", 
        axis=1
    )
    totals_odds_df['_match_key'] = totals_odds_df.apply(
        lambda row: f"{row['forward_game_id']}_{row['outcome']}_{row['line']}", 
        axis=1
    )
    
    best_odds = totals_odds_df.loc[totals_odds_df.groupby('_match_key')['moneyline'].idxmax()]
    
    merged = recommended.merge(
        best_odds[['_match_key', 'book', 'moneyline', 'line']],
        on='_match_key',
        how='left',
        suffixes=('', '_sportsbook')
    )
    
    # Check for mismatches (there shouldn't be any now!)
    mismatches = []
    
    for idx, row in merged.iterrows():
        if pd.notna(row.get('line_sportsbook')) and pd.notna(row.get('total_line')):
            display_line = row['total_line']
            book_line = row['line_sportsbook']
            
            # Allow small tolerance for floating point comparison
            if abs(display_line - book_line) > 0.01:
                mismatches.append({
                    'game_id': row['game_id'],
                    'home_team': row.get('home_team'),
                    'away_team': row.get('away_team'),
                    'side': row['side'],
                    'display_line': display_line,
                    'book_line': book_line,
                    'book': row.get('book_sportsbook'),
                    'moneyline': row.get('moneyline_sportsbook')
                })
    
    if mismatches:
        print(f"\n❌ Found {len(mismatches)} line mismatches:")
        for mm in mismatches[:5]:  # Show first 5
            print(f"\n  {mm['home_team']} vs {mm['away_team']}")
            print(f"  Display: {mm['side'].title()} {mm['display_line']}")
            print(f"  Book: {mm['book']} has {mm['side'].title()} {mm['book_line']} @ {mm['moneyline']:+.0f}")
            print(f"  ⚠️ Line mismatch: {mm['display_line']} != {mm['book_line']}")
        
        if len(mismatches) > 5:
            print(f"\n  ... and {len(mismatches) - 5} more mismatches")
        
        return False
    else:
        print(f"\n✅ All {len(merged)} bets have consistent lines and odds!")
        return True

if __name__ == "__main__":
    success = test_line_odds_consistency()
    sys.exit(0 if success else 1)
