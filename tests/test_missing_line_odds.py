import pandas as pd
import sys
from pathlib import Path
import pytest

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.dashboard.data import get_overunder_recommendations, get_totals_odds_for_recommended, load_forward_test_data

def test_all_bets_have_sportsbook_data():
    """
    Test that ALL recommended bets get sportsbook data (odds, book, and line).
    
    With the new approach:
    - We select the BEST odds for each game/side from any available line
    - We use the sportsbook's line, not the predicted line
    - Every bet should have sportsbook data
    """
    print("Loading forward test data...")
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    
    edge_threshold = 0.0
    recommended = get_overunder_recommendations(df, edge_threshold=edge_threshold)
    
    if recommended.empty:
        print("❌ No recommendations found.")
        pytest.skip("No recommendations found.")
    
    # Fetch fresh odds
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    
    if totals_odds_df.empty:
        print("❌ No fresh odds found.")
        pytest.skip("No fresh odds found.")
    
    print(f"\nTesting {len(recommended)} recommended bets...")
    
    # Simulate the NEW merge logic from app.py (best odds per game/side)
    best_odds = totals_odds_df.loc[totals_odds_df.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
    
    merged = recommended.merge(
        best_odds[['forward_game_id', 'outcome', 'book', 'moneyline', 'line']],
        left_on=['game_id', 'side'],
        right_on=['forward_game_id', 'outcome'],
        how='left',
        suffixes=('', '_sportsbook')
    )
    
    # Check how many have sportsbook data
    has_sportsbook_data = merged['moneyline_sportsbook'].notna()
    num_with_data = has_sportsbook_data.sum()
    num_without_data = (~has_sportsbook_data).sum()
    
    print(f"\nResults:")
    print(f"  With sportsbook data: {num_with_data} ({100 * num_with_data / len(merged):.1f}%)")
    print(f"  Without sportsbook data: {num_without_data} ({100 * num_without_data / len(merged):.1f}%)")
    
    if num_without_data > 0:
        print(f"\n⚠️ {num_without_data} bets have NO sportsbook data!")
        missing_df = merged[~has_sportsbook_data]
        for idx, row in missing_df.head(3).iterrows():
            print(f"\n  {row.get('home_team')} vs {row.get('away_team')}")
            print(f"  Side: {row['side']}")
            print(f"  Game ID: {row['game_id']}")
        
        if num_without_data > 3:
            print(f"\n  ... and {num_without_data - 3} more")
        
        pytest.fail(f"{num_without_data} bets have no sportsbook data")
    else:
        print(f"\n✅ ALL {len(merged)} bets have sportsbook data!")
        
        # Show a few examples
        print(f"\nExample bets:")
        for idx, row in merged.head(3).iterrows():
            pred_line = row['total_line']
            sb_line = row.get('line_sportsbook', row.get('line'))
            sb_book = row.get('book_sportsbook', row.get('book'))
            print(f"\n  {row.get('home_team')} vs {row.get('away_team')}")
            print(f"  Predicted line: {row['side'].title()} {pred_line}")
            print(f"  Best available: {sb_book} {row['side'].title()} {sb_line} @ {row['moneyline_sportsbook']:+.0f}")
            if abs(pred_line - sb_line) > 0.5:
                print(f"    (Line adjusted from {pred_line} to {sb_line})")
        
        assert num_without_data == 0

if __name__ == "__main__":
    success = test_all_bets_have_sportsbook_data()
    sys.exit(0 if success else 1)
