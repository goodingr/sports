import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.dashboard.data import get_overunder_recommendations, get_totals_odds_for_recommended, load_forward_test_data
from src.dashboard.app import update_overunder_page

def test_dashboard_odds_merge_logic():
    """
    Simulate the logic in update_overunder_page to see why odds aren't updating.
    """
    print("Loading forward test data...")
    # Load data similar to how app.py does
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    
    # Filter for Lazio vs Lecce game if possible, or just check the merge logic generally
    edge_threshold = 0.0
    recommended = get_overunder_recommendations(df, edge_threshold=edge_threshold)
    
    # Find Lazio vs Lecce in recommended
    lazio_game = recommended[
        (recommended['home_team'].str.contains('Lazio', case=False, na=False)) | 
        (recommended['away_team'].str.contains('Lazio', case=False, na=False))
    ]
    
    if lazio_game.empty:
        print("❌ Lazio game not found in recommendations. Cannot reproduce.")
        return

    print(f"\nFound Lazio game in recommendations:")
    cols = ['game_id', 'home_team', 'away_team', 'side', 'moneyline']
    if 'book' in lazio_game.columns:
        cols.append('book')
    print(lazio_game[cols].to_string())
    
    # Fetch fresh odds
    print("\nFetching fresh odds from DB...")
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    
    if totals_odds_df.empty:
        print("❌ No fresh odds found.")
        return
        
    # Check if we have odds for this game
    game_id = lazio_game.iloc[0]['game_id']
    game_odds = totals_odds_df[totals_odds_df['forward_game_id'] == game_id]
    
    print(f"\nFresh odds for game {game_id}:")
    if game_odds.empty:
        print("❌ No odds for this specific game in the fetched results.")
    else:
        print(game_odds[['book', 'outcome', 'moneyline']].to_string())

    # --- REPLICATE APP.PY MERGE LOGIC ---
    print("\n--- Simulating App.py Merge ---")
    
    # Group by game_id and side to get the best odds
    best_odds = totals_odds_df.loc[totals_odds_df.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
    
    print("\nBest odds (pre-merge):")
    lazio_best = best_odds[best_odds['forward_game_id'] == game_id]
    print(lazio_best[['book', 'outcome', 'moneyline']].to_string())
    
    # Check for potential merge key mismatches
    rec_side = lazio_game.iloc[0]['side']
    odds_outcome = lazio_best.iloc[0]['outcome'] if not lazio_best.empty else "N/A"
    
    print(f"\nMerge Keys Check:")
    print(f"Recommended 'side': '{rec_side}' (Type: {type(rec_side)})")
    print(f"Odds 'outcome':     '{odds_outcome}' (Type: {type(odds_outcome)})")
    
    if str(rec_side).lower() != str(odds_outcome).lower():
         print("⚠️ POTENTIAL MISMATCH: Case difference?")
    
    # Perform the merge exactly as in app.py
    merged = recommended.merge(
        best_odds[['forward_game_id', 'outcome', 'book', 'moneyline']],
        left_on=['game_id', 'side'],
        right_on=['forward_game_id', 'outcome'],
        how='left',
        suffixes=('', '_sportsbook')
    )
    
    # Check the result for Lazio game
    merged_lazio = merged[merged['game_id'] == game_id]
    
    print("\nMerged Result (Raw Columns):")
    print(f"Columns: {merged.columns.tolist()}")
    
    cols_to_show = ['game_id', 'side', 'moneyline']
    if 'moneyline_sportsbook' in merged.columns:
        cols_to_show.append('moneyline_sportsbook')
    if 'book' in merged.columns:
        cols_to_show.append('book')
    if 'book_sportsbook' in merged.columns:
        cols_to_show.append('book_sportsbook')
    # Add outcome if it exists
    if 'outcome' in merged_lazio.columns:
        cols_to_show.append('outcome')
        
    print(merged_lazio[cols_to_show].to_string())
    
    # Apply the fillna logic
    if 'book' not in merged.columns:
        merged['book'] = ""
        
    if 'book_sportsbook' in merged.columns:
        merged['book'] = merged['book_sportsbook'].fillna(merged['book'])
    else:
        print("⚠️ 'book_sportsbook' column missing - Merge likely failed to find matches!")

    if 'moneyline_sportsbook' in merged.columns:
        merged['moneyline'] = merged['moneyline_sportsbook'].fillna(merged['moneyline'])
    else:
        print("⚠️ 'moneyline_sportsbook' column missing - Merge likely failed to find matches!")
    
    final_lazio = merged[merged['game_id'] == game_id]
    print("\nFinal Result (After fillna):")
    print(final_lazio[['game_id', 'side', 'moneyline', 'book']].to_string())
    
    # Assertion
    expected_ml = 125.0 # From previous debug
    actual_ml = final_lazio.iloc[0]['moneyline']
    
    if abs(actual_ml - expected_ml) < 0.1:
        print(f"\n✅ SUCCESS: Moneyline updated to {actual_ml}")
    else:
        print(f"\n❌ FAILURE: Moneyline is {actual_ml}, expected {expected_ml}")

if __name__ == "__main__":
    test_dashboard_odds_merge_logic()
