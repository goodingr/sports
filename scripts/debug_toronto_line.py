import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.dashboard.data import get_overunder_recommendations, get_totals_odds_for_recommended, load_forward_test_data

def debug_toronto_nj():
    """
    Debug the specific Toronto vs NJ game to see if the line is being updated.
    """
    print("Loading forward test data...")
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    
    edge_threshold = 0.0
    recommended = get_overunder_recommendations(df, edge_threshold=edge_threshold)
    
    # Find Toronto or NJ game
    tor_game = recommended[
        (recommended['home_team'].str.contains('Toronto', case=False, na=False)) | 
        (recommended['away_team'].str.contains('Toronto', case=False, na=False)) |
        (recommended['home_team'].str.contains('Jersey', case=False, na=False)) | 
        (recommended['away_team'].str.contains('Jersey', case=False, na=False))
    ]
    
    if tor_game.empty:
        print("❌ Toronto/NJ game not found!")
        return
    
    print(f"\n=== BEFORE MERGE ===")
    for _, row in tor_game.iterrows():
        print(f"{row['home_team']} vs {row['away_team']}")
        print(f"  {row['side'].title()} {row['total_line']}")
        print(f"  Predicted odds: {row.get('moneyline', 'N/A')}")
    
    # Fetch odds
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    
    if totals_odds_df.empty:
        print("❌ No odds found!")
        return
    
    # Check what odds are available for this game
    game_id = tor_game.iloc[0]['game_id']
    side = tor_game.iloc[0]['side']
    
    print(f"\n=== GAME ID INFO ===")
    print(f"Prediction game_id: {game_id}")
    print(f"Prediction side: {side}")
    
    # Check if this game is in the odds dataframe
    game_odds_check = totals_odds_df[totals_odds_df['forward_game_id'] == game_id]
    print(f"Odds with matching forward_game_id: {len(game_odds_check)}")
    
    if not game_odds_check.empty:
        print(f"\n=== DATABASE ODDS (for forward_game_id={game_id}) ===")
        print(game_odds_check[['book', 'outcome', 'line', 'moneyline']].to_string())
    else:
        print(f"\n⚠️ No odds found with forward_game_id={game_id}")
        print(f"\nAll forward_game_ids in odds dataframe:")
        print(totals_odds_df['forward_game_id'].unique()[:10])  # Show first 10
    
    # Simulate the merge logic from app.py
    best_odds = totals_odds_df.loc[totals_odds_df.groupby(['forward_game_id', 'outcome'])['moneyline'].idxmax()]
    
    print(f"\n=== BEST ODDS (all games) ===")
    print(f"Total best odds records: {len(best_odds)}")
    
    # Check if there's a best odds entry for this game/side
    best_for_game = best_odds[
        (best_odds['forward_game_id'] == game_id) & 
        (best_odds['outcome'] == side)
    ]
    
    if not best_for_game.empty:
        print(f"\nBest odds for this game/side:")
        print(best_for_game[['forward_game_id', 'outcome', 'book', 'line', 'moneyline']].to_string())
    else:
        print(f"\n⚠️ No best odds found for game_id={game_id}, side={side}")
    
    merged = recommended.merge(
        best_odds[['forward_game_id', 'outcome', 'book', 'moneyline', 'line']],
        left_on=['game_id', 'side'],
        right_on=['forward_game_id', 'outcome'],
        how='left',
        suffixes=('', '_sportsbook')
    )
    
    # Find Toronto game in merged
    tor_merged = merged[merged['game_id'] == game_id]
    
    print(f"\n=== AFTER MERGE (BEFORE LINE UPDATE) ===")
    cols = ['home_team', 'away_team', 'side', 'total_line', 'moneyline']
    if 'line_sportsbook' in tor_merged.columns:
        cols.extend(['line_sportsbook', 'moneyline_sportsbook', 'book_sportsbook'])
    print(tor_merged[cols].to_string())
    
    # Apply the line update logic
    if 'line_sportsbook' in merged.columns:
        has_sportsbook_data = merged['line_sportsbook'].notna()
        merged.loc[has_sportsbook_data, 'total_line'] = merged.loc[has_sportsbook_data, 'line_sportsbook']
        
        print(f"\n=== AFTER LINE UPDATE ===")
        tor_final = merged[merged['game_id'] == game_id]
        print(f"{tor_final.iloc[0]['home_team']} vs {tor_final.iloc[0]['away_team']}")
        print(f"  {tor_final.iloc[0]['side'].title()} {tor_final.iloc[0]['total_line']} @ {tor_final.iloc[0]['moneyline']:+.0f}")
        print(f"  Book: {tor_final.iloc[0].get('book_sportsbook', 'N/A')}")
        
        # Check if it matches expected
        expected_line = 230.0
        actual_line = tor_final.iloc[0]['total_line']
        
        if abs(actual_line - expected_line) < 0.1:
            print(f"\n✅ Line correctly updated to {actual_line}!")
        else:
            print(f"\n❌ Line is {actual_line}, expected {expected_line}")
    else:
        print("\n❌ No line_sportsbook column!")

if __name__ == "__main__":
    debug_toronto_nj()
