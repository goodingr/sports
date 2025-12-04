import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.dashboard.data import (
    get_overunder_recommendations, 
    get_totals_odds_for_recommended, 
    load_forward_test_data,
    _map_game_ids_by_odds_api,
    _match_games_to_db
)

def debug_game_id_matching():
    """
    Debug why game IDs aren't matching
    """
    print("Loading forward test data...")
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    
    edge_threshold = 0.0
    recommended = get_overunder_recommendations(df, edge_threshold=edge_threshold)
    
    print(f"\nTotal recommendations: {len(recommended)}")
    print(f"Sample game IDs from recommendations:")
    print(recommended['game_id'].head(5).tolist())
    
    # Try the odds API mapping first
    print("\n=== Trying _map_game_ids_by_odds_api ===")
    mapping = _map_game_ids_by_odds_api(recommended)
    print(f"Mapped via odds API: {len(mapping)} games")
    if not mapping.empty:
        print("Sample mappings:")
        print(mapping[['prediction_game_id', 'db_game_id']].head(3).to_string())
    
    # Check what's left
    if not mapping.empty:
        matched_ids = set(mapping["prediction_game_id"])
        remaining = recommended[~recommended["game_id"].isin(matched_ids)]
    else:
        remaining = recommended
        
    print(f"\nRemaining after odds API mapping: {len(remaining)} games")
    
    # Try the DB matching
    if not remaining.empty:
        print("\n=== Trying _match_games_to_db ===")
        extra = _match_games_to_db(remaining)
        print(f"Matched via DB: {len(extra)} games")
        if not extra.empty:
            print("Sample DB mappings:")
            print(extra[['prediction_game_id', 'db_game_id']].head(3).to_string())
    
    # Now check the final totals_odds result
    print("\n=== Final get_totals_odds_for_recommended ===")
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    print(f"Total odds records returned: {len(totals_odds_df)}")
    print(f"Unique games with odds: {totals_odds_df['forward_game_id'].nunique() if not totals_odds_df.empty else 0}")
    
    if not totals_odds_df.empty:
        print("\nSample odds:")
        print(totals_odds_df[['forward_game_id', 'book', 'outcome', 'line', 'moneyline']].head(5).to_string())
    
    # Summary
    total_recs = len(recommended)
    total_with_odds = totals_odds_df['forward_game_id'].nunique() if not totals_odds_df.empty else 0
    pct = 100 * total_with_odds / total_recs if total_recs > 0 else 0
    
    print(f"\n=== SUMMARY ===")
    print(f"Recommendations: {total_recs}")
    print(f"With sportsbook odds: {total_with_odds} ({pct:.1f}%)")
    print(f"Without sportsbook odds: {total_recs - total_with_odds} ({100-pct:.1f}%)")

if __name__ == "__main__":
    debug_game_id_matching()
