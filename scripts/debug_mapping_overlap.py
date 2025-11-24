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

def debug_mapping():
    """
    Check what mappings are being created and if they match recommendations
    """
    print("Loading data...")
    df = load_forward_test_data(force_refresh=False, league=None, model_type="ensemble")
    recommended = get_overunder_recommendations(df, edge_threshold=0.0)
    
    print(f"\nRecommendations: {len(recommended)}")
    print(f"Sample recommendation game_ids:")
    rec_sample = recommended['game_id'].head(10).tolist()
    for gid in rec_sample:
        print(f"  {gid}")
    
    # Try odds API mapping
    print("\n=== Trying _map_game_ids_by_odds_api ===")
    mapping = _map_game_ids_by_odds_api(recommended)
    print(f"Mappings found: {len(mapping)}")
    
    if not mapping.empty:
        print(f"\nSample mappings:")
        print(mapping.head(5).to_string())
        
        # Check if any mappings match recommendations
        rec_ids = set(recommended['game_id'])
        mapped_pred_ids = set(mapping['prediction_game_id'])
        overlap = rec_ids & mapped_pred_ids
        
        print(f"\nRecommendation game_ids: {len(rec_ids)}")
        print(f"Mapping prediction_game_ids: {len(mapped_pred_ids)}")
        print(f"Overlap: {len(overlap)}")
        
        if overlap:
            print(f"\nExample overlapping IDs:")
            for gid in list(overlap)[:3]:
                print(f"  {gid}")
        else:
            print("\n!!! NO OVERLAP - This is the bug!")
            print("\nRecommendation IDs are NOT in mapping prediction_game_ids")
            print("\nThis means the function is mapping the WRONG game IDs!")
    
    # Now check final odds
    print("\n=== Final totals_odds_df ===")
    totals_odds_df = get_totals_odds_for_recommended(recommended)
    print(f"Odds records: {len(totals_odds_df)}")
    
    if not totals_odds_df.empty:
        forward_ids = set(totals_odds_df['forward_game_id'].unique())
        rec_ids = set(recommended['game_id'])
        final_overlap = rec_ids & forward_ids
        
        print(f"\nRecommendation game_ids: {len(rec_ids)}")
        print(f"Odds forward_game_ids: {len(forward_ids)}")
        print(f"Final overlap: {len(final_overlap)}")
        
        if final_overlap:
            print(f"\n✓ {len(final_overlap)} games have matching odds")
        else:
            print("\n✗ ZERO games have matching odds - Merge will fail!")

if __name__ == "__main__":
    debug_mapping()
