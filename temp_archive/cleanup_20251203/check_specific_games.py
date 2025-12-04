import pandas as pd
from src.db.core import connect

def check_specific_games():
    games_to_check = [
        "EPL_4545", "BUNDESLIGA_746834", "CFB_401761666", "CFB_401752788", "NBA_0022400744"
    ]
    
    print("Checking for specific games in predictions...")
    try:
        # Check parquet first as it's the source for the API
        df = pd.read_parquet("data/forward_test/ensemble/predictions_master.parquet")
        
        found_count = 0
        for game_id in games_to_check:
            # The game_id in parquet might be the full ID or just the numeric part depending on how it's stored
            # Let's try to match by substring if exact match fails
            match = df[df["game_id"].astype(str).str.contains(game_id.split("_")[-1])]
            
            if not match.empty:
                print(f"\nFound {game_id}:")
                print(match[["game_id", "home_team", "away_team", "commence_time", "home_edge", "away_edge", "over_edge", "under_edge"]].to_string())
                found_count += 1
            else:
                print(f"\nMissed {game_id}")
                
        print(f"\nFound {found_count}/{len(games_to_check)} games.")
        
    except Exception as e:
        print(f"Error checking parquet: {e}")

if __name__ == "__main__":
    check_specific_games()
