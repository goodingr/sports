import pandas as pd
from src.dashboard.data import load_forward_test_data, get_completed_bets, get_overunder_completed

def verify():
    print("Loading data...")
    df = load_forward_test_data(force_refresh=True)
    print(f"Loaded {len(df)} predictions.")
    
    # Check for the specific game
    target_game_id = "NBA_0022400744" # Wizards vs Hawks, Nov 26
    
    # Verify get_completed_bets
    print("\nChecking get_completed_bets...")
    completed = get_completed_bets(df, edge_threshold=0.0) # Set threshold to 0 to ensure we see it
    
    # Check if target game is in completed
    if "game_id" in completed.columns:
        target = completed[completed["game_id"] == target_game_id]
        if not target.empty:
            print(f"SUCCESS: Found target game in completed bets!")
            print(target[["game_id", "home_team_name", "away_team_name", "commence_time", "won", "result"]].iloc[0])
        else:
            print(f"FAILURE: Target game {target_game_id} NOT found in completed bets.")
            # Debug: check if it exists in df
            in_df = df[df["game_id"] == target_game_id]
            if not in_df.empty:
                print("Game exists in raw data:")
                print(in_df[["game_id", "commence_time", "result"]].iloc[0])
                print(f"Commence time: {in_df['commence_time'].iloc[0]}")
                print(f"Now (UTC): {pd.Timestamp.now(tz='UTC')}")
            else:
                print("Game NOT found in raw data.")

    # Verify get_overunder_completed
    print("\nChecking get_overunder_completed...")
    ou_completed = get_overunder_completed(df, edge_threshold=0.0)
    
    if "game_id" in ou_completed.columns:
        target_ou = ou_completed[ou_completed["game_id"] == target_game_id]
        if not target_ou.empty:
            print(f"SUCCESS: Found target game in over/under completed!")
            print(target_ou[["game_id", "home_team", "away_team", "commence_time", "won"]].iloc[0])
        else:
            print(f"FAILURE: Target game {target_game_id} NOT found in over/under completed.")

if __name__ == "__main__":
    verify()
