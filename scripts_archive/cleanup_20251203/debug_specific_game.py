import pandas as pd

def check_specific_game():
    df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')
    
    # Search for Ole Miss vs Mississippi State
    # User text: "Nov 28 12pm CFB Ole Miss Mississippi State Bulldogs"
    
    mask = (df['home_team'].str.contains('Ole Miss', case=False) | 
            df['away_team'].str.contains('Ole Miss', case=False)) & \
           (df['home_team'].str.contains('Mississippi', case=False) | 
            df['away_team'].str.contains('Mississippi', case=False))
            
    game = df[mask]
    
    if not game.empty:
        print("Found game:")
        print(game[['game_id', 'commence_time', 'league', 'home_team', 'away_team', 'version']].to_string())
    else:
        print("Game not found.")

    # Also check for NaNs in version
    print(f"\nTotal rows: {len(df)}")
    print(f"Rows with null version: {df['version'].isnull().sum()}")
    print(f"Rows with empty string version: {(df['version'] == '').sum()}")
    
    # Check distinct versions
    print("\nDistinct versions:")
    print(df['version'].value_counts(dropna=False))

if __name__ == "__main__":
    check_specific_game()
