import pandas as pd
from pathlib import Path

def check_parquet():
    path = Path("data/forward_test/ensemble/predictions_master.parquet")
    if not path.exists():
        print(f"File not found: {path}")
        return

    df = pd.read_parquet(path)
    print(f"Total rows: {len(df)}")
    
    # Filter for Memphis
    memphis_games = df[
        (df["home_team"].str.contains("Memphis", case=False, na=False)) | 
        (df["away_team"].str.contains("Memphis", case=False, na=False)) |
        (df["home_team"] == "MEM") |
        (df["away_team"] == "MEM")
    ]
    
    if memphis_games.empty:
        print("No Memphis games found in parquet.")
    else:
        print(f"Found {len(memphis_games)} Memphis games:")
        for _, row in memphis_games.iterrows():
            print(f"  Game ID: {row.get('game_id')}")
            print(f"  Date: {row.get('commence_time')}")
            print(f"  Home: {row.get('home_team')} (ML: {row.get('home_moneyline')})")
            print(f"  Away: {row.get('away_team')} (ML: {row.get('away_moneyline')})")
            print(f"  Predicted At: {row.get('predicted_at')}")
            print("-" * 30)

if __name__ == "__main__":
    check_parquet()
