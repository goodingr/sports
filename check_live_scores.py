import pandas as pd
from pathlib import Path

def check_live_scores():
    path = Path("data/forward_test/ensemble/predictions_master.parquet")
    if not path.exists():
        print(f"File not found: {path}")
        return

    df = pd.read_parquet(path)
    
    # Filter for NCAAB games that have scores but might not have a result (or have result=None)
    # We look for games where home_score is not null
    ncaab_live = df[
        (df["league"] == "NCAAB") & 
        (df["home_score"].notna()) & 
        (df["result"].isna())
    ]
    
    print(f"Total NCAAB games in parquet: {len(df[df['league'] == 'NCAAB'])}")
    print(f"NCAAB games with scores but no result (Live/Ongoing): {len(ncaab_live)}")
    
    if not ncaab_live.empty:
        print("\nSample Live Games:")
        print(ncaab_live[["game_id", "home_team", "away_team", "home_score", "away_score", "commence_time"]].head())
    else:
        print("\nNo live NCAAB games found in parquet file.")

if __name__ == "__main__":
    check_live_scores()
