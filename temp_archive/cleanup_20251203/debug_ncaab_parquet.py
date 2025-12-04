import pandas as pd
from datetime import datetime, timezone

def check_ncaab_parquet():
    path = "data/forward_test/ensemble/predictions_master.parquet"
    df = pd.read_parquet(path)
    
    # Filter for NCAAB
    ncaab = df[df["league"] == "NCAAB"].copy()
    
    print(f"Total NCAAB games: {len(ncaab)}")
    
    # Filter upcoming
    now = datetime.now(timezone.utc)
    ncaab["commence_time"] = pd.to_datetime(ncaab["commence_time"], utc=True)
    upcoming = ncaab[ncaab["commence_time"] > now]
    
    print(f"Upcoming NCAAB games: {len(upcoming)}")
    
    # Check for over/under data
    print(f"\nGames with total_line: {upcoming['total_line'].notna().sum()}")
    print(f"Games with over_prob: {upcoming['over_prob'].notna().sum()}")
    print(f"Games with under_prob: {upcoming['under_prob'].notna().sum()}")
    
    # Show sample
    print("\nSample upcoming NCAAB games:")
    for _, row in upcoming.head(5).iterrows():
        print(f"{row['away_team']} @ {row['home_team']}")
        print(f"  Time: {row['commence_time']}")
        print(f"  Total Line: {row['total_line']}")
        print(f"  Over Prob: {row.get('over_prob')}, Under Prob: {row.get('under_prob')}")
        print()

if __name__ == "__main__":
    check_ncaab_parquet()
