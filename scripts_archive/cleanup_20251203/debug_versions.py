import pandas as pd
from datetime import datetime, timezone

def check_versions():
    df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')
    
    now = pd.Timestamp.now(tz="UTC")
    
    # Filter for future games
    future = df[df['commence_time'] > now].copy()
    
    print(f"Total future games: {len(future)}")
    
    # Check value counts of version
    print("\nVersion counts for future games:")
    print(future['version'].value_counts())
    
    # Show some examples of v0.1 future games
    v01_future = future[future['version'] == 'v0.1']
    if not v01_future.empty:
        print("\nExamples of future games labeled as v0.1:")
        print(v01_future[['commence_time', 'league', 'home_team', 'away_team', 'version']].head(10).to_string())
    else:
        print("\nNo future games found with v0.1 label.")

if __name__ == "__main__":
    check_versions()
