import requests
import pandas as pd
from datetime import datetime, timezone

def check_api():
    try:
        response = requests.get("http://localhost:8000/api/bets/upcoming")
        if response.status_code == 200:
            data = response.json()
            print(f"API returned {data.get('count', 0)} upcoming bets")
            for bet in data.get("data", [])[:5]:
                print(f"  - {bet.get('league')} {bet.get('home_team')} vs {bet.get('away_team')} (Edge: {bet.get('edge')})")
        else:
            print(f"API failed with status {response.status_code}")
    except Exception as e:
        print(f"API request failed: {e}")

def check_parquet():
    try:
        df = pd.read_parquet("data/forward_test/ensemble/predictions_master.parquet")
        print(f"\nTotal predictions in parquet: {len(df)}")
        
        # Filter for NBA
        nba = df[df["league"] == "NBA"]
        print(f"Total NBA predictions: {len(nba)}")
        
        # Filter for upcoming
        now = pd.Timestamp.now(tz="UTC")
        if "commence_time" in nba.columns:
            # Ensure commence_time is tz-aware
            nba["commence_time"] = pd.to_datetime(nba["commence_time"], utc=True)
            upcoming = nba[nba["commence_time"] > now]
            print(f"Upcoming NBA predictions: {len(upcoming)}")
            
            if not upcoming.empty:
                print("\nSample upcoming NBA predictions:")
                print(upcoming[["commence_time", "home_team", "away_team", "home_edge", "away_edge", "over_edge", "under_edge"]].head())
                
                # Check edge filter
                high_edge = upcoming[
                    (upcoming["home_edge"] >= 0.06) | 
                    (upcoming["away_edge"] >= 0.06) | 
                    (upcoming["over_edge"] >= 0.06) | 
                    (upcoming["under_edge"] >= 0.06)
                ]
                print(f"\nUpcoming NBA predictions with edge >= 0.06: {len(high_edge)}")
    except Exception as e:
        print(f"Parquet check failed: {e}")

if __name__ == "__main__":
    print("Checking API...")
    check_api()
    print("\nChecking Parquet...")
    check_parquet()
