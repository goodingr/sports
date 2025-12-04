import pandas as pd
from datetime import datetime, timezone

def check_upcoming():
    path = "data/forward_test/ensemble/predictions_master.parquet"
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"Error reading parquet: {e}")
        return

    now = datetime.now(timezone.utc)
    print(f"Current UTC time: {now}")

    print(f"Columns: {df.columns.tolist()}")
    
    date_col = "commence_time"
    if date_col not in df.columns:
        print("Could not find date column")
        return

    # Ensure date column is datetime
    df["date"] = pd.to_datetime(df[date_col], utc=True)
    
    leagues = ["NBA", "NHL"]
    for league in leagues:
        print(f"\n--- {league} ---")
        league_df = df[df["league"] == league]
        
        if league_df.empty:
            print("No games found.")
            continue
            
        # Filter upcoming
        upcoming = league_df[league_df["date"] > now]
        print(f"Total games: {len(league_df)}")
        print(f"Upcoming games: {len(upcoming)}")
        
        if not upcoming.empty:
            print("Sample upcoming games:")
            for _, row in upcoming.head(5).iterrows():
                print(f"  {row['date']} - {row['away_team']} @ {row['home_team']}")
                print(f"    Moneyline: {row.get('home_moneyline')} / {row.get('away_moneyline')}")
                print(f"    Total Line: {row.get('total_line')}")
                print(f"    Over Prob: {row.get('over_prob')}, Under Prob: {row.get('under_prob')}")

if __name__ == "__main__":
    check_upcoming()
