import pandas as pd
import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone

def check_counts():
    leagues = ["NBA", "NHL"]
    db_path = "data/betting.db"
    parquet_path = "data/forward_test/ensemble/predictions_master.parquet"
    
    print("--- Game Counts Investigation ---")
    
    # 1. Check Raw Odds (Latest Snapshot)
    print("\n1. Raw Odds (Latest Snapshot):")
    for league in leagues:
        # Map league to sport key folder
        sport_key = "basketball_nba" if league == "NBA" else "icehockey_nhl"
        raw_dir = Path(f"data/raw/odds/{sport_key}")
        if not raw_dir.exists():
            print(f"  {league}: Raw directory not found ({raw_dir})")
            continue
            
        # Get latest file
        files = sorted(raw_dir.glob("odds_*.json"))
        if not files:
            print(f"  {league}: No raw files found")
            continue
            
        latest_file = files[-1]
        try:
            with open(latest_file, "r") as f:
                data = json.load(f)
                # Handle both list and dict (some snapshots might be wrapped)
                results = data if isinstance(data, list) else data.get("results", [])
                print(f"  {league}: {len(results)} games in {latest_file.name}")
        except Exception as e:
            print(f"  {league}: Error reading {latest_file.name}: {e}")

    # 2. Check Database
    print("\n2. Database (Upcoming Games):")
    with sqlite3.connect(db_path) as conn:
        for league in leagues:
            # Count games in DB that are upcoming (status 'scheduled')
            # We need to join with sports table to filter by league
            cursor = conn.execute("""
                SELECT count(*) 
                FROM games g
                JOIN sports s ON g.sport_id = s.sport_id
                WHERE s.league = ? AND g.status = 'scheduled'
            """, (league,))
            count = cursor.fetchone()[0]
            print(f"  {league}: {count} scheduled games")

    # 3. Check Predictions
    print("\n3. Predictions (Master Parquet):")
    if Path(parquet_path).exists():
        df = pd.read_parquet(parquet_path)
        for league in leagues:
            league_df = df[df["league"] == league]
            print(f"  {league}: {len(league_df)} games")
    else:
        print(f"  Parquet file not found: {parquet_path}")

if __name__ == "__main__":
    check_counts()
