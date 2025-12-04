import sqlite3
import pandas as pd
from src.db.core import connect
from src.models.forward_test import load_games_from_database
import sys

def check_ncaab_data():
    with open("debug_ncaab_output.txt", "w") as f:
        sys.stdout = f
        print("Checking NCAAB data in database...")
        with connect() as conn:
            # Check snapshots
            snapshots = pd.read_sql("""
                SELECT snapshot_id, fetched_at_utc FROM odds_snapshots 
                WHERE sport_id = (SELECT sport_id FROM sports WHERE league = 'NCAAB')
                ORDER BY fetched_at_utc DESC LIMIT 5
            """, conn)
            print("\nRecent NCAAB Snapshots:")
            print(snapshots)
            
            # Check upcoming games in DB
            print("\nChecking upcoming NCAAB games in DB (raw query):")
            games = pd.read_sql("""
                SELECT game_id, start_time_utc FROM games 
                WHERE sport_id = (SELECT sport_id FROM sports WHERE league = 'NCAAB')
                AND start_time_utc > datetime('now', '-1 day')
                ORDER BY start_time_utc LIMIT 10
            """, conn)
            print(games)

        print("\nTesting load_games_from_database('NCAAB'):")
        try:
            games = load_games_from_database("NCAAB")
            print(f"Found {len(games)} games via load_games_from_database")
            if games:
                print("First game sample:", games[0])
        except Exception as e:
            print(f"Error loading games: {e}")

if __name__ == "__main__":
    check_ncaab_data()
