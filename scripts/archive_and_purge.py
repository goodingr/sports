import pandas as pd
import sqlite3
import shutil
import os
import time
from src.db.core import connect, DB_PATH

HISTORICAL_DB_PATH = os.path.join(os.path.dirname(DB_PATH), "betting_historical.db")
CUTOFF_DATE = "2025-11-03" # Start of active predictions

def archive_and_purge():
    print(f"Active DB: {DB_PATH}")
    print(f"Archive DB: {HISTORICAL_DB_PATH}")
    
    # 1. Archive
    if os.path.exists(HISTORICAL_DB_PATH):
        print(f"Archive {HISTORICAL_DB_PATH} already exists. Skipping backup to preserve original data.")
    else:
        print("Creating backup...")
        try:
            shutil.copy2(DB_PATH, HISTORICAL_DB_PATH)
            print("Backup created successfully.")
        except Exception as e:
            print(f"Backup failed: {e}")
            return

    # 2. Purge
    print("Starting purge of historical/orphan data from ACTIVE database...")
    
    # Logic: 
    # 1. ORPHAN (No ID + No Results)
    # 2. HISTORICAL (Before Cutoff)
    # 3. NULL TIME (Invalid data)
    # 4. OLD SEASONS (Before 2025)
    target_games_subquery = f"""
        SELECT g.game_id
        FROM games g
        LEFT JOIN game_results gr ON g.game_id = gr.game_id
        WHERE 
            (
                (g.odds_api_id IS NULL OR g.odds_api_id = '')
                AND
                (
                    gr.game_id IS NULL OR 
                    (gr.home_moneyline_close IS NULL AND gr.away_moneyline_close IS NULL)
                )
            )
            OR (g.start_time_utc < '{CUTOFF_DATE}')
            OR (g.start_time_utc IS NULL)
            OR (g.season < 2025)
    """

    tables_to_clean = [
        "game_results", "team_features", "model_input", "model_predictions",
        "predictions", "recommendations", "player_stats", "odds"
    ]

    with connect() as conn:
        cursor = conn.cursor()
        
        # Count deletion candidates
        cursor.execute(f"SELECT COUNT(*) FROM ({target_games_subquery})")
        count = cursor.fetchone()[0]
        print(f"Games targeted for deletion: {count}")
        
        if count == 0:
            print("No games matched criteria.")
            return

        print("Deleting dependent records...")
        for table in tables_to_clean:
            try:
                # Check table existence
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
                if not cursor.fetchone(): continue

                delete_query = f"DELETE FROM {table} WHERE game_id IN ({target_games_subquery})"
                cursor.execute(delete_query)
                print(f"  - {table}: {cursor.rowcount} rows deleted")
                conn.commit()
            except Exception as e:
                print(f"  Error cleaning {table}: {e}")

        print("Deleting games...")
        cursor.execute(f"DELETE FROM games WHERE game_id IN ({target_games_subquery})")
        deleted_games = cursor.rowcount
        print(f"Total games deleted: {deleted_games}")
        conn.commit()
        
        # 3. Vacuum
        print("Vacuuming database to reclaim space...")
        try:
            conn.execute("VACUUM")
            print("Vacuum complete.")
        except Exception as e:
            print(f"Vacuum failed (might be locked): {e}")

    print("Purge complete.")

if __name__ == "__main__":
    archive_and_purge()
