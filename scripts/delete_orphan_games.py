import pandas as pd
import sqlite3
import time
from src.db.core import connect, DB_PATH

def delete_orphans():
    print(f"Connecting to database at {DB_PATH}...")
    
    # Define the core logic for identifying orphan games
    # Logic: (No odds_api_id) AND (No valid result/closing lines)
    target_games_subquery = """
        SELECT g.game_id
        FROM games g
        LEFT JOIN game_results gr ON g.game_id = gr.game_id
        WHERE 
            (g.odds_api_id IS NULL OR g.odds_api_id = '')
            AND
            (
                gr.game_id IS NULL
                OR
                (
                    gr.home_moneyline_close IS NULL AND
                    gr.away_moneyline_close IS NULL AND
                    gr.spread_close IS NULL AND
                    gr.total_close IS NULL
                )
            )
    """

    tables_to_clean = [
        "game_results",
        "team_features",
        "model_input",
        "model_predictions",
        "predictions",
        "recommendations",
        "player_stats",
        "odds"
    ]

    with connect() as conn:
        cursor = conn.cursor()
        
        # 1. Count target games
        cursor.execute(f"SELECT COUNT(*) FROM ({target_games_subquery})")
        count = cursor.fetchone()[0]
        print(f"Found {count} games matching deletion criteria.")
        
        if count == 0:
            print("No games to delete.")
            return

        print("Proceeding with deletion...")
        start_time = time.time()

        # 2. Delete from dependent tables first
        for table in tables_to_clean:
            print(f"cleaning {table}...")
            try:
                # Check if table exists
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
                if not cursor.fetchone():
                    print(f"  Skipping {table} (not found)")
                    continue
                
                delete_query = f"DELETE FROM {table} WHERE game_id IN ({target_games_subquery})"
                cursor.execute(delete_query)
                print(f"  Deleted {cursor.rowcount} rows from {table}")
                conn.commit()
            except Exception as e:
                print(f"  Error deleting from {table}: {e}")

        # 3. Delete from games table
        print("Deleting from games table...")
        delete_games_query = f"DELETE FROM games WHERE game_id IN ({target_games_subquery})"
        cursor.execute(delete_games_query)
        deleted_games = cursor.rowcount
        print(f"Deleted {deleted_games} rows from games table.")
        conn.commit()
        
        elapsed = time.time() - start_time
        print(f"Done in {elapsed:.2f} seconds.")
        
        # Verify
        cursor.execute(f"SELECT COUNT(*) FROM ({target_games_subquery})")
        remaining = cursor.fetchone()[0]
        print(f"Remaining matching games: {remaining}")

if __name__ == "__main__":
    delete_orphans()
