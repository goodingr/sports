import pandas as pd
from src.db.core import connect

def inspect_dates():
    with connect() as conn:
        print("Checking Minimum Dates in All Tables:")
        print("-" * 40)
        
        # 1. Games
        try:
            val = conn.execute("SELECT MIN(start_time_utc) FROM games").fetchone()[0]
            print(f"Games (start): {val}")
            
            bad = conn.execute("SELECT COUNT(*) FROM games WHERE start_time_utc < '2025-11-01'").fetchone()[0]
            print(f"  < Nov 1: {bad}")
        except Exception as e: print(f"Games error: {e}")

        # 2. Predictions
        try:
            val = conn.execute("SELECT MIN(predicted_at) FROM predictions").fetchone()[0]
            print(f"Predictions (date): {val}")
            
            bad = conn.execute("SELECT COUNT(*) FROM predictions WHERE predicted_at < '2025-11-01'").fetchone()[0]
            print(f"  < Nov 1: {bad}")
        except Exception as e: print(f"Predictions error: {e}")

        # 3. Odds (via Snapshots)
        try:
            query = """
                SELECT MIN(s.fetched_at_utc) 
                FROM odds o 
                JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
            """
            val = conn.execute(query).fetchone()[0]
            print(f"Odds (fetched): {val}")
            
            count_query = """
                SELECT COUNT(*) 
                FROM odds o 
                JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
                WHERE s.fetched_at_utc < '2025-11-01'
            """
            bad = conn.execute(count_query).fetchone()[0]
            print(f"  < Nov 1: {bad}")
        except Exception as e: print(f"Odds error: {e}")
        
        # 4. Game Results
        try:
            # game_results doesn't have a reliable timestamp column other than tr_retrieved_at which might be null
            # or source_version. But it's 1:1 with games. If games are clean, results are likely clean.
            # We'll check tr_retrieved_at just in case.
            val = conn.execute("SELECT MIN(tr_retrieved_at) FROM game_results WHERE tr_retrieved_at IS NOT NULL").fetchone()[0]
            print(f"Results (retrieved): {val}")
        except Exception as e: print(f"Results error: {e}")

if __name__ == "__main__":
    inspect_dates()
