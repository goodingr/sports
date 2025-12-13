import pandas as pd
from src.db.core import connect

def check_orphans():
    print("Checking for orphan prediction records...")
    
    tables_to_check = ["predictions", "model_predictions"]
    
    with connect() as conn:
        for table in tables_to_check:
            print(f"\nChecking table: {table}")
            try:
                # Count total
                total = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                
                # Count orphans
                query = f"""
                    SELECT count(*) 
                    FROM {table} t 
                    LEFT JOIN games g ON t.game_id = g.game_id 
                    WHERE g.game_id IS NULL
                """
                orphans = conn.execute(query).fetchone()[0]
                
                print(f"  Total Rows: {total:,.0f}")
                print(f"  Orphan Rows: {orphans:,.0f}")
                
                if orphans > 0:
                    pct = (orphans / total) * 100 if total > 0 else 0
                    print(f"  ({pct:.1f}% of data is orphaned)")
            except Exception as e:
                print(f"  Error checking {table}: {e}")

if __name__ == "__main__":
    check_orphans()
