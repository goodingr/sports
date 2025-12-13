import time
from src.db.core import connect

def cleanup_orphan_predictions():
    print("Starting cleanup of orphan PREDICTION records...")
    
    tables = ["model_predictions", "predictions"]
    
    with connect() as conn:
        for table in tables:
            print(f"\nCleaning {table}...")
            start = time.time()
            
            # Using subquery deletion which is standard for SQLite
            delete_sql = f"""
                DELETE FROM {table} 
                WHERE game_id NOT IN (SELECT game_id FROM games)
            """
            
            cursor = conn.execute(delete_sql)
            deleted_count = cursor.rowcount
            conn.commit()
            duration = time.time() - start
            
            print(f"  Deleted {deleted_count:,} orphan rows.")
            print(f"  Time taken: {duration:.2f} seconds")
            
            # Verify
            remaining_query = f"""
                SELECT count(*) 
                FROM {table} t 
                LEFT JOIN games g ON t.game_id = g.game_id 
                WHERE g.game_id IS NULL
            """
            remaining = conn.execute(remaining_query).fetchone()[0]
            print(f"  Remaining orphans: {remaining}")

if __name__ == "__main__":
    cleanup_orphan_predictions()
