import time
from src.db.core import connect

def cleanup_orphans():
    print("Starting cleanup of orphan odds records...")
    
    delete_sql = """
        DELETE FROM odds 
        WHERE game_id NOT IN (SELECT game_id FROM games)
    """
    
    with connect() as conn:
        start = time.time()
        cursor = conn.execute(delete_sql)
        deleted_count = cursor.rowcount
        conn.commit()
        duration = time.time() - start
        
        print(f"Cleanup complete.")
        print(f"Deleted {deleted_count:,} orphan rows from 'odds' table.")
        print(f"Time taken: {duration:.2f} seconds")
        
        # Verify
        remaining = conn.execute("""
            SELECT count(*) 
            FROM odds o 
            LEFT JOIN games g ON o.game_id = g.game_id 
            WHERE g.game_id IS NULL
        """).fetchone()[0]
        print(f"Remaining orphans: {remaining}")

if __name__ == "__main__":
    cleanup_orphans()
