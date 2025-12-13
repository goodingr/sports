import pandas as pd
from src.db.core import connect

def db_stats():
    print(f"Database Row Counts:")
    print("-" * 35)
    
    with connect() as conn:
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        
        results = []
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                results.append((table, count))
            except Exception as e:
                results.append((table, f"Error: {e}"))
        
        # Sort by count desc
        results.sort(key=lambda x: x[1] if isinstance(x[1], int) else -1, reverse=True)
        
        for table, count in results:
            if isinstance(count, int):
                print(f"{table:<20}: {count:,.0f}")
            else:
                print(f"{table:<20}: {count}")

    print("-" * 35)

if __name__ == "__main__":
    db_stats()
