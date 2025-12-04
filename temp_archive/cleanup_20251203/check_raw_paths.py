import sqlite3
import pandas as pd

def check_raw_paths():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking raw paths for recent snapshots:")
    query = """
    SELECT snapshot_id, fetched_at_utc, raw_path
    FROM odds_snapshots
    ORDER BY fetched_at_utc DESC
    LIMIT 5
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    check_raw_paths()
