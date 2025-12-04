import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def check_db_results():
    conn = sqlite3.connect('data/betting.db')
    
    # Check distinct sports in game_results
    print("Sports in game_results table:")
    query = """
    SELECT sport_key, COUNT(*) as count 
    FROM game_results 
    GROUP BY sport_key
    """
    try:
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error querying game_results: {e}")
        
    # Check recent completed games (last 60 days)
    print("\nRecent completed games (last 60 days) in DB:")
    cutoff = (datetime.utcnow() - timedelta(days=60)).isoformat()
    query_recent = f"""
    SELECT sport_key, COUNT(*) as count
    FROM game_results
    WHERE commence_time > '{cutoff}'
    GROUP BY sport_key
    """
    try:
        df_recent = pd.read_sql_query(query_recent, conn)
        print(df_recent.to_string(index=False))
    except Exception as e:
        print(f"Error querying recent games: {e}")

    conn.close()

if __name__ == "__main__":
    check_db_results()
