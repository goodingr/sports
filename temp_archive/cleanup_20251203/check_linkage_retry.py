import sqlite3
import pandas as pd

def check_linkage_retry():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking linkage for EPL games with scores but missing odds:")
    query = """
    SELECT 
        g.game_id, 
        g.odds_api_id, 
        g.start_time_utc
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    WHERE s.league = 'EPL'
    AND gr.home_score IS NOT NULL
    AND gr.home_moneyline_close IS NULL
    LIMIT 10
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
    
    conn.close()

if __name__ == "__main__":
    check_linkage_retry()
