import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def check_missing_odds():
    conn = sqlite3.connect('data/betting.db')
    
    cutoff = (datetime.utcnow() - timedelta(days=60)).isoformat()
    
    print("Completed games (last 60 days) with MISSING closing lines:")
    query = f"""
    SELECT 
        s.name as sport,
        s.league as league,
        COUNT(*) as count
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    WHERE g.start_time_utc > '{cutoff}'
    AND gr.home_score IS NOT NULL
    AND (gr.home_moneyline_close IS NULL OR gr.away_moneyline_close IS NULL)
    GROUP BY s.name, s.league
    ORDER BY count DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error querying missing odds: {e}")
        
    conn.close()

if __name__ == "__main__":
    check_missing_odds()
