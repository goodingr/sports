import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def check_recent_ncaab_scores():
    conn = sqlite3.connect('data/betting.db')
    
    cutoff = (datetime.utcnow() - timedelta(days=90)).isoformat()
    print(f"Checking NCAAB games with scores since {cutoff}:")
    
    query = f"""
    SELECT COUNT(*) as count
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    WHERE s.league = 'NCAAB'
    AND gr.home_score IS NOT NULL
    AND g.start_time_utc > '{cutoff}'
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(f"Recent NCAAB games with scores: {df.iloc[0]['count']}")
    except Exception as e:
        print(f"Error: {e}")
        
    conn.close()

if __name__ == "__main__":
    check_recent_ncaab_scores()
