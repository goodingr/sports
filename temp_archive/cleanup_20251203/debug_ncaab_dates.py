import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def debug_ncaab_dates():
    conn = sqlite3.connect('data/betting.db')
    
    cutoff = (datetime.utcnow() - timedelta(days=60)).isoformat()
    print(f"Cutoff: {cutoff}")
    
    query = f"""
    SELECT 
        g.start_time_utc,
        s.league,
        ht.name as home,
        at.name as away,
        gr.home_score
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE s.league = 'NCAAB'
    AND g.start_time_utc > '{cutoff}'
    ORDER BY g.start_time_utc DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(f"Found {len(df)} games after cutoff")
        print(df.head(10).to_string(index=False))
        print("\nTail:")
        print(df.tail(10).to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
        
    conn.close()

if __name__ == "__main__":
    debug_ncaab_dates()
