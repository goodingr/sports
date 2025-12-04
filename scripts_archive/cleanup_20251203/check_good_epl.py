import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def check_good_epl_dates():
    conn = sqlite3.connect('data/betting.db')
    
    print("Dates of EPL games with scores AND odds (last 90 days):")
    cutoff = (datetime.utcnow() - timedelta(days=90)).isoformat()
    
    query = f"""
    SELECT 
        g.start_time_utc,
        ht.name as home,
        at.name as away,
        gr.home_score,
        gr.home_moneyline_close
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE s.league = 'EPL'
    AND g.start_time_utc > '{cutoff}'
    AND gr.home_score IS NOT NULL
    AND gr.home_moneyline_close IS NOT NULL
    ORDER BY g.start_time_utc DESC
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    check_good_epl_dates()
