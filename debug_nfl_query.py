import sqlite3
import pandas as pd

def debug_nfl_query():
    conn = sqlite3.connect('data/betting.db')
    
    lookback_days = 30
    print(f"Querying NFL games for last {lookback_days} days...")
    
    query = """
    SELECT 
        gr.game_id,
        s.league,
        g.start_time_utc as commence_time,
        ht.name as home_team,
        at.name as away_team,
        gr.home_score,
        gr.away_score,
        gr.home_moneyline_close,
        gr.away_moneyline_close
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE s.league = 'NFL'
    AND gr.home_score IS NOT NULL
    AND gr.away_score IS NOT NULL
    AND gr.home_moneyline_close IS NOT NULL
    AND gr.away_moneyline_close IS NOT NULL
    AND g.start_time_utc >= datetime('now', ?)
    ORDER BY g.start_time_utc DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=(f'-{lookback_days} days',))
        print(f"Found {len(df)} rows")
        if not df.empty:
            print(df.head().to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
        
    conn.close()

if __name__ == "__main__":
    debug_nfl_query()
