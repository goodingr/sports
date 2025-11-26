import sqlite3
import pandas as pd

def check_nfl_dates():
    conn = sqlite3.connect('data/betting.db')
    
    print("Recent completed NFL games:")
    query = """
    SELECT 
        g.start_time_utc,
        ht.name as home,
        at.name as away,
        gr.home_score,
        gr.away_score,
        gr.home_moneyline_close
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE s.league = 'NFL'
    AND gr.home_score IS NOT NULL
    ORDER BY g.start_time_utc DESC
    LIMIT 10
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error querying NFL games: {e}")
        
    conn.close()

if __name__ == "__main__":
    check_nfl_dates()
