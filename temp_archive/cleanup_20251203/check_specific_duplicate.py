import sqlite3
import pandas as pd

def check_specific_duplicate():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking for Chelsea vs Wolves games:")
    query = """
    SELECT 
        g.game_id, 
        g.start_time_utc,
        ht.name as home,
        at.name as away,
        gr.home_score,
        g.odds_api_id,
        gr.home_moneyline_close
    FROM games g
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    LEFT JOIN game_results gr ON g.game_id = gr.game_id
    WHERE s.league = 'EPL'
    AND ht.name LIKE '%Chelsea%'
    AND at.name LIKE '%Wolverhampton%'
    ORDER BY g.start_time_utc DESC
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    check_specific_duplicate()
