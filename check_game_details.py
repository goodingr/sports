import sqlite3
import pandas as pd

def check_game_details():
    conn = sqlite3.connect('data/betting.db')
    
    print("Details for EPL_740699:")
    query = """
    SELECT 
        g.game_id, 
        g.start_time_utc,
        ht.name as home,
        at.name as away,
        gr.home_score,
        g.odds_api_id
    FROM games g
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    LEFT JOIN game_results gr ON g.game_id = gr.game_id
    WHERE g.game_id = 'EPL_740699'
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    check_game_details()
