import sqlite3
import pandas as pd

def list_nov_games():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking for duplicates by team names:")
    query = """
    SELECT 
        ht.name as home,
        at.name as away,
        COUNT(*) as count,
        GROUP_CONCAT(g.game_id) as ids,
        GROUP_CONCAT(g.start_time_utc) as times,
        GROUP_CONCAT(g.odds_api_id) as odds_ids,
        GROUP_CONCAT(gr.home_score) as scores
    FROM games g
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    LEFT JOIN game_results gr ON g.game_id = gr.game_id
    WHERE s.league = 'EPL'
    AND g.start_time_utc LIKE '2025-11%'
    GROUP BY ht.name, at.name
    HAVING count > 1
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    list_nov_games()
