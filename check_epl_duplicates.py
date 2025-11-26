import sqlite3
import pandas as pd

def check_duplicates():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking for duplicate EPL games (same home/away, similar time):")
    
    query = """
    SELECT 
        g1.game_id as id1, g2.game_id as id2,
        g1.start_time_utc as time1, g2.start_time_utc as time2,
        ht.name as home, at.name as away,
        gr1.home_score as score1, gr2.home_score as score2,
        gr1.home_moneyline_close as odds1, gr2.home_moneyline_close as odds2
    FROM games g1
    JOIN games g2 ON g1.home_team_id = g2.home_team_id 
        AND g1.away_team_id = g2.away_team_id
        AND g1.game_id < g2.game_id
    JOIN sports s ON g1.sport_id = s.sport_id
    JOIN teams ht ON g1.home_team_id = ht.team_id
    JOIN teams at ON g1.away_team_id = at.team_id
    LEFT JOIN game_results gr1 ON g1.game_id = gr1.game_id
    LEFT JOIN game_results gr2 ON g2.game_id = gr2.game_id
    WHERE s.league = 'EPL'
    AND g1.start_time_utc > '2025-10-01'
    AND abs(julianday(g1.start_time_utc) - julianday(g2.start_time_utc)) < 1
    ORDER BY g1.start_time_utc DESC
    LIMIT 20
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    check_duplicates()
