import sqlite3
import pandas as pd

def check_restorable_dates():
    conn = sqlite3.connect('data/betting.db')
    
    print("Dates of restorable EPL games (scores + odds):")
    query = """
    SELECT 
        g.start_time_utc,
        ht.name as home,
        at.name as away
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE s.league = 'EPL'
    AND gr.home_score IS NOT NULL
    AND gr.home_moneyline_close IS NOT NULL
    ORDER BY g.start_time_utc DESC
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    print(f"\nTotal restorable: {len(df)}")
    print(f"Post-Nov 3 restorable: {len(df[df['start_time_utc'] >= '2025-11-03'])}")
    
    conn.close()

if __name__ == "__main__":
    check_restorable_dates()
