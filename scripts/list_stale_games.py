import sqlite3
import pandas as pd

def list_stale_games():
    conn = sqlite3.connect('data/betting.db')
    
    print("=== Stale Ongoing Games (Past Start Time, Not Final) ===")
    query = """
    SELECT 
        g.game_id, 
        s.league,
        ht.name as home_team,
        at.name as away_team,
        g.start_time_utc,
        g.status
    FROM games g
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE g.start_time_utc < datetime('now')
    AND g.status != 'final'
    ORDER BY g.start_time_utc ASC
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("No stale ongoing games found.")
    else:
        print(f"Found {len(df)} stale games.")
        print("\nBreakdown by League:")
        print(df['league'].value_counts())
        print("\nSample Stale Games:")
        print(df.head(20).to_string())
        
        # Save to csv for inspection if needed
        # df.to_csv('stale_games.csv', index=False)

    conn.close()

if __name__ == "__main__":
    list_stale_games()
