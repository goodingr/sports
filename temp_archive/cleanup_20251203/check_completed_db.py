import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def check_completed_by_sport():
    conn = sqlite3.connect('data/betting.db')
    
    # Count completed games by sport (joining games and game_results)
    print("\nCompleted games in DB (last 60 days):")
    cutoff = (datetime.utcnow() - timedelta(days=60)).isoformat()
    
    query = f"""
    SELECT 
        s.name as sport,
        s.league as league,
        COUNT(*) as count
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    WHERE g.start_time_utc > '{cutoff}'
    GROUP BY s.name, s.league
    ORDER BY count DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error querying completed games: {e}")
        
    conn.close()

if __name__ == "__main__":
    check_completed_by_sport()
