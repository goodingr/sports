import sqlite3
import pandas as pd
from datetime import datetime

def check_data():
    conn = sqlite3.connect('data/betting.db')
    
    # 1. Detailed Missing Scores
    print("=== Missing Scores Analysis ===")
    query = """
    SELECT 
        s.league,
        ht.name as home_team, 
        at.name as away_team,
        g.start_time_utc
    FROM games g
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    LEFT JOIN game_results gr ON g.game_id = gr.game_id
    WHERE g.start_time_utc BETWEEN '2026-01-02' AND '2026-01-10'
    AND g.start_time_utc < datetime('now')
    AND gr.home_score IS NULL
    ORDER BY s.league, g.start_time_utc
    """
    df = pd.read_sql_query(query, conn)
    
    if not df.empty:
        print(f"Total missing: {len(df)}")
        print("\nMissing by League:")
        print(df['league'].value_counts())
        print("\nSample Missing Games:")
        print(df.head(20).to_string())
    else:
        print("No missing scores found in this period.")

    # 2. Check Predictions Schema
    print("\n=== Predictions Table Schema ===")
    cursor = conn.execute("PRAGMA table_info(predictions)")
    columns = [row[1] for row in cursor.fetchall()]
    print(columns)
    
    # 3. Check Future Games Count (any logic)
    print("\n=== Future Games ===")
    future_counts = conn.execute("""
        SELECT s.league, COUNT(*) 
        FROM games g
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE g.start_time_utc > datetime('now')
        GROUP BY s.league
    """).fetchall()
    print(future_counts)

    conn.close()

if __name__ == "__main__":
    check_data()
