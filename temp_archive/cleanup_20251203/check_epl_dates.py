import sqlite3
import pandas as pd

def check_epl_dates():
    conn = sqlite3.connect('data/betting.db')
    
    print("Date distribution of EPL games with scores but NO odds:")
    query = """
    SELECT 
        strftime('%Y-%m', g.start_time_utc) as month,
        COUNT(*) as count
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    WHERE s.league = 'EPL'
    AND gr.home_score IS NOT NULL
    AND gr.home_moneyline_close IS NULL
    GROUP BY month
    ORDER BY month
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")
        
    conn.close()

if __name__ == "__main__":
    check_epl_dates()
