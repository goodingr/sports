import sqlite3
import pandas as pd

def check_multiple_sources():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking for multiple sources per target:")
    query = """
    SELECT 
        g1.game_id as target_id, 
        COUNT(g2.game_id) as source_count,
        GROUP_CONCAT(g2.game_id) as source_ids
    FROM games g1
    JOIN games g2 ON abs(julianday(g1.start_time_utc) - julianday(g2.start_time_utc)) < 0.1
        AND g1.game_id != g2.game_id
    JOIN sports s ON g1.sport_id = s.sport_id
    LEFT JOIN game_results gr1 ON g1.game_id = gr1.game_id
    LEFT JOIN game_results gr2 ON g2.game_id = gr2.game_id
    WHERE s.league = 'EPL'
    AND g1.start_time_utc LIKE '2025-11%'
    AND gr1.home_score IS NOT NULL
    AND gr1.home_moneyline_close IS NULL
    AND gr2.home_moneyline_close IS NOT NULL
    GROUP BY g1.game_id
    HAVING source_count > 1
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    check_multiple_sources()
