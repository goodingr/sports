import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def debug_epl_data():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking EPL games in DB (last 90 days):")
    cutoff = (datetime.utcnow() - timedelta(days=90)).isoformat()
    
    # Check total EPL games
    query_total = f"""
    SELECT COUNT(*) as count
    FROM games g
    JOIN sports s ON g.sport_id = s.sport_id
    WHERE s.league = 'EPL'
    AND g.start_time_utc > '{cutoff}'
    """
    total = pd.read_sql_query(query_total, conn).iloc[0]['count']
    print(f"  Total EPL games: {total}")
    
    # Check completed (with scores)
    query_scores = f"""
    SELECT COUNT(*) as count
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    WHERE s.league = 'EPL'
    AND g.start_time_utc > '{cutoff}'
    AND gr.home_score IS NOT NULL
    """
    scores = pd.read_sql_query(query_scores, conn).iloc[0]['count']
    print(f"  With scores: {scores}")
    
    # Check completed with odds
    query_odds = f"""
    SELECT COUNT(*) as count
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    WHERE s.league = 'EPL'
    AND g.start_time_utc > '{cutoff}'
    AND gr.home_score IS NOT NULL
    AND gr.home_moneyline_close IS NOT NULL
    """
    odds = pd.read_sql_query(query_odds, conn).iloc[0]['count']
    print(f"  With scores AND odds: {odds}")
    
    # Sample of games with scores but NO odds
    print("\nSample EPL games with scores but MISSING odds:")
    query_missing = f"""
    SELECT 
        g.start_time_utc,
        ht.name as home,
        at.name as away,
        gr.home_score,
        gr.home_moneyline_close
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE s.league = 'EPL'
    AND g.start_time_utc > '{cutoff}'
    AND gr.home_score IS NOT NULL
    AND gr.home_moneyline_close IS NULL
    ORDER BY g.start_time_utc DESC
    LIMIT 10
    """
    df_missing = pd.read_sql_query(query_missing, conn)
    print(df_missing.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    debug_epl_data()
