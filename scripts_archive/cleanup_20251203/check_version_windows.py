import sqlite3
import pandas as pd

def check_version_windows():
    conn = sqlite3.connect('data/betting.db')
    
    windows = [
        ('v0.1', '2025-11-03', '2025-11-13'),
        ('v0.2', '2025-11-14', '2025-11-20')
    ]
    
    for version, start, end in windows:
        print(f"\nChecking {version} ({start} to {end}):")
        
        # Check total games
        query_total = f"""
        SELECT COUNT(*) as count
        FROM games
        WHERE start_time_utc >= '{start}' AND start_time_utc <= '{end}T23:59:59'
        """
        total = pd.read_sql_query(query_total, conn).iloc[0]['count']
        print(f"  Total games: {total}")
        
        # Check completed games with odds (restorable)
        query_restorable = f"""
        SELECT COUNT(*) as count
        FROM game_results gr
        JOIN games g ON gr.game_id = g.game_id
        WHERE g.start_time_utc >= '{start}' AND g.start_time_utc <= '{end}T23:59:59'
        AND gr.home_score IS NOT NULL
        AND gr.home_moneyline_close IS NOT NULL
        """
        restorable = pd.read_sql_query(query_restorable, conn).iloc[0]['count']
        print(f"  Restorable (scores + odds): {restorable}")
        
        # Breakdown by league for restorable
        query_league = f"""
        SELECT s.league, COUNT(*) as count
        FROM game_results gr
        JOIN games g ON gr.game_id = g.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE g.start_time_utc >= '{start}' AND g.start_time_utc <= '{end}T23:59:59'
        AND gr.home_score IS NOT NULL
        AND gr.home_moneyline_close IS NOT NULL
        GROUP BY s.league
        ORDER BY count DESC
        """
        league_counts = pd.read_sql_query(query_league, conn)
        if not league_counts.empty:
            print("  By League:")
            print(league_counts.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    check_version_windows()
