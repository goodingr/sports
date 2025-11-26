import sqlite3
import pandas as pd

def check_chelsea_games():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking Chelsea home games in Nov 2025:")
    query = """
    SELECT 
        g.game_id, 
        g.start_time_utc,
        ht.name as home,
        at.name as away,
        gr.home_score,
        g.odds_api_id,
        gr.home_moneyline_close
    FROM games g
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    LEFT JOIN game_results gr ON g.game_id = gr.game_id
    WHERE ht.name LIKE '%Chelsea%'
    AND g.start_time_utc LIKE '2025-11%'
    ORDER BY g.start_time_utc
    """
    
    df = pd.read_sql_query(query, conn)
    for _, row in df.iterrows():
        print(f"ID: {row['game_id']}")
        print(f"  Time: {row['start_time_utc']}")
        print(f"  Match: {row['home']} vs {row['away']}")
        print(f"  Score: {row['home_score']}")
        print(f"  OddsID: {row['odds_api_id']}")
        print(f"  Odds: {row['home_moneyline_close']}")
        print("-" * 20)
    
    conn.close()

if __name__ == "__main__":
    check_chelsea_games()
