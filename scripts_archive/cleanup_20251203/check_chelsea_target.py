import sqlite3
import pandas as pd

def check_chelsea_target():
    conn = sqlite3.connect('data/betting.db')
    
    game_id = 'EPL_740699'
    print(f"Checking target game {game_id}:")
    
    query = f"""
    SELECT 
        g.game_id, 
        g.odds_api_id,
        gr.home_moneyline_close
    FROM games g
    LEFT JOIN game_results gr ON g.game_id = gr.game_id
    WHERE g.game_id = '{game_id}'
    """
    
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))
    
    # Check odds
    query_odds = f"SELECT count(*) as count FROM odds WHERE game_id = '{game_id}'"
    print("Odds count:", pd.read_sql_query(query_odds, conn).iloc[0]['count'])
    
    conn.close()

if __name__ == "__main__":
    check_chelsea_target()
