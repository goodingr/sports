import sqlite3
import pandas as pd

def check_chelsea_source():
    conn = sqlite3.connect('data/betting.db')
    
    game_id = 'EPL_id1000001761300711'
    print(f"Checking source game {game_id}:")
    
    # Check game existence
    query_game = f"SELECT * FROM games WHERE game_id = '{game_id}'"
    df_game = pd.read_sql_query(query_game, conn)
    print("Game found:", not df_game.empty)
    if not df_game.empty:
        print(df_game.to_string(index=False))
        
    # Check odds existence
    query_odds = f"SELECT count(*) FROM odds WHERE game_id = '{game_id}'"
    print("Odds count:", pd.read_sql_query(query_odds, conn).iloc[0]['count'])
    
    # Check game_results existence
    query_results = f"SELECT * FROM game_results WHERE game_id = '{game_id}'"
    df_results = pd.read_sql_query(query_results, conn)
    print("Results found:", not df_results.empty)
    if not df_results.empty:
        print(df_results.to_string(index=False))
        
    conn.close()

if __name__ == "__main__":
    check_chelsea_source()
