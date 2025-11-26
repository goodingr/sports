import sqlite3
import pandas as pd

def check_duplicate_games():
    conn = sqlite3.connect('data/betting.db')
    
    # Query the specific game IDs found in the previous debug step
    ids = ['BUNDESLIGA_746817', 'BUNDESLIGA_746818']
    placeholders = ','.join(['?'] * len(ids))
    
    query = f"""
    SELECT *
    FROM games
    WHERE game_id IN ({placeholders})
    """
    
    df = pd.read_sql_query(query, conn, params=ids)
    conn.close()
    
    if df.empty:
        print("No games found with these IDs.")
    else:
        print("Found games:")
        # Transpose for easier comparison
        print(df.T)

if __name__ == "__main__":
    check_duplicate_games()
