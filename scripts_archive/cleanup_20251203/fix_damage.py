import sqlite3
import pandas as pd

def fix_damage():
    conn = sqlite3.connect('data/betting.db')
    cursor = conn.cursor()
    
    print("Fixing corrupted EPL games...")
    
    # Get bad games
    query = """
    SELECT 
        g.game_id
    FROM odds o
    JOIN games g ON o.game_id = g.game_id
    JOIN sports s_game ON g.sport_id = s_game.sport_id
    JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
    JOIN sports s_snap ON os.sport_id = s_snap.sport_id
    WHERE s_game.sport_id != s_snap.sport_id
    GROUP BY g.game_id
    """
    
    df = pd.read_sql_query(query, conn)
    bad_ids = df['game_id'].tolist()
    print(f"Found {len(bad_ids)} bad games: {bad_ids}")
    
    if not bad_ids:
        conn.close()
        return
        
    for game_id in bad_ids:
        print(f"Cleaning {game_id}...")
        
        # 1. Delete bad odds
        cursor.execute("DELETE FROM odds WHERE game_id = ?", (game_id,))
        
        # 2. Reset odds_api_id
        cursor.execute("UPDATE games SET odds_api_id = NULL WHERE game_id = ?", (game_id,))
        
        # 3. Clear game_results odds
        cursor.execute("""
            UPDATE game_results 
            SET home_moneyline_close = NULL, away_moneyline_close = NULL 
            WHERE game_id = ?
        """, (game_id,))
        
        print("  ✓ Cleaned")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    fix_damage()
