import sqlite3
import pandas as pd

def check_damage():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking for mismatched odds (Game Sport != Snapshot Sport):")
    
    query = """
    SELECT 
        g.game_id, 
        s_game.league as game_league,
        s_snap.league as snap_league,
        count(*) as bad_odds_count
    FROM odds o
    JOIN games g ON o.game_id = g.game_id
    JOIN sports s_game ON g.sport_id = s_game.sport_id
    JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
    JOIN sports s_snap ON os.sport_id = s_snap.sport_id
    WHERE s_game.sport_id != s_snap.sport_id
    GROUP BY g.game_id
    """
    
    df = pd.read_sql_query(query, conn)
    print(f"Found {len(df)} games with mismatched odds.")
    if not df.empty:
        print(df.head(20).to_string(index=False))
        
    conn.close()

if __name__ == "__main__":
    check_damage()
