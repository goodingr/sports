import sqlite3
import pandas as pd

def merge_duplicates():
    conn = sqlite3.connect('data/betting.db')
    cursor = conn.cursor()
    
    print("Finding duplicates to merge...")
    
    # Find pairs of games within 2 hours of each other
    # One has scores (target), one has odds (source)
    query = """
    SELECT 
        g1.game_id as target_id, 
        g2.game_id as source_id,
        g1.start_time_utc as time,
        ht1.name as home1, ht2.name as home2,
        gr2.home_moneyline_close, gr2.away_moneyline_close,
        g2.odds_api_id
    FROM games g1
    JOIN games g2 ON abs(julianday(g1.start_time_utc) - julianday(g2.start_time_utc)) < 0.1
        AND g1.game_id != g2.game_id
    JOIN sports s ON g1.sport_id = s.sport_id
    JOIN teams ht1 ON g1.home_team_id = ht1.team_id
    JOIN teams ht2 ON g2.home_team_id = ht2.team_id
    LEFT JOIN game_results gr1 ON g1.game_id = gr1.game_id
    LEFT JOIN game_results gr2 ON g2.game_id = gr2.game_id
    WHERE s.league = 'EPL'
    AND g1.start_time_utc LIKE '2025-11%'
    AND gr1.home_score IS NOT NULL
    AND gr1.home_moneyline_close IS NULL
    AND gr2.home_moneyline_close IS NOT NULL
    """
    
    df = pd.read_sql_query(query, conn)
    print(f"Found {len(df)} potential pairs.")
    
    if df.empty:
        conn.close()
        return

    # Filter by team name similarity
    valid_merges = []
    for _, row in df.iterrows():
        home1 = row['home1']
        home2 = row['home2']
        
        # Simple containment check (case insensitive)
        # or use normalize_team_code if available
        # Here we use a robust check:
        if home1.lower() in home2.lower() or home2.lower() in home1.lower():
            valid_merges.append(row)
        else:
            print(f"Skipping mismatch: {home1} vs {home2}")
            
    print(f"Found {len(valid_merges)} VALID pairs to merge.")
    
    for row in valid_merges:
        target_id = row['target_id']
        source_id = row['source_id']
        odds_id = row['odds_api_id']
        home_ml = row['home_moneyline_close']
        away_ml = row['away_moneyline_close']
        
        print(f"Merging {source_id} -> {target_id}...")
        
        # 1. Update odds table to point to target_id
        try:
            cursor.execute("UPDATE odds SET game_id = ? WHERE game_id = ?", (target_id, source_id))
        except sqlite3.IntegrityError:
            print(f"  ! Odds conflict for {source_id} -> {target_id}. Deleting source odds.")
            cursor.execute("DELETE FROM odds WHERE game_id = ?", (source_id,))
        
        # 2. Update target game with odds_api_id
        cursor.execute("UPDATE games SET odds_api_id = ? WHERE game_id = ?", (odds_id, target_id))
        
        # 3. Update target game_results with odds
        cursor.execute("""
            UPDATE game_results 
            SET home_moneyline_close = ?, away_moneyline_close = ? 
            WHERE game_id = ?
        """, (home_ml, away_ml, target_id))
        
        # 4. Delete source game and results
        cursor.execute("DELETE FROM game_results WHERE game_id = ?", (source_id,))
        cursor.execute("DELETE FROM games WHERE game_id = ?", (source_id,))
        
        print("  ✓ Merged")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    merge_duplicates()
