import sqlite3
import pandas as pd

def check_alternative_odds():
    conn = sqlite3.connect('data/betting.db')
    
    print("Checking for alternative odds in 'odds' table for EPL games with missing closing lines:")
    
    # Get a few game_ids with missing closing odds in Nov 2025
    query_games = """
    SELECT g.game_id, g.start_time_utc, ht.name as home, at.name as away
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE s.league = 'EPL'
    AND g.start_time_utc LIKE '2025-11%'
    AND gr.home_score IS NOT NULL
    AND gr.home_moneyline_close IS NULL
    LIMIT 5
    """
    
    games_df = pd.read_sql_query(query_games, conn)
    
    if games_df.empty:
        print("No matching games found.")
        conn.close()
        return
        
    print("\nSample games:")
    print(games_df.to_string(index=False))
    
    print("\nChecking odds table for these games:")
    for _, row in games_df.iterrows():
        game_id = row['game_id']
        print(f"\nGame: {row['home']} vs {row['away']} ({game_id})")
        
        query_odds = f"""
        SELECT COUNT(*) as count, MIN(os.fetched_at_utc) as first_seen, MAX(os.fetched_at_utc) as last_seen
        FROM odds o
        JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
        WHERE o.game_id = '{game_id}'
        """
        try:
            odds_stats = pd.read_sql_query(query_odds, conn).iloc[0]
            print(f"  Odds entries: {odds_stats['count']}")
            if odds_stats['count'] > 0:
                print(f"  Range: {odds_stats['first_seen']} to {odds_stats['last_seen']}")
                
                # Get latest odds
                query_latest = f"""
                SELECT o.book_id, o.price_decimal, o.outcome, os.fetched_at_utc
                FROM odds o
                JOIN odds_snapshots os ON o.snapshot_id = os.snapshot_id
                WHERE o.game_id = '{game_id}'
                AND o.market = 'h2h'
                ORDER BY os.fetched_at_utc DESC
                LIMIT 6
                """
                latest = pd.read_sql_query(query_latest, conn)
                print(f"  Latest odds samples: {latest.to_dict('records')}")
        except Exception as e:
            print(f"  Error querying odds: {e}")

    conn.close()

if __name__ == "__main__":
    check_alternative_odds()
