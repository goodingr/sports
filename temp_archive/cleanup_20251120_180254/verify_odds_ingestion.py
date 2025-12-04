"""
Verify that LAC @ ORL now has detailed sportsbook odds after ingestion
"""
from src.db.core import connect

print("CHECKING FOR LAC @ ORL SPORTSBOOK ODDS")
print("=" * 80)

with connect() as conn:
    # Find the game
    game = conn.execute("""
        SELECT g.game_id, g.odds_api_id, g.start_time_utc,
               ht.code as home_team, at.code as away_team
        FROM games g
        JOIN teams ht ON ht.team_id = g.home_team_id
        JOIN teams at ON at.team_id = g.away_team_id
        WHERE ht.code = 'ORL' AND at.code = 'LAC'
        ORDER BY g.start_time_utc DESC
        LIMIT 1
    """).fetchone()
    
    if game:
        game_id, odds_api_id, start_time, home, away = game
        print(f"\nFound game: {home} vs {away}")
        print(f"  DB Game ID: {game_id}")
        print(f"  Odds API ID: {odds_api_id}")
        print(f"  Start Time: {start_time}")
        
        # Get moneyline odds by sportsbook
        odds = conn.execute("""
            SELECT DISTINCT o.bookmaker_name, o.outcome, o.price_american
            FROM odds o
            JOIN odds_snapshots s ON s.snapshot_id = o.snapshot_id
            WHERE o.game_id = ? AND o.market = 'h2h'
            ORDER BY o.bookmaker_name, o.outcome
        """, (game_id,)).fetchall()
        
        if odds:
            print(f"\n✅ Found {len(odds)} moneyline odds from sportsbooks:")
            print("-" * 80)
            
            current_book = None
            for book, outcome, price in odds:
                if book != current_book:
                    print(f"\n{book}:")
                    current_book = book
                print(f"  {outcome}: {price:+.0f}")
            
            print("\n" + "=" * 80)
            print("SUCCESS! The dashboard should now show real sportsbook odds")
            print("instead of 'Forward Test' when you click on this game.")
            print("=" * 80)
        else:
            print("\n❌ No moneyline odds found in database")
            print("The odds may not have been ingested yet.")
    else:
        print("\n❌ Game not found in database")
        print("The game hasn't been ingested yet.")
