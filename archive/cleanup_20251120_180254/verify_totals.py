"""
Verify MEM @ SAC totals odds are in database
"""
from src.db.core import connect

print("CHECKING MEM @ SAC TOTALS ODDS")
print("=" * 80)

with connect() as conn:
    # Find the game
    game = conn.execute("""
        SELECT g.game_id, g.odds_api_id,
               ht.code as home_team, at.code as away_team
        FROM games g
        JOIN teams ht ON ht.team_id = g.home_team_id
        JOIN teams at ON at.team_id = g.away_team_id
        WHERE ht.code = 'SAC' AND at.code = 'MEM'
        ORDER BY g.start_time_utc DESC
        LIMIT 1
    """).fetchone()
    
    if game:
        game_id, odds_api_id, home, away = game
        print(f"\nFound game: {home} vs {away}")
        print(f"  DB Game ID: {game_id}")
        
        # Count totals odds
        totals_count = conn.execute("""
            SELECT COUNT(*)
            FROM odds o
            WHERE o.game_id = ? AND o.market = 'totals'
        """, (game_id,)).fetchone()[0]
        
        print(f"\n✅ Found {totals_count} totals odds entries")
        
        if totals_count > 0:
            # Show sample
            sample = conn.execute("""
                SELECT o.outcome, o.point, o.price_american
                FROM odds o
                WHERE o.game_id = ? AND o.market = 'totals'
                LIMIT 10
            """, (game_id,)).fetchall()
            
            print("\nSample totals odds:")
            for outcome, point, price in sample:
                print(f"  {outcome} {point}: {price:+.0f}")
            
            print("\n" + "=" * 80)
            print("SUCCESS! Dashboard should now show real sportsbook totals odds")
            print("=" * 80)
        else:
            print("\n❌ No totals odds found")
    else:
        print("\n❌ Game not found")
