"""
Investigate why LAC @ ORL game has fallback odds instead of real sportsbook odds
"""
import pandas as pd
from src.db.core import connect

# Load the prediction
df = pd.read_parquet("data/forward_test/predictions_master.parquet")
nba = df[df['league'] == 'NBA'].copy()

# Find LAC @ ORL
game = nba[((nba['home_team'] == 'ORL') & (nba['away_team'] == 'LAC'))].iloc[0]

print("LAC @ ORL GAME ANALYSIS")
print("=" * 80)
print(f"Game ID: {game['game_id']}")
print(f"Commence Time: {game['commence_time']}")
print(f"Predicted At: {game['predicted_at']}")
print()
print("MONEYLINE ODDS IN PREDICTION:")
print(f"  Home (ORL): {game.get('home_moneyline', 'MISSING')}")
print(f"  Away (LAC): {game.get('away_moneyline', 'MISSING')}")
print()

# Check if there's an odds_api_id or other game identifier
print("GAME IDENTIFIERS:")
for col in ['game_id', 'odds_api_id']:
    if col in game.index:
        print(f"  {col}: {game.get(col, 'N/A')}")

# Try to find this game in the database
print("\n" + "=" * 80)
print("CHECKING DATABASE FOR ODDS")
print("=" * 80)

with connect() as conn:
    # Try to find the game by teams and date
    game_id_str = str(game['game_id'])
    
    # Check if game exists in database
    db_game = conn.execute("""
        SELECT g.game_id, g.odds_api_id, g.start_time_utc,
               ht.code as home_team, at.code as away_team
        FROM games g
        JOIN teams ht ON ht.team_id = g.home_team_id
        JOIN teams at ON at.team_id = g.away_team_id
        WHERE (ht.code = 'ORL' AND at.code = 'LAC')
           OR (ht.code = 'LAC' AND at.code = 'ORL')
        ORDER BY g.start_time_utc DESC
        LIMIT 5
    """).fetchall()
    
    if db_game:
        print(f"\nFound {len(db_game)} ORL/LAC games in database:")
        for row in db_game:
            print(f"  Game ID: {row[0]}, Odds API ID: {row[1]}")
            print(f"  Start: {row[2]}, {row[3]} vs {row[4]}")
            
            # Check for moneyline odds
            odds = conn.execute("""
                SELECT book, outcome, price_american
                FROM odds o
                JOIN odds_snapshots s ON s.snapshot_id = o.snapshot_id
                WHERE o.game_id = ? AND o.market = 'h2h'
                ORDER BY s.fetched_at_utc DESC
                LIMIT 10
            """, (row[0],)).fetchall()
            
            if odds:
                print(f"    Found {len(odds)} moneyline odds")
                for odd in odds[:3]:
                    print(f"      {odd[0]}: {odd[1]} @ {odd[2]}")
            else:
                print(f"    NO MONEYLINE ODDS FOUND")
            print()
    else:
        print("\nNo ORL vs LAC games found in database!")
        print("This means the Odds API hasn't provided this game yet,")
        print("or the game wasn't ingested into the database.")

print("=" * 80)
print("DIAGNOSIS:")
print("=" * 80)
print("If the game shows 'Forward Test' as the odds source, it means:")
print("  1. The Odds API didn't have this game when predictions were made")
print("  2. The forward test system used fallback/estimated odds")
print("  3. Real sportsbook odds are not available for this game")
print()
print("SOLUTION:")
print("  - Wait for Odds API to ingest the game")
print("  - Re-run forward test predictions to fetch real odds")
print("  - Or manually verify odds from sportsbooks")
print("=" * 80)
