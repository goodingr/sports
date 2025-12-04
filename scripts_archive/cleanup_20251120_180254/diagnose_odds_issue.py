"""
Investigate why detailed sportsbook odds aren't being stored in the database
for the LAC @ ORL game.
"""
import pandas as pd
from src.db.core import connect

print("INVESTIGATING MISSING SPORTSBOOK ODDS DATA")
print("=" * 80)

# Load the prediction
df = pd.read_parquet("data/forward_test/predictions_master.parquet")
nba = df[df['league'] == 'NBA'].copy()
lac_orl = nba[((nba['home_team'] == 'ORL') & (nba['away_team'] == 'LAC'))].iloc[0]

game_id = lac_orl['game_id']
print(f"\nPrediction Game ID: {game_id}")
print(f"Commence Time: {lac_orl['commence_time']}")
print(f"Moneyline: ORL {lac_orl['home_moneyline']}, LAC {lac_orl['away_moneyline']}")

# Check database for this game
with connect() as conn:
    print("\n" + "=" * 80)
    print("STEP 1: Check if game exists in database")
    print("=" * 80)
    
    # Look for games around this time with these teams
    games = conn.execute("""
        SELECT g.game_id, g.odds_api_id, g.start_time_utc,
               ht.code as home_team, at.code as away_team
        FROM games g
        JOIN teams ht ON ht.team_id = g.home_team_id
        JOIN teams at ON at.team_id = g.away_team_id
        WHERE ht.code = 'ORL' AND at.code = 'LAC'
        ORDER BY g.start_time_utc DESC
        LIMIT 3
    """).fetchall()
    
    if games:
        print(f"\nFound {len(games)} ORL vs LAC game(s) in database:")
        for g in games:
            print(f"  DB Game ID: {g[0]}")
            print(f"  Odds API ID: {g[1]}")
            print(f"  Start Time: {g[2]}")
            print()
    else:
        print("\n❌ NO ORL vs LAC games found in database!")
        print("This means the Odds API game wasn't ingested into the database.")
        print("\nPossible reasons:")
        print("  1. Odds API hasn't provided this game yet")
        print("  2. The odds ingestion script hasn't run")
        print("  3. The game is too new")
        
    print("\n" + "=" * 80)
    print("STEP 2: Check odds table for any NBA games")
    print("=" * 80)
    
    # Check recent odds snapshots
    recent_odds = conn.execute("""
        SELECT s.snapshot_id, s.fetched_at_utc, COUNT(*) as odds_count
        FROM odds_snapshots s
        JOIN odds o ON o.snapshot_id = s.snapshot_id
        JOIN games g ON g.game_id = o.game_id
        JOIN sports sp ON sp.sport_id = g.sport_id
        WHERE sp.league = 'NBA'
        GROUP BY s.snapshot_id
        ORDER BY s.fetched_at_utc DESC
        LIMIT 5
    """).fetchall()
    
    if recent_odds:
        print(f"\nFound {len(recent_odds)} recent NBA odds snapshots:")
        for snapshot in recent_odds:
            print(f"  Snapshot {snapshot[0]}: {snapshot[1]} ({snapshot[2]} odds)")
    else:
        print("\n❌ NO NBA odds found in database!")
        print("The odds table is empty for NBA games.")
    
    print("\n" + "=" * 80)
    print("STEP 3: Check odds_snapshots table structure")
    print("=" * 80)
    
    # Get table schema
    schema = conn.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='table' AND name='odds'
    """).fetchone()
    
    if schema:
        print("\nOdds table schema:")
        print(schema[0])

print("\n" + "=" * 80)
print("DIAGNOSIS")
print("=" * 80)
print("\nThe issue is likely one of these:")
print("\n1. ODDS NOT INGESTED:")
print("   - The Odds API provided the game to forward_test.py")
print("   - But the odds weren't stored in the database")
print("   - Forward test uses Odds API directly, not the database")
print("\n2. TIMING ISSUE:")
print("   - Game is too new, odds haven't been ingested yet")
print("   - Odds ingestion runs on a schedule")
print("\n3. MISSING SPORTSBOOK DETAIL:")
print("   - Only 'best line' was stored, not individual sportsbooks")
print("   - Dashboard needs per-sportsbook odds to display properly")
print("=" * 80)
