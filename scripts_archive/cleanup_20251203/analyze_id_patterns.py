import sqlite3

conn = sqlite3.connect("data/betting.db")
cursor = conn.cursor()

# Analyze ID patterns
print("="*80)
print("ANALYZING GAME ID PATTERNS")
print("="*80)

# Check Serie A games
query = """
SELECT 
    g.game_id,
    g.odds_api_id,
    ht.name as home_team,
    at.name as away_team,
    CASE 
        WHEN g.game_id LIKE 'SERIEA_%' THEN 'ESPN-style'
        WHEN LENGTH(g.game_id) = 32 THEN 'Hash'
        ELSE 'Other'
    END as game_id_type,
    CASE 
        WHEN g.odds_api_id IS NULL THEN 'NULL'
        WHEN LENGTH(g.odds_api_id) = 32 THEN 'Hash'
        ELSE 'Other'
    END as odds_api_id_type
FROM games g
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id
JOIN sports s ON g.sport_id = s.sport_id
WHERE s.league = 'SERIEA'
  AND datetime(g.start_time_utc) > datetime('now')
LIMIT 10
"""

print("\nSample SERIEA games:\n")
rows = cursor.execute(query).fetchall()
for row in rows:
    game_id, odds_api_id, home, away, gid_type, oid_type = row
    print(f"game_id: {game_id} ({gid_type})")
    print(f"odds_api_id: {odds_api_id} ({oid_type})")
    print(f"Matchup: {away} @ {home}")
    print()

# Check if there are games with NULL odds_api_id
print("="*80)
print("NULL odds_api_id CHECK")
print("="*80)

null_check = """
SELECT 
    s.league,
    COUNT(*) as total_games,
    SUM(CASE WHEN g.odds_api_id IS NULL THEN 1 ELSE 0 END) as null_odds_api_id,
    SUM(CASE WHEN g.odds_api_id IS NOT NULL THEN 1 ELSE 0 END) as has_odds_api_id
FROM games g
JOIN sports s ON g.sport_id = s.sport_id
WHERE datetime(g.start_time_utc) > datetime('now')
GROUP BY s.league
ORDER BY s.league
"""

print("\nGames by league and odds_api_id presence:\n")
print(f"{'League':<15} {'Total':<10} {'NULL odds_api_id':<20} {'Has odds_api_id':<20}")
print("-"*65)
for row in cursor.execute(null_check).fetchall():
    league, total, null_count, has_count = row
    print(f"{league:<15} {total:<10} {null_count:<20} {has_count:<20}")

# Check for duplicates using odds_api_id
print("\n" + "="*80)
print("DUPLICATE CHECK BY odds_api_id")
print("="*80)

dup_check = """
SELECT 
    g.odds_api_id,
    COUNT(*) as count,
    GROUP_CONCAT(g.game_id) as game_ids
FROM games g
WHERE g.odds_api_id IS NOT NULL
  AND datetime(g.start_time_utc) > datetime('now')
GROUP BY g.odds_api_id
HAVING COUNT(*) > 1
"""

dup_rows = cursor.execute(dup_check).fetchall()
if dup_rows:
    print(f"\nFound {len(dup_rows)} duplicate odds_api_ids:\n")
    for row in dup_rows:
        odds_id, count, game_ids = row
        print(f"odds_api_id: {odds_id}")
        print(f"  Count: {count}")
        print(f"  game_ids: {game_ids}")
        print()
else:
    print("\n✓ No duplicate odds_api_ids found!")

conn.close()
