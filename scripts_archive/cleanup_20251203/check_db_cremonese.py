import sqlite3

conn = sqlite3.connect("data/betting.db")
cursor = conn.cursor()

# Find the Cremonese vs Bologna games
query = """
SELECT 
    g.game_id,
    g.odds_api_id,
    g.start_time_utc,
    ht.name as home_team,
    at.name as away_team,
    s.league
FROM games g
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id
JOIN sports s ON g.sport_id = s.sport_id
WHERE (ht.name LIKE '%Bologna%' OR at.name LIKE '%Bologna%')
  AND (ht.name LIKE '%Cremonese%' OR at.name LIKE '%Cremonese%')
  AND datetime(start_time_utc) >= '2025-12-01'
  AND datetime(start_time_utc) < '2025-12-02'
"""

rows = cursor.execute(query).fetchall()

print(f"Found {len(rows)} Cremonese vs Bologna games in database:\n")
for row in rows:
    game_id, odds_api_id, start_time, home, away, league = row
    print(f"game_id: {game_id}")
    print(f"odds_api_id: {odds_api_id}")
    print(f"Matchup: {away} @ {home}")
    print(f"League: {league}")
    print(f"Time: {start_time}")
    print()

conn.close()
