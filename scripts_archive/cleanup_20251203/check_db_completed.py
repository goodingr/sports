import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta

conn = sqlite3.connect("data/betting.db")

# Check how many completed games we have in the database
query = """
SELECT 
    g.game_id,
    s.league,
    g.start_time_utc,
    g.home_score,
    g.away_score,
    g.status,
    ht.name as home_team,
    at.name as away_team
FROM games g
JOIN sports s ON g.sport_id = s.sport_id
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id
WHERE g.status = 'final'
AND g.home_score IS NOT NULL
AND g.away_score IS NOT NULL
ORDER BY g.start_time_utc DESC
LIMIT 20
"""

completed_games = pd.read_sql_query(query, conn)
print(f"Total completed games in database: {len(completed_games)}")
print(f"\nSample of recent completed games:")
print(completed_games[['league', 'start_time_utc', 'away_team', 'home_team', 'away_score', 'home_score']].to_string(index=False))

# Also check how many completed games total
count_query = "SELECT COUNT(*) as total FROM games WHERE status = 'final' AND home_score IS NOT NULL"
total = pd.read_sql_query(count_query, conn)
print(f"\n\nTotal completed games with scores in database: {total['total'].iloc[0]}")

conn.close()
