import sqlite3
from datetime import datetime, timezone

conn = sqlite3.connect("database.sqlite")
cursor = conn.cursor()

now = datetime.now(timezone.utc).isoformat()

query = """
    SELECT g.game_id, g.commence_time, t1.name as home, t2.name as away
    FROM games g
    JOIN teams t1 ON g.home_team_id = t1.team_id
    JOIN teams t2 ON g.away_team_id = t2.team_id
    WHERE g.league = 'NBA'
      AND g.commence_time > ?
    ORDER BY g.commence_time
    LIMIT 10
"""

rows = cursor.execute(query, (now,)).fetchall()

print(f"Future NBA Games in DB: {len(rows)}")
for row in rows:
    print(row)

conn.close()
