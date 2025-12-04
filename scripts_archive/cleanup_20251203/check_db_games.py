import sqlite3
import pandas as pd

def check_db():
    conn = sqlite3.connect("data/betting.db")
    
    # Get NBA sport_id
    sport_id = conn.execute("SELECT sport_id FROM sports WHERE league = 'NBA'").fetchone()[0]
    print(f"NBA Sport ID: {sport_id}")
    
    # Get latest snapshot
    snap = conn.execute("SELECT snapshot_id, fetched_at_utc FROM odds_snapshots WHERE sport_id = ? ORDER BY fetched_at_utc DESC LIMIT 1", (sport_id,)).fetchone()
    print(f"Latest Snapshot: {snap}")
    snapshot_id = snap[0]
    
    # Check games in this snapshot
    query = """
    SELECT g.game_id, g.start_time_utc, ht.name as home, at.name as away
    FROM games g
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE g.sport_id = ?
    AND g.start_time_utc > datetime('now', '-24 hours')
    AND EXISTS (
        SELECT 1 FROM odds o
        WHERE o.game_id = g.game_id
        AND o.snapshot_id = ?
    )
    ORDER BY g.start_time_utc
    """
    
    games = pd.read_sql_query(query, conn, params=(sport_id, snapshot_id))
    print(f"\nGames in latest snapshot (last 24h): {len(games)}")
    print(games.to_string())

if __name__ == "__main__":
    check_db()
