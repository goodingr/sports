
import sqlite3
from pathlib import Path

DB_PATH = Path("data/betting.db")

def verify():
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()


    # Query games in Dec 26-31 2025
    query = """
        SELECT 
            strftime('%Y-%m-%d', start_time_utc) as day,
            l.name as league_name,
            COUNT(*) as total_games,
            SUM(CASE WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL THEN 1 ELSE 0 END) as games_with_scores
        FROM games g
        LEFT JOIN game_results r ON g.game_id = r.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN leagues l ON s.league = l.name
        WHERE start_time_utc BETWEEN '2025-12-26' AND '2025-12-31'
        AND l.name IN ('NHL', 'NBA', 'NCAAB')
        GROUP BY day, league_name
        ORDER BY day, league_name
    """
    
    print("=== Verification Results (Dec 26-31) ===")
    cursor.execute("ATTACH DATABASE 'data/betting.db' AS betting") # Just in case, though we connected directly
    # The previous script didn't attach, just used direct connection. The query above joins 'leagues' which might be in the schema but not populated? 
    # Wait, 'leagues' table might not exist or be named differently.
    # Let's check schema.sql or just use s.league from sports table directly.
    pass


    query = """
        SELECT 
            strftime('%Y-%m-%d', start_time_utc) as day,
            s.league,
            COUNT(*) as total_games,
            SUM(CASE WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL THEN 1 ELSE 0 END) as games_with_scores,
            SUM(CASE WHEN g.status = 'final' THEN 1 ELSE 0 END) as games_final
        FROM games g
        LEFT JOIN game_results r ON g.game_id = r.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE start_time_utc BETWEEN '2025-12-01' AND '2026-01-02'
        AND s.league IN ('NHL', 'NBA', 'NCAAB')
        GROUP BY day, s.league
        ORDER BY day, s.league
    """

    rows = cursor.execute(query).fetchall()
    if not rows:
        print("No NHL/NBA/NCAAB games found in Dec 1 - Jan 2.")
    else:
        print(f"{'Day':<12} | {'League':<10} | {'Total':<10} | {'Scored':<10} | {'Final':<10}")
        print("-" * 65)
        for row in rows:
            print(f"{row[0]:<12} | {row[1]:<10} | {row[2]:<10} | {row[3]:<10} | {row[4]:<10}")

    # Check for specific missing games
    print("\n=== Remaining Missing Scores (Sample) ===")
    query_missing = """
        SELECT 
            g.start_time_utc,
            s.league,
            ht.name,
            at.name
        FROM games g
        LEFT JOIN game_results r ON g.game_id = r.game_id
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        WHERE start_time_utc BETWEEN '2025-12-01' AND '2026-01-02'
        AND s.league IN ('NHL', 'NBA', 'NCAAB')
        AND (r.home_score IS NULL OR r.away_score IS NULL)
        LIMIT 10
    """
    rows_missing = cursor.execute(query_missing).fetchall()
    for row in rows_missing:
        print(f"MISSING: {row[0]} {row[1]} {row[2]} vs {row[3]}")

    conn.close()

if __name__ == "__main__":
    verify()
