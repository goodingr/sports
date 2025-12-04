import sqlite3
from pathlib import Path

VALID_NFL_TEAMS = {
    "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills",
    "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns",
    "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers",
    "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Kansas City Chiefs",
    "Las Vegas Raiders", "Los Angeles Chargers", "Los Angeles Rams", "Miami Dolphins",
    "Minnesota Vikings", "New England Patriots", "New Orleans Saints", "New York Giants",
    "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers", "San Francisco 49ers",
    "Seattle Seahawks", "Tampa Bay Buccaneers", "Tennessee Titans", "Washington Commanders",
    # Historical names/codes that might be in DB
    "Oakland Raiders", "San Diego Chargers", "St. Louis Rams", "Washington Football Team", "Washington Redskins",
    "LA", "OAK", "SD", "STL" 
}

def cleanup(dry_run=True):
    db_path = "data/betting.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("--- Cleanup Misclassified NFL Games ---")
    
    # Find all teams in sport_id=1 (NFL)
    cursor.execute("SELECT team_id, name, code FROM teams WHERE sport_id = 1")
    teams = cursor.fetchall()
    
    to_delete = []
    for team in teams:
        if team["name"] not in VALID_NFL_TEAMS and team["code"] not in VALID_NFL_TEAMS:
            to_delete.append(team)
            
    print(f"Found {len(to_delete)} teams to delete from sport_id=1:")
    for team in to_delete:
        print(f"  ID: {team['team_id']}, Name: {team['name']}, Code: {team['code']}")
        
    if not to_delete:
        print("No teams to delete.")
        return

    if dry_run:
        print("\n[DRY RUN] No changes made. Set dry_run=False to execute.")
        return

    # Delete games and teams
    team_ids = [t["team_id"] for t in to_delete]
    placeholders = ",".join("?" for _ in team_ids)
    
    # 1. Delete games
    cursor.execute(f"""
        DELETE FROM games 
        WHERE sport_id = 1 
          AND (home_team_id IN ({placeholders}) OR away_team_id IN ({placeholders}))
    """, team_ids + team_ids)
    deleted_games = cursor.rowcount
    print(f"\nDeleted {deleted_games} misclassified games.")

    # 2. Delete teams
    cursor.execute(f"DELETE FROM teams WHERE team_id IN ({placeholders})", team_ids)
    deleted_teams = cursor.rowcount
    print(f"Deleted {deleted_teams} misclassified teams.")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    cleanup(dry_run=False)
