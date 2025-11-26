import json
from pathlib import Path
from src.data.team_mappings import normalize_team_code

def check_team_mappings():
    # Check NBA
    print("=== NBA Teams ===")
    nba_file = sorted(Path("data/raw/odds/basketball_nba").glob("*.json"))[-1]
    with open(nba_file, "r") as f:
        nba_data = json.load(f)
    
    nba_results = nba_data if isinstance(nba_data, list) else nba_data.get("results", [])
    print(f"Total NBA games in raw odds: {len(nba_results)}\n")
    
    nba_teams_raw = set()
    nba_teams_normalized = set()
    for game in nba_results:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        nba_teams_raw.add(home)
        nba_teams_raw.add(away)
        nba_teams_normalized.add(normalize_team_code("NBA", home))
        nba_teams_normalized.add(normalize_team_code("NBA", away))
    
    print("Raw team names from Odds API:")
    for team in sorted(nba_teams_raw):
        normalized = normalize_team_code("NBA", team)
        print(f"  {team} -> {normalized}")
    
    # Check NHL
    print("\n=== NHL Teams ===")
    nhl_file = sorted(Path("data/raw/odds/icehockey_nhl").glob("*.json"))[-1]
    with open(nhl_file, "r") as f:
        nhl_data = json.load(f)
    
    nhl_results = nhl_data if isinstance(nhl_data, list) else nhl_data.get("results", [])
    print(f"Total NHL games in raw odds: {len(nhl_results)}\n")
    
    nhl_teams_raw = set()
    nhl_teams_normalized = set()
    for game in nhl_results:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        nhl_teams_raw.add(home)
        nhl_teams_raw.add(away)
        nhl_teams_normalized.add(normalize_team_code("NHL", home))
        nhl_teams_normalized.add(normalize_team_code("NHL", away))
    
    print("Raw team names from Odds API:")
    for team in sorted(nhl_teams_raw):
        normalized = normalize_team_code("NHL", team)
        print(f"  {team} -> {normalized}")

if __name__ == "__main__":
    check_team_mappings()
