import pandas as pd
from pathlib import Path
from src.data.team_mappings import get_full_team_name

def check_team_names():
    path = Path("data/forward_test/ensemble/predictions_master.parquet")
    if not path.exists():
        print(f"File not found: {path}")
        return

    df = pd.read_parquet(path)
    df = pd.read_parquet(path)
    
    leagues = df["league"].unique()
    print(f"Checking teams for leagues: {leagues}")
    
    for league in sorted(leagues):
        print(f"\n--- {league} ---")
        league_df = df[df["league"] == league]
        unique_teams = set(league_df["home_team"].unique()) | set(league_df["away_team"].unique())
        
        for team in sorted(unique_teams):
            full_name = get_full_team_name(league, team)
            # Check if full_name is same as code (implies no mapping) or if it looks like an abbreviation
            # Also check if the "full name" is just the code (which happens if get_full_team_name returns the input)
            if full_name == team and (len(team) <= 4 and team.isupper()):
                 print(f"  '{team}' -> '{full_name}' (Likely missing mapping)")
            elif full_name == team and team.isupper(): # Catch longer codes like "BRENTFORD" if they are codes
                 # Heuristic: if it's all upper case, it might be a code
                 print(f"  '{team}' -> '{full_name}' (Potential missing mapping)")

if __name__ == "__main__":
    check_team_names()
