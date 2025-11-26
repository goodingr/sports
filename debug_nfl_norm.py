from src.data.team_mappings import normalize_team_code

def check_nfl_norm():
    teams = [
        "Washington Commanders",
        "Jacksonville Jaguars",
        "Tennessee Titans",
        "Philadelphia Eagles",
        "New Orleans Saints",
        "Denver Broncos"
    ]
    
    print("Checking NFL normalization:")
    for team in teams:
        code = normalize_team_code("NFL", team)
        print(f"  '{team}' -> '{code}'")

if __name__ == "__main__":
    check_nfl_norm()
