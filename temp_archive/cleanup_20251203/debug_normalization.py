from src.data.team_mappings import normalize_team_code

def check_normalization():
    league = "CFB"
    teams = [
        "Memphis Tigers", 
        "Navy Midshipmen", 
        "Memphis", 
        "Navy",
        "MEM",
        "NAVY"
    ]
    
    print(f"Checking normalization for league: {league}")
    for team in teams:
        code = normalize_team_code(league, team)
        print(f"  '{team}' -> '{code}'")

if __name__ == "__main__":
    check_normalization()
