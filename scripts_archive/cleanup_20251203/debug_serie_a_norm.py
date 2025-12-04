from src.data.team_mappings import normalize_team_code

def check_serie_a_normalization():
    league = "SERIEA"
    teams = [
        "Como", "Sassuolo", "Genoa", "Hellas Verona", "Parma", "Udinese", 
        "Fiorentina", "Inter Milan", "Atalanta BC", "Pisa"
    ]
    
    print(f"Checking normalization for league: {league}")
    for team in teams:
        code = normalize_team_code(league, team)
        print(f"  '{team}' -> '{code}'")

if __name__ == "__main__":
    check_serie_a_normalization()
