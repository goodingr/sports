from src.data.team_mappings import normalize_team_code

def test_normalization():
    print("Testing NFL normalization:")
    print(f"'Houston Texans' -> {normalize_team_code('NFL', 'Houston Texans')}")
    print(f"'Denver Broncos' -> {normalize_team_code('NFL', 'Denver Broncos')}")
    
    print("\nTesting NCAAB normalization:")
    # Need to check what NCAAB names look like in DB
    # Assuming standard names
    print(f"'Duke Blue Devils' -> {normalize_team_code('NCAAB', 'Duke Blue Devils')}")

if __name__ == "__main__":
    test_normalization()
