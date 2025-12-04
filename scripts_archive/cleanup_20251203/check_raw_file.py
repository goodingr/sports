import json
from pathlib import Path

def check_raw_file():
    path = Path(r'C:\Users\Bobby\Desktop\sports\data\raw\odds\basketball_nba\odds_2025-11-25T05-25-28Z.json')
    if not path.exists():
        print(f"File not found: {path}")
        return

    with open(path) as f:
        data = json.load(f)
    
    print(f"Total games: {len(data['results'])}")
    print("Games:")
    for g in data['results']:
        print(f"{g['commence_time']} {g['away_team']} @ {g['home_team']}")

if __name__ == "__main__":
    check_raw_file()
