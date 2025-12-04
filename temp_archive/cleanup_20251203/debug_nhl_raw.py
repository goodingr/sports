import json
from pathlib import Path

def check_nhl_raw():
    path = Path("data/raw/odds/icehockey_nhl/odds_2025-11-25T02-49-22Z.json")
    if not path.exists():
        print(f"File not found: {path}")
        return

    with open(path, "r") as f:
        data = json.load(f)

    results = data.get("results", [])
    print(f"Total NHL events: {len(results)}")
    
    for event in results:
        print(f"Game: {event.get('away_team')} @ {event.get('home_team')}")
        print(f"  ID: {event.get('id')}")
        print(f"  Commence Time: {event.get('commence_time')}")
        print("-" * 20)

if __name__ == "__main__":
    check_nhl_raw()
