import json
from pathlib import Path

def check_nba_raw():
    # Find latest NBA odds file
    raw_dir = Path("data/raw/odds/basketball_nba")
    files = sorted(raw_dir.glob("odds_*.json"))
    if not files:
        print("No NBA odds files found.")
        return

    latest_file = files[-1]
    print(f"Reading {latest_file}")
    
    with open(latest_file, "r") as f:
        data = json.load(f)

    results = data if isinstance(data, list) else data.get("results", [])
    print(f"Total NBA events: {len(results)}")
    
    for event in results:
        print(f"Game: {event.get('away_team')} @ {event.get('home_team')}")
        print(f"  Commence Time: {event.get('commence_time')}")

if __name__ == "__main__":
    check_nba_raw()
