import json
from pathlib import Path

def check_nhl_bookmakers():
    path = "data/raw/odds/icehockey_nhl/odds_2025-11-25T03-41-54Z.json"
    if not Path(path).exists():
        print(f"File not found: {path}")
        # Try finding any json file
        files = sorted(Path("data/raw/odds/icehockey_nhl").glob("*.json"))
        if files:
            path = files[-1]
            print(f"Using latest file: {path}")
        else:
            return

    with open(path, "r") as f:
        data = json.load(f)

    results = data if isinstance(data, list) else data.get("results", [])
    print(f"Total NHL events: {len(results)}")
    
    for event in results:
        print(f"Game: {event.get('away_team')} @ {event.get('home_team')}")
        bookmakers = event.get("bookmakers", [])
        print(f"  Bookmakers: {len(bookmakers)}")
        if bookmakers:
            print(f"  First book: {bookmakers[0].get('title')}")

if __name__ == "__main__":
    check_nhl_bookmakers()
