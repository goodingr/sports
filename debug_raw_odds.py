import json
import sys
from pathlib import Path

def check_odds():
    path = Path("data/raw/odds/americanfootball_ncaaf/odds_2025-11-25T02-49-19Z.json")
    if not path.exists():
        print(f"File not found: {path}")
        return

    with open(path, "r") as f:
        data = json.load(f)

    results = data.get("results", [])
    print(f"Total events: {len(results)}")
    
    found = False
    for event in results:
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        if "Memphis" in home or "Memphis" in away:
            found = True
            print(f"Found Game: {away} @ {home}")
            print(f"  ID: {event.get('id')}")
            print(f"  Commence Time: {event.get('commence_time')}")
            
            bookmakers = event.get("bookmakers", [])
            print(f"  Bookmakers: {len(bookmakers)}")
            
            for book in bookmakers:
                print(f"    Book: {book.get('title')}")
                for market in book.get("markets", []):
                    print(f"      Market: {market.get('key')}")
                    for outcome in market.get("outcomes", []):
                        print(f"        {outcome.get('name')}: {outcome.get('price')}")

    if not found:
        print("Memphis game not found in raw data.")

if __name__ == "__main__":
    check_odds()
