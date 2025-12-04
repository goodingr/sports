import json
from pathlib import Path

def check_serie_a_odds():
    path = Path("data/raw/odds/soccer_italy_serie_a/odds_2025-11-25T02-49-30Z.json")
    if not path.exists():
        print(f"File not found: {path}")
        return

    with open(path, "r") as f:
        data = json.load(f)

    results = data.get("results", [])
    print(f"Total events: {len(results)}")
    
    targets = ["Como", "Sassuolo", "Genoa", "Verona", "Parma", "Udinese"]
    
    for event in results:
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        
        # Check if any target team is in this event
        if any(t in home or t in away for t in targets):
            print(f"Found Game: {away} @ {home}")
            print(f"  ID: {event.get('id')}")
            print(f"  Commence Time: {event.get('commence_time')}")
            
            bookmakers = event.get("bookmakers", [])
            print(f"  Bookmakers: {len(bookmakers)}")
            
            # Check for h2h odds
            has_h2h = False
            for book in bookmakers:
                for market in book.get("markets", []):
                    if market.get("key") == "h2h":
                        has_h2h = True
                        print(f"    Book: {book.get('title')} has h2h")
                        for outcome in market.get("outcomes", []):
                            print(f"      {outcome.get('name')}: {outcome.get('price')}")
                        break
                if has_h2h: break
            
            if not has_h2h:
                print("  NO h2h odds found!")

if __name__ == "__main__":
    check_serie_a_odds()
