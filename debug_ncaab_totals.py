import json
from pathlib import Path
from datetime import datetime, timezone

def check_ncaab():
    print("=== NCAAB Games Check ===\n")
    
    # Check raw odds
    ncaab_dir = Path("data/raw/odds/basketball_ncaab")
    if not ncaab_dir.exists():
        print("No NCAAB odds directory found!")
        return
    
    files = sorted(ncaab_dir.glob("*.json"))
    if not files:
        print("No NCAAB odds files found!")
        return
    
    latest_file = files[-1]
    print(f"Latest file: {latest_file.name}\n")
    
    with open(latest_file, "r") as f:
        data = json.load(f)
    
    results = data if isinstance(data, list) else data.get("results", [])
    print(f"Total NCAAB games: {len(results)}\n")
    
    now = datetime.now(timezone.utc)
    
    upcoming_count = 0
    for game in results[:10]:  # Check first 10 games
        commence_time = game.get("commence_time", "")
        game_time = None
        time_diff = 0
        
        try:
            game_time = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
            time_diff = (game_time - now).total_seconds() / 3600
        except:
            continue
        
        if time_diff > 0:  # Only upcoming games
                upcoming_count += 1
                print(f"{game.get('away_team')} @ {game.get('home_team')}")
                print(f"  Time: {commence_time} ({time_diff:.1f} hours from now)")
                
                # Check for totals in bookmakers
                has_totals = False
                bookmakers = game.get("bookmakers", [])
                print(f"  Bookmakers: {len(bookmakers)}")
                
                for bookmaker in bookmakers:
                    for market in bookmaker.get("markets", []):
                        if market.get("key") == "totals":
                            has_totals = True
                            outcomes = market.get("outcomes", [])
                            if outcomes:
                                print(f"    {bookmaker.get('title')}: Totals market found")
                                for outcome in outcomes[:2]:
                                    print(f"      {outcome.get('name')}: {outcome.get('point')} @ {outcome.get('price')}")
                            break
                    if has_totals:
                        break
                
                if not has_totals:
                    print("    ⚠️ NO TOTALS DATA FOUND")
                print()
    
    print(f"Total upcoming games checked: {upcoming_count}")

if __name__ == "__main__":
    check_ncaab()
