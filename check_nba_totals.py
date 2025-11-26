import json
from pathlib import Path

# Check latest NBA odds
nba_file = sorted(Path("data/raw/odds/basketball_nba").glob("*.json"))[-1]
print(f"Checking: {nba_file.name}\n")

with open(nba_file) as f:
    data = json.load(f)

results = data.get("results", [])
print(f"Total NBA games: {len(results)}\n")

for game in results:
    print(f"{game['away_team']} @ {game['home_team']}")
    print(f"  Time: {game['commence_time']}")
    
    # Check markets
    has_h2h = False
    has_totals = False
    
    for bookmaker in game.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            market_key = market.get("key")
            if market_key == "h2h":
                has_h2h = True
            elif market_key == "totals":
                has_totals = True
                # Show first total line found
                outcomes = market.get("outcomes", [])
                if outcomes and not has_totals:
                    print(f"  Totals from {bookmaker.get('title')}:")
                    for outcome in outcomes[:2]:
                        print(f"    {outcome.get('name')}: {outcome.get('point')} @ {outcome.get('price')}")
                has_totals = True
    
    print(f"  Markets: h2h={'✓' if has_h2h else '✗'}, totals={'✓' if has_totals else '✗'}")
    
    # Show first totals line
    if has_totals:
        for bookmaker in game.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                if market.get("key") == "totals":
                    outcomes = market.get("outcomes", [])
                    if outcomes:
                        print(f"  Example total line ({bookmaker.get('title')}): {outcomes[0].get('point')}")
                        break
            if has_totals:
                break
    print()
