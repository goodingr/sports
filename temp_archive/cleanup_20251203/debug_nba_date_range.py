import json
from pathlib import Path
from datetime import datetime, timezone

# Check NBA odds
nba_file = sorted(Path("data/raw/odds/basketball_nba").glob("*.json"))[-1]
print(f"Latest NBA file: {nba_file.name}\n")

with open(nba_file) as f:
    data = json.load(f)

params = data.get("request", {}).get("params", {})
print("Request parameters:")
print(f"  commenceTimeFrom: {params.get('commenceTimeFrom')}")
print(f"  commenceTimeTo: {params.get('commenceTimeTo')}")
print(f"  markets: {params.get('markets')}")
print(f"  regions: {params.get('regions')}")

results = data.get("results", [])
print(f"\nTotal games returned: {len(results)}")

# Group by date
now = datetime.now(timezone.utc)
dates = {}
for game in results:
    try:
        game_time = datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00'))
        if game_time > now:
            date = game_time.date()
            if date not in dates:
                dates[date] = []
            dates[date].append(f"{game['away_team']} @ {game['home_team']}")
    except:
        pass

print("\nUpcoming games by date:")
for date in sorted(dates.keys()):
    print(f"\n{date} ({len(dates[date])} games):")
    for game in dates[date]:
        print(f"  - {game}")
