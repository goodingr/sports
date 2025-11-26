import json
from pathlib import Path
from datetime import datetime, timezone
import pytz

est = pytz.timezone('US/Eastern')
nba_file = sorted(Path('data/raw/odds/basketball_nba').glob('*.json'))[-1]

with open(nba_file) as f:
    data = json.load(f)

now = datetime.now(timezone.utc)
print(f"Current time EST: {now.astimezone(est).strftime('%b %d %I:%M %p %Z')}\n")

print("NBA games in raw odds:")
for g in data.get('results', []):
    game_time_utc = datetime.fromisoformat(g['commence_time'].replace('Z', '+00:00'))
    game_time_est = game_time_utc.astimezone(est)
    print(f"  {game_time_est.strftime('%b %d %I:%M %p %Z')}: {g['away_team']} @ {g['home_team']}")

params = data.get('request', {}).get('params', {})
print(f"\nRequest date range:")
print(f"  From: {params.get('commenceTimeFrom')}")
print(f"  To: {params.get('commenceTimeTo')}")
print(f"\nTotal games returned: {len(data.get('results', []))}")
