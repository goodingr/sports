"""Test what the Odds API returns without date filters"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("ODDS_API_KEY")
url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"

# Try without date filters
params = {
    "apiKey": api_key,
    "regions": "us",
    "markets": "h2h,totals",
    "oddsFormat": "american",
    "dateFormat": "iso",
}

print("Requesting NBA odds WITHOUT date filters...")
print(f"URL: {url}")
print(f"Params: {params}\n")

response = requests.get(url, params=params, timeout=10)
response.raise_for_status()

data = response.json()
print(f"Total games returned: {len(data)}")

# Show game dates
from datetime import datetime
import pytz
est = pytz.timezone('US/Eastern')

dates = {}
for game in data:
    game_time = datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00'))
    game_time_est = game_time.astimezone(est)
    date_key = game_time_est.strftime('%b %d')
    if date_key not in dates:
        dates[date_key] = []
    dates[date_key].append(f"{game['away_team']} @ {game['home_team']}")

print("\nGames by date:")
for date in sorted(dates.keys()):
    print(f"\n{date} ({len(dates[date])} games):")
    for game in dates[date]:
        print(f"  - {game}")
