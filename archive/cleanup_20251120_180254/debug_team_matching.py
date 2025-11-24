"""Debug team name matching in odds loading."""

from src.db.core import connect
from src.data.load_espn_odds import _convert_espn_csv_to_payload
from pathlib import Path
import json

# Load a sample ESPN CSV and convert to payload
csv_path = Path('data/raw/sources/nba/espn_odds/2025-11-03T20-11-47Z/odds.csv')
payload = _convert_espn_csv_to_payload(csv_path, 'nba')

print("Sample event from payload:")
if payload["results"]:
    event = payload["results"][0]
    print(f"Event ID: {event.get('id')}")
    print(f"Home Team: {event.get('home_team')}")
    print(f"Away Team: {event.get('away_team')}")
    print(f"\nBookmakers:")
    for bookmaker in event.get("bookmakers", []):
        print(f"  Bookmaker: {bookmaker.get('title')}")
        for market in bookmaker.get("markets", []):
            if market.get("key") == "h2h":
                print(f"    Market: {market.get('key')}")
                for outcome in market.get("outcomes", []):
                    print(f"      Outcome: name={outcome.get('name')}, price={outcome.get('price')}")
    
    # Now check what happens in load_odds_snapshot
    print(f"\n\nTeam matching logic:")
    home_name = event.get("home_team", "")
    away_name = event.get("away_team", "")
    print(f"home_name: '{home_name}'")
    print(f"away_name: '{away_name}'")
    
    for bookmaker in event.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            if market.get("key") == "h2h":
                for outcome in market.get("outcomes", []):
                    outcome_name = outcome.get("name", "").strip()
                    print(f"\n  Outcome name: '{outcome_name}'")
                    print(f"    Matches home? {outcome_name.lower() == home_name.lower()}")
                    print(f"    Matches away? {outcome_name.lower() == away_name.lower()}")

