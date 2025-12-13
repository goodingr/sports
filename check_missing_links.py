import pandas as pd
from src.api.routes.bets import get_totals_data

# Get the data that the API returns
df = get_totals_data(model_type="ensemble")

# Filter for the games mentioned
bayer = df[
    (df["home_team"].str.contains("Bayer Leverkusen", case=False, na=False)) | 
    (df["away_team"].str.contains("Bayer Leverkusen", case=False, na=False))
]

ohio = df[
    (df["home_team"].str.contains("Ohio State", case=False, na=False)) | 
    (df["away_team"].str.contains("Ohio State", case=False, na=False))
]

print("=== BAYER LEVERKUSEN GAME ===")
if not bayer.empty:
    for _, row in bayer.iterrows():
        print(f"Game ID: {row['game_id']}")
        print(f"Teams: {row['home_team']} vs {row['away_team']}")
        print(f"Side: {row['side']}")
        print(f"Book: '{row.get('book', '')}'")
        print(f"Book URL: '{row.get('book_url', '')}'")
        print(f"Status: {row.get('status')}")
        print()
else:
    print("Not found in API data")

print("\n=== OHIO STATE GAME ===")
if not ohio.empty:
    for _, row in ohio.iterrows():
        print(f"Game ID: {row['game_id']}")
        print(f"Teams: {row['home_team']} vs {row['away_team']}")
        print(f"Side: {row['side']}")
        print(f"Book: '{row.get('book', '')}'")
        print(f"Book URL: '{row.get('book_url', '')}'")
        print(f"Status: {row.get('status')}")
        print()
else:
    print("Not found in API data")
