import pandas as pd
from src.api.routes.bets import get_totals_data

# Get the API data
df = get_totals_data(model_type="ensemble")

# Find the Ohio State vs Indiana game
ohio_indiana = df[df['game_id'] == 'CFB_401520156']

if not ohio_indiana.empty:
    print("Game FOUND in API response!")
    for _, row in ohio_indiana.iterrows():
        print(f"\nSide: {row['side']}")
        print(f"  Line: {row.get('total_line')}")
        print(f"  Book: '{row.get('book', 'MISSING')}'")
        print(f"  Book URL: '{row.get('book_url', 'MISSING')}'")
        print(f"  Moneyline: {row.get('moneyline')}")
else:
    print("Game NOT found in API response")
    print(f"\nTotal games in API: {len(df)}")
    print(f"Games with book data: {df['book'].notna().sum()}")
