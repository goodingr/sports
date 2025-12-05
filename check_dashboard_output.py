import pandas as pd
from src.dashboard.data import load_forward_test_data, get_overunder_completed

# Load the raw data
df = load_forward_test_data(model_type="ensemble")

# Get completed (including ongoing) games
completed = get_overunder_completed(df, edge_threshold=0.06, stake=100.0)

# Filter for Green Bay Phoenix
green_bay = completed[
    (completed["home_team"].str.contains("Green Bay", case=False, na=False)) | 
    (completed["away_team"].str.contains("Green Bay", case=False, na=False))
]

if not green_bay.empty:
    print("Green Bay games in completed bets:")
    for _, row in green_bay.iterrows():
        print(f"\nGame: {row['home_team']} vs {row['away_team']}")
        print(f"  Side: {row['side']}")
        print(f"  Description: {row.get('description')}")
        print(f"  Won: {row.get('won')}")
        print(f"  Won type: {type(row.get('won'))}")
        print(f"  Profit: {row.get('profit')}")
        print(f"  Profit type: {type(row.get('profit'))}")
        print(f"  Result: {row.get('result')}")
else:
    print("No Green Bay games found in completed bets")
