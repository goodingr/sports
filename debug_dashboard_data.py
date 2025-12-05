import pandas as pd
from src.dashboard.data import load_forward_test_data, _expand_totals

# Load the raw data
df = load_forward_test_data(model_type="ensemble")

# Find Green Bay game
green_bay_raw = df[
    (df["home_team"].str.contains("Green Bay", case=False, na=False)) | 
    (df["away_team"].str.contains("Green Bay", case=False, na=False))
]

print("RAW DATA:")
for _, row in green_bay_raw.iterrows():
    print(f"\nGame: {row['home_team']} vs {row['away_team']}")
    print(f"  Home Score: {row.get('home_score')}")
    print(f"  Away Score: {row.get('away_score')}")
    print(f"  Result: {row.get('result')}")
    print(f"  Result type: {type(row.get('result'))}")
    print(f"  pd.notna(result): {pd.notna(row.get('result'))}")

# Now expand it
if not green_bay_raw.empty:
    expanded = _expand_totals(green_bay_raw)
    print("\n\nEXPANDED DATA:")
    for _, row in expanded.iterrows():
        print(f"\nSide: {row['side']}, Line: {row.get('total_line')}")
        print(f"  Won: {row.get('won')}")
        print(f"  Profit: {row.get('profit')}")
        print(f"  Result from source: {row.get('result')}")
