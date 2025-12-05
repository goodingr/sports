import pandas as pd

# Load the parquet file
df = pd.read_parquet("data/forward_test/ensemble/predictions_master.parquet")

# Filter for Green Bay Phoenix game
green_bay = df[
    (df["home_team"].str.contains("Green Bay", case=False, na=False)) | 
    (df["away_team"].str.contains("Green Bay", case=False, na=False))
]

if not green_bay.empty:
    print("Found Green Bay game(s):")
    for _, row in green_bay.iterrows():
        print(f"\nGame ID: {row['game_id']}")
        print(f"Teams: {row['home_team']} vs {row['away_team']}")
        print(f"Commence Time: {row.get('commence_time')}")
        print(f"Home Score: {row.get('home_score')}")
        print(f"Away Score: {row.get('away_score')}")
        print(f"Result: {row.get('result')}")
        print(f"Result column type: {type(row.get('result'))}")
        print(f"Result is null: {pd.isna(row.get('result'))}")
else:
    print("No Green Bay game found")
