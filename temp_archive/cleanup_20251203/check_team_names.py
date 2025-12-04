import pandas as pd

# Check the predictions master file
df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')

# Check NFL games
nfl = df[df['league'] == 'NFL'].sort_values('commence_time', ascending=False).head(5)

print("Latest NFL predictions:")
for _, row in nfl.iterrows():
    print(f"{row['away_team']} @ {row['home_team']}")

print("\nChecking columns:")
print(df.columns.tolist()[:20])

print("\nChecking if team_code columns exist:")
print(f"home_team_code exists: {'home_team_code' in df.columns}")
print(f"away_team_code exists: {'away_team_code' in df.columns}")
