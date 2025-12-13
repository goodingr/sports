import pandas as pd

# Check what's in the gradient_boosting parquet for NCAAB Dec 5
df = pd.read_parquet('data/forward_test/gradient_boosting/predictions_master.parquet')

# Filter to NCAAB games around Dec 5
ncaab_games = df[df['game_id'].str.contains('NCAAB', na=False)]
print(f"Total NCAAB games in parquet: {len(ncaab_games)}")
print(f"With predicted_total_points: {(~ncaab_games['predicted_total_points'].isna()).sum()}")

# Sample
print("\nSample NCAAB games:")
print(ncaab_games[['game_id', 'total_line', 'predicted_total_points']].head(10))
