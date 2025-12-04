import pandas as pd
from pathlib import Path

path = Path('data/forward_test/ensemble/predictions_master.parquet')
df = pd.read_parquet(path)

# Filter for completed games
completed = df[df['result'].notna()].copy()

# Convert timestamps
completed['result_updated_at'] = pd.to_datetime(completed['result_updated_at'])
completed['date'] = completed['result_updated_at'].dt.date

print(f"Total completed games: {len(completed)}")
print("\nCompleted games by date:")
print(completed['date'].value_counts().sort_index())

# Check for games settled on Nov 22
nov22 = completed[completed['date'] == pd.to_datetime('2025-11-22').date()]
print(f"\nGames settled on Nov 22: {len(nov22)}")
if not nov22.empty:
    print(nov22[['game_id', 'league', 'home_team', 'away_team', 'result_updated_at']].head())
