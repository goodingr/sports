import pandas as pd
from pathlib import Path

path = Path('data/forward_test/ensemble/predictions_master.parquet')
df = pd.read_parquet(path)

# Filter for completed games
completed = df[df['result'].notna()].copy()
completed['result_updated_at'] = pd.to_datetime(completed['result_updated_at'])
completed['date'] = completed['result_updated_at'].dt.date

# Check edge column
if 'edge' not in completed.columns:
    # Calculate edge if missing (simplified)
    # edge = prob - implied_prob
    # implied_prob = 1 / decimal_odds
    pass

print("Columns:", completed.columns)

# Filter for edge > 0 (default threshold)
if 'edge' in completed.columns:
    recommended = completed[completed['edge'] > 0]
    print(f"\nTotal recommended bets (edge > 0): {len(recommended)}")
    print("\nRecommended bets by date:")
    print(recommended['date'].value_counts().sort_index())
else:
    print("\n'edge' column not found in predictions")
