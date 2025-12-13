import pandas as pd

# Read the parquet file
df = pd.read_parquet('data/forward_test/gradient_boosting/predictions_master.parquet')

print(f'Total rows: {len(df)}')
print(f'Has predicted_total_points: {(~df["predicted_total_points"].isna()).sum()}')
print(f'Sample of predicted_total_points:')
print(df[['game_id', 'predicted_total_points', 'total_line']].head(10))
