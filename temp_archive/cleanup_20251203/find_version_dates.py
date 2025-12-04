import pandas as pd

df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')

# Convert timestamps
df['predicted_at'] = pd.to_datetime(df['predicted_at'], utc=True)
df['commence_time'] = pd.to_datetime(df['commence_time'], utc=True)

# Find date ranges for each version
print("Version date ranges based on predicted_at timestamps:")
print("=" * 60)

for version in ['v1', 'v2', 'v3']:
    version_df = df[df['version'] == version]
    if len(version_df) > 0:
        # Use predicted_at if available, otherwise use commence_time
        if version_df['predicted_at'].notna().any():
            timestamps = version_df['predicted_at'].dropna()
        else:
            timestamps = version_df['commence_time']
        
        earliest = timestamps.min()
        latest = timestamps.max()
        count = len(version_df)
        
        print(f"\n{version}:")
        print(f"  Count: {count}")
        print(f"  Earliest: {earliest}")
        print(f"  Latest: {latest}")
        
        # Show sample games
        print(f"  Sample games:")
        sample = version_df.sort_values('predicted_at' if 'predicted_at' in df.columns else 'commence_time').head(3)
        for _, row in sample.iterrows():
            pred_time = row.get('predicted_at', row['commence_time'])
            print(f"    {pred_time}: {row['away_team']} @ {row['home_team']}")
