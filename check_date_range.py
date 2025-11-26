import pandas as pd

df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')
df['commence_time'] = pd.to_datetime(df['commence_time'])

print('Date range:')
print(f'Oldest: {df["commence_time"].min()}')
print(f'Newest: {df["commence_time"].max()}')

print(f'\nGames before Nov 3: {len(df[df["commence_time"] < "2025-11-03"])}')
print(f'Games after Nov 3: {len(df[df["commence_time"] >= "2025-11-03"])}')

# Show the October game
oct_games = df[df["commence_time"] < "2025-11-03"]
if len(oct_games) > 0:
    print(f'\nSample old games:')
    print(oct_games[['commence_time', 'league', 'home_team', 'away_team', 'result']].head(5).to_string())
