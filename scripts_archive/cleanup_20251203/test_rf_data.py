import pandas as pd

try:
    df = pd.read_parquet('data/forward_test/random_forest/predictions_master.parquet')
    print(f'SUCCESS: Loaded {len(df)} predictions')
    print(f'Has data: {not df.empty}')
    print(f'Columns: {list(df.columns)[:15]}')
    if not df.empty:
        print(f'\nFirst row sample:')
        sample = df.head(1).iloc[0]
        for col in ['game_id', 'league', 'home_team', 'away_team', 'home_predicted_prob', 'result']:
            if col in df.columns:
                print(f'  {col}: {sample[col]}')
except Exception as e:
    print(f'ERROR: {e}')
    import traceback
    traceback.print_exc()
