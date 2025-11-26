import pandas as pd

# Check all model types for completed games
model_types = ['ensemble', 'random_forest', 'gradient_boosting']

for model_type in model_types:
    path = f'data/forward_test/{model_type}/predictions_master.parquet'
    try:
        df = pd.read_parquet(path)
        completed = df['result'].notna().sum()
        print(f"{model_type}: {completed} completed / {len(df)} total")
    except Exception as e:
        print(f"{model_type}: Error - {e}")
