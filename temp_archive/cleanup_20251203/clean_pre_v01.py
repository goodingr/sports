import pandas as pd
from pathlib import Path

def clean_pre_v01():
    model_types = ['ensemble', 'random_forest', 'gradient_boosting']
    start_date = pd.Timestamp('2025-11-03', tz='UTC')
    
    for model_type in model_types:
        path = Path(f"data/forward_test/{model_type}/predictions_master.parquet")
        if not path.exists():
            continue
            
        print(f"Processing {model_type}...")
        df = pd.read_parquet(path)
        df['commence_time'] = pd.to_datetime(df['commence_time'], utc=True)
        
        initial = len(df)
        df = df[df['commence_time'] >= start_date]
        removed = initial - len(df)
        
        print(f"  Removed {removed} pre-v0.1 games")
        print(f"  Remaining: {len(df)}")
        
        df.to_parquet(path, index=False)

if __name__ == "__main__":
    clean_pre_v01()
