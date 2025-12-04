import pandas as pd
from pathlib import Path

def update_versions():
    model_types = ['ensemble', 'random_forest', 'gradient_boosting']
    
    # Version config
    v1_start = pd.Timestamp('2025-11-03', tz='UTC')
    v2_start = pd.Timestamp('2025-11-14', tz='UTC')
    v3_start = pd.Timestamp('2025-11-21', tz='UTC')
    
    for model_type in model_types:
        path = Path(f"data/forward_test/{model_type}/predictions_master.parquet")
        if not path.exists():
            continue
            
        print(f"Processing {model_type}...")
        df = pd.read_parquet(path)
        
        # Ensure commence_time is datetime
        df['commence_time'] = pd.to_datetime(df['commence_time'], utc=True)
        
        # Drop predicted_at if it exists (sanity check)
        if 'predicted_at' in df.columns:
            print("  Dropping predicted_at column")
            df = df.drop(columns=['predicted_at'])
            
        # Recalculate version
        def get_version(row):
            ts = row['commence_time']
            if ts >= v3_start:
                return 'v0.3'
            elif ts >= v2_start:
                return 'v0.2'
            elif ts >= v1_start:
                return 'v0.1'
            else:
                return 'pre-v0.1'
                
        df['version'] = df.apply(get_version, axis=1)
        
        # Filter out pre-v0.1 just in case
        df = df[df['version'] != 'pre-v0.1']
        
        print("  New version counts:")
        print(df['version'].value_counts().sort_index())
        
        df.to_parquet(path, index=False)

if __name__ == "__main__":
    update_versions()
