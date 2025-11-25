"""
Fix predicted_at timestamps for restored predictions.
Set predicted_at to None so dashboard uses commence_time for versioning.
"""
import pandas as pd
from pathlib import Path

def fix_predicted_at():
    model_types = ['ensemble', 'random_forest', 'gradient_boosting']
    
    for model_type in model_types:
        master_path = Path(f"data/forward_test/{model_type}/predictions_master.parquet")
        
        if not master_path.exists():
            continue
        
        print(f"\nProcessing {model_type}...")
        df = pd.read_parquet(master_path)
        
        if 'predicted_at' in df.columns:
            print(f"  Dropping predicted_at column")
            df = df.drop(columns=['predicted_at'])
        
        df.to_parquet(master_path, index=False)
        print(f"  ✓ Saved")

if __name__ == "__main__":
    fix_predicted_at()
    print("\n✓ Fixed! Dashboard will now use commence_time for versioning.")
