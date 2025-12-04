"""
Remove future completed games that were incorrectly restored.
Only keep completed games from before today (Nov 25, 2025).
"""
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

def fix_future_completed():
    model_types = ['ensemble', 'random_forest', 'gradient_boosting']
    now = pd.Timestamp('2025-11-25', tz='UTC')
    
    for model_type in model_types:
        master_path = Path(f"data/forward_test/{model_type}/predictions_master.parquet")
        
        if not master_path.exists():
            continue
        
        print(f"\nProcessing {model_type}...")
        df = pd.read_parquet(master_path)
        df['commence_time'] = pd.to_datetime(df['commence_time'], utc=True)
        
        initial = len(df)
        completed = df['result'].notna().sum()
        
        # Find future games marked as completed
        future_completed = df[(df['commence_time'] > now) & (df['result'].notna())]
        print(f"  Total: {initial}")
        print(f"  Completed: {completed}")
        print(f"  Future games with results (BAD): {len(future_completed)}")
        
        # Clear results for future games
        df.loc[df['commence_time'] > now, 'result'] = pd.NA
        df.loc[df['commence_time'] > now, 'home_score'] = pd.NA
        df.loc[df['commence_time'] > now, 'away_score'] = pd.NA
        df.loc[df['commence_time'] > now, 'result_updated_at'] = pd.NaT
        
        final_completed = df['result'].notna().sum()
        print(f"  Completed after fix: {final_completed}")
        print(f"  Removed: {completed - final_completed} future results")
        
        df.to_parquet(master_path, index=False)
        print(f"  ✓ Saved")

if __name__ == "__main__":
    fix_future_completed()
    print("\n✓ Fixed! Future games no longer have results.")
