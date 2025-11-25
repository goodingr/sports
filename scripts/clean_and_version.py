"""
Clean up predictions to remove games before user started (Nov 3, 2025)
and add version tracking (v1, v2, v3).
"""
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

def clean_and_version_predictions():
    """
    Remove games before Nov 3, 2025 and add version field.
    v1: Started Nov 3
    v2: Model improvements (define date if known)
    v3: Started 3-4 days ago (Nov 21-22, 2025)
    """
    
    model_types = ['ensemble', 'random_forest', 'gradient_boosting']
    
    # Define version cutoffs
    # v3 started around Nov 21-22 (3-4 days ago from Nov 25)
    v3_start = pd.Timestamp('2025-11-21', tz='UTC')
    v2_start = pd.Timestamp('2025-11-10', tz='UTC')  # Adjust if you have a specific date
    user_start = pd.Timestamp('2025-11-03', tz='UTC')
    
    for model_type in model_types:
        master_path = Path(f"data/forward_test/{model_type}/predictions_master.parquet")
        
        if not master_path.exists():
            print(f"Skipping {model_type} - file not found")
            continue
        
        print(f"\nProcessing {model_type}...")
        df = pd.read_parquet(master_path)
        
        # Convert timestamps
        df['commence_time'] = pd.to_datetime(df['commence_time'], utc=True)
        if 'predicted_at' in df.columns:
            df['predicted_at'] = pd.to_datetime(df['predicted_at'], utc=True)
        
        initial_count = len(df)
        print(f"  Initial predictions: {initial_count}")
        
        # Remove games before Nov 3
        old_games = df[df['commence_time'] < user_start]
        print(f"  Games before Nov 3: {len(old_games)}")
        
        df = df[df['commence_time'] >= user_start]
        print(f"  After filtering: {len(df)}")
        
        # Add version field based on predicted_at or commence_time
        # Use predicted_at if available, otherwise use commence_time as approximation
        timestamp_col = 'predicted_at' if 'predicted_at' in df.columns else 'commence_time'
        
        df['version'] = 'v1'  # Default
        df.loc[df[timestamp_col] >= v2_start, 'version'] = 'v2'
        df.loc[df[timestamp_col] >= v3_start, 'version'] = 'v3'
        
        # Count by version
        version_counts = df['version'].value_counts()
        print(f"  Version breakdown:")
        for version in ['v1', 'v2', 'v3']:
            count = version_counts.get(version, 0)
            if count > 0:
                print(f"    {version}: {count}")
        
        # Save
        df.to_parquet(master_path, index=False)
        print(f"  ✓ Saved cleaned predictions with versioning")
    
    print("\n✓ Cleanup complete!")

if __name__ == "__main__":
    clean_and_version_predictions()
