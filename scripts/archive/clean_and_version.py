"""
Clean up predictions to remove games before user started (Nov 3, 2025)
and add version tracking (v0.1, v0.2, v0.3).
"""
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

def clean_and_version_predictions():
    """
    Remove games before Nov 3, 2025 and add version field.
    Uses dates from config/versions.yml:
    v0.1: Nov 3-13, 2025 (initial version)
    v0.2: Nov 14-20, 2025 (model improvements) 
    v0.3: Nov 21+, 2025 (full team names version)
    
    Uses commence_time for versioning (not predicted_at).
    """
    
    model_types = ['ensemble', 'random_forest', 'gradient_boosting']
    
    # Define version cutoffs from config/versions.yml
    v0_3_start = pd.Timestamp('2025-11-21', tz='UTC')
    v0_2_start = pd.Timestamp('2025-11-14', tz='UTC')
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
        
        initial_count = len(df)
        print(f"  Initial predictions: {initial_count}")
        
        # Remove games before Nov 3
        old_games = df[df['commence_time'] < user_start]
        print(f"  Games before Nov 3: {len(old_games)}")
        
        df = df[df['commence_time'] >= user_start]
        print(f"  After filtering: {len(df)}")
        
        # Add version field based on commence_time
        df['version'] = 'v0.1'  # Default
        df.loc[df['commence_time'] >= v0_2_start, 'version'] = 'v0.2'
        df.loc[df['commence_time'] >= v0_3_start, 'version'] = 'v0.3'
        
        # Count by version
        version_counts = df['version'].value_counts()
        print(f"  Version breakdown:")
        for version in ['v0.1', 'v0.2', 'v0.3']:
            count = version_counts.get(version, 0)
            if count > 0:
                print(f"    {version}: {count}")
        
        # Save
        df.to_parquet(master_path, index=False)
        print(f"  ✓ Saved cleaned predictions with versioning")
    
    print("\n✓ Cleanup complete!")

if __name__ == "__main__":
    clean_and_version_predictions()
