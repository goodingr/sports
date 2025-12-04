import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.append(str(Path.cwd()))

from src.dashboard.data import load_forward_test_data, filter_by_version

def debug_rf_versions():
    print("Loading Random Forest data...")
    df = load_forward_test_data(model_type="random_forest")
    
    print(f"Total rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    
    if "version" in df.columns:
        print("\nVersion distribution:")
        print(df["version"].value_counts())
        
        print("\nDate range by version:")
        for version in df["version"].unique():
            version_df = df[df["version"] == version]
            if "predicted_at" in version_df.columns:
                min_date = version_df["predicted_at"].min()
                max_date = version_df["predicted_at"].max()
                print(f"  {version}: {min_date} to {max_date} ({len(version_df)} rows)")
    else:
        print("No version column found!")
        
    if "predicted_at" in df.columns:
        print("\nOverall date range:")
        print(f"  Min: {df['predicted_at'].min()}")
        print(f"  Max: {df['predicted_at'].max()}")
        
    # Test version filtering
    print("\n\nTesting filter_by_version:")
    for version in ["v0.1", "v0.2", "v0.3", "all"]:
        filtered = filter_by_version(df, version)
        print(f"  {version}: {len(filtered)} rows")

if __name__ == "__main__":
    debug_rf_versions()
