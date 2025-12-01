import pandas as pd
from datetime import datetime, timezone
import shutil
from pathlib import Path

def fix_versions():
    file_path = Path("data/forward_test/ensemble/predictions_master.parquet")
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    # Backup
    backup_path = file_path.with_suffix(".parquet.bak")
    shutil.copy(file_path, backup_path)
    print(f"Backed up to {backup_path}")

    df = pd.read_parquet(file_path)
    print(f"Loaded {len(df)} rows")

    # Ensure commence_time is datetime
    df["commence_time"] = pd.to_datetime(df["commence_time"], utc=True)
    
    # Backfill predicted_at with commence_time if missing
    if "predicted_at" not in df.columns:
        df["predicted_at"] = df["commence_time"]
    else:
        df["predicted_at"] = pd.to_datetime(df["predicted_at"], utc=True)
        df["predicted_at"] = df["predicted_at"].fillna(df["commence_time"])

    def get_version(dt):
        if pd.isna(dt):
            return "unknown"
        if dt >= pd.Timestamp("2025-11-21", tz="UTC"):
            return "v0.3"
        elif dt >= pd.Timestamp("2025-11-14", tz="UTC"):
            return "v0.2"
        elif dt >= pd.Timestamp("2025-11-03", tz="UTC"):
            return "v0.1"
        else:
            return "pre-v0.1"

    # Apply version fix
    df["version"] = df["commence_time"].apply(get_version)

    # Save back
    df.to_parquet(file_path, index=False)
    print("Saved fixed versions")
    
    # Verify
    print("\nVersion counts after fix:")
    print(df["version"].value_counts())

if __name__ == "__main__":
    fix_versions()
