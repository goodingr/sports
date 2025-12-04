import pandas as pd

def check_versions():
    try:
        df = pd.read_parquet("data/forward_test/ensemble/predictions_master.parquet")
        print("Version counts in parquet:")
        print(df["version"].value_counts())
        
        print("\nChecking predicted_at:")
        if "predicted_at" in df.columns:
            print("predicted_at exists.")
            print(df["predicted_at"].head())
            print(f"Null count: {df['predicted_at'].isna().sum()}")
            
            # Check range
            df["predicted_at"] = pd.to_datetime(df["predicted_at"], utc=True)
            print(f"Min predicted_at: {df['predicted_at'].min()}")
            print(f"Max predicted_at: {df['predicted_at'].max()}")
        else:
            print("predicted_at column MISSING.")
            
        print("\nChecking commence_time:")
        df["commence_time"] = pd.to_datetime(df["commence_time"], utc=True)
        print(f"Min commence_time: {df['commence_time'].min()}")
        print(f"Max commence_time: {df['commence_time'].max()}")
        
    except Exception as e:
        print(f"Error reading parquet: {e}")

if __name__ == "__main__":
    check_versions()
