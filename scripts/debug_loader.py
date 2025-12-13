
import sys
from pathlib import Path
import pandas as pd
import logging

sys.path.append(str(Path.cwd()))
from src.models.feature_loader import FeatureLoader

logging.basicConfig(level=logging.DEBUG)

def test_loader():
    league = "NBA"
    print(f"Testing FeatureLoader for {league}...")
    loader = FeatureLoader(league)
    
    # Test 1: Load Raw
    df = loader.load_rolling_metrics()
    print(f"Load Result: {len(df)} rows")
    if not df.empty:
        print("Columns:", df.columns.tolist())
        print("Sample:", df.head(1).to_dict("records"))
        
        # Check team column normalization
        print("Teams:", df["team"].unique()[:5])
        
        # Check date parsing
        if "game_date" in df.columns:
            print("Date Type:", df["game_date"].dtype)
            print("First Date:", df["game_date"].iloc[0])

    # Test 2: Get Rolling Metric
    # Pick a team that we know exists
    team = "MIA" # Miami Heat
    metric = "rolling_pace_20" # Common metric
    
    val = loader.get_rolling_metric(team, metric)
    print(f"Rolling Metric ({team}, {metric}): {val}")

if __name__ == "__main__":
    test_loader()
