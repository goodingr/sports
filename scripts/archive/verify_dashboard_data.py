import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.append(str(Path.cwd()))

from src.dashboard.data import load_forward_test_data, compare_model_predictions

def test_load_data():
    print("Testing load_forward_test_data...")
    
    # Test Ensemble
    print("  Loading Ensemble...")
    df_ensemble = load_forward_test_data(model_type="ensemble")
    if not df_ensemble.empty:
        print(f"    Success: Loaded {len(df_ensemble)} rows")
        if "predicted_at" in df_ensemble.columns:
            min_date = df_ensemble["predicted_at"].min()
            max_date = df_ensemble["predicted_at"].max()
            print(f"    Date Range: {min_date} to {max_date}")
    else:
        print("    Warning: Ensemble data empty (might be expected if no predictions yet)")

    # Test Random Forest
    print("  Loading Random Forest...")
    df_rf = load_forward_test_data(model_type="random_forest")
    if not df_rf.empty:
        print(f"    Success: Loaded {len(df_rf)} rows")
    else:
        print("    Warning: Random Forest data empty")

    # Test Gradient Boosting
    print("  Loading Gradient Boosting...")
    df_gb = load_forward_test_data(model_type="gradient_boosting")
    if not df_gb.empty:
        print(f"    Success: Loaded {len(df_gb)} rows")
    else:
        print("    Warning: Gradient Boosting data empty")

def test_comparison():
    print("\nTesting compare_model_predictions...")
    
    # Test comparison for NBA
    print("  Comparing models for NBA...")
    df_compare = compare_model_predictions(league="NBA")
    
    if not df_compare.empty:
        print(f"    Success: Comparison dataframe has {len(df_compare)} rows")
        print(f"    Columns: {df_compare.columns.tolist()}")
        
        # Check for expected columns
        expected_cols = ["ensemble_home_prob", "random_forest_home_prob", "gradient_boosting_home_prob"]
        found_cols = [c for c in expected_cols if c in df_compare.columns]
        print(f"    Found model columns: {found_cols}")
    else:
        print("    Warning: Comparison dataframe empty")

if __name__ == "__main__":
    test_load_data()
    test_comparison()
