
import pandas as pd
from pathlib import Path

def compare_models():
    base_dir = Path("data/forward_test")
    rf_path = base_dir / "random_forest" / "predictions_master.parquet"
    gb_path = base_dir / "gradient_boosting" / "predictions_master.parquet"
    
    if not rf_path.exists() or not gb_path.exists():
        print("One or both prediction files missing.")
        return

    df_rf = pd.read_parquet(rf_path)
    df_gb = pd.read_parquet(gb_path)
    
    # Filter for recent games to avoid noise from old versions if any
    # But user said "completed bets", so let's check everything or a sample.
    
    # Merge on game_id to compare
    merged = df_rf.merge(df_gb, on="game_id", suffixes=("_rf", "_gb"))
    
    print(f"Total common games: {len(merged)}")
    
    # Compare predicted probabilities
    # Assuming columns like 'over_predicted_prob', 'under_predicted_prob'
    
    prob_cols = [c for c in df_rf.columns if "prob" in c]
    print(f"Comparing columns: {prob_cols}")
    
    for col in prob_cols:
        rf_col = f"{col}_rf"
        gb_col = f"{col}_gb"
        
        if rf_col not in merged.columns or gb_col not in merged.columns:
            continue
            
        diff = (merged[rf_col] - merged[gb_col]).abs()
        mean_diff = diff.mean()
        max_diff = diff.max()
        exact_matches = (diff == 0).sum()
        
        print(f"\nColumn: {col}")
        print(f"Mean difference: {mean_diff:.6f}")
        print(f"Max difference:  {max_diff:.6f}")
        print(f"Exact matches:   {exact_matches} / {len(merged)} ({exact_matches/len(merged):.2%})")
        
        if max_diff < 1e-9:
            print("-> IDENTICAL")
        else:
            print("-> DIFFERENT")

if __name__ == "__main__":
    compare_models()
