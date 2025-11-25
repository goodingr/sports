"""
Safe script to remove predictions with abbreviated team names.
This version PRESERVES completed games with results.
"""
import pandas as pd
from pathlib import Path

def safe_remove_abbreviations():
    model_types = ['ensemble', 'random_forest', 'gradient_boosting']
    
    for model_type in model_types:
        master_path = Path(f"data/forward_test/{model_type}/predictions_master.parquet")
        
        if not master_path.exists():
            print(f"Skipping {model_type} - file not found")
            continue
        
        print(f"\nProcessing {model_type}...")
        df = pd.read_parquet(master_path)
        
        initial_count = len(df)
        print(f"  Initial predictions: {initial_count}")
        
        # Identify abbreviations
        has_abbrev = (df['home_team'].str.len() <= 3) | (df['away_team'].str.len() <= 3)
        abbrev_count = has_abbrev.sum()
        print(f"  Abbreviations: {abbrev_count}")
        
        # CRITICAL: Only remove abbreviations that DON'T have results
        has_result = df['result'].notna()
        completed_abbrevs = (has_abbrev & has_result).sum()
        
        print(f"  Completed games with abbreviations: {completed_abbrevs}")
        print(f"  ** PRESERVING completed games **")
        
        # Remove only upcoming games with abbreviations
        to_remove = has_abbrev & ~has_result
        df_clean = df[~to_remove]
        
        final_count = len(df_clean)
        removed_count = initial_count - final_count
        
        print(f"  Final predictions: {final_count}")
        print(f"  Removed (upcoming only): {removed_count}")
        print(f"  Preserved (completed): {has_result.sum()}")
        
        # Save the cleaned file
        df_clean.to_parquet(master_path, index=False)
        print(f"  ✓ Saved cleaned predictions")

if __name__ == "__main__":
    print("Removing abbreviations while PRESERVING completed games...")
    print("=" * 60)
    safe_remove_abbreviations()
    print("\n✓ Cleanup complete! All completed games preserved.")
