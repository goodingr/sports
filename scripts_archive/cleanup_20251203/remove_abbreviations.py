"""
Remove all predictions with abbreviated team names (3 chars or less).
This will force a clean state where only full team names remain.
"""
import pandas as pd
from pathlib import Path

def remove_abbreviations():
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
        
        # Count abbreviations before
        has_abbrev = (df['home_team'].str.len() <= 3) | (df['away_team'].str.len() <= 3)
        abbrev_count = has_abbrev.sum()
        print(f"  Abbreviations: {abbrev_count}")
        
        # Remove all rows with abbreviations
        df_clean = df[~has_abbrev]
        
        final_count = len(df_clean)
        removed_count = initial_count - final_count
        
        print(f"  Final predictions: {final_count}")
        print(f"  Removed: {removed_count}")
        
        # Save the cleaned file
        df_clean.to_parquet(master_path, index=False)
        print(f"  ✓ Saved cleaned predictions")

if __name__ == "__main__":
    print("Removing all predictions with abbreviated team names...")
    remove_abbreviations()
    print("\n✓ Cleanup complete!")
    print("\nYou should now re-run the prediction pipeline to regenerate predictions for the removed games.")
