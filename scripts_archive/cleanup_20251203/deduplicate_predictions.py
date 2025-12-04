"""
Deduplicate predictions, keeping only the most recent version of each game.
This will remove old predictions with abbreviated team names and keep the new ones with full names.
"""
import pandas as pd
from pathlib import Path

def deduplicate_predictions():
    # Process each model type
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
        df['has_abbrev'] = (df['home_team'].str.len() <= 3) | (df['away_team'].str.len() <= 3)
        abbrev_before = df['has_abbrev'].sum()
        print(f"  Abbreviations before: {abbrev_before}")
        
        # Sort by a timestamp column if available, otherwise keep last occurrence
        # Prefer predictions with:
        # 1. Populated home_team_code (indicates new format)
        # 2. Longer team names (full names vs abbreviations)
        df['has_code'] = df['home_team_code'].notna()
        df['team_name_length'] = df['home_team'].str.len() + df['away_team'].str.len()
        
        # Sort so the best records are first
        df = df.sort_values(['has_code', 'team_name_length'], ascending=[False, False])
        
        # Deduplicate based on league and commence_time
        # This handles the case where game_id changed but it's the same game
        df_dedup = df.drop_duplicates(subset=['league', 'commence_time'], keep='first')
        
        # Drop temporary columns
        df_dedup = df_dedup.drop(columns=['has_abbrev', 'has_code', 'team_name_length'])
        
        final_count = len(df_dedup)
        duplicates_removed = initial_count - final_count
        
        abbrev_after = ((df_dedup['home_team'].str.len() <= 3) | (df_dedup['away_team'].str.len() <= 3)).sum()
        
        print(f"  Final predictions: {final_count}")
        print(f"  Duplicates removed: {duplicates_removed}")
        print(f"  Abbreviations after: {abbrev_after}")
        
        # Save the deduplicated file
        df_dedup.to_parquet(master_path, index=False)
        print(f"  ✓ Saved deduplicated predictions")

if __name__ == "__main__":
    print("Deduplicating predictions...")
    deduplicate_predictions()
    print("\n✓ Deduplication complete!")
