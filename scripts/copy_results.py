"""
Copy game results from ensemble predictions to other model types.
This allows us to update results for random_forest and gradient_boosting
without making additional API calls.
"""
import pandas as pd
from pathlib import Path

# Paths
forward_test_dir = Path("data/forward_test")
ensemble_master = forward_test_dir / "ensemble" / "predictions_master.parquet"
model_types = ["random_forest", "gradient_boosting"]

# Load ensemble predictions
print(f"Loading ensemble predictions from {ensemble_master}")
ensemble_df = pd.read_parquet(ensemble_master)

# Get completed games (those with results)
completed_mask = ensemble_df["result"].notna()
completed_games = ensemble_df[completed_mask].copy()
print(f"Found {len(completed_games)} completed games in ensemble predictions")

# Result columns to copy
result_columns = ["home_score", "away_score", "result", "result_updated_at"]

# For each other model type
for model_type in model_types:
    model_dir = forward_test_dir / model_type
    master_path = model_dir / "predictions_master.parquet"
    
    if not master_path.exists():
        print(f"WARNING: No predictions file found for {model_type} at {master_path}")
        continue
    
    # Load the model's predictions
    print(f"\nProcessing {model_type}...")
    model_df = pd.read_parquet(master_path)
    print(f"  Loaded {len(model_df)} predictions")
    
    # Count how many already have results
    existing_results = model_df["result"].notna().sum()
    print(f"  Already has {existing_results} completed games")
    
    # For each completed game in ensemble, update the corresponding game in this model
    updates_made = 0
    for _, completed_game in completed_games.iterrows():
        # Find matching game by game_id
        game_id = completed_game["game_id"]
        mask = model_df["game_id"] == game_id
        
        if mask.any():
            # Copy result columns
            for col in result_columns:
                model_df.loc[mask, col] = completed_game[col]
            updates_made += 1
    
    print(f"  Updated {updates_made} games with results")
    
    # Save back to parquet
    model_df.to_parquet(master_path, index=False)
    print(f"  Saved updated predictions to {master_path}")
    
    # Verify
    new_results = model_df["result"].notna().sum()
    print(f"  Now has {new_results} completed games (added {new_results - existing_results})")

print("\n✓ Done! All model types now have the same game results.")
