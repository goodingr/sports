import pandas as pd
from pathlib import Path
import shutil

# Paths
FORWARD_TEST_DIR = Path("data/forward_test")
BACKUP_DIR = Path("backups")
BACKUP_DIR.mkdir(exist_ok=True)

def cleanup_file(path: Path):
    if not path.exists():
        return

    print(f"Processing {path}...")
    df = pd.read_parquet(path)
    
    # Backup
    backup_path = BACKUP_DIR / f"{path.name}.bak"
    shutil.copy(path, backup_path)
    print(f"Backed up to {backup_path}")

    # Deduplicate
    # We want to keep one entry per game.
    # Key: home_team, away_team, commence_time, league
    # Preference: Internal IDs (e.g. starting with EPL_, NBA_, etc.) over Hash IDs (32 chars)
    
    if "game_id" not in df.columns:
        print("No game_id column, skipping.")
        return

    # Helper to score IDs
    def id_score(game_id):
        if not isinstance(game_id, str):
            return 0
        if "_" in game_id and len(game_id) < 32: # Likely internal ID like EPL_123
            return 2
        return 1

    df["id_score"] = df["game_id"].apply(id_score)
    
    # Sort by score (descending) and then by predicted_at (descending) to keep latest internal ID
    if "predicted_at" in df.columns:
        df = df.sort_values(["id_score", "predicted_at"], ascending=[False, False])
    else:
        df = df.sort_values(["id_score"], ascending=[False])

    # Drop duplicates
    before = len(df)
    df = df.drop_duplicates(
        subset=["home_team", "away_team", "commence_time", "league"],
        keep="first"
    )
    after = len(df)
    
    if before > after:
        print(f"Removed {before - after} duplicates.")
        # Drop helper
        df = df.drop(columns=["id_score"])
        df.to_parquet(path)
        print("Saved cleaned file.")
    else:
        print("No duplicates found.")

# Clean all model files
for model_dir in ["ensemble", "random_forest", "gradient_boosting"]:
    path = FORWARD_TEST_DIR / model_dir / "predictions_master.parquet"
    cleanup_file(path)

# Clean root file
cleanup_file(FORWARD_TEST_DIR / "predictions_master.parquet")
