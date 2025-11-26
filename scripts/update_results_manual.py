import sys
from pathlib import Path
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.models.forward_test import update_results, FORWARD_TEST_DIR
from src.db.core import connect
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def update_zombie_results():
    print("Running update_results to fetch scores from DB...")
    # This will update the parquet file in place
    update_results(model_type="ensemble")
    
    # Now load the file to check for remaining zombies
    master_path = FORWARD_TEST_DIR / "ensemble" / "predictions_master.parquet"
    if not master_path.exists():
        print("Master predictions file not found.")
        return
        
    df = pd.read_parquet(master_path)
    
    # Identify zombie games (past games with no result)
    # Specifically the ones user mentioned around Nov 8-9
    start_date = pd.Timestamp('2025-11-07', tz='UTC')
    end_date = pd.Timestamp('2025-11-10', tz='UTC')
    
    # Ensure commence_time is datetime
    df['commence_time'] = pd.to_datetime(df['commence_time'], utc=True)
    
    mask = (df['commence_time'] >= start_date) & \
           (df['commence_time'] <= end_date) & \
           (df['result'].isnull())
           
    zombies = df[mask].copy()
    
    if zombies.empty:
        print("No zombie games found after update. All fixed!")
        return

    print(f"Found {len(zombies)} zombie games still missing results after update.")
    print(zombies[['game_id', 'commence_time', 'home_team', 'away_team']].to_string())
    
    print("\nRemoving these zombie games from master file as requested...")
    # Remove rows that are in 'zombies'
    df = df[~df['game_id'].isin(zombies['game_id'])]
    print(f"Removed {len(zombies)} games.")
        
    # Save back
    df.to_parquet(master_path, index=False)
    print("Saved updated predictions to parquet.")

if __name__ == "__main__":
    update_zombie_results()
