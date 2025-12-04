import sys
from pathlib import Path
import pandas as pd
import logging

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.models.forward_test import update_results, FORWARD_TEST_DIR

# Setup logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def update_recent_results():
    print("Running update_results to fetch scores for recent games...")
    
    # This will update the parquet file in place
    # It automatically checks for games that are past their commence_time
    # and tries to find results in the database or via API (for soccer)
    update_results(model_type="ensemble")
    
    # Verify updates for the specific dates mentioned by user (Nov 21-24)
    master_path = FORWARD_TEST_DIR / "ensemble" / "predictions_master.parquet"
    if not master_path.exists():
        print("Master predictions file not found.")
        return
        
    df = pd.read_parquet(master_path)
    df['commence_time'] = pd.to_datetime(df['commence_time'], utc=True)
    
    start_date = pd.Timestamp('2025-11-21', tz='UTC')
    end_date = pd.Timestamp('2025-11-25', tz='UTC')
    
    mask = (df['commence_time'] >= start_date) & \
           (df['commence_time'] <= end_date)
           
    recent = df[mask].copy()
    
    print(f"\nChecking games from {start_date.date()} to {end_date.date()}:")
    print(f"Total games: {len(recent)}")
    
    completed = recent[recent['result'].notnull()]
    pending = recent[recent['result'].isnull()]
    
    print(f"Completed: {len(completed)}")
    print(f"Pending: {len(pending)}")
    
    if not pending.empty:
        print("\nStill pending games (sample):")
        print(pending[['game_id', 'commence_time', 'league', 'home_team', 'away_team']].head(10).to_string())
    else:
        print("\nAll recent games have results!")

if __name__ == "__main__":
    update_recent_results()
