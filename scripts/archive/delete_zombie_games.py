import pandas as pd
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.models.forward_test import FORWARD_TEST_DIR

def delete_zombie_games():
    master_path = FORWARD_TEST_DIR / "ensemble" / "predictions_master.parquet"
    if not master_path.exists():
        print("Master predictions file not found.")
        return
        
    df = pd.read_parquet(master_path)
    print(f"Total games before deletion: {len(df)}")
    
    # Identify zombie games (Nov 8-9)
    # Teams: Burnley, West Ham, Brighton, Crystal Palace, Manchester City, Liverpool
    teams = ['Burnley', 'West Ham', 'Brighton', 'Crystal Palace', 'Manchester City', 'Liverpool']
    
    # Filter by date first
    df['commence_time'] = pd.to_datetime(df['commence_time'], utc=True)
    start_date = pd.Timestamp('2025-11-07', tz='UTC')
    end_date = pd.Timestamp('2025-11-10', tz='UTC')
    
    date_mask = (df['commence_time'] >= start_date) & (df['commence_time'] <= end_date)
    
    # Filter by team
    team_mask = df['home_team'].str.contains('|'.join(teams), case=False) | \
                df['away_team'].str.contains('|'.join(teams), case=False)
                
    zombies = df[date_mask & team_mask]
    
    if zombies.empty:
        print("No zombie games found to delete.")
        return
        
    print(f"Found {len(zombies)} games to delete:")
    print(zombies[['game_id', 'commence_time', 'home_team', 'away_team']].to_string())
    
    # Delete them
    df_clean = df[~(date_mask & team_mask)]
    
    print(f"Total games after deletion: {len(df_clean)}")
    print(f"Deleted {len(df) - len(df_clean)} rows.")
    
    # Save
    df_clean.to_parquet(master_path, index=False)
    print("Successfully saved cleaned parquet file.")

if __name__ == "__main__":
    delete_zombie_games()
