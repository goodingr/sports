import sys
from pathlib import Path
import pandas as pd
import logging

# Add project root to path
sys.path.append(str(Path(__file__).parent))

# Setup logging
logging.basicConfig(level=logging.INFO)

from src.api.routes.bets import get_totals_data

def debug_api():
    print("Calling get_totals_data...")
    df = get_totals_data(model_type="ensemble")
    
    # Filter for the zombie games
    teams = ['Burnley', 'West Ham', 'Brighton', 'Crystal Palace', 'Manchester City', 'Liverpool']
    mask = df['home_team'].str.contains('|'.join(teams), case=False) | \
           df['away_team'].str.contains('|'.join(teams), case=False)
    
    zombies = df[mask].copy()
    
    # Filter for dates around Nov 8-9
    # commence_time is likely in ET now
    zombies['commence_time'] = pd.to_datetime(zombies['commence_time'], utc=True)
    start_date = pd.Timestamp('2025-11-07', tz='UTC')
    end_date = pd.Timestamp('2025-11-10', tz='UTC')
    
    zombies = zombies[(zombies['commence_time'] >= start_date) & (zombies['commence_time'] <= end_date)]
    
    print(f"\nFound {len(zombies)} zombie games in API data:")
    if not zombies.empty:
        cols = ['game_id', 'commence_time', 'home_team', 'away_team', 'result', 'won', 'status']
        print(zombies[cols].to_string())
        
        print("\nUnique results:", zombies['result'].unique())
        print("Unique statuses:", zombies['status'].unique())
        print("Unique won:", zombies['won'].unique())

if __name__ == "__main__":
    debug_api()
