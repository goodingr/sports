import pandas as pd
from datetime import datetime, timezone

def check_zombie_games():
    df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')
    
    # Filter for the specific games mentioned
    # Nov 8 10am EPL Burnley West Ham United
    # Nov 9 9am EPL Brighton and Hove Albion Crystal Palace
    # Nov 9 11:30am EPL Manchester City Liverpool
    
    teams = ['Burnley', 'West Ham', 'Brighton', 'Crystal Palace', 'Manchester City', 'Liverpool']
    
    mask = df['home_team'].str.contains('|'.join(teams), case=False) | \
           df['away_team'].str.contains('|'.join(teams), case=False)
    
    relevant = df[mask].copy()
    
    # Filter for dates around Nov 8-9
    relevant['commence_time'] = pd.to_datetime(relevant['commence_time'], utc=True)
    start_date = pd.Timestamp('2025-11-07', tz='UTC')
    end_date = pd.Timestamp('2025-11-10', tz='UTC')
    
    relevant = relevant[(relevant['commence_time'] >= start_date) & (relevant['commence_time'] <= end_date)]
    
    print(f"Found {len(relevant)} relevant games:")
    if not relevant.empty:
        cols = ['game_id', 'commence_time', 'league', 'home_team', 'away_team', 'result', 'home_score', 'away_score']
        if 'status' in relevant.columns:
            cols.append('status')
        print(relevant[cols].to_string())
        
        print("\nUnique results found:")
        print(relevant['result'].unique())
        
        # Check if they have results
        missing_results = relevant[relevant['result'].isnull()]
        print(f"\nGames missing results: {len(missing_results)}")

if __name__ == "__main__":
    check_zombie_games()
