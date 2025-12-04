import pandas as pd
from datetime import datetime, timezone
from src.dashboard.data import get_upcoming_calendar

def verify_fix():
    # Load data
    df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')
    
    # Run the function
    upcoming = get_upcoming_calendar(df)
    
    print(f"Current Time (UTC): {datetime.now(timezone.utc)}")
    print(f"Upcoming games count: {len(upcoming)}")
    
    # Check for past games
    now = pd.Timestamp.now(tz="UTC")
    past_games = upcoming[upcoming['commence_time'] <= now]
    
    if not past_games.empty:
        print("\nFAILED: Found past games in upcoming list:")
        print(past_games[['commence_time', 'team', 'opponent']].to_string())
    else:
        print("\nSUCCESS: No past games found in upcoming list.")
        
    # Check specifically for the reported games
    teams = ['Blackhawks', 'Red Wings', 'Mammoth', 'Senators', 'Jazz', 'Warriors']
    mask = upcoming['team'].str.contains('|'.join(teams), case=False) | \
           upcoming['opponent'].str.contains('|'.join(teams), case=False)
    
    relevant = upcoming[mask]
    if not relevant.empty:
        print("\nWARNING: Found relevant games (should only be future ones):")
        print(relevant[['commence_time', 'team', 'opponent']].to_string())
    else:
        print("\nVerified: Reported past games are gone.")

if __name__ == "__main__":
    verify_fix()
