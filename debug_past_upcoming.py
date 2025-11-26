import pandas as pd
from datetime import datetime, timezone

def check_past_upcoming():
    df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')
    
    # Search for the teams mentioned
    teams = ['Blackhawks', 'Red Wings', 'Mammoth', 'Senators', 'Jazz', 'Warriors']
    
    # Filter for games involving these teams
    mask = df['home_team'].str.contains('|'.join(teams), case=False) | \
           df['away_team'].str.contains('|'.join(teams), case=False)
    
    relevant = df[mask].copy()
    
    print(f"Current Time (UTC): {datetime.now(timezone.utc)}")
    print("\nRelevant Games Found:")
    
    cols = ['game_id', 'commence_time', 'league', 'home_team', 'away_team', 'result', 'home_score', 'away_score']
    if 'status' in df.columns:
        cols.append('status')
        
    print(relevant[cols].to_string())

if __name__ == "__main__":
    check_past_upcoming()
