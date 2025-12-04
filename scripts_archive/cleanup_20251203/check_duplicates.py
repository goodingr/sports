import pandas as pd
from pathlib import Path

def check_duplicates():
    df = pd.read_parquet("data/forward_test/ensemble/predictions_master.parquet")
    
    # Group by league and commence_time
    # We expect 1 game per league/time usually, but concurrent games exist.
    # So we can't just count.
    
    # Let's look at games that are very close in time (same minute)
    # and have similar team names?
    
    # Actually, let's just look at the abbreviations.
    # Filter for rows with abbreviations
    abbrevs = df[(df['home_team'].str.len() <= 3) | (df['away_team'].str.len() <= 3)]
    fulls = df[(df['home_team'].str.len() > 3) & (df['away_team'].str.len() > 3)]
    
    print(f"Abbreviations: {len(abbrevs)}")
    print(f"Full names: {len(fulls)}")
    
    # Check if we have overlapping games
    # Join on commence_time and league
    merged = pd.merge(
        abbrevs[['game_id', 'league', 'commence_time', 'home_team', 'away_team']],
        fulls[['game_id', 'league', 'commence_time', 'home_team', 'away_team']],
        on=['league', 'commence_time'],
        suffixes=('_abbrev', '_full')
    )
    
    print(f"Potential matches found: {len(merged)}")
    
    if len(merged) > 0:
        print("\nSample matches:")
        print(merged[['game_id_abbrev', 'game_id_full', 'home_team_abbrev', 'home_team_full']].head(10).to_string())

if __name__ == "__main__":
    check_duplicates()
