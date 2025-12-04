"""
Script to update predictions with abbreviated team names to use full names.
"""
import pandas as pd
from pathlib import Path

def update_team_names():
    master_path = Path("data/forward_test/ensemble/predictions_master.parquet")
    
    # Load current predictions
    df = pd.read_parquet(master_path)
    
    print(f"Total predictions: {len(df)}")
    
    # Identify predictions with abbreviated team names (3 chars or less)
    df['home_is_abbrev'] = df['home_team'].str.len() <= 3
    df['away_is_abbrev'] = df['away_team'].str.len() <= 3
    df['has_abbrev'] = df['home_is_abbrev'] | df['away_is_abbrev']
    
    abbrev_count = df['has_abbrev'].sum()
    print(f"Predictions with abbreviations: {abbrev_count}")
    
    # Show breakdown by league
    print("\nAbbreviations by league:")
    abbrev_by_league = df[df['has_abbrev']].groupby('league').size().sort_values(ascending=False)
    for league, count in abbrev_by_league.items():
        print(f"  {league}: {count}")
    
    # Drop the temporary columns
    df = df.drop(columns=['home_is_abbrev', 'away_is_abbrev', 'has_abbrev'])
    
    return abbrev_by_league.index.tolist()

if __name__ == "__main__":
    leagues_to_update = update_team_names()
    print(f"\nLeagues that need updating: {', '.join(leagues_to_update)}")
