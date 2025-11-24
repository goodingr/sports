import pandas as pd
from pathlib import Path

master_path = Path("data/forward_test/predictions_master.parquet")

if master_path.exists():
    df = pd.read_parquet(master_path)
    print(f"Total predictions: {len(df)}")
    print(f"\nLeagues: {df['league'].value_counts().to_dict() if 'league' in df.columns else 'No league column'}")
    print(f"\nDate range: {df['predicted_at'].min()} to {df['predicted_at'].max()}" if 'predicted_at' in df.columns else "No predicted_at column")
    
    # Check for NBA predictions
    if 'league' in df.columns:
        nba_df = df[df['league'] == 'NBA']
        print(f"\nNBA predictions: {len(nba_df)}")
        if len(nba_df) > 0:
            print(f"NBA date range: {nba_df['predicted_at'].min()} to {nba_df['predicted_at'].max()}" if 'predicted_at' in nba_df.columns else "")
            print(f"Sample NBA prediction:")
            print(nba_df[['game_id', 'home_team', 'away_team', 'predicted_at']].head(1).T if len(nba_df) > 0 else "")
else:
    print("predictions_master.parquet does not exist")
