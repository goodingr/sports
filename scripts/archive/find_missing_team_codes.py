"""Find what team codes in predictions don't have matches in the database."""
import pandas as pd
from pathlib import Path
from src.db.core import connect

# Load predictions
predictions_path = Path("data/forward_test/ensemble/predictions_master.parquet")
if predictions_path.exists():
    df = pd.read_parquet(predictions_path)
    
    # Get all unique team names
    home_teams = set(df["home_team"].dropna().unique())
    away_teams = set(df["away_team"].dropna().unique())
    all_teams = home_teams | away_teams
    
    # Filter to 3-letter codes
    codes = {t for t in all_teams if isinstance(t, str) and len(t) == 3 and t.isalpha()}
    
    print(f"Found {len(codes)} 3-letter team codes in predictions:")
    for code in sorted(codes):
        print(f"  {code}")
    
    # Check which ones are in the database
    with connect() as conn:
        # Get all team codes from database
        db_teams = conn.execute("SELECT DISTINCT code FROM teams").fetchall()
        db_codes = {row[0] for row in db_teams if row[0]}
        
        # Find codes not in database
        missing = codes - db_codes
        
        print(f"\n{len(missing)} codes NOT in database:")
        for code in sorted(missing):
            # Find games with this code
            games = df[(df["home_team"] == code) | (df["away_team"] == code)].head(3)
            print(f"\n  {code}:")
            for _, game in games.iterrows():
                print(f"    {game['home_team']} vs {game['away_team']} ({game.get('league', 'N/A')})")
else:
    print(f"Predictions file not found: {predictions_path}")
