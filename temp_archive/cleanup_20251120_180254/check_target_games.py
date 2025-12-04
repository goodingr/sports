"""
Script to regenerate predictions for specific dates using the new model
and compare with old model predictions.
"""
import pandas as pd
import pickle
from datetime import datetime, timedelta
from pathlib import Path

# Load the new model
print("Loading new enhanced NBA model...")
with open("models/nba_lightgbm_calibrated_moneyline.pkl", "rb") as f:
    model_data = pickle.load(f)

new_model = model_data["model"]
feature_names = model_data.get("feature_names", [])

print(f"Model loaded with {len(feature_names)} features")
print(f"Enhanced features include: {[f for f in feature_names if 'rolling' in f][:5]}...")

# Load existing predictions to find games from Nov 18-20
print("\nLoading existing predictions...")
df = pd.read_parquet("data/forward_test/predictions_master.parquet")
nba = df[df['league'] == 'NBA'].copy()
nba['commence_time'] = pd.to_datetime(nba['commence_time'])

# Filter for Nov 18-20
start_date = pd.to_datetime('2025-11-18')
end_date = pd.to_datetime('2025-11-20 23:59:59')
target_games = nba[(nba['commence_time'] >= start_date) & (nba['commence_time'] <= end_date)].copy()

print(f"\nFound {len(target_games)} NBA games between Nov 18-20, 2025")

if len(target_games) == 0:
    print("\nNo games found in that date range. Checking what dates are available...")
    print(f"Available date range: {nba['commence_time'].min()} to {nba['commence_time'].max()}")
else:
    print("\nGames to compare:")
    for idx, row in target_games.iterrows():
        print(f"  {row['commence_time']}: {row['home_team']} vs {row['away_team']}")
    
    print("\n" + "="*70)
    print("NOTE: To regenerate predictions with the new model, we would need:")
    print("="*70)
    print("1. Access to the same input features used during forward testing")
    print("2. The feature engineering pipeline to create rolling metrics")
    print("3. The same odds data that was available at prediction time")
    print("\nThe forward_test.py script handles this automatically.")
    print("For a fair comparison, we should use the forward test infrastructure.")
