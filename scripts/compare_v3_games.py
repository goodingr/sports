import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.append(str(Path.cwd()))

from src.dashboard.data import load_forward_test_data

def compare_today_games():
    print("Loading data...")
    df_ensemble = load_forward_test_data(model_type="ensemble")
    df_rf = load_forward_test_data(model_type="random_forest")
    
    # Filter for v0.3 (>= 2025-11-21)
    start_date = pd.Timestamp("2025-11-21", tz="UTC")
    
    df_ensemble_v3 = df_ensemble[(df_ensemble["predicted_at"] >= start_date) & (df_ensemble["league"] == "NBA")]
    df_rf_v3 = df_rf[(df_rf["predicted_at"] >= start_date) & (df_rf["league"] == "NBA")]
    
    print(f"Ensemble v3 games: {len(df_ensemble_v3)}")
    print(f"Random Forest v3 games: {len(df_rf_v3)}")
    
    ensemble_games = set(df_ensemble_v3["game_id"])
    rf_games = set(df_rf_v3["game_id"])
    
    extra_in_ensemble = ensemble_games - rf_games
    extra_in_rf = rf_games - ensemble_games
    
    if extra_in_ensemble:
        print(f"\nExtra games in Ensemble ({len(extra_in_ensemble)}):")
        for gid in extra_in_ensemble:
            row = df_ensemble_v3[df_ensemble_v3["game_id"] == gid].iloc[0]
            print(f"  {gid} ({row['league']}): {row['home_team']} vs {row['away_team']} (Predicted at: {row['predicted_at']})")
            
    if extra_in_rf:
        print(f"\nExtra games in Random Forest ({len(extra_in_rf)}):")
        for gid in extra_in_rf:
            print(f"  {gid}")

if __name__ == "__main__":
    compare_today_games()
