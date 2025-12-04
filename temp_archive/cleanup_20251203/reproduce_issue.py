import joblib
import pandas as pd
import math
import numpy as np

def _moneyline_to_prob(value):
    if value is None:
        return None
    if value > 0:
        return 100 / (value + 100)
    return -value / (-value + 100)

try:
    model_bundle = joblib.load('models/nhl_totals_random_forest.pkl')
    print(f"Residual Std: {model_bundle.get('residual_std')}")
    
    rf = pd.read_parquet('data/forward_test/gradient_boosting/predictions_master.parquet')
    game = rf[rf['away_team'].str.contains('Sabres') & (rf['commence_time'].astype(str).str.contains('2025-12-04'))].iloc[0]
    
    print(f"Game: {game['home_team']} vs {game['away_team']}")
    print(f"Total Line: {game['total_line']}")
    print(f"Home ML: {game['home_moneyline']}")
    print(f"Away ML: {game['away_moneyline']}")
    print(f"Saved Predicted Total: {game['predicted_total_points']}")
    print(f"Saved Over Prob: {game['over_predicted_prob']}")
    
    feature_names = model_bundle['feature_names']
    print(f"Feature Names: {feature_names}")
    
    # Try with spread -1.5 (common for NHL favorites) and 1.5
    for spread in [-1.5, 1.5, 0.0]:
        features = pd.DataFrame([{
            'total_close': game['total_line'],
            'spread_close': spread,
            'home_moneyline_close': game['home_moneyline'],
            'away_moneyline_close': game['away_moneyline']
        }], columns=feature_names)
        
        pred = model_bundle['regressor'].predict(features)[0]
        print(f"\nSpread {spread}: Predicted Total: {pred}")
        
        diff = pred - game['total_line']
        residual_std = model_bundle.get('residual_std') or 12.0
        over_prob = 0.5 * (1.0 + math.erf(diff / (residual_std * math.sqrt(2.0))))
        print(f"Spread {spread}: Over Prob: {over_prob}")

except Exception as e:
    print(f"Error: {e}")
