import pandas as pd
from src.dashboard.data import get_totals_odds_for_recommended
import json

# Mock input dataframe
df = pd.DataFrame([{
    "game_id": "EPL_740735",
    "side": "over",
    "league": "EPL",
    "home_team": "Manchester United",
    "away_team": "West Ham United",
    "commence_time": "2025-12-04 20:00:00+00:00"
}])

try:
    odds_df = get_totals_odds_for_recommended(df)
    if not odds_df.empty:
        print(odds_df.to_json(orient="records", indent=2))
    else:
        print("No odds found.")
except Exception as e:
    print(f"Error: {e}")
