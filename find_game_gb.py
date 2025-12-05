import pandas as pd
from pathlib import Path
import json

path = Path("data/forward_test/gradient_boosting/predictions_master.parquet")
if not path.exists():
    print(f"File not found: {path}")
    exit()

df = pd.read_parquet(path)
mask = (df["home_team"].str.contains("West Ham", case=False) | df["away_team"].str.contains("West Ham", case=False)) & \
       (df["home_team"].str.contains("Manchester United", case=False) | df["away_team"].str.contains("Manchester United", case=False))

relevant = df[mask].sort_values("commence_time", ascending=False).head(1)

cols = [
    "game_id", "home_team", "away_team", "total_line", 
    "over_moneyline", "under_moneyline", 
    "over_implied_prob", "under_implied_prob",
    "over_predicted_prob", "under_predicted_prob",
    "over_edge", "under_edge",
    "predicted_total_points"
]

if not relevant.empty:
    print(json.dumps(relevant[cols].iloc[0].to_dict(), indent=2))
else:
    print("Game not found.")
