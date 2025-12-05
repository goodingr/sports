import pandas as pd
from pathlib import Path
import json

path = Path("data/forward_test/predictions_master.parquet")
if not path.exists():
    print(f"File not found: {path}")
    exit()

df = pd.read_parquet(path)
mask = (df["home_team"].str.contains("West Ham", case=False) | df["away_team"].str.contains("West Ham", case=False)) & \
       (df["home_team"].str.contains("Manchester United", case=False) | df["away_team"].str.contains("Manchester United", case=False))

relevant = df[mask].sort_values("commence_time", ascending=False)

cols = [
    "game_id", "commence_time", "total_line", 
    "predicted_total_points", "over_edge", "under_edge"
]

print(relevant[cols].to_string())
