import pandas as pd
from pathlib import Path
import json

path = Path("data/forward_test/predictions_master.parquet")
df = pd.read_parquet(path)
row = df[df["game_id"] == "EPL_740735"]

if not row.empty:
    cols = [
        "game_id", "commence_time", "total_line", 
        "predicted_total_points", "over_edge", "under_edge"
    ]
    data = row[cols].iloc[0].to_dict()
    data["commence_time"] = str(data["commence_time"])
    print(json.dumps(data, indent=2))
else:
    print("Game not found.")
