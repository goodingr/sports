import pandas as pd
from pathlib import Path

path = Path("data/forward_test/ensemble/predictions_master.parquet")
df = pd.read_parquet(path)

wizards = df[
    (df["home_team"].str.contains("Wizards")) | 
    (df["away_team"].str.contains("Wizards"))
]

print(f"Wizards games found: {len(wizards)}")
if not wizards.empty:
    with open("wizards_result.txt", "w", encoding="utf-8") as f:
        f.write(wizards[["game_id", "commence_time", "home_team", "away_team", "result"]].sort_values("commence_time", ascending=False).to_string())
    print("Done writing to wizards_result.txt")
