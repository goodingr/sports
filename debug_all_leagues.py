import pandas as pd
from pathlib import Path
import numpy as np

def check_all_leagues():
    path = Path("data/forward_test/ensemble/predictions_master.parquet")
    if not path.exists():
        print(f"File not found: {path}")
        return

    df = pd.read_parquet(path)
    
    if "league" not in df.columns:
        print("Error: 'league' column missing from predictions.")
        return

    print(f"Total Predictions: {len(df)}")
    
    # Group by league and count valid moneylines
    stats = []
    for league, group in df.groupby("league"):
        total = len(group)
        # Check for valid moneyline (not NaN, not None)
        has_ml = group[
            (group["home_moneyline"].notna()) & 
            (group["away_moneyline"].notna())
        ]
        ml_count = len(has_ml)
        
        # Check for valid edge
        has_edge = group[
            (group["home_edge"].notna()) | 
            (group["away_edge"].notna())
        ]
        edge_count = len(has_edge)
        
        stats.append({
            "League": league,
            "Total Games": total,
            "With Moneyline": ml_count,
            "With Edge": edge_count,
            "Missing ML %": f"{(1 - ml_count/total)*100:.1f}%" if total > 0 else "0.0%"
        })
    
    stats_df = pd.DataFrame(stats)
    problematic = stats_df[stats_df["Missing ML %"] != "0.0%"]
    
    if problematic.empty:
        print("\nAll leagues have 100% odds coverage!")
    else:
        print("\nLeagues with missing odds:")
        print(problematic.to_string(index=False))
    
    # Drill down into problematic leagues
    for league in problematic["League"]:
        print(f"\n--- {league} Details ---")
        league_df = df[df["league"] == league]
        missing_ml = league_df[league_df["home_moneyline"].isna()]
        print(f"Total: {len(league_df)}, Missing: {len(missing_ml)}")
        if not missing_ml.empty:
            print("Sample games missing ML:")
            for _, row in missing_ml.head(3).iterrows():
                print(f"  {row['home_team']} vs {row['away_team']} ({row['commence_time']})")

if __name__ == "__main__":
    check_all_leagues()
