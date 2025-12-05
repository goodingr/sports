import pandas as pd
import sys
from src.api.routes.bets import get_totals_data

def verify_live_status():
    print("Loading totals data...")
    df = get_totals_data(model_type="ensemble")
    
    if df.empty:
        print("No data returned.")
        return

    # Filter for NCAAB games that have scores
    # We look for games where home_score is not null
    ncaab_live = df[
        (df["league"] == "NCAAB") & 
        (df["home_score"].notna()) &
        (df["result"].isna()) # These are the ones that should be pending
    ]
    
    print(f"Found {len(ncaab_live)} live NCAAB games (scores present, result missing).")
    
    if ncaab_live.empty:
        print("No live NCAAB games found to verify.")
        return

    # Check status and won columns
    print("\nVerifying status and won columns:")
    errors = 0
    for _, row in ncaab_live.iterrows():
        status = row.get("status")
        won = row.get("won")
        profit = row.get("profit")
        
        print(f"Game {row['game_id']}: Status='{status}', Won={won}, Profit={profit}")
        
        if status != "Pending":
            print("  ERROR: Status should be 'Pending'")
            errors += 1
        if pd.notna(won):
            print("  ERROR: Won should be None/NaN")
            errors += 1
            
    if errors == 0:
        print("\nSUCCESS: All live games have correct status (Pending) and no result.")
    else:
        print(f"\nFAILURE: Found {errors} errors.")

if __name__ == "__main__":
    verify_live_status()
