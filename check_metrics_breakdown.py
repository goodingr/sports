import pandas as pd
from src.dashboard.data import _expand_predictions, _expand_totals, load_forward_test_data

def check_metrics():
    print("Loading data...")
    df = load_forward_test_data(model_type="ensemble")
    
    # Moneyline Metrics (Dashboard Overview)
    print("\n--- Moneyline Metrics (Dashboard Overview) ---")
    ml_bets = _expand_predictions(df)
    if not ml_bets.empty:
        # Filter for recommended (edge >= 0.06)
        ml_rec = ml_bets[ml_bets["edge"] >= 0.06].copy()
        ml_completed = ml_rec[ml_rec["won"].notna()]
        
        print(f"Total Recommended: {len(ml_rec)}")
        print(f"Completed: {len(ml_completed)}")
        print(f"Profit: ${ml_completed['profit'].sum():.2f}")
        print(f"ROI: {(ml_completed['profit'].sum() / (len(ml_completed) * 100)) * 100:.2f}%")
    else:
        print("No Moneyline bets found.")

    # Totals Metrics (Web App)
    print("\n--- Totals Metrics (Web App - All Versions) ---")
    tot_bets = _expand_totals(df)
    if not tot_bets.empty:
        # Filter for recommended (edge >= 0.06)
        tot_rec = tot_bets[tot_bets["edge"] >= 0.06].copy()
        tot_completed = tot_rec[tot_rec["won"].notna()]
        
        print(f"Total Recommended: {len(tot_rec)}")
        print(f"Completed: {len(tot_completed)}")
        print(f"Profit: ${tot_completed['profit'].sum():.2f}")
        print(f"ROI: {(tot_completed['profit'].sum() / (len(tot_completed) * 100)) * 100:.2f}%")
        
        # Check v0.3 specific metrics
        print("\n--- Totals Metrics (v0.3 Only) ---")
        # Filter by version using the same logic as dashboard
        # We need to assign versions first if not already done by load_forward_test_data
        # load_forward_test_data calls _assign_versions, so 'version' col should be there
        if "version" in df.columns:
            # We need to filter the *expanded* bets by version. 
            # The expanded df might not have 'version' unless we merge it back or if _expand_totals preserves it?
            # _expand_totals does NOT seem to preserve 'version' column in the records list in data.py snippet I saw earlier.
            # Let's check data.py again or just merge it.
            # Actually, looking at data.py snippet:
            # records.append({ ..., "league": row.get("league"), ... })
            # It does NOT appear to include "version".
            
            # Let's re-merge version from original df
            tot_bets_v3 = tot_bets.merge(df[["game_id", "version"]], on="game_id", how="left")
            tot_rec_v3 = tot_bets_v3[(tot_bets_v3["edge"] >= 0.06) & (tot_bets_v3["version"] == "v0.3")].copy()
            tot_completed_v3 = tot_rec_v3[tot_rec_v3["won"].notna()]
            
            print(f"Total Recommended: {len(tot_rec_v3)}")
            print(f"Completed: {len(tot_completed_v3)}")
            print(f"Profit: ${tot_completed_v3['profit'].sum():.2f}")
            print(f"ROI: {(tot_completed_v3['profit'].sum() / (len(tot_completed_v3) * 100)) * 100:.2f}%")
        else:
            print("Version column missing in dataframe.")

    # Check Dashboard Default (edge >= 0.0, v0.3)
    print("\n--- Dashboard Simulation (Edge >= 0.0, v0.3 Only) ---")
    if "version" in df.columns:
        tot_bets_v3 = tot_bets.merge(df[["game_id", "version"]], on="game_id", how="left")
        # Dashboard defaults to v0.3 and edge >= 0.0
        tot_rec_dash = tot_bets_v3[(tot_bets_v3["edge"] >= 0.0) & (tot_bets_v3["version"] == "v0.3")].copy()
        tot_completed_dash = tot_rec_dash[tot_rec_dash["won"].notna()]
        
        print(f"Total Recommended: {len(tot_rec_dash)}")
        print(f"Completed: {len(tot_completed_dash)}")
        print(f"Profit: ${tot_completed_dash['profit'].sum():.2f}")
        print(f"ROI: {(tot_completed_dash['profit'].sum() / (len(tot_completed_dash) * 100)) * 100:.2f}%")

    # Check Dashboard Simulation (Edge >= 0.0, All Versions) - Just in case user selected All Versions
    print("\n--- Dashboard Simulation (Edge >= 0.0, All Versions) ---")
    tot_rec_all = tot_bets[(tot_bets["edge"] >= 0.0)].copy()
    tot_completed_all = tot_rec_all[tot_rec_all["won"].notna()]
    
    print(f"Total Recommended: {len(tot_rec_all)}")
    print(f"Completed: {len(tot_completed_all)}")
    print(f"Profit: ${tot_completed_all['profit'].sum():.2f}")
    print(f"ROI: {(tot_completed_all['profit'].sum() / (len(tot_completed_all) * 100)) * 100:.2f}%")

if __name__ == "__main__":
    check_metrics()
