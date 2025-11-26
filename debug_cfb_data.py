
import pandas as pd
from src.dashboard.data import load_forward_test_data, _expand_predictions, _expand_totals

def analyze_cfb_data():
    print("Loading data...")
    df = load_forward_test_data(force_refresh=False)
    
    if df.empty:
        print("No data found.")
        return

    # Filter for CFB
    # Check if league column exists, if not try to infer or check game_id
    if "league" in df.columns:
        cfb_df = df[df["league"] == "CFB"]
    else:
        # Fallback inference if league col missing (though it should be there based on app.py logic)
        cfb_df = df[df["game_id"].str.startswith("CFB_")]
        
    print(f"Total CFB rows in master data: {len(cfb_df)}")
    
    if cfb_df.empty:
        return

    # Check Moneyline availability
    has_ml = cfb_df[cfb_df["home_moneyline"].notna() & cfb_df["away_moneyline"].notna()]
    print(f"CFB rows with valid Moneyline: {len(has_ml)}")
    
    # Check Totals availability
    if "total_line" in cfb_df.columns:
        has_total = cfb_df[cfb_df["total_line"].notna()]
        print(f"CFB rows with valid Total Line: {len(has_total)}")
    else:
        print("total_line column missing")

    # Expand predictions (Moneyline)
    try:
        ml_bets = _expand_predictions(cfb_df)
        print(f"Expanded Moneyline bets: {len(ml_bets)}")
        
        # Check upcoming
        upcoming_ml = ml_bets[ml_bets["won"].isna()]
        print(f"Upcoming Moneyline bets: {len(upcoming_ml)}")
        
        # Check edges
        positive_edge_ml = upcoming_ml[upcoming_ml["edge"] > 0]
        print(f"Upcoming Moneyline bets with >0 edge: {len(positive_edge_ml)}")
        
    except Exception as e:
        print(f"Error expanding moneylines: {e}")

    # Expand Totals
    try:
        total_bets = _expand_totals(cfb_df)
        print(f"Expanded Total bets: {len(total_bets)}")
        
        # Check upcoming
        upcoming_totals = total_bets[total_bets["won"].isna()]
        print(f"Upcoming Total bets: {len(upcoming_totals)}")
        
        # Check edges
        positive_edge_totals = upcoming_totals[upcoming_totals["edge"] > 0]
        print(f"Upcoming Total bets with >0 edge: {len(positive_edge_totals)}")
        
    except Exception as e:
        print(f"Error expanding totals: {e}")

if __name__ == "__main__":
    analyze_cfb_data()
