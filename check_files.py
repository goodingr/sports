
import pandas as pd
from pathlib import Path

def check_files():
    root_path = Path("data/forward_test/predictions_master.parquet")
    ensemble_path = Path("data/forward_test/ensemble/predictions_master.parquet")
    
    print(f"Checking Root: {root_path}")
    if root_path.exists():
        df_root = pd.read_parquet(root_path)
        print(f"Root Rows: {len(df_root)}")
        utah = df_root[df_root["home_team"].str.contains("Utah", case=False) | df_root["away_team"].str.contains("Utah", case=False)]
        print(f"Utah in Root: {len(utah)}")
        wolves = df_root[df_root["home_team"].str.contains("Wolverhampton", case=False) | df_root["away_team"].str.contains("Wolverhampton", case=False)]
        print(f"Wolves in Root: {len(wolves)}")
    else:
        print("Root file not found")
        
    # print(f"\nChecking Ensemble: {ensemble_path}")
    # if ensemble_path.exists():
    #     df_ens = pd.read_parquet(ensemble_path)
    #     print(f"Ensemble Rows: {len(df_ens)}")
    #     utah = df_ens[df_ens["home_team"].str.contains("Utah", case=False) | df_ens["away_team"].str.contains("Utah", case=False)]
    #     print(f"Utah in Ensemble: {len(utah)}")
    #     wolves = df_ens[df_ens["home_team"].str.contains("Wolverhampton", case=False) | df_ens["away_team"].str.contains("Wolverhampton", case=False)]
    #     print(f"Wolves in Ensemble: {len(wolves)}")
    # else:
    #     print("Ensemble file not found")

if __name__ == "__main__":
    check_files()
