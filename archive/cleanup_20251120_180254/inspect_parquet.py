import pandas as pd

df = pd.read_parquet("data/processed/model_input/moneyline_nba_2023_2025.parquet")
print("Columns:", df.columns.tolist())
print("\nSample data for rolling metrics:")
cols = [c for c in df.columns if "rolling_" in c]
print(df[cols].head().T)
print("\nMissing values:")
print(df[cols].isna().sum())
