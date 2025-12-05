import pandas as pd
from pathlib import Path

path = Path("data/forward_test/predictions_master.parquet")
df = pd.read_parquet(path)
print(df.columns.tolist())
