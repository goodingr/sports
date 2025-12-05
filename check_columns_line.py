import pandas as pd
from pathlib import Path

path = Path("data/forward_test/predictions_master.parquet")
df = pd.read_parquet(path)
cols = sorted(df.columns.tolist())
print([c for c in cols if "line" in c.lower()])
