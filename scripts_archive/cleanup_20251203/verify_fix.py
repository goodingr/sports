import pandas as pd
from pathlib import Path
import sys
from src.dashboard.data import _expand_totals

# Load data
path = Path('data/forward_test/ensemble/predictions_master.parquet')
df = pd.read_parquet(path)

# Run expansion
totals = _expand_totals(df)

# Check settled_at dates
if not totals.empty:
    totals['settled_date'] = totals['settled_at'].dt.date
    print("\nSettled dates distribution:")
    print(totals['settled_date'].value_counts().sort_index())
    
    # Check specifically for Nov 21 and Nov 22
    nov21 = totals[totals['settled_date'] == pd.to_datetime('2025-11-21').date()]
    nov22 = totals[totals['settled_date'] == pd.to_datetime('2025-11-22').date()]
    
    print(f"\nTotals settled on Nov 21: {len(nov21)}")
    print(f"Totals settled on Nov 22: {len(nov22)}")
else:
    print("No totals data found")
