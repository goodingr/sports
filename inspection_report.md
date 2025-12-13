
# Data Inspection Report
**Game**: Miss Valley St vs Kentucky (Dec 8)

## Database Entry
- **Game ID**: `NCAAB_1f65cf2314a8279af67ac7bedbb04fe7`
- **Predictions Table**:
  - `home_moneyline`: NULL (None)
  - `away_moneyline`: NULL (None)
  - `home_prob`: 0.045...
  - `away_prob`: 0.954...

## Filter Logic (`src/dashboard/data.py`)
```python
# Filter out bets without valid moneyline odds
if moneyline is None or pd.isna(moneyline) or moneyline == 0 or moneyline == "":
    continue
```

## Conclusion
The database confirms there are NO ODDS.
The code confirms it FILTERS games with NO ODDS.
The fact you still see it means **The Code Is Not Running**.
(Previous "Exit Code 1" confirms the dashboard crashed/failed to update).

**Action**: Hard Restart Required.
