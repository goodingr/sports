# Prediction Data Management

## ⚠️ CRITICAL: Protecting Completed Predictions

Completed predictions (games with results) are **irreplaceable historical data** that cannot be recovered if deleted. They contain:
- Model predictions (probabilities, edges)
- Bet recommendations
- Win/loss records
- Historical performance metrics

## Safeguards in Place

### 1. Automatic Backups (`scripts/backup_predictions.py`)
Creates timestamped backups before any cleanup operations:
```powershell
python scripts/backup_predictions.py
```

Backups are stored in: `data/backups/predictions/`

### 2. Safe Cleanup Script (`scripts/safe_remove_abbreviations.py`)
**Always use this instead of the unsafe `remove_abbreviations.py`**

This script:
- ✅ Preserves ALL completed games (even with abbreviations)
- ✅ Only removes upcoming games with abbreviations
- ✅ Reports how many completed games were preserved

```powershell
python scripts/safe_remove_abbreviations.py
```

### 3. Manual Backup Locations
Backups are automatically created at:
- `data/backups/predictions/predictions_master_{model_type}_{timestamp}.parquet`

### 4. What's NOT in Git
⚠️ Parquet files are excluded from git (`.gitignore: data/**/*.parquet`)

This means git cannot help recover deleted predictions. Always create backups before cleanup!

## Best Practices

1. **Before any cleanup**: Run `python scripts/backup_predictions.py`
2. **For removing abbreviations**: Use `scripts/safe_remove_abbreviations.py` (NOT `remove_abbreviations.py`)
3. **Keep backups**: Periodically copy backups to a separate location
4. **Never delete**: Completed games with `result` column populated

## Recovery

If predictions are accidentally deleted, restore from the most recent backup:
```powershell
# Example: Restore ensemble predictions
cp data/backups/predictions/predictions_master_ensemble_20251125_010315.parquet data/forward_test/ensemble/predictions_master.parquet
```

## Current Status

- ✅ Safe cleanup script created
- ✅ Backup script created  
- ✅ Initial backup created: `20251125_010315`
- ✅ Completed predictions preserved: 19 games
