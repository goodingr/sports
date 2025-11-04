# Modeling Pipeline

1. **Feature assembly** (`src/features/moneyline_dataset.py`)
   - For NFL, loads historical schedules, betting lines, play-by-play, weather, and injury reports from `nfl_data_py`, crafting rich rolling features.
   - For NBA (and other leagues), builds a streamlined dataset from the SQLite warehouse using game results plus the latest available moneyline odds.
   - Saves outputs to `data/processed/model_input/moneyline_<league>_<start>_<end>.parquet`.

2. **Training & evaluation** (`src/models/train.py`)
   - Supports multiple estimators: scikit HistGradientBoosting, LightGBM, XGBoost, logistic regression, MLP, or a weighted ensemble (members chosen via log-loss pruning).
   - Performs time-aware calibration (Platt/isotonic) using rolling folds and logs seasonal drift diagnostics.
   - Writes model artifacts under `models/<league>_<model>_moneyline.pkl` and metrics/predictions in `reports/backtests/<league>_...`.

3. **Bet selection & bankroll simulation** (`src/models/bet_selector.py`)
   - Calculates edges between model probability and market implied probability.
   - Applies capped Kelly sizing (2% max stake) to estimate bankroll trajectory, reporting both “all games” and “recommended only” bankroll summaries.
   - Exports recommendation CSV + JSON summary in `reports/recommendations/`.

## Next Steps

- Evaluate additional contextual signals (e.g., travel distance, QB depth tiers) and monitor feature drift.
- Experiment with model stacking/ensembles and per-season cross-validation diagnostics.
- Replace historical test predictions with live odds snapshots for daily recommendations.

