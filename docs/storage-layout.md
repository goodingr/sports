# Storage Layout

```
data/
  raw/
    odds/            # JSON snapshots of sportsbook odds by request timestamp
    results/         # Parquet schedules and betting history from nfl_data_py
    sources/         # Raw artifacts from scripted scrapers (organized by league/source)
    teams/           # (optional) legacy location for team stats
  processed/
    features/        # Feature matrices ready for modeling
    model_input/     # Aggregated datasets for training/validation (e.g., moneyline_nfl_1999_2023.parquet)
reports/
  backtests/        # ROI and bankroll simulation outputs
  recommendations/  # Daily bet recommendation exports

data/betting.db     # SQLite warehouse (initialize via src/db/init_db.py)
```

- Raw data is append-only; never mutate existing files. Create new snapshots when data refreshes.
- Processed data can be regenerated; include versioning in filenames, e.g., `features_v1.parquet`.
- Use Parquet for structured tables and JSON for API payloads to retain original metadata.

