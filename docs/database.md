# Database Schema

The project ships with a SQLite warehouse (`data/betting.db`) to consolidate raw feeds, modeling inputs, and recommendations. Run the initializer to create the schema:

```
poetry run python -m src.db.init_db
```

Key tables:

- `sports`, `teams`, `games`: canonical entities for multi-league support.
- `odds_snapshots`, `odds`, `books`: time-stamped sportsbook prices.
- `game_results`, `team_features`, `model_input`: data used to train models.
- `models`, `model_predictions`, `recommendations`: registry of trained models and generated bets.
- `ingestion_runs`, `data_files`: operational metadata for ETL auditing.
- `data_sources`, `source_runs`, `source_files`: multi-source scraping registry and ingestion history.
- `injury_reports`: normalized injury logs merged into both NFL and NBA feature pipelines.
- Run `poetry run python -m src.db.inspect summary` to verify row counts, or `... models` / `... recommendations` for quick snapshots.

See `src/db/schema.sql` for full DDL definitions and adjust as new sports or features are introduced.

