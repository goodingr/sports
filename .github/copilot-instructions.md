# Sports Betting Analytics — Agent Playbook

## Architecture essentials
- Pipelines center on `src/data` (ingestion), `src/features` (dataset builders), `src/models` (training/forward tests), and orchestration scripts under `scripts/`. Training/prediction always expect processed parquet outputs in `data/processed/model_input`.
- `scripts/run_hourly_pipeline.ps1` is the authoritative job: it ingests odds/results (`poetry run python -c "...ingest_*..."`), rebuilds datasets per league (with soccer season augmentation + fallback), trains via `src.models.train`, then runs `src.models.forward_test` predict/update passes. Prefer extending this script rather than duplicating logic.
- Moneyline features are produced by `src/features/moneyline_dataset`. New columns must be wired here and mirrored in `FEATURE_COLUMNS` inside `src/models/train.py`.
- Forward testing writes to `data/forward_test/predictions_*.parquet` and consolidates into `predictions_master.parquet`; the Dash app in `src/dashboard` reads from this file.
- The SQLite warehouse (`database.sqlite`) is updated through `src/db/*` loaders—avoid writing ad-hoc SQL; use the helpers in `src/db/loaders.py`.

## Critical workflows & commands
- Install & run: `poetry install`, then `poetry run python -m src.data.ingest_sources …` / `poetry run python -m src.models.train --league NFL` etc. All automated jobs assume `poetry` virtualenv.
- Hourly job variants: `.\scripts\run_hourly_pipeline.ps1` (full) and `.\scripts\run_hourly_pipeline.ps1 -SoccerOnly`. Logs land in `logs/hourly_pipeline_*.log`; tail these when debugging Task Scheduler issues.
- Forward-test only (used by Task Scheduler): `scripts/run_forward_test_predict.ps1` and `scripts/run_forward_test_update.ps1` wrappers call into `src.models.forward_test`.
- Dashboard: `poetry run python -m src.dashboard --port 8050` reads the latest `data/forward_test/predictions_master.parquet`.
- Tests: `poetry run pytest tests/` (targeted suites include `tests/test_forward_test_scores.py`, `tests/test_dashboard_helpers.py`, etc.).

## Project-specific conventions
- Environments: `.env` must provide `ODDS_API_KEY`; optional keys include `CFBD_API_KEY`. Set `ODDS_API_MIN_FETCH_MINUTES` to control API throttling (see `src/data/config.py`).
- Odds API throttling: `src/data/ingest_odds.py` caches snapshots in `data/raw/odds/<sport>/` and replays them if they are newer than `min_fetch_interval_minutes`. Respect this behavior when adding new callers; don’t bypass it unless you truly need fresh odds (use `--force-refresh`).
- Soccer handling: The hourly script dynamically detects available seasons from `data/raw/results/schedules_*_soccer_database_*.parquet`, mixes recent and historical seasons, and skips training if a dataset lacks files. When adding leagues, plug them into `$soccerLeagues` and ensure names match odds/ingest modules.
- Model training: `_time_series_split` in `src/models/train.py` already guards against tiny datasets; keep dataset sizes sane (or adjust the `splits` param if changing season ranges). The evaluator now tolerates single-class folds by passing `labels=[0,1]` to `log_loss`.
- Data paths: Raw inputs live under `data/raw/{results,odds,sources}`; processed parquet under `data/processed/model_input`; reports under `reports/backtests`. Scripts should never hardcode absolute paths—use `PROJECT_ROOT`/`RAW_DATA_DIR` helpers in `src/data/config.py`.

## External integrations & references
- Odds & injuries: `src.data.sources.espn_odds`, `espn_odds_*`, `nba_injuries_espn`, etc. rely on upstream APIs—wrap calls in `_safe_run` like the hourly script to prevent hard failures.
- College football advanced stats use `CFBD_API_KEY` via `src.data.sources.cfbd_advanced_stats`; ensure throttling/respect for the API limits similar to odds ingestion.
- Docs worth scanning before major edits: `docs/dashboard.md`, `docs/data-sources.md`, `docs/storage-layout.md`, `HOURLY_AUTOMATION_SETUP.md`, `SETUP_SCHEDULED_TASKS.md`.

Let me know if any part of this guide is unclear or missing context from the codebase so I can refine it.***
