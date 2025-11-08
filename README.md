## Sports Betting Analytics

- **Initial focus**: NFL moneyline outcomes for regular season and playoffs.
- **Expansion**: Architecture now supports NBA and FBS College Football (CFB) alongside NFL; adding new leagues requires minimal refactoring.
- **Data horizon**: Use historical data from 2010 onward to capture modern play styles; refresh weekly.
- **Bankroll assumptions**: Starting bankroll $10,000 with risk capped at 2% per wager unless Kelly recommends less.
- **KPIs**:
  - Predictive: accuracy, Brier score, log loss, calibration slope.
  - Financial: ROI, cumulative profit, max drawdown, Sharpe-like risk-adjusted return.
- **Deployment cadence**: Daily batch recommendations; live odds update optional future enhancement.

### Supported Leagues

The pipelines currently support NFL, NBA, CFB, and European soccer (EPL/La Liga/Bundesliga/Serie A/Ligue 1). Additional sports such as MLB are disabled until their ingestion stack is completed to production standards.

### Project Structure (planned)

```
data/
  raw/
  processed/
notebooks/
src/
  data/
  features/
  models/
reports/
```

- Use `.env` for API keys and configuration.
- Employ `poetry` for Python environment management.
- Document workflow updates in `docs/` as features grow.

### Data Pipeline Quickstart

1. Install dependencies:
   - `pip install poetry`
   - `poetry install`
2. Copy `.env.example` to `.env` and add your The Odds API key.
3. Pull upcoming odds snapshots:
   - NFL: `poetry run python -m src.data.ingest_odds --sport americanfootball_nfl`
   - NBA: `poetry run python -m src.data.ingest_odds --sport basketball_nba`
   - CFB: `poetry run python -m src.data.ingest_odds --sport americanfootball_ncaaf`
   - Historical fill: `poetry run python -m src.data.backfill_odds 2023-10-01 2023-11-01 --sport basketball_nba --step-days 7`
   - ESPN scoreboard odds: `poetry run python -m src.data.ingest_sources --source espn_odds_nfl` (add `--source espn_odds_nba` or `--source espn_odds_cfb`)
   - Historical NBA odds: `poetry run python -m src.data.backfill_historical_odds --source all --league nba 2023-10-01 2023-11-01 --step-days 1`
4. Run structured source ingestions (Wave 1 + Wave 3):
   - List sources: `poetry run python -m src.data.ingest_sources --list`
   - Execute all enabled sources: `poetry run python -m src.data.ingest_sources`
     - Historical/bootstrap feeds run automatically the first time (to seed a new machine) and are skipped afterward so hourly jobs don't redownload the same archives. Pass `--full-refresh` when you explicitly want to re-fetch those sources.
   - Limit to NFL wave: `poetry run python -m src.data.ingest_sources --league nfl --season-start 2019 --season-end 2023`
   - Limit to CFB: `poetry run python -m src.data.ingest_sources --league cfb --seasons 2024`
   - Target injuries only: `poetry run python -m src.data.ingest_sources --source nflverse_injuries` (or `nba_injuries`)
   - Team metrics: `poetry run python -m src.data.ingest_sources --source nfl_team_metrics --season-start 2021 --season-end 2024` and `--source nba_team_metrics --season-start 2022 --season-end 2024`
5. Download historical schedules and betting closes:
   - `poetry run python -m src.data.ingest_results` (default covers 1999-2023; add `--seasons` to customize)
   - For NBA: `poetry run python -m src.data.ingest_results_nba --seasons 2015 2016 ...`
   - For CFB: `poetry run python -m src.data.ingest_results_cfb --seasons 2024 --season-type regular` (requires `CFBD_API_KEY`)
6. Inspect saved files under `data/raw/odds/`, `data/raw/results/`, and `data/raw/sources/`.
7. Initialize the SQLite warehouse (optional but recommended):
   - `poetry run python -m src.db.init_db`

See `docs/data-sources.md` and `docs/storage-layout.md` for details on inputs and storage conventions.

### Modeling & Recommendations

1. Build processed dataset (run implicitly by training):
   - `poetry run python -m src.features.moneyline_dataset` (defaults to seasons 1999-2023)
2. Train upgraded model and write metrics/predictions:
   - NFL: `poetry run python -m src.models.train --league NFL --model-type gradient_boosting --calibration sigmoid`
   - NBA: `poetry run python -m src.models.train --league NBA --model-type ensemble --calibration sigmoid`
   - CFB: `poetry run python -m src.models.train --league CFB --model-type gradient_boosting --calibration sigmoid`
   - Alternative model families: `lightgbm`, `xgboost`, `mlp`, or `ensemble` (averages surviving members with log-loss weighting).
   - All models support per-fold calibration (`--calibration sigmoid` or `--calibration isotonic`); use `--calibration none` to disable.
3. Generate recommended bets from holdout predictions:
   - NFL: `poetry run python -m src.models.bet_selector --league NFL --edge-threshold 0.06`
   - NBA: `poetry run python -m src.models.bet_selector --league NBA --edge-threshold 0.06`
   - CFB: `poetry run python -m src.models.bet_selector --league CFB --edge-threshold 0.06`
4. Inspect outputs under `reports/backtests/` (e.g., `gradient_boosting_calibrated_metrics.json`) and `reports/recommendations/`.

Upcoming improvements: richer features (team form, injuries), alternative model ensembles, and live odds ingestion with message queues.

### Data Warehouse Overview

- SQLite database lives at `data/betting.db`. Run `src/db/init_db.py` to create tables defined in `src/db/schema.sql`.
- Core tables cover `sports`, `teams`, `games`, odds snapshots, model inputs, model registry (with league column), and recommendations.
- Source registry tables (`data_sources`, `source_runs`, `source_files`) track scraping history and raw file locations.
- `injury_reports` captures normalized player availability from nflverse and NBA live feeds; injury feature columns are derived during dataset builds.
- `src/db/inspect.py` supplies quick checks: `poetry run python -m src.db.inspect summary` or `... models` or `... source-runs`.
- Monitor source health: `poetry run python -m src.data.monitor_sources health --hours 24` (or `failures`, `stale`, `check`).
- Existing ingestion scripts currently persist to parquet/JSON; future work can sync those outputs into the database for cross-sport analytics.

### Multi-League Tips

- Fetch odds by sport key: `poetry run python -m src.data.ingest_odds --sport americanfootball_nfl`, `--sport basketball_nba`, or `--sport americanfootball_ncaaf`.
- Historical odds can be backfilled with repeated calls (use `--sport` and date filters when extending the script).
- Multi-source scraping config lives in `config/sources.yml`; update handlers/params there before running the orchestrator.
- ESPN odds (`espn_odds_*`) and team metric sources (`*_team_metrics`) populate additional features such as `espn_moneyline_close` and season-level EPA/estimated rating columns.
- **Historical NBA Odds**: Use `src.data.backfill_historical_odds` to backfill from OddsShark, VegasInsider, or Covers. Sources use Selenium for JavaScript-rendered content. Load CSV files into database with `src.data.load_historical_odds`.
- Model artifacts and predictions names now include the league prefix, e.g., `models/nfl_ensemble_calibrated_moneyline.pkl` and `reports/backtests/nba_gradient_boosting_calibrated_metrics.json`.
- College Football ingestion uses the CollegeFootballData API. Set `CFBD_API_KEY` in `.env` before running CFB commands.

### Monitoring & Scheduling

- **Source Health Monitoring**: Use `src/data/monitor_sources.py` to check ingestion status:
  - `poetry run python -m src.data.monitor_sources health --hours 24` - Show success rates per source
  - `poetry run python -m src.data.monitor_sources failures --hours 48` - List recent failures
  - `poetry run python -m src.data.monitor_sources stale --threshold 48` - Find sources that haven't run recently
  - `poetry run python -m src.data.monitor_sources check --min-success-rate 80` - Exit non-zero if unhealthy (for alerts)
- **Scheduling**: Set up automated ingestion with cron (Linux/macOS) or Task Scheduler (Windows):
  - See `scripts/schedule_ingestion.sh` for cron examples (hourly odds, daily full ingestion, weekly metrics)
  - See `scripts/schedule_ingestion.ps1` for Windows Task Scheduler setup
- **Historical Backfill**: Use `src/data/backfill_espn_odds.py` to fetch historical ESPN odds snapshots (note: ESPN may not provide historical data):
  - `poetry run python -m src.data.backfill_espn_odds 2024-09-01 2024-11-01 --league nfl --step-days 1 --sleep 1.0`

### Testing & CI

- Run the automated test suite with `poetry run pytest`.
- Continuous Integration runs via `.github/workflows/ci.yml`, which executes Ruff and Pytest on every push/pull request.

### Forward Testing Dashboard

- Interactive Dash application to review forward testing performance across leagues (NBA, NFL, CFB).
- Launch locally: `poetry run python -m src.dashboard --port 8050`
- Features include:
  - Overview cards for win rate, ROI, and recommendation counts (respects edge slider)
  - Time-series charts (cumulative profit, ROI, win rate) with selectable aggregation (daily/weekly/monthly)
  - Recent predictions table with filtering/sorting and completed result tracking
  - Recommended bets view showing upcoming games above the edge threshold and a calendar roll-up
  - Edge analysis tab with ROI by edge bucket and supporting table
  - Manual refresh button plus date-range and edge-threshold filters
- Data is read from `data/forward_test/predictions_master.parquet`. Use the forward testing scripts to keep snapshots current (`src/models/forward_test.py` and `scripts/run_forward_test_*.ps1`).
- Forward-testing helper scripts now include league-specific wrappers under `scripts/run_forward_test_*_cfb.ps1` for Task Scheduler integration.
- See `docs/dashboard.md` for screenshots, component breakdown, and deployment notes.
