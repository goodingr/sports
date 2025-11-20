# Software Requirements Specification (SRS)
## Sports Betting Analytics Platform

---

## 1. Introduction
### 1.1 Purpose
This SRS defines the functional and non-functional requirements of the Sports Betting Analytics Platform (SBAP). It translates the Product Requirements Document (PRD) into concrete specifications for development, QA, operations, and stakeholders.

### 1.2 Scope
SBAP ingests global sports data, builds datasets, trains predictive models, generates betting recommendations, and exposes insights via automated forward testing and a dashboard. Supported leagues currently include NFL, NBA, CFB, EPL, La Liga, Bundesliga, Serie A, Ligue 1, NHL, and NCAAB.

### 1.3 Definitions & Acronyms
| Term | Definition |
|------|------------|
| SBAP | Sports Betting Analytics Platform |
| Killersports | Historical SDQL odds provider |
| The Odds API | Live bookmaker aggregation service |
| Forward Test | Process that generates live predictions and updates results |
| Edge | Difference between model probability and implied probability |
| Pipeline | Hourly orchestration script `scripts/run_hourly_pipeline.ps1` |

### 1.4 References
- `docs/product_requirements.md`
- `HOURLY_AUTOMATION_SETUP.md`
- `README.md`
- `scripts/run_hourly_pipeline.ps1`

---

## 2. Overall Description
### 2.1 Product Perspective
SBAP is a data/ML pipeline with:
- Ingestion layer for odds, schedules, results, advanced stats.
- Processing layer (SQLite DB, feature engineering, dataset building).
- Modeling layer producing moneyline and totals models per league.
- Prediction layer (forward test + dashboard).
- Automation layer (hourly PowerShell script).

### 2.2 User Classes & Characteristics
| User Class | Description | Needs |
|------------|-------------|-------|
| Analysts / Data Scientists | Build datasets, train/evaluate models | Access to ingestion logs, datasets, metrics |
| Traders / Bettors | Consume predictions & ROI metrics | Reliable live odds, dashboard filters, recommended bets |
| Operations | Maintain system health | Task scheduler status, logs, error handling |

### 2.3 Operating Environment
- Windows 11 (Task Scheduler, PowerShell 7).
- Python 3.11 (Poetry-managed venv).
- SQLite database (`data/betting.db`).
- Raw data stored on local filesystem under `data/raw`.
- Dash dashboard served via `run_dashboard` script (Flask/Dash).

### 2.4 Design Constraints
- Killersports seasonal API limits (5k rows; require per-season pulls).
- The Odds API call quotas per key.
- Windows Task Scheduler to run hourly tasks (12+ commands); must handle network outages gracefully.
- No external cloud dependencies (offline-friendly storage).

### 2.5 Assumptions & Dependencies
- `.env` contains credentials: `KILLERSPORTS_USERNAME`, `KILLERSPORTS_PASSWORD`, `ODDS_API_KEY`.
- Network access available for APIs.
- SQLite DB not excessively large (<1 GB for smooth queries).
- Machine has PowerShell execution policy allowing scripts (`setup_hourly_task.ps1`).

---

## 3. System Features & Requirements
### 3.1 Data Ingestion
**Description**: Fetch raw odds, schedules, results, advanced metrics.

**Functional Requirements**
1. Hourly pipeline shall call ESPN odds ingestion for leagues defined in `$targetLeagues`.
2. Pipeline shall call Killersports ingestion for `killersports_*` sources defined in `config/sources.yml`.
3. Pipeline shall call The Odds API snapshots for every league in `$targetLeagues`.
4. NCAA schedules/results shall be loaded via March Madness Kaggle dataset using `src/data/ingest_ncaab_mm.py`.
5. Raw files must be stored under `data/raw` with timestamped directories.

### 3.2 Data Processing
1. Ingested odds/results shall be normalized into `games`, `teams`, `game_results`, and `odds` tables within SQLite.
2. Neutral-site games must set `games.is_neutral` flag.
3. NCAA team aliases shall resolve via `src/data/team_mappings.py` auto-generated map.
4. Dataset builder (`src/features/moneyline_dataset.py`) shall support `--league NCAAB` and `--league NHL` producing Parquet files in `data/processed/model_input/`.

### 3.3 Modeling
1. `src/models/train.py` must accept `--league NCAAB`/`--league NHL` and use the latest dataset.
2. Models shall be stored under `models/<league>_gradient_boosting_calibrated_moneyline.pkl`.
3. Training outputs metrics JSON under `reports/backtests/<league>_gradient_boosting_calibrated_metrics.json`.
4. Totals models (`train_totals.py`) shall produce `<league>_totals_gradient_boosting.pkl` and metrics JSON.

### 3.4 Forward Testing & Predictions
1. `src/models/forward_test.py predict --league <LEAGUE>` must ingest live odds (Else warn and skip).
2. Master predictions file `data/forward_test/predictions_master.parquet` must include `league`, `game_id`, edges, moneylines.
3. Hourly pipeline Step 5 shall invoke `predict` and `update` for every league in `$targetLeagues`, regardless of whether that league retrained during the current run.
4. Forward test must store at least 7 days of predictions as historical record.

### 3.5 Dashboard
1. League filter options must list every supported league defined in `$targetLeagues`.
2. Dashboard shall display recommended bets where `edge >= configured threshold` (default 6%).
3. Dashboard data loader must parse `predictions_master.parquet` and deduce `league` from each `game_id` prefix.

### 3.6 Automation
1. `scripts/run_hourly_pipeline.ps1` shall:
   - Log start/end events with timestamp.
   - Process every league listed in `$targetLeagues` (core, soccer, or any future additions).
   - Fetch The Odds API snapshots for all leagues in `$targetLeagues`.
   - Run predictions and result updates for all leagues processed, even if a specific league did not retrain during that run.
2. Task Scheduler configuration (see HOURLY_AUTOMATION_SETUP.md) must run hourly with highest privileges and log outputs.
3. Failures in one step shall log warnings and continue to subsequent steps whenever feasible.

---

## 4. External Interface Requirements
### 4.1 User Interfaces
- Dash dashboard: accessible via `run_dashboard` (Bootstrap theme, filters, tables).
- CLI: all ingestion/training scripts invoked via `poetry run python ...`.

### 4.2 Hardware Interfaces
- Windows machine with consistent power/network; no additional hardware.

### 4.3 Software Interfaces
- HTTP APIs (Killersports, The Odds API, ESPN, Understat, football-data, Kaggle).
- SQLite database via Python `sqlite3`.
- PowerShell 7 for automation.
- GitHub for version control (`main` branch).

### 4.4 Communication Interfaces
- HTTPS for all API calls.
- Logging to local file system (`logs/` directory).

---

## 5. Non-Functional Requirements
### 5.1 Performance
- Hourly pipeline must complete within 60 minutes under normal loads.
- Dataset rebuild must handle datasets up to ~100k rows per league.
- Forward testing should produce predictions within 2 minutes per league.

### 5.2 Reliability & Availability
- Task Scheduler should auto-restart missed runs (Run task as soon as possible when missed).
- Scripts must handle transient HTTP failures with retries/backoff.
- Predictions stored even if some ingestions fail.

### 5.3 Scalability
- Support adding new leagues via `config/sources.yml`, `LEAGUE_TO_SPORT_KEY`, dataset builder, and forward tester without major refactors.

### 5.4 Security
- `.env` containing API credentials must not be committed to repo.
- Access to `database.sqlite`/`betting.db` limited to authorized users on machine.

### 5.5 Maintainability
- Codebase organized with `src/` modules and `scripts/`.
- PRD and SRS documents updated when scope changes.
- Logging facilitates debugging.

### 5.6 Usability
- Dash UI must render on desktop browsers; filters intuitive.
- Command-line scripts output human-readable logs.

---

## 6. Data Requirements
- Raw data retention: keep latest snapshot per season per provider (archives optional).
- Database tables (games, teams, odds, game_results, predictions) should have indexes on `game_id`, `league`.
- Sensitive credentials stored only in `.env`.

---

## 7. Security & Privacy
- API keys stored locally; not logged in plaintext.
- Windows user account controlling scheduler should have limited permissions.
- Database backups optional but recommended; encryption not mandated for current scope.

---

## 8. Operational Considerations
- **Monitoring**: review `logs/hourly_pipeline_*.log` daily; `SCHEDULED_TASKS_STATUS.md` updated when changes occur.
- **Recovery**: rerun specific ingestion scripts when failures occur; re-trigger Task Scheduler manually via `Start-ScheduledTask`.
- **Deployment**: changes merged to `main` and pulled onto automation machine; ensure Poetry deps installed (`poetry install`).

---

## 9. Appendices
### A. Task Scheduler Summary
- Task Name: `SportsAnalyticsHourly`.
- Action: `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_hourly_pipeline.ps1`.
- Triggers: hourly, indefinite duration.

### B. League-Specific Configurations
| League | Odds Source(s) | Dataset Seasons (default) | Model Output |
|--------|----------------|---------------------------|--------------|
| NFL | ESPN Odds, Killersports, Odds API | last 5 completed seasons | models/nfl_gradient... |
| NBA | ESPN Odds, Killersports, Odds API | last 5 seasons | models/nba_gradient... |
| CFB | ESPN Odds, Killersports, Odds API | last 5 seasons | models/cfb_gradient... |
| NCAAB | Killersports, The Odds API | 2015-2024 dataset (refreshable) | models/ncaab_gradient... |
| NHL | Killersports, The Odds API | (future dataset TBD) | models/nhl_gradient... |
| Soccer (EPL, etc.) | ESPN, Understat, football-data | 2014+ | models/<league>_gradient... |

---

**End of Document**
