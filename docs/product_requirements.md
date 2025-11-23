# Sports Betting Analytics Platform — Product Requirements Document (PRD)

## 1. Purpose & Goals
The Sports Betting Analytics Platform delivers data-driven betting recommendations across professional and collegiate leagues (NFL, NBA, NCAAB, NHL, CFB, European soccer). The product solves the lack of centralized, timely, and trustworthy analytics by:

- Aggregating odds, schedules, and advanced stats from diverse providers (Killersports, The Odds API, ESPN, Understat, football-data, Kaggle, etc.).
- Producing machine-learning driven probabilities and totals projections.
- Surfacing actionable insights via a forward-testing dashboard with bankroll tracking and bet recommendations.

**Business Objectives**
1. Maintain automated coverage for all supported leagues.
2. Improve model accuracy and consistency through continual dataset refreshes.
3. Provide actionable recommendations with configurable edge thresholds and transparent tracking.

## 2. Target Audience & Use Cases
**Primary Users**
- **Data/Quant Analysts** – Need reproducible pipelines for ingestion, feature generation, and model evaluation.
- **Sports Traders / Bettors** – Need near real-time probabilities, recommended bets, and bankroll metrics.
- **Product / Ops Stakeholders** – Need visibility into pipeline health, logging, and outcomes.

**Key Use Cases**
1. **Hourly Automation** – Task scheduler runs ingestion, dataset updates, model retraining, and predictions.
2. **Live Odds Snapshot** – Fetch latest prices from The Odds API.
3. **Historical Backfills** – Pull season-by-season odds/results for modelling (Killersports, Kaggle, etc.).
4. **Dataset/Data Science Workflow** – Build league-specific feature sets, train models, evaluate metrics.
5. **Forward Testing & Dashboard** – Generate recommendations, update results, display in Dash app with filters, ROI, edge analysis.

## 3. Features & Functionality
### 3.1 Data Ingestion
- **Odd Sources**: ESPN scoreboard (NFL/NBA/CFB/Soccer), Killersports (NBA/NHL/NCAAB/MLB/ATP/WTA), The Odds API (NCAAB/NHL + others).
- **Schedules/Results**: Kaggle (March Madness), NBA API, nflfastR, ESPN Soccer, etc.
- **Advanced Stats**: Understat/football-data, CollegeFootballData, NBA rolling metrics, MLB advanced stats.
- **Automation**: `scripts/run_hourly_pipeline.ps1` orchestrates ingestion, dataset rebuilds, model training, predictions, and result updates.

### 3.2 Data Processing & Storage
- **Database**: SQLite (`data/betting.db`) for teams, games, odds, results, predictions.
- **Raw Storage**: Timestamped folders under `data/raw` for reproducibility.
- **Feature Extraction**: `src/features/moneyline_dataset.py` plus league-specific loaders and `src/models/feature_loader.py`.

### 3.3 Modeling
- Gradient Boosting / other classifiers (LightGBM/XGBoost logistic/MLP optional) via `src/models/train.py`.
- Totals regression (`src/models/train_totals.py`) for over/under predictions.
- Calibration (sigmoid/isotonic) for probability refinement.

### 3.4 Forward Testing & Dashboard
- `src/models/forward_test.py` generates predictions, calculates edges, updates completed games.
- Dash UI (`src/dashboard/app.py`, `src/dashboard/components.py`, `src/dashboard/data.py`) shows filters, metrics, and recommended bets.
- Supports NCAA & NHL with live odds, plus existing leagues.

### 3.5 Automation Enhancements
- Hourly pipeline now includes:
  - ESPN odds ingestion for core leagues.
  - Killersports and The Odds API snapshots.
  - Dataset rebuilds and model training for leagues with new data.
  - Forward test predictions + result updates for trained leagues.

## 4. Design & Technical Requirements
### 4.1 Architecture
- **Language**: Python 3.11 (Poetry-managed virtual environment).
- **Data Sources**: HTTP/REST API, Selenium-scraped HTML, downloaded CSV/Parquet.
- **Scheduling**: Windows Task Scheduler running `scripts/run_hourly_pipeline.ps1`. Script must log to `logs/hourly_pipeline_*.log`.
- **Directory Structure**: Adhere to `data/raw`, `data/processed`, `models`, `reports`, `scripts`, `docs`.

### 4.2 Automation Script Requirements
- Accepts `SoccerOnly` flag for selective runs.
- Maintains JSON lists of leagues for ingestion/training/prediction.
- Executes ingestion steps (ESPN, Killersports, Odds API, Understat, football-data).
- Retrains models only when datasets updated to avoid redundant compute.
- Handles errors gracefully, logging warnings but allowing pipeline to continue.

### 4.3 Data Quality
- Validate that each ingestion populates `game_results` and `odds` tables with unique `game_id`.
- Track number of rows ingested, matched vs. created games.
- For NCAA, ensure Kaggle schedule results map to team codes via `team_mappings` with canonical alias coverage.

### 4.4 Dashboard Requirements
- Master predictions file `data/forward_test/predictions_master.parquet` is the single source of truth for dashboard data.
- Display recommended bets with edges ≥ 6% (configurable threshold).
- Bankroll charts showing cumulative profit, ROI, wins/losses per league.

## 5. Release Criteria
1. **Odds Coverage**: Odds snapshots for NCAA/NHL succeed on two consecutive hourly runs (Killersports + The Odds API).
2. **Pipeline Health**: No ingestion/model/prediction errors in `logs/hourly_pipeline_*` for at least 24 hours.
3. **Dashboard Verification**: NCAA & NHL predictions visible with correct filters and metrics.
4. **Model Quality**: NCAAB moneyline model test accuracy ≥ 0.65, ROC AUC ≥ 0.70; NHL model meets previously established benchmarks.
5. **Documentation**: HOURLY_AUTOMATION_SETUP.md updated; README references new capabilities; PRD (this document) approved.
6. **Version Control**: Committed and pushed changes for automation updates, ingestion handlers, data mapping.

## 6. Alignment & Communication
- **Central Source**: This PRD lives under `docs/product_requirements.md` and is referenced in HOURLY_AUTOMATION_SETUP.md and README.
- **Stakeholders**: Product, Data Engineering, Modeling, Ops.
- **Status Updates**: Use `logs/hourly_pipeline_*.log` plus `SCHEDULED_TASKS_STATUS.md` for operational checkpoints.
- **Next Steps**: Once NCAA/NHL loop runs reliably, plan future PRD revisions for ATP/WTA/MLB expansion, player-level modeling, and TeamRankings-based edge filters.

## 7. Appendix (Supporting References)
- `HOURLY_AUTOMATION_SETUP.md` – pipeline setup instructions.
- `scripts/run_hourly_pipeline.ps1` – orchestration script.
- `src/data/ingest_odds.py`, `src/data/sources/odds_api.py` – The Odds API integration.
- `src/models/forward_test.py` – prediction/edge logic.
- `docs/data-sources.md` – inventory of ingested data providers.

---
This PRD serves as the canonical blueprint for extending and operating the Sports Betting Analytics Platform. It ensures all contributors understand the business goals, technical scope, and release expectations needed to deliver reliable NCAA & NHL analytics alongside existing leagues.
