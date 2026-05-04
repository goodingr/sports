# Sports Betting Analytics Platform

A production-grade machine learning platform for multi-league sports betting predictions. This system ingests data from dozens of sources, engineers advanced features, trains calibrated models (Ensemble, Random Forest, Gradient Boosting), and delivers actionable insights via a real-time dashboard.

---

## 🚀 Key Features

-   **Multi-League Support**: NFL, NBA, CFB, NCAAB, NHL, and major European Soccer leagues (EPL, La Liga, Bundesliga, Serie A, Ligue 1).
-   **Smart Ingestion**: Intelligent data manager that automatically handles historical backfilling and fast incremental updates.
-   **Advanced Modeling**:
    -   **Ensemble**: Voting classifier combining Random Forest, Gradient Boosting, and Logistic Regression.
    -   **Calibration**: Sigmoid calibration for accurate probability estimation.
    -   **Totals**: Dedicated models for Over/Under predictions.
-   **Real-Time Dashboard**: Interactive web app for tracking live odds, model performance, and ROI analysis.
-   **Automated Pipeline**: Single-command orchestration for ingestion, training, and prediction. [Learn more](docs/AUTOMATION.md).

---

## 🛠️ Quick Start

### 1. Installation

```bash
# Install dependencies
pip install poetry
poetry install

# Initialize database
poetry run python -m src.db.init_db
```

### 2. Configuration

Copy `.env.example` to `.env` and configure your API keys:

```env
ODDS_API_KEY=your_key
CFBD_API_KEY=your_key
# ... add other keys as needed
```

### 3. Run the Pipeline

The master pipeline handles everything: **Ingestion -> Training -> Prediction**.

```powershell
# Run full pipeline (Recommended)
.\scripts\pipeline.ps1

# Run without the paid-picks benchmark (Faster/hourly)
.\scripts\pipeline.ps1 -SkipTraining
```

### 4. Launch Dashboard

View predictions and analyze performance:

```powershell
.\run_dashboard
```

Visit `http://localhost:8050` in your browser.

---

## 🏗️ Architecture

For a detailed diagram and component overview, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Pipeline Flow

1.  **Ingest All Data** (`scripts/ingest_all.ps1`)
    -   **Smart History**: Checks DB history. If missing, runs full backfill. If present, runs fast update.
    -   **Live Odds**: Fetches latest odds from The Odds API.
    -   **Live Scores**: Fetches latest scores from The Odds API.
2.  **Resolve Data Hygiene**
    -   Prunes release-league orphan results.
    -   Backfills stale completed scores from ESPN.
    -   Refreshes NBA injury availability data.
3.  **Benchmark Paid Rules** (`src.models.train_betting --benchmark`)
    -   Runs the predeclared rolling-origin grid in `config/betting_benchmark.yml`.
    -   Ranks candidates by market Brier, ROI, bootstrap ROI, CLV, book slices, and timing slices.
    -   Never auto-approves a paid rule.
4.  **Generate Current Predictions** (`scripts/predict.ps1`)
    -   Refreshes current prediction rows used as raw input.
    -   These are not subscriber-facing unless the publish gate approves them.
5.  **Publish Paid Picks** (`src.predict.publishable_bets`)
    -   Writes a paid list only when an approved rule passes the strict gate.
    -   Removes stale paid lists and exits fail-closed when no rule passes.

### Repository Structure

```
data/
  raw/            # Source snapshots (JSON/Parquet)
  processed/      # Feature-ready tables
docs/             # Documentation
scripts/          # Automation scripts (pipeline.ps1, etc.)
src/
  data/           # Ingestion & Normalization
  features/       # Feature Engineering
  models/         # Training & Evaluation
  dashboard/      # Web Application
```

---

## 📊 Supported Leagues

| League | Status | Data Sources |
| :--- | :--- | :--- |
| **NFL** | 🟢 Active | `nfl_data_py`, Odds API, ESPN |
| **NBA** | 🟢 Active | `nba_api`, ESPN, Odds API |
| **CFB** | 🟢 Active | `cfbd`, Odds API |
| **NCAAB** | 🟢 Active | Kaggle, ESPN, Odds API |
| **NHL** | 🟢 Active | Killersports, ESPN, Odds API |
| **Soccer** | 🟢 Active | Football-Data, Understat, ESPN |

---

## 🧰 Cheatsheet

### Manual Data Ingestion

| Task | Command |
| :--- | :--- |
| **Smart Ingestion** (History + Updates) | `poetry run python -m src.data.ingest_manager --leagues NFL,NBA` |
| **Force Full Backfill** | `poetry run python -m src.data.ingest_manager --leagues NFL --force-backfill` |
| **Live Odds Only** | `poetry run python -m src.data.ingest_odds --league NFL` |
| **Live Scores Only** | `poetry run python -m src.data.ingest_scores --leagues NFL` |

### Manual Training

```powershell
# Run the paid-picks benchmark grid
poetry run python -m src.models.train_betting --benchmark `
  --benchmark-config config/betting_benchmark.yml `
  --benchmark-output-dir reports/betting_benchmarks
```

### Manual Prediction

```powershell
# Update predictions for specific league
poetry run python -m src.models.forward_test update --league NBA --model-type ensemble
```

---

## 📈 Dashboard Features

The dashboard provides three specialized views to analyze model performance and find value.

### 1. Moneyline Dashboard (`/`)
Focused on profitability and ROI for moneyline (winner) bets.
-   **Overview**: Summary cards (Net Profit, ROI, Win Rate), Bankroll evolution, and Cumulative Profit charts.
-   **Performance**: ROI and Win Rate over time, broken down by league.
-   **Recommended Bets**: Live value bets where the model's probability > implied odds + edge threshold.
-   **Edge Analysis**: Visualizes how profitability correlates with edge size.

### 2. Winner Predictions (`/predictions`)
A head-to-head comparison of Model vs. Sportsbooks.
-   **Accuracy Tracking**: Tracks how often the model correctly predicts the winner compared to the favorite.
-   **Consensus vs. Model**: Highlights games where the model disagrees with the sportsbook consensus.
-   **League Breakdown**: Detailed accuracy metrics per league.

### 3. Over/Under Dashboard (`/overunder`)
Dedicated analysis for Totals (O/U) betting.
-   **Totals-Specific Metrics**: Separate ROI and Win Rate tracking for Over/Under bets.
-   **Line Analysis**: Compares model projected totals against sportsbook lines.
-   **Performance**: Tracks the success rate of Over vs. Under predictions across different leagues.

---

## 🤝 Contributing

1.  Make changes in a feature branch.
2.  Add tests in `tests/`.
3.  Run sanity checks:
    ```bash
    poetry run pytest
    poetry run python -m src.features.moneyline_dataset --league NFL --seasons 2023
    ```
4.  Submit a Pull Request.

---

## ✅ Release Gate

Run these locally before opening a PR. CI mirrors them in
`.github/workflows/ci.yml`. The lint/type scope is intentionally narrow —
only files known to be clean. To expand, clean a file first, then add it to
`RUFF_SCOPE` / `MYPY_SCOPE` in the workflow.

### Python

```bash
# Lint (release-scoped)
poetry run ruff check \
  src/data/ingest_sources.py \
  src/features/betting_model_input.py \
  src/models/betting_benchmark.py \
  src/models/prediction_quality.py \
  src/models/train_betting.py \
  src/predict/publishable_bets.py \
  tests/test_betting_benchmark.py \
  tests/test_betting_model_input.py \
  tests/test_prediction_quality.py \
  tests/test_publishable_bets.py \
  tests/test_train_betting.py \
  tests/api/test_readiness.py

# Types (release-scoped)
poetry run mypy \
  src/data/ingest_sources.py \
  src/data/sources/espn_odds.py \
  src/data/sources/nba_injuries_espn.py \
  src/features/betting_model_input.py \
  src/models/feature_loader.py \
  src/models/prediction_quality.py \
  src/predict/publishable_bets.py

# Full pytest (skip-reasons surfaced)
poetry run pytest -q -rs

# Release-gate slice (subset that CI gates as a separate job)
poetry run pytest \
  tests/test_prediction_quality.py \
  tests/test_betting_benchmark.py \
  tests/test_publishable_bets.py \
  tests/api/test_readiness.py \
  -q -rs
```

### Web

```bash
cd web-app
npm ci
npm run lint
npm run typecheck
npm test
npm run build
npm run test:e2e   # Playwright; not yet a hard gate -- see ci.yml
```

---

