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
-   **Automated Pipeline**: Single-command orchestration for ingestion, training, and prediction.

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

# Run without retraining models (Faster)
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

### Pipeline Flow

1.  **Ingest All Data** (`scripts/ingest_all.ps1`)
    -   **Smart History**: Checks DB history. If missing, runs full backfill. If present, runs fast update.
    -   **Live Odds**: Fetches latest odds from The Odds API.
    -   **Live Scores**: Fetches latest scores from The Odds API.
2.  **Train Models** (`scripts/train.ps1`)
    -   Computes advanced stats (rolling averages, EPA, etc.).
    -   Rebuilds training datasets.
    -   Retrains models (Ensemble, RF, GB) for Moneyline and Totals.
3.  **Generate Predictions** (`scripts/predict.ps1`)
    -   Loads latest odds.
    -   Generates probabilities and edges.
    -   Syncs results across all models.

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
# Train specific league and model
poetry run python -m src.models.train --league NBA --model-type ensemble
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

