# Pipeline Documentation

This document provides a detailed overview of the sports betting prediction pipeline, including how to run it, its components, and configuration.

## Overview

The pipeline is orchestrated by `scripts/pipeline.ps1` and follows a unified data flow:
1.  **Ingest**: Fetch all necessary data (History + Live Odds + Live Scores).
2.  **Train** (Optional): Retrain models using the latest data.
3.  **Predict**: Generate predictions for upcoming games.

## Quick Start

### 1. Master Pipeline (Recommended)
Run the end-to-end pipeline. By default, it runs ingestion, training, and prediction.

```powershell
.\scripts\pipeline.ps1
```

**Options:**
- `-SkipTraining`: Skip the model training step (runs Ingest -> Predict).
  ```powershell
  .\scripts\pipeline.ps1 -SkipTraining
  ```
- `-SkipOdds`: Skip fetching new odds/scores (uses cached data).
  ```powershell
  .\scripts\pipeline.ps1 -SkipOdds
  ```
- `-SoccerOnly`: Run only for soccer leagues.

### 2. Individual Components
You can run specific parts of the pipeline manually if needed.

**Ingestion Only:**
```powershell
.\scripts\ingest_all.ps1
```

**Training Only:**
```powershell
.\scripts\train.ps1 -SkipIngestion
```

**Prediction Only:**
```powershell
.\scripts\predict.ps1 -SkipHistory -SkipOdds
```

## Pipeline Steps

### Master Pipeline (`scripts/pipeline.ps1`)
This script orchestrates the entire workflow:

1.  **Backup**: Creates a backup of the database and current predictions.
2.  **Ingest All Data**: Calls `scripts/ingest_all.ps1`.
    - **Smart History**: Checks DB history. If missing, runs full backfill. If present, runs fast update.
    - **Live Odds**: Fetches latest odds from The Odds API.
    - **Live Scores**: Fetches latest scores from The Odds API.
3.  **Train Models** (if `-Train` is set): Calls `scripts/train.ps1`.
    - *Note:* Runs with `-SkipIngestion` since step 2 already handled it.
    - Computes advanced stats.
    - Rebuilds datasets.
    - Retrains models.
4.  **Generate Predictions**: Calls `scripts/predict.ps1`.
    - *Note:* Runs with `-SkipHistory` and `-SkipOdds` since step 2 already handled it.
    - Generates probabilities and edges.
    - Syncs results.

## Key Components

### Data Ingestion
- **`src/data/ingest_manager.py`**: The **Smart Ingestion Controller**. Checks DB history and decides whether to run a full backfill or a partial update.
- **`src/data/backfill_*.py`**: League-specific scripts (e.g., `backfill_nfl.py`) for fetching historical data. These are called by the manager.
- **`src/data/ingest_odds.py`**: Fetches upcoming game odds. Handles **API key rotation** automatically.
- **`src/data/ingest_scores.py`**: Fetches final scores for recent games from The Odds API. Used for fast, live updates.
- **`scripts/ingest_all.ps1`**: Wrapper script that runs Manager + Odds + Scores in sequence.

### Modeling
- **`src/models/forward_test.py`**: The core prediction engine. It loads the trained models and applies them to new odds data.
- **`src/models/train_model.py`**: Handles the training logic, including hyperparameter tuning (if configured) and model serialization.

### Configuration (`.env`)
The pipeline relies on the `.env` file for configuration, particularly API keys.

```env
# API Keys (Rotated automatically)
ODDS_API_KEY=your_primary_key
ODDS_API_KEY_2=your_secondary_key
ODDS_API_KEY_3=...

# Settings
ODDS_API_REGION=us
ODDS_API_MARKET=h2h
```

## Troubleshooting

### "No updates" in Dashboard
If recently completed games are not showing as updated:
1.  **Check Ingestion**: Run `ingest_scores.py` manually with debug logging.
    ```powershell
    poetry run python -m src.data.ingest_scores --leagues NFL --log-level DEBUG --dotenv .env
    ```
2.  **Check `odds_api_id`**: The system links API scores to database games via `odds_api_id`. If a game in the database lacks this ID, it won't update automatically.

### API Errors (401 Unauthorized)
- The system is designed to rotate keys automatically.
- If you see this error, it means **all** configured keys in `.env` are exhausted or invalid.
- Add more keys to `.env` (`ODDS_API_KEY_n`) to resolve this.
